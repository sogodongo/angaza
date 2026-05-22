"""
producer/aqi_producer.py — Angaza AQI data producer.

Simulates AQI sensor readings for African cities and streams them
to the Angaza Kinesis stream in batches of 10 records.

Features:
  - Realistic AQI data for 10 African cities
  - Exponential backoff on Kinesis failures
  - Handles partial batch failures (FailedRecordCount)
  - Partition key = city_id for even shard distribution

Run:
    python3 producer/aqi_producer.py            # runs until Ctrl+C
    python3 producer/aqi_producer.py --once     # sends one batch then exits
"""

import argparse
import json
import logging
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

CITIES: list[dict[str, Any]] = [
    {"id": "NBO", "name": "Nairobi",       "country": "KE", "aqi_base": 85,  "variance": 40},
    {"id": "MBA", "name": "Mombasa",       "country": "KE", "aqi_base": 55,  "variance": 25},
    {"id": "DAR", "name": "Dar es Salaam", "country": "TZ", "aqi_base": 90,  "variance": 35},
    {"id": "KLA", "name": "Kampala",       "country": "UG", "aqi_base": 110, "variance": 45},
    {"id": "ADD", "name": "Addis Ababa",   "country": "ET", "aqi_base": 95,  "variance": 50},
    {"id": "LGS", "name": "Lagos",         "country": "NG", "aqi_base": 140, "variance": 60},
    {"id": "ACC", "name": "Accra",         "country": "GH", "aqi_base": 80,  "variance": 30},
    {"id": "CPT", "name": "Cape Town",     "country": "ZA", "aqi_base": 45,  "variance": 20},
    {"id": "JNB", "name": "Johannesburg",  "country": "ZA", "aqi_base": 100, "variance": 45},
    {"id": "KHM", "name": "Khartoum",      "country": "SD", "aqi_base": 160, "variance": 55},
]


def _make_record(city: dict[str, Any]) -> dict[str, Any]:
    """Build one AQI sensor reading for a city."""
    aqi_score = max(0, min(500, int(random.gauss(city["aqi_base"], city["variance"]))))
    category, severity = config.get_aqi_category(aqi_score)
    pm2_5 = round(aqi_score * 0.45 + random.uniform(-2, 2), 2)
    pm10  = round(aqi_score * 0.72 + random.uniform(-3, 3), 2)
    return {
        "city_id":   city["id"],
        "city_name": city["name"],
        "country":   city["country"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "aqi_score": aqi_score,
        "category":  category,
        "severity":  severity,
        "pm2_5":     pm2_5,
        "pm10":      pm10,
        "alert":     aqi_score > config.AQI_ALERT_THRESHOLD,
    }


def _build_kinesis_entry(record: dict[str, Any]) -> dict[str, str | bytes]:
    """Wrap a record dict into the shape Kinesis PutRecords expects."""
    return {
        "Data":         json.dumps(record).encode("utf-8"),
        "PartitionKey": record["city_id"],
    }


def _put_with_retry(
    kinesis: boto3.client,
    stream:  str,
    entries: list[dict],
    attempt: int = 0,
) -> int:
    """Put a batch onto Kinesis with exponential backoff on failures."""
    if not entries:
        return 0
    try:
        response = kinesis.put_records(StreamName=stream, Records=entries)
    except ClientError as exc:
        if attempt >= config.MAX_RETRIES:
            log.error("Max retries reached. Dropping %d records.", len(entries))
            return 0
        wait = config.BACKOFF_BASE_SEC * (2 ** attempt)
        log.warning("Kinesis error (%s). Retry %d in %.1fs",
                    exc.response["Error"]["Code"], attempt + 1, wait)
        time.sleep(wait)
        return _put_with_retry(kinesis, stream, entries, attempt + 1)

    failed_count = response.get("FailedRecordCount", 0)
    if failed_count == 0:
        return len(entries)

    failed_entries = [
        entries[i]
        for i, result in enumerate(response["Records"])
        if "ErrorCode" in result
    ]
    if attempt >= config.MAX_RETRIES:
        log.error("Max retries reached. Dropping %d failed records.", failed_count)
        return len(entries) - failed_count

    wait = config.BACKOFF_BASE_SEC * (2 ** attempt)
    log.warning("%d records failed. Retry %d in %.1fs", failed_count, attempt + 1, wait)
    time.sleep(wait)
    delivered = len(entries) - failed_count
    delivered += _put_with_retry(kinesis, stream, failed_entries, attempt + 1)
    return delivered


def send_batch(kinesis: boto3.client, stream: str) -> int:
    """Generate and send one batch of AQI records (one per city)."""
    records  = [_make_record(city) for city in CITIES]
    entries  = [_build_kinesis_entry(r) for r in records]
    delivered = 0
    for i in range(0, len(entries), config.PRODUCER_BATCH_SIZE):
        chunk     = entries[i : i + config.PRODUCER_BATCH_SIZE]
        delivered += _put_with_retry(kinesis, stream, chunk)
    alerts = sum(1 for r in records if r["alert"])
    log.info(
        "Batch sent — %d/%d delivered | %d alert(s) | %s",
        delivered, len(records), alerts,
        ", ".join(f"{r['city_id']}={r['aqi_score']}" for r in records),
    )
    return delivered


def run(once: bool = False) -> None:
    """Main producer loop.

    Args:
        once: If True, send one batch and exit. Useful for testing.
    """
    kinesis = boto3.client("kinesis", region_name=config.AWS_REGION)
    stream  = config.AQI_STREAM_NAME
    log.info("─" * 60)
    log.info("Angaza producer starting")
    log.info("  Stream   : %s", stream)
    log.info("  Cities   : %d", len(CITIES))
    log.info("  Interval : %.1fs", config.PRODUCER_INTERVAL_SEC)
    log.info("─" * 60)
    total = 0
    try:
        while True:
            total += send_batch(kinesis, stream)
            if once:
                break
            time.sleep(config.PRODUCER_INTERVAL_SEC)
    except KeyboardInterrupt:
        log.info("Producer stopped. Total records delivered: %d", total)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Angaza AQI producer")
    parser.add_argument("--once", action="store_true",
                        help="Send one batch and exit")
    args = parser.parse_args()
    run(once=args.once)
