"""
producer/data_seeder.py — Angaza historical data seeder.

Generates 30 days of AQI readings for all cities and writes them
to S3 under the raw/ prefix, partitioned by date and city.

Partition layout:
  raw/year=YYYY/month=MM/day=DD/city=XXX/readings.json

Each file is newline-delimited JSON (one record per line).
Glue crawler will pick up the partition keys automatically.

Run:
    python3 producer/data_seeder.py
    python3 producer/data_seeder.py --days 7     # seed last 7 days only
    python3 producer/data_seeder.py --dry-run    # print without writing to S3
"""

import argparse
import json
import logging
import random
import sys
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config
from producer.aqi_producer import CITIES, _make_record

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# Readings per city per day (every 30 minutes = 48 readings/day)
READINGS_PER_DAY = 48


def _readings_for_city_day(
    city: dict[str, Any],
    date: datetime,
) -> list[dict[str, Any]]:
    """Generate one full day of readings for a single city.

    Readings are spaced 30 minutes apart starting at midnight UTC.
    AQI follows a realistic diurnal pattern — higher during rush hours.
    """
    records = []
    for slot in range(READINGS_PER_DAY):
        # Simulate rush-hour spikes at 07:00 and 17:00
        hour = (slot * 30) // 60
        rush_factor = 1.3 if hour in (7, 8, 17, 18) else 1.0

        ts = date.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(minutes=slot * 30)

        # Temporarily bump the city base to simulate rush hour
        adjusted_city = {**city, "aqi_base": int(city["aqi_base"] * rush_factor)}
        record = _make_record(adjusted_city)
        record["timestamp"] = ts.isoformat()
        records.append(record)

    return records


def _s3_key(city_id: str, date: datetime) -> str:
    """Build the S3 key for a city+date partition."""
    return (
        f"{config.S3_PREFIX_RAW}"
        f"year={date.year}/"
        f"month={date.month:02d}/"
        f"day={date.day:02d}/"
        f"city={city_id}/"
        f"readings.json"
    )


def _write_to_s3(
    s3:      boto3.client,
    bucket:  str,
    key:     str,
    records: list[dict[str, Any]],
) -> None:
    """Write records as newline-delimited JSON to S3."""
    body = "\n".join(json.dumps(r) for r in records).encode("utf-8")
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")


def seed(days: int = 30, dry_run: bool = False) -> None:
    """Seed S3 with historical AQI data.

    Args:
        days:    Number of past days to generate data for.
        dry_run: If True, print keys without writing to S3.
    """
    s3     = boto3.client("s3", region_name=config.AWS_REGION)
    bucket = config.S3_BUCKET
    today  = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    dates  = [today - timedelta(days=d) for d in range(days, 0, -1)]

    total_records = 0
    total_files   = 0

    log.info("─" * 60)
    log.info("Angaza data seeder")
    log.info("  Bucket  : s3://%s/", bucket)
    log.info("  Days    : %d  (%s → %s)",
             days,
             dates[0].strftime("%Y-%m-%d"),
             dates[-1].strftime("%Y-%m-%d"))
    log.info("  Cities  : %d", len(CITIES))
    log.info("  Files   : %d", days * len(CITIES))
    log.info("  Dry run : %s", dry_run)
    log.info("─" * 60)

    for date in dates:
        for city in CITIES:
            records = _readings_for_city_day(city, date)
            key     = _s3_key(city["id"], date)

            if dry_run:
                log.info("[DRY]   s3://%s/%s  (%d records)", bucket, key, len(records))
            else:
                try:
                    _write_to_s3(s3, bucket, key, records)
                    log.info("[OK]    s3://%s/%s  (%d records)", bucket, key, len(records))
                except ClientError as exc:
                    log.error("[ERR]   %s — %s", key, exc.response["Error"]["Message"])
                    continue

            total_records += len(records)
            total_files   += 1

    log.info("─" * 60)
    log.info("Seeding complete")
    log.info("  Files   : %d", total_files)
    log.info("  Records : %d", total_records)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Angaza historical data seeder")
    parser.add_argument("--days",    type=int,  default=30, help="Days of history to generate")
    parser.add_argument("--dry-run", action="store_true",   help="Print keys without writing")
    args = parser.parse_args()
    seed(days=args.days, dry_run=args.dry_run)
