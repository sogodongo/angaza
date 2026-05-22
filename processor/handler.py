"""
lambda/handler.py — Angaza Stats Stream processor.

Triggered by the Stats Kinesis stream. For each record:
  1. Decodes and validates the payload
  2. Publishes AQIScore and PM25Level metrics to CloudWatch
  3. Publishes an SNS alert if AQI exceeds the threshold

Structured JSON logging throughout — queryable via CloudWatch Insights.

Environment variables required at runtime:
  AWS_REGION         — set automatically by Lambda
  AQI_SNS_TOPIC_ARN  — ARN of the angaza-alerts SNS topic
  AQI_CW_NAMESPACE   — CloudWatch namespace (default: AngazaMonitor)
  AQI_ALERT_THRESHOLD — AQI score above which alerts fire (default: 150)
"""

import base64
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import ClientError

# When running locally for tests, resolve config from project root
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config

# ── Logging ───────────────────────────────────────────────────────────────────
# Structured JSON so CloudWatch Insights can filter by field
class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        base = {
            "time":    datetime.now(timezone.utc).isoformat(),
            "level":   record.levelname,
            "message": record.getMessage(),
        }
        if hasattr(record, "extra"):
            base.update(record.extra)
        return json.dumps(base)

handler = logging.StreamHandler()
handler.setFormatter(_JsonFormatter())
log = logging.getLogger(__name__)
log.addHandler(handler)
log.setLevel(logging.INFO)
log.propagate = False

# ── AWS clients (initialised outside the handler for Lambda reuse) ────────────
_cw  = boto3.client("cloudwatch", region_name=config.AWS_REGION)
_sns = boto3.client("sns",        region_name=config.AWS_REGION)

# SNS topic ARN comes from env at runtime — not hardcoded
SNS_TOPIC_ARN: str = os.getenv("AQI_SNS_TOPIC_ARN", "")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _decode_record(kinesis_record: dict[str, Any]) -> dict[str, Any] | None:
    """Base64-decode and JSON-parse a single Kinesis record.

    Returns None if the record cannot be parsed — Lambda will not
    retry individual bad records, so we log and skip them.
    """
    try:
        raw  = base64.b64decode(kinesis_record["kinesis"]["data"])
        return json.loads(raw)
    except (KeyError, ValueError, json.JSONDecodeError) as exc:
        log.warning(json.dumps({"message": "Failed to decode record", "error": str(exc)}))
        return None


def _publish_metrics(payload: dict[str, Any]) -> None:
    """Push AQIScore and PM25Level to CloudWatch as custom metrics.

    Uses a single put_metric_data call with two metric datums
    so we stay within the 25-metrics-per-call limit easily.
    """
    city_id = payload["city_id"]

    _cw.put_metric_data(
        Namespace=config.CW_NAMESPACE,
        MetricData=[
            {
                "MetricName": "AQIScore",
                "Dimensions": [{"Name": "City", "Value": city_id}],
                "Value":      float(payload["aqi_score"]),
                "Unit":       "None",
            },
            {
                "MetricName": "PM25Level",
                "Dimensions": [{"Name": "City", "Value": city_id}],
                "Value":      float(payload["pm2_5"]),
                "Unit":       "None",
            },
        ],
    )
    log.info(json.dumps({
        "message":   "Metrics published",
        "city_id":   city_id,
        "aqi_score": payload["aqi_score"],
        "pm2_5":     payload["pm2_5"],
    }))


def _should_alert(payload: dict[str, Any]) -> bool:
    """Return True if this record warrants an SNS alert."""
    return int(payload.get("aqi_score", 0)) > config.AQI_ALERT_THRESHOLD


def _publish_alert(payload: dict[str, Any]) -> None:
    """Publish an SNS alert for a city that crossed the AQI threshold.

    Message is JSON so downstream subscribers can parse it directly.
    """
    if not SNS_TOPIC_ARN:
        log.warning(json.dumps({"message": "SNS_TOPIC_ARN not set — skipping alert"}))
        return

    message = json.dumps({
        "source":    "angaza",
        "city_id":   payload["city_id"],
        "city_name": payload.get("city_name", ""),
        "country":   payload.get("country", ""),
        "timestamp": payload.get("timestamp", ""),
        "aqi_score": payload["aqi_score"],
        "category":  payload.get("category", ""),
        "severity":  payload.get("severity", 0),
        "pm2_5":     payload.get("pm2_5", 0),
        "alert":     True,
    })

    subject = (
        f"[Angaza Alert] {payload.get('city_name', payload['city_id'])} "
        f"AQI {payload['aqi_score']} — {payload.get('category', '')}"
    )

    try:
        _sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=message,
            Subject=subject,
        )
        log.info(json.dumps({
            "message":   "SNS alert published",
            "city_id":   payload["city_id"],
            "aqi_score": payload["aqi_score"],
            "category":  payload.get("category"),
        }))
    except ClientError as exc:
        log.error(json.dumps({
            "message": "SNS publish failed",
            "error":   exc.response["Error"]["Message"],
            "city_id": payload["city_id"],
        }))


# ── Lambda entry point ────────────────────────────────────────────────────────

def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Process a batch of Kinesis records from the Stats stream.

    Lambda invokes this with up to LAMBDA_BATCH_SIZE records at once.
    We process each record independently — one bad record never blocks
    the rest of the batch.

    Returns a summary dict (visible in Lambda execution logs).
    """
    records = event.get("Records", [])

    processed = 0
    alerted   = 0
    skipped   = 0

    for kinesis_record in records:
        payload = _decode_record(kinesis_record)

        if payload is None:
            skipped += 1
            continue

        try:
            _publish_metrics(payload)

            if _should_alert(payload):
                _publish_alert(payload)
                alerted += 1

            processed += 1

        except ClientError as exc:
            log.error(json.dumps({
                "message":  "AWS call failed for record",
                "city_id":  payload.get("city_id", "unknown"),
                "error":    exc.response["Error"]["Message"],
            }))
            skipped += 1

    summary = {
        "total":     len(records),
        "processed": processed,
        "alerted":   alerted,
        "skipped":   skipped,
    }
    log.info(json.dumps({"message": "Batch complete", **summary}))
    return summary
