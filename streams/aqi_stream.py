"""
streams/aqi_stream.py — provision the AQI Kinesis Data Stream.

This is the entry point for all raw sensor data.
2 shards → supports up to 2 MB/s write, 4 MB/s read.

Run:  python streams/aqi_stream.py
"""

import logging
import sys
import time
from pathlib import Path

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


def stream_exists(kinesis: boto3.client, name: str) -> bool:
    """Return True if the stream already exists in any non-DELETING state."""
    try:
        resp = kinesis.describe_stream_summary(StreamName=name)
        status = resp["StreamDescriptionSummary"]["StreamStatus"]
        return status != "DELETING"
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ResourceNotFoundException":
            return False
        raise


def create_stream(kinesis: boto3.client, name: str, shards: int) -> None:
    """Create a Kinesis stream and wait until it is ACTIVE."""
    kinesis.create_stream(StreamName=name, ShardCount=shards)
    log.info("[OK]    Stream creation requested: %s (%d shards)", name, shards)
    _wait_for_active(kinesis, name)


def set_retention(kinesis: boto3.client, name: str, hours: int) -> None:
    """Set retention period in hours (must be between 24 and 8760)."""
    kinesis.increase_stream_retention_period(
        StreamName=name,
        RetentionPeriodHours=hours,
    )
    log.info("[OK]    Retention set to %d hours", hours)


def _wait_for_active(kinesis: boto3.client, name: str, timeout: int = 60) -> None:
    """Poll until stream status is ACTIVE or timeout expires."""
    log.info("        Waiting for stream to become ACTIVE ...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = kinesis.describe_stream_summary(StreamName=name)
        status = resp["StreamDescriptionSummary"]["StreamStatus"]
        if status == "ACTIVE":
            log.info("[OK]    Stream is ACTIVE: %s", name)
            return
        time.sleep(3)
    raise TimeoutError(f"Stream {name!r} did not become ACTIVE within {timeout}s")


def setup_aqi_stream() -> str:
    """Create the AQI stream if needed.

    Returns the stream name.
    """
    kinesis = boto3.client("kinesis", region_name=config.AWS_REGION)
    name   = config.AQI_STREAM_NAME
    shards = config.AQI_STREAM_SHARDS

    log.info("─" * 55)
    log.info("Setting up AQI stream")
    log.info("  Stream : %s", name)
    log.info("  Shards : %d", shards)
    log.info("─" * 55)

    if stream_exists(kinesis, name):
        log.info("[SKIP]  Stream already exists: %s", name)
    else:
        create_stream(kinesis, name, shards)
        # Retention can only be increased above the 24-h default
        if config.STREAM_RETENTION_HOURS > 24:
            set_retention(kinesis, name, config.STREAM_RETENTION_HOURS)

    log.info("AQI stream ready")
    return name


if __name__ == "__main__":
    setup_aqi_stream()
