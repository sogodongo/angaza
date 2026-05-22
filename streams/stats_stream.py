"""
streams/stats_stream.py — provision the Stats Kinesis Data Stream.

Receives aggregated, enriched records from the AQI Analysis (Flink) app.
1 shard is enough for aggregated output volume.

Run:  python streams/stats_stream.py
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
    """Create stream and wait until ACTIVE."""
    kinesis.create_stream(StreamName=name, ShardCount=shards)
    log.info("[OK]    Stream creation requested: %s (%d shard)", name, shards)
    _wait_for_active(kinesis, name)


def _wait_for_active(kinesis: boto3.client, name: str, timeout: int = 60) -> None:
    """Poll until stream status is ACTIVE."""
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


def setup_stats_stream() -> str:
    """Create the Stats stream if needed.

    Returns the stream name.
    """
    kinesis = boto3.client("kinesis", region_name=config.AWS_REGION)
    name   = config.STATS_STREAM_NAME
    shards = config.STATS_STREAM_SHARDS

    log.info("─" * 55)
    log.info("Setting up Stats stream")
    log.info("  Stream : %s", name)
    log.info("  Shards : %d", shards)
    log.info("─" * 55)

    if stream_exists(kinesis, name):
        log.info("[SKIP]  Stream already exists: %s", name)
    else:
        create_stream(kinesis, name, shards)

    log.info("Stats stream ready")
    return name


if __name__ == "__main__":
    setup_stats_stream()
