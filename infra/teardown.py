"""
infra/teardown.py — Angaza full platform teardown.

Deletes all AWS resources in reverse dependency order:
  1. CloudWatch alarms + dashboard
  2. Glue crawler + database
  3. SNS topic + subscriptions
  4. Kinesis Firehose
  5. Kinesis streams
  6. S3 bucket (empties it first)

Prints coloured [OK] / [SKIP] / [ERROR] for every step.

WARNING: This deletes real AWS resources and all data in S3.
         You will be prompted to confirm before anything is deleted.

Run:
    python3 infra/teardown.py
    python3 infra/teardown.py --force   # skip confirmation prompt
"""

import argparse
import logging
import sys
import time
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config
from producer.aqi_producer import CITIES

# ── Coloured output ───────────────────────────────────────────────────────────
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
BLUE   = "\033[94m"
RESET  = "\033[0m"
BOLD   = "\033[1m"

def ok(msg: str)   -> None: print(f"  {GREEN}[OK]{RESET}    {msg}")
def skip(msg: str) -> None: print(f"  {YELLOW}[SKIP]{RESET}  {msg}")
def err(msg: str)  -> None: print(f"  {RED}[ERROR]{RESET} {msg}")
def info(msg: str) -> None: print(f"  {BLUE}[INFO]{RESET}  {msg}")
def header(msg: str)-> None: print(f"\n{BOLD}{msg}{RESET}")

logging.basicConfig(level=logging.WARNING)


# ── Teardown functions ────────────────────────────────────────────────────────

def _delete_cloudwatch(cw: boto3.client) -> None:
    """Delete all Angaza CloudWatch alarms and the dashboard."""
    # Alarms — one per city
    alarm_names = [
        f"angaza-aqi-{city['id'].lower()}-high"
        for city in CITIES
    ]
    try:
        cw.delete_alarms(AlarmNames=alarm_names)
        ok(f"CloudWatch alarms deleted ({len(alarm_names)})")
    except ClientError as exc:
        err(f"Alarms: {exc.response['Error']['Message']}")

    # Dashboard
    try:
        cw.delete_dashboards(DashboardNames=["Angaza-AQI-Dashboard"])
        ok("CloudWatch dashboard deleted")
    except ClientError as exc:
        err(f"Dashboard: {exc.response['Error']['Message']}")


def _delete_glue(glue: boto3.client) -> None:
    """Stop and delete the Glue crawler, then drop the database."""
    crawler = config.GLUE_CRAWLER_NAME
    db      = config.GLUE_DATABASE

    # Stop crawler if running
    try:
        resp   = glue.get_crawler(Name=crawler)
        state  = resp["Crawler"]["State"]
        if state == "RUNNING":
            glue.stop_crawler(Name=crawler)
            info("Waiting for crawler to stop ...")
            time.sleep(15)
    except ClientError:
        pass  # crawler doesn't exist — nothing to stop

    # Delete crawler
    try:
        glue.delete_crawler(Name=crawler)
        ok(f"Glue crawler deleted: {crawler}")
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code == "EntityNotFoundException":
            skip(f"Crawler not found: {crawler}")
        else:
            err(f"Crawler: {exc.response['Error']['Message']}")

    # Drop database
    try:
        glue.delete_database(Name=db)
        ok(f"Glue database deleted: {db}")
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code == "EntityNotFoundException":
            skip(f"Database not found: {db}")
        else:
            err(f"Database: {exc.response['Error']['Message']}")


def _delete_sns(sns: boto3.client) -> None:
    """Delete all subscriptions then the SNS topic."""
    name = config.SNS_TOPIC_NAME

    # Find topic ARN
    topic_arn = None
    paginator = sns.get_paginator("list_topics")
    for page in paginator.paginate():
        for topic in page["Topics"]:
            if topic["TopicArn"].endswith(f":{name}"):
                topic_arn = topic["TopicArn"]
                break

    if not topic_arn:
        skip(f"SNS topic not found: {name}")
        return

    # Delete all subscriptions first
    paginator = sns.get_paginator("list_subscriptions_by_topic")
    try:
        for page in paginator.paginate(TopicArn=topic_arn):
            for sub in page["Subscriptions"]:
                if sub["SubscriptionArn"] not in ("PendingConfirmation", "Deleted"):
                    sns.unsubscribe(SubscriptionArn=sub["SubscriptionArn"])
        ok("SNS subscriptions removed")
    except ClientError as exc:
        err(f"Subscriptions: {exc.response['Error']['Message']}")

    # Delete topic
    try:
        sns.delete_topic(TopicArn=topic_arn)
        ok(f"SNS topic deleted: {name}")
    except ClientError as exc:
        err(f"Topic: {exc.response['Error']['Message']}")


def _delete_firehose(firehose: boto3.client) -> None:
    """Delete the Kinesis Firehose delivery stream."""
    name = config.FIREHOSE_NAME
    try:
        firehose.delete_delivery_stream(
            DeliveryStreamName=name,
            AllowForceDelete=True,
        )
        ok(f"Firehose deleted: {name}")
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code == "ResourceNotFoundException":
            skip(f"Firehose not found: {name}")
        else:
            err(f"Firehose: {exc.response['Error']['Message']}")


def _delete_stream(kinesis: boto3.client, name: str) -> None:
    """Delete a single Kinesis stream."""
    try:
        kinesis.delete_stream(StreamName=name, EnforceConsumerDeletion=True)
        ok(f"Kinesis stream deleted: {name}")
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code == "ResourceNotFoundException":
            skip(f"Stream not found: {name}")
        else:
            err(f"Stream {name}: {exc.response['Error']['Message']}")


def _empty_and_delete_bucket(s3: boto3.client, bucket: str) -> None:
    """Delete all objects and versions, then delete the bucket."""
    # Delete all object versions (required before bucket delete)
    paginator = s3.get_paginator("list_object_versions")
    try:
        pages = paginator.paginate(Bucket=bucket)
        for page in pages:
            objects = []
            for v in page.get("Versions", []):
                objects.append({"Key": v["Key"], "VersionId": v["VersionId"]})
            for m in page.get("DeleteMarkers", []):
                objects.append({"Key": m["Key"], "VersionId": m["VersionId"]})
            if objects:
                s3.delete_objects(
                    Bucket=bucket,
                    Delete={"Objects": objects, "Quiet": True},
                )
        ok(f"Bucket emptied: {bucket}")
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code == "NoSuchBucket":
            skip(f"Bucket not found: {bucket}")
            return
        err(f"Empty bucket: {exc.response['Error']['Message']}")
        return

    # Now delete the empty bucket
    try:
        s3.delete_bucket(Bucket=bucket)
        ok(f"Bucket deleted: {bucket}")
    except ClientError as exc:
        err(f"Delete bucket: {exc.response['Error']['Message']}")


def _confirm() -> bool:
    """Ask the user to type the project name to confirm teardown."""
    print(f"\n  {RED}{BOLD}WARNING — this will delete ALL Angaza AWS resources{RESET}")
    print(f"  {RED}including all data in S3. This cannot be undone.{RESET}\n")
    answer = input("  Type 'angaza' to confirm: ").strip().lower()
    return answer == "angaza"


def teardown(force: bool = False) -> None:
    """Tear down the full Angaza platform in reverse dependency order.

    Args:
        force: Skip the confirmation prompt.
    """
    if not force and not _confirm():
        print("\n  Teardown cancelled.\n")
        sys.exit(0)

    start = time.time()
    print(f"\n{BOLD}{'─' * 60}{RESET}")
    print(f"{BOLD}  Angaza — tearing down ...{RESET}")
    print(f"{BOLD}{'─' * 60}{RESET}")

    cw       = boto3.client("cloudwatch", region_name=config.AWS_REGION)
    glue     = boto3.client("glue",       region_name=config.AWS_REGION)
    sns      = boto3.client("sns",        region_name=config.AWS_REGION)
    firehose = boto3.client("firehose",   region_name=config.AWS_REGION)
    kinesis  = boto3.client("kinesis",    region_name=config.AWS_REGION)
    s3       = boto3.client("s3",         region_name=config.AWS_REGION)

    header("Step 1 — CloudWatch")
    _delete_cloudwatch(cw)

    header("Step 2 — Glue")
    _delete_glue(glue)

    header("Step 3 — SNS")
    _delete_sns(sns)

    header("Step 4 — Firehose")
    _delete_firehose(firehose)

    header("Step 5 — Kinesis streams")
    _delete_stream(kinesis, config.AQI_STREAM_NAME)
    _delete_stream(kinesis, config.STATS_STREAM_NAME)

    header("Step 6 — S3")
    _empty_and_delete_bucket(s3, config.S3_BUCKET)

    elapsed = time.time() - start
    print(f"\n{BOLD}{'─' * 60}{RESET}")
    print(f"{BOLD}  {GREEN}Teardown complete{RESET}{BOLD} in {elapsed:.0f}s{RESET}")
    print(f"{BOLD}{'─' * 60}{RESET}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Angaza platform teardown")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Skip confirmation prompt",
    )
    args = parser.parse_args()
    teardown(force=args.force)
