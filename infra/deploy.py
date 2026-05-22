"""
infra/deploy.py — Angaza full platform deployment orchestrator.

Creates all AWS resources in dependency order:
  1. S3 data lake
  2. Kinesis streams (AQI + Stats)
  3. Kinesis Firehose
  4. SNS alert topic
  5. Glue database + crawler
  6. CloudWatch dashboard + alarms

Fully idempotent — safe to run multiple times.
Prints coloured [OK] / [SKIP] / [ERROR] for every step.

Run:
    python3 infra/deploy.py
    python3 infra/deploy.py --dry-run    # validate config without creating anything
"""

import argparse
import logging
import sys
import time
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config
from storage.s3_setup          import setup_data_lake
from streams.aqi_stream        import setup_aqi_stream
from streams.stats_stream      import setup_stats_stream
from firehose.aqi_firehose     import create_firehose
from alerting.sns_alerts       import setup_alerts
from catalog.glue_crawler      import setup_catalog
from monitoring.cloudwatch_dashboard import setup_monitoring

# ── Coloured output ───────────────────────────────────────────────────────────
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
BLUE   = "\033[94m"
RESET  = "\033[0m"
BOLD   = "\033[1m"

def ok(msg: str)    -> None: print(f"  {GREEN}[OK]{RESET}    {msg}")
def skip(msg: str)  -> None: print(f"  {YELLOW}[SKIP]{RESET}  {msg}")
def err(msg: str)   -> None: print(f"  {RED}[ERROR]{RESET} {msg}")
def info(msg: str)  -> None: print(f"  {BLUE}[INFO]{RESET}  {msg}")
def header(msg: str)-> None: print(f"\n{BOLD}{msg}{RESET}")


logging.basicConfig(
    level=logging.WARNING,      # suppress boto3 noise during deploy
    format="%(levelname)s  %(message)s",
)

# ── Deployment steps ──────────────────────────────────────────────────────────

def _step(name: str, fn, *args, **kwargs):
    """Run a deployment step, catch and report errors without aborting."""
    print(f"\n  ── {name}")
    try:
        result = fn(*args, **kwargs)
        return result
    except Exception as exc:
        err(f"{name} failed: {exc}")
        return None


def deploy(dry_run: bool = False) -> None:
    """Deploy the full Angaza platform.

    Args:
        dry_run: Print what would be deployed without creating anything.
    """
    start = time.time()

    print(f"\n{BOLD}{'─' * 60}{RESET}")
    print(f"{BOLD}  Angaza — AQI Intelligence Platform{RESET}")
    print(f"{BOLD}  Deployment starting ...{RESET}")
    print(f"{BOLD}{'─' * 60}{RESET}")

    info(f"Region     : {config.AWS_REGION}")
    info(f"Bucket     : {config.S3_BUCKET}")
    info(f"AQI stream : {config.AQI_STREAM_NAME}")
    info(f"Namespace  : {config.CW_NAMESPACE}")

    if dry_run:
        print(f"\n  {YELLOW}DRY RUN — no resources will be created{RESET}\n")
        _print_deployment_plan()
        return

    # Resolve AWS account ID once — used by IAM and ARN construction
    header("Step 1 of 6 — Resolving AWS identity")
    try:
        account_id = config.get_account_id()
        ok(f"Account ID: {account_id}")
    except Exception as exc:
        err(f"Cannot resolve AWS account ID: {exc}")
        err("Check your AWS credentials and try again.")
        sys.exit(1)

    # ── 1. S3 ─────────────────────────────────────────────────────────────────
    header("Step 2 of 6 — S3 data lake")
    _step("S3 setup", setup_data_lake)

    # ── 2. Kinesis streams ────────────────────────────────────────────────────
    header("Step 3 of 6 — Kinesis streams")
    _step("AQI stream",   setup_aqi_stream)
    _step("Stats stream", setup_stats_stream)

    # ── 3. Firehose ───────────────────────────────────────────────────────────
    header("Step 4 of 6 — Kinesis Firehose")
    firehose_arn = _step("Firehose", create_firehose, account_id)
    if firehose_arn:
        ok(f"Firehose ARN: {firehose_arn}")

    # ── 4. SNS ────────────────────────────────────────────────────────────────
    header("Step 5 of 6 — SNS alerts")
    sns_topic_arn = _step("SNS topic", setup_alerts, account_id)
    if sns_topic_arn:
        ok(f"Topic ARN: {sns_topic_arn}")

    # ── 5. Glue ───────────────────────────────────────────────────────────────
    header("Step 6 of 6 — Glue catalog")
    _step("Glue catalog", setup_catalog)

    # ── 6. CloudWatch ─────────────────────────────────────────────────────────
    header("Step 7 of 6 — CloudWatch monitoring")
    _step("CloudWatch", setup_monitoring, sns_topic_arn or "")

    # ── Summary ───────────────────────────────────────────────────────────────
    elapsed = time.time() - start
    print(f"\n{BOLD}{'─' * 60}{RESET}")
    print(f"{BOLD}  {GREEN}Angaza deployed successfully{RESET}{BOLD} in {elapsed:.0f}s{RESET}")
    print(f"{BOLD}{'─' * 60}{RESET}\n")

    _print_next_steps(sns_topic_arn or "")


def _print_deployment_plan() -> None:
    """Print what deploy would create, without doing it."""
    steps = [
        ("S3",        f"s3://{config.S3_BUCKET}/"),
        ("Kinesis",   f"{config.AQI_STREAM_NAME}  +  {config.STATS_STREAM_NAME}"),
        ("Firehose",  config.FIREHOSE_NAME),
        ("SNS",       config.SNS_TOPIC_NAME),
        ("Glue",      f"{config.GLUE_DATABASE}  /  {config.GLUE_CRAWLER_NAME}"),
        ("CloudWatch",f"{config.CW_NAMESPACE}  dashboard + {10} alarms"),
    ]
    print(f"  {'Resource':<14} {'Name'}")
    print(f"  {'─' * 13} {'─' * 40}")
    for resource, name in steps:
        print(f"  {resource:<14} {name}")
    print()


def _print_next_steps(sns_topic_arn: str) -> None:
    """Print actionable next steps after deployment."""
    print(f"  {BOLD}Next steps:{RESET}")
    print(f"  1. Seed historical data:")
    print(f"     python3 producer/data_seeder.py")
    print()
    print(f"  2. Start the live producer:")
    print(f"     python3 producer/aqi_producer.py")
    print()
    print(f"  3. Set Lambda environment variable:")
    if sns_topic_arn:
        print(f"     AQI_SNS_TOPIC_ARN={sns_topic_arn}")
    else:
        print(f"     AQI_SNS_TOPIC_ARN=<your-sns-topic-arn>")
    print()
    print(f"  4. Open the dashboard:")
    print(f"     https://{config.AWS_REGION}.console.aws.amazon.com"
          f"/cloudwatch/home#dashboards:name=Angaza-AQI-Dashboard")
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Angaza platform deployer")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print deployment plan without creating resources",
    )
    args = parser.parse_args()
    deploy(dry_run=args.dry_run)
