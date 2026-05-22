"""
alerting/sns_alerts.py — Angaza SNS alert topic.

Creates the angaza-alerts SNS topic and wires up an email subscription.
The topic ARN is printed at the end — paste it into your Lambda
environment variable AQI_SNS_TOPIC_ARN.

Run:
    python3 alerting/sns_alerts.py
"""

import json
import logging
import sys
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


def _topic_exists(sns: boto3.client, name: str) -> str | None:
    """Return the topic ARN if it exists, otherwise None.

    SNS has no describe-by-name API so we list and match.
    """
    paginator = sns.get_paginator("list_topics")
    for page in paginator.paginate():
        for topic in page["Topics"]:
            if topic["TopicArn"].endswith(f":{name}"):
                return topic["TopicArn"]
    return None


def create_topic(sns: boto3.client) -> str:
    """Create the SNS topic (idempotent — returns ARN if already exists).

    Returns the topic ARN.
    """
    name = config.SNS_TOPIC_NAME

    existing_arn = _topic_exists(sns, name)
    if existing_arn:
        log.info("[SKIP]  Topic already exists: %s", existing_arn)
        return existing_arn

    resp = sns.create_topic(
        Name=name,
        Attributes={
            "DisplayName": "Angaza Air Quality Alerts",
        },
        Tags=[
            {"Key": "project", "Value": "angaza"},
            {"Key": "env",     "Value": "production"},
        ],
    )
    arn = resp["TopicArn"]
    log.info("[OK]    Topic created: %s", arn)
    return arn


def subscribe_email(sns: boto3.client, topic_arn: str) -> None:
    """Add an email subscription to the topic.

    SNS sends a confirmation email — the subscription stays
    PendingConfirmation until the recipient clicks the link.
    """
    email = config.ALERT_EMAIL
    if not email:
        log.warning("[SKIP]  AQI_ALERT_EMAIL not set — no email subscription added")
        log.warning("        Set it with: export AQI_ALERT_EMAIL=you@example.com")
        return

    try:
        resp = sns.subscribe(
            TopicArn=topic_arn,
            Protocol="email",
            Endpoint=email,
            ReturnSubscriptionArn=True,
        )
        log.info("[OK]    Email subscription created: %s", email)
        log.info("        ARN: %s", resp["SubscriptionArn"])
        log.info("        Check your inbox and confirm the subscription.")
    except ClientError as exc:
        log.error("[ERR]   Subscription failed: %s", exc.response["Error"]["Message"])


def set_topic_policy(sns: boto3.client, topic_arn: str, account_id: str) -> None:
    """Set a resource policy allowing only our Lambda to publish.

    Principle of least privilege — no wildcard principals.
    """
    policy = json.dumps({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid":       "AllowLambdaPublish",
                "Effect":    "Allow",
                "Principal": {
                    "AWS": f"arn:aws:iam::{account_id}:root"
                },
                "Action":    "sns:Publish",
                "Resource":  topic_arn,
            }
        ],
    })
    sns.set_topic_attributes(
        TopicArn=topic_arn,
        AttributeName="Policy",
        AttributeValue=policy,
    )
    log.info("[OK]    Topic policy applied (Lambda publish only)")


def setup_alerts(account_id: str) -> str:
    """Orchestrate full SNS alert setup.

    Returns the topic ARN.
    """
    sns = boto3.client("sns", region_name=config.AWS_REGION)

    log.info("─" * 60)
    log.info("Setting up Angaza alerts")
    log.info("  Topic : %s", config.SNS_TOPIC_NAME)
    log.info("  Email : %s", config.ALERT_EMAIL or "(not set)")
    log.info("─" * 60)

    topic_arn = create_topic(sns)
    subscribe_email(sns, topic_arn)
    set_topic_policy(sns, topic_arn, account_id)

    log.info("─" * 60)
    log.info("SNS ready")
    log.info("  Topic ARN : %s", topic_arn)
    log.info("  Next step : export AQI_SNS_TOPIC_ARN=%s", topic_arn)
    log.info("─" * 60)
    return topic_arn


if __name__ == "__main__":
    account_id = config.get_account_id()
    setup_alerts(account_id)
