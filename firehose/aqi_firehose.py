"""
firehose/aqi_firehose.py — Angaza Kinesis Firehose delivery stream.

Creates a Firehose that reads from the AQI Kinesis stream and delivers
records to S3 under the raw/ prefix, buffered at 60s / 64MB.

Partition prefix layout:
  raw/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/

Run:
    python3 firehose/aqi_firehose.py
"""

import json
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


def _get_or_create_firehose_role(iam: boto3.client, account_id: str) -> str:
    """Create the IAM role Firehose needs to write to S3 and read Kinesis.

    Returns the role ARN.
    """
    role_name = "angaza-firehose-role"

    # Trust policy — allows Firehose service to assume this role
    trust_policy = json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect":    "Allow",
            "Principal": {"Service": "firehose.amazonaws.com"},
            "Action":    "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": account_id
                }
            }
        }]
    })

    # Permissions Firehose needs
    inline_policy = json.dumps({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:PutObject",
                    "s3:PutObjectAcl",
                    "s3:GetBucketLocation",
                    "s3:ListBucket",
                ],
                "Resource": [
                    f"arn:aws:s3:::{config.S3_BUCKET}",
                    f"arn:aws:s3:::{config.S3_BUCKET}/*",
                ]
            },
            {
                "Effect": "Allow",
                "Action": [
                    "kinesis:GetRecords",
                    "kinesis:GetShardIterator",
                    "kinesis:DescribeStream",
                    "kinesis:ListShards",
                ],
                "Resource": (
                    f"arn:aws:kinesis:{config.AWS_REGION}:{account_id}"
                    f":stream/{config.AQI_STREAM_NAME}"
                )
            },
            {
                "Effect":   "Allow",
                "Action":   ["logs:PutLogEvents"],
                "Resource": "*"
            }
        ]
    })

    try:
        resp = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=trust_policy,
            Description="Angaza Firehose — S3 write + Kinesis read",
        )
        arn = resp["Role"]["Arn"]
        log.info("[OK]    IAM role created: %s", role_name)
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "EntityAlreadyExists":
            arn = iam.get_role(RoleName=role_name)["Role"]["Arn"]
            log.info("[SKIP]  IAM role already exists: %s", role_name)
        else:
            raise

    # Attach inline policy (idempotent — overwrites if already exists)
    iam.put_role_policy(
        RoleName=role_name,
        PolicyName="angaza-firehose-policy",
        PolicyDocument=inline_policy,
    )
    log.info("[OK]    IAM policy attached")

    # IAM changes take a few seconds to propagate
    log.info("        Waiting 10s for IAM propagation ...")
    time.sleep(10)

    return arn


def _firehose_exists(firehose: boto3.client, name: str) -> bool:
    """Return True if the delivery stream already exists."""
    try:
        firehose.describe_delivery_stream(DeliveryStreamName=name)
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ResourceNotFoundException":
            return False
        raise


def _wait_for_active(firehose: boto3.client, name: str, timeout: int = 120) -> None:
    """Poll until Firehose stream status is ACTIVE."""
    log.info("        Waiting for Firehose to become ACTIVE ...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp   = firehose.describe_delivery_stream(DeliveryStreamName=name)
        status = resp["DeliveryStreamDescription"]["DeliveryStreamStatus"]
        if status == "ACTIVE":
            log.info("[OK]    Firehose is ACTIVE: %s", name)
            return
        time.sleep(5)
    raise TimeoutError(f"Firehose {name!r} did not become ACTIVE within {timeout}s")


def create_firehose(account_id: str) -> str:
    """Create the Angaza Firehose delivery stream.

    Returns the delivery stream ARN.
    """
    firehose = boto3.client("firehose", region_name=config.AWS_REGION)
    iam      = boto3.client("iam",      region_name=config.AWS_REGION)
    name     = config.FIREHOSE_NAME

    log.info("─" * 60)
    log.info("Setting up Angaza Firehose")
    log.info("  Stream  : %s", name)
    log.info("  Source  : %s", config.AQI_STREAM_NAME)
    log.info("  Bucket  : s3://%s/%s", config.S3_BUCKET, config.S3_PREFIX_RAW)
    log.info("─" * 60)

    if _firehose_exists(firehose, name):
        log.info("[SKIP]  Firehose already exists: %s", name)
        resp = firehose.describe_delivery_stream(DeliveryStreamName=name)
        return resp["DeliveryStreamDescription"]["DeliveryStreamARN"]

    role_arn = _get_or_create_firehose_role(iam, account_id)

    resp = firehose.create_delivery_stream(
        DeliveryStreamName=name,
        DeliveryStreamType="KinesisStreamAsSource",

        # Source — read from the AQI Kinesis stream
        KinesisStreamSourceConfiguration={
            "KinesisStreamARN": (
                f"arn:aws:kinesis:{config.AWS_REGION}:{account_id}"
                f":stream/{config.AQI_STREAM_NAME}"
            ),
            "RoleARN": role_arn,
        },

        # Destination — write to S3 with dynamic date partitioning
        ExtendedS3DestinationConfiguration={
            "RoleARN":   role_arn,
            "BucketARN": f"arn:aws:s3:::{config.S3_BUCKET}",

            # Dynamic prefix using Firehose timestamp expressions
            "Prefix": (
                f"{config.S3_PREFIX_RAW}"
                "year=!{timestamp:yyyy}/"
                "month=!{timestamp:MM}/"
                "day=!{timestamp:dd}/"
            ),
            "ErrorOutputPrefix": (
                f"{config.S3_PREFIX_RAW}errors/"
                "!{firehose:error-output-type}/"
                "year=!{timestamp:yyyy}/month=!{timestamp:MM}/"
            ),

            # Buffer — flush every 60s or when 64MB accumulates
            "BufferingHints": {
                "SizeInMBs":         config.FIREHOSE_BUFFER_MB,
                "IntervalInSeconds": config.FIREHOSE_BUFFER_SECONDS,
            },

            "CompressionFormat": "UNCOMPRESSED",  # JSON stays readable

            "CloudWatchLoggingOptions": {
                "Enabled":       True,
                "LogGroupName":  "/aws/kinesisfirehose/angaza",
                "LogStreamName": "S3Delivery",
            },
        },
    )

    arn = resp["DeliveryStreamARN"]
    log.info("[OK]    Firehose created: %s", name)
    _wait_for_active(firehose, name)
    return arn


if __name__ == "__main__":
    import config as cfg
    account_id = cfg.get_account_id()
    arn = create_firehose(account_id)
    log.info("Firehose ARN: %s", arn)
