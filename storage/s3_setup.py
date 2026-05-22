"""
storage/s3_setup.py — provision the AQI data lake bucket.

Creates one bucket with:
  - Versioning enabled
  - Three logical prefixes:  raw/  analytical/  checkpoints/
  - Lifecycle rule: raw/ → S3-IA after 30 days, Glacier after 90 days
  - Block all public access (safety default)

Run:  python storage/s3_setup.py
"""

import json
import logging
import sys
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

# allow imports from project root
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def create_bucket(s3: boto3.client, bucket: str, region: str) -> bool:
    """Create S3 bucket if it does not already exist.

    Returns True if created, False if it already existed.
    """
    try:
        if region == "us-east-1":
            s3.create_bucket(Bucket=bucket)
        else:
            s3.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
        log.info("[OK]    Bucket created: %s", bucket)
        return True
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            log.info("[SKIP]  Bucket already exists: %s", bucket)
            return False
        raise


def enable_versioning(s3: boto3.client, bucket: str) -> None:
    """Enable versioning on the bucket."""
    s3.put_bucket_versioning(
        Bucket=bucket,
        VersioningConfiguration={"Status": "Enabled"},
    )
    log.info("[OK]    Versioning enabled")


def block_public_access(s3: boto3.client, bucket: str) -> None:
    """Block all public access — data lake should never be public."""
    s3.put_public_access_block(
        Bucket=bucket,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls":       True,
            "IgnorePublicAcls":      True,
            "BlockPublicPolicy":     True,
            "RestrictPublicBuckets": True,
        },
    )
    log.info("[OK]    Public access blocked")


def apply_lifecycle_policy(s3: boto3.client, bucket: str) -> None:
    """Transition raw/ objects to cheaper storage tiers over time."""
    policy = {
        "Rules": [
            {
                "ID":     "raw-tiering",
                "Status": "Enabled",
                "Filter": {"Prefix": config.S3_PREFIX_RAW},
                "Transitions": [
                    {"Days": 30,  "StorageClass": "STANDARD_IA"},
                    {"Days": 90,  "StorageClass": "GLACIER"},
                ],
            }
        ]
    }
    s3.put_bucket_lifecycle_configuration(
        Bucket=bucket,
        LifecycleConfiguration=policy,
    )
    log.info("[OK]    Lifecycle policy applied (raw/ → IA@30d, Glacier@90d)")


def create_prefix_markers(s3: boto3.client, bucket: str) -> None:
    """Write zero-byte marker objects so prefixes appear in the console."""
    prefixes = [
        config.S3_PREFIX_RAW,
        config.S3_PREFIX_ANALYTICAL,
        config.S3_PREFIX_CHECKPOINTS,
    ]
    for prefix in prefixes:
        key = f"{prefix}.keep"
        s3.put_object(Bucket=bucket, Key=key, Body=b"")
        log.info("[OK]    Prefix marker created: s3://%s/%s", bucket, key)


def setup_data_lake() -> str:
    """Orchestrate full S3 data lake setup.

    Returns the bucket name.
    """
    s3 = boto3.client("s3", region_name=config.AWS_REGION)
    bucket = config.S3_BUCKET

    log.info("─" * 55)
    log.info("Setting up AQI data lake")
    log.info("  Bucket : %s", bucket)
    log.info("  Region : %s", config.AWS_REGION)
    log.info("─" * 55)

    create_bucket(s3, bucket, config.AWS_REGION)
    enable_versioning(s3, bucket)
    block_public_access(s3, bucket)
    apply_lifecycle_policy(s3, bucket)
    create_prefix_markers(s3, bucket)

    log.info("─" * 55)
    log.info("Data lake ready: s3://%s/", bucket)
    return bucket


if __name__ == "__main__":
    setup_data_lake()
