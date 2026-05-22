"""
catalog/glue_crawler.py — Angaza Glue Data Catalog and Athena queries.

Creates:
  - Glue database: angaza_analytics_db
  - IAM role for the crawler to read S3
  - Glue crawler pointed at the analytical/ prefix
  - Three ready-to-run Athena query strings

Run:
    python3 catalog/glue_crawler.py
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


# ── Athena queries ─────────────────────────────────────────────────────────────
# These run against the Glue catalog after the crawler has run at least once.

QUERY_HOURLY_TREND = """
-- Hourly average AQI per city over the last 7 days
SELECT
    city_id,
    city_name,
    DATE_TRUNC('hour', from_iso8601_timestamp(timestamp)) AS hour,
    ROUND(AVG(aqi_score), 1)                              AS avg_aqi,
    ROUND(AVG(pm2_5), 2)                                  AS avg_pm25,
    MAX(aqi_score)                                        AS max_aqi
FROM {database}.aqi_readings
WHERE from_iso8601_timestamp(timestamp) >= NOW() - INTERVAL '7' DAY
GROUP BY 1, 2, 3
ORDER BY hour DESC, avg_aqi DESC;
""".strip()

QUERY_TOP_POLLUTED = """
-- Top 5 most polluted cities by average AQI (last 30 days)
SELECT
    city_id,
    city_name,
    country,
    ROUND(AVG(aqi_score), 1) AS avg_aqi,
    ROUND(AVG(pm2_5), 2)     AS avg_pm25,
    MAX(aqi_score)           AS peak_aqi,
    COUNT(*)                 AS total_readings
FROM {database}.aqi_readings
WHERE from_iso8601_timestamp(timestamp) >= NOW() - INTERVAL '30' DAY
GROUP BY 1, 2, 3
ORDER BY avg_aqi DESC
LIMIT 5;
""".strip()

QUERY_DAILY_ALERTS = """
-- Daily alert count per city (readings where AQI exceeded threshold)
SELECT
    city_id,
    city_name,
    CAST(from_iso8601_timestamp(timestamp) AS DATE) AS date,
    COUNT(*)                                        AS alert_count,
    ROUND(AVG(aqi_score), 1)                        AS avg_aqi_on_alert_days
FROM {database}.aqi_readings
WHERE alert = true
GROUP BY 1, 2, 3
ORDER BY date DESC, alert_count DESC;
""".strip()


def _get_or_create_crawler_role(iam: boto3.client) -> str:
    """Create IAM role that allows Glue crawler to read S3.

    Returns the role ARN.
    """
    role_name = "angaza-glue-crawler-role"

    trust_policy = json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect":    "Allow",
            "Principal": {"Service": "glue.amazonaws.com"},
            "Action":    "sts:AssumeRole",
        }]
    })

    inline_policy = json.dumps({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:ListBucket",
                ],
                "Resource": [
                    f"arn:aws:s3:::{config.S3_BUCKET}",
                    f"arn:aws:s3:::{config.S3_BUCKET}/*",
                ],
            },
            {
                "Effect": "Allow",
                "Action": [
                    "glue:*",
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                "Resource": "*",
            },
        ]
    })

    try:
        resp     = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=trust_policy,
            Description="Angaza Glue crawler — S3 read access",
        )
        role_arn = resp["Role"]["Arn"]
        log.info("[OK]    Crawler IAM role created: %s", role_name)
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "EntityAlreadyExists":
            role_arn = iam.get_role(RoleName=role_name)["Role"]["Arn"]
            log.info("[SKIP]  Crawler IAM role exists: %s", role_name)
        else:
            raise

    iam.put_role_policy(
        RoleName=role_name,
        PolicyName="angaza-glue-policy",
        PolicyDocument=inline_policy,
    )
    log.info("[OK]    Crawler policy attached")

    # Wait for IAM to propagate
    log.info("        Waiting 10s for IAM propagation ...")
    time.sleep(10)
    return role_arn


def create_database(glue: boto3.client) -> None:
    """Create the Glue database if it does not exist."""
    name = config.GLUE_DATABASE
    try:
        glue.create_database(
            DatabaseInput={
                "Name":        name,
                "Description": "Angaza AQI analytics — managed by glue_crawler.py",
            }
        )
        log.info("[OK]    Glue database created: %s", name)
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "AlreadyExistsException":
            log.info("[SKIP]  Glue database exists: %s", name)
        else:
            raise


def create_crawler(glue: boto3.client, role_arn: str) -> None:
    """Create the Glue crawler pointed at the analytical/ prefix."""
    name   = config.GLUE_CRAWLER_NAME
    target = f"s3://{config.S3_BUCKET}/{config.S3_PREFIX_ANALYTICAL}"

    try:
        glue.create_crawler(
            Name=name,
            Role=role_arn,
            DatabaseName=config.GLUE_DATABASE,
            Description="Crawls Angaza analytical S3 prefix every 15 minutes",
            Targets={
                "S3Targets": [
                    {
                        "Path":     target,
                        "Exclusions": ["**.keep"],   # skip our marker files
                    }
                ]
            },
            Schedule="cron(0/15 * * * ? *)",     # every 15 minutes
            SchemaChangePolicy={
                "UpdateBehavior": "UPDATE_IN_DATABASE",
                "DeleteBehavior": "LOG",           # log deletions, never auto-drop
            },
            RecrawlPolicy={"RecrawlBehavior": "CRAWL_NEW_FOLDERS_ONLY"},
            Tags={"project": "angaza"},
        )
        log.info("[OK]    Crawler created: %s", name)
        log.info("        Target: %s", target)
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "AlreadyExistsException":
            log.info("[SKIP]  Crawler already exists: %s", name)
        else:
            raise


def print_athena_queries() -> None:
    """Print the three sample Athena queries to stdout."""
    db = config.GLUE_DATABASE
    queries = {
        "Hourly AQI trend":       QUERY_HOURLY_TREND.format(database=db),
        "Top 5 polluted cities":  QUERY_TOP_POLLUTED.format(database=db),
        "Daily alert count":      QUERY_DAILY_ALERTS.format(database=db),
    }
    log.info("─" * 60)
    log.info("Sample Athena queries")
    for title, sql in queries.items():
        log.info("")
        log.info("── %s", title)
        for line in sql.splitlines():
            log.info("   %s", line)
    log.info("─" * 60)


def setup_catalog() -> None:
    """Orchestrate Glue database, crawler, and print Athena queries."""
    glue = boto3.client("glue", region_name=config.AWS_REGION)
    iam  = boto3.client("iam",  region_name=config.AWS_REGION)

    log.info("─" * 60)
    log.info("Setting up Angaza data catalog")
    log.info("  Database : %s", config.GLUE_DATABASE)
    log.info("  Crawler  : %s", config.GLUE_CRAWLER_NAME)
    log.info("  Schedule : every 15 minutes")
    log.info("─" * 60)

    role_arn = _get_or_create_crawler_role(iam)
    create_database(glue)
    create_crawler(glue, role_arn)
    print_athena_queries()

    log.info("Catalog setup complete")


if __name__ == "__main__":
    setup_catalog()
