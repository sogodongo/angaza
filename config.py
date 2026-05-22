"""
config.py — central configuration for the AQI platform.

All values read from environment variables with sane defaults.
Never import boto3 here — this file must be importable without AWS credentials.
"""

import os
import boto3

# ── AWS ──────────────────────────────────────────────────────────────────────
AWS_REGION: str = os.getenv("AWS_REGION", "us-east-1")
AWS_ACCOUNT_ID: str = os.getenv("AWS_ACCOUNT_ID", "")  # required at deploy time

# ── S3 ───────────────────────────────────────────────────────────────────────
S3_BUCKET: str = os.getenv(
    "AQI_S3_BUCKET",
    f"angaza-data-lake-{AWS_ACCOUNT_ID}" if AWS_ACCOUNT_ID else "angaza-data-lake-local",
)
S3_PREFIX_RAW:        str = "raw/"
S3_PREFIX_ANALYTICAL: str = "analytical/"
S3_PREFIX_CHECKPOINTS: str = "checkpoints/"

# ── Kinesis streams ───────────────────────────────────────────────────────────
AQI_STREAM_NAME:   str = os.getenv("AQI_STREAM_NAME",   "angaza-aqi-stream")
STATS_STREAM_NAME: str = os.getenv("STATS_STREAM_NAME", "angaza-stats-stream")
AQI_STREAM_SHARDS:   int = int(os.getenv("AQI_STREAM_SHARDS",   "2"))
STATS_STREAM_SHARDS: int = int(os.getenv("STATS_STREAM_SHARDS", "1"))
STREAM_RETENTION_HOURS: int = int(os.getenv("STREAM_RETENTION_HOURS", "24"))

# ── Firehose ──────────────────────────────────────────────────────────────────
FIREHOSE_NAME:          str = os.getenv("AQI_FIREHOSE_NAME", "angaza-batch-firehose")
FIREHOSE_BUFFER_SECONDS: int = int(os.getenv("FIREHOSE_BUFFER_SECONDS", "60"))
FIREHOSE_BUFFER_MB:      int = int(os.getenv("FIREHOSE_BUFFER_MB",      "64"))

# ── Lambda ────────────────────────────────────────────────────────────────────
LAMBDA_FUNCTION_NAME: str = os.getenv("AQI_LAMBDA_NAME",    "angaza-stats-processor")
LAMBDA_BATCH_SIZE:    int = int(os.getenv("LAMBDA_BATCH_SIZE", "100"))
LAMBDA_DLQ_NAME:      str = os.getenv("AQI_DLQ_NAME",       "angaza-lambda-dlq")

# ── SNS ───────────────────────────────────────────────────────────────────────
SNS_TOPIC_NAME:  str = os.getenv("AQI_SNS_TOPIC",   "angaza-alerts")
ALERT_EMAIL:     str = os.getenv("AQI_ALERT_EMAIL", "")   # set before deploying

# ── Glue / Athena ─────────────────────────────────────────────────────────────
GLUE_DATABASE:      str = os.getenv("AQI_GLUE_DB",      "angaza_analytics_db")
GLUE_CRAWLER_NAME:  str = os.getenv("AQI_CRAWLER_NAME", "angaza-analytical-crawler")
ATHENA_OUTPUT_PREFIX: str = os.getenv("AQI_ATHENA_OUTPUT", "s3://aws-athena-query-results/aqi/")

# ── CloudWatch ────────────────────────────────────────────────────────────────
CW_NAMESPACE: str = os.getenv("AQI_CW_NAMESPACE", "AngazaMonitor")

# ── Producer behaviour ────────────────────────────────────────────────────────
PRODUCER_BATCH_SIZE:     int   = int(os.getenv("PRODUCER_BATCH_SIZE",     "10"))
PRODUCER_INTERVAL_SEC:   float = float(os.getenv("PRODUCER_INTERVAL_SEC", "1.0"))
AQI_ALERT_THRESHOLD:     int   = int(os.getenv("AQI_ALERT_THRESHOLD",     "150"))

# ── Retry / backoff ───────────────────────────────────────────────────────────
MAX_RETRIES:     int   = int(os.getenv("MAX_RETRIES",     "5"))
BACKOFF_BASE_SEC: float = float(os.getenv("BACKOFF_BASE_SEC", "0.5"))

# ── AQI categories ────────────────────────────────────────────────────────────
# Maps (lower_bound, upper_bound) → (category, severity_int)
AQI_CATEGORIES: list[tuple[int, int, str, int]] = [
    (0,   50,  "Good",                  1),
    (51,  100, "Moderate",              2),
    (101, 150, "Unhealthy for Sensitive", 3),
    (151, 200, "Unhealthy",             4),
    (201, 300, "Very Unhealthy",        5),
    (301, 500, "Hazardous",             6),
]

def get_aqi_category(score: int) -> tuple[str, int]:
    """Return (category_label, severity_int) for a given AQI score."""
    for low, high, label, severity in AQI_CATEGORIES:
        if low <= score <= high:
            return label, severity
    return "Hazardous", 6


def get_account_id() -> str:
    """Fetch AWS account ID at runtime (requires valid credentials)."""
    sts = boto3.client("sts", region_name=AWS_REGION)
    return sts.get_caller_identity()["Account"]
