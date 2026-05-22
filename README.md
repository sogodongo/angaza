# Angaza

> *Angaza* - Swahili for "to illuminate, to shed light"

[![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat&logo=python&logoColor=white)](https://python.org)
[![AWS](https://img.shields.io/badge/AWS-Kinesis%20%7C%20Lambda%20%7C%20Glue-FF9900?style=flat&logo=amazonaws&logoColor=white)](https://aws.amazon.com)
[![Tests](https://img.shields.io/badge/Tests-46%20passing-2ECC71?style=flat&logo=pytest&logoColor=white)](tests/)
[![License](https://img.shields.io/badge/License-MIT-blue?style=flat)](LICENSE)

Air quality across African cities is poorly monitored, unevenly reported, and almost never available in real time. Angaza changes that.

It is a production-grade streaming and batch analytics platform that ingests AQI sensor data from 10 African cities, processes it through a fully managed AWS pipeline, and fires targeted alerts within seconds of a hazardous threshold breach - all from a single `python3 infra/deploy.py`.

Built in Nairobi. Engineered against the infrastructure constraints most tutorials never mention.

---

## How it works

Data moves in two paths simultaneously.

The **real-time path** runs continuously: a Python producer puts AQI readings onto a Kinesis stream every second, Kinesis Data Analytics applies 5-minute tumbling window aggregations via Apache Flink, and Lambda picks up the enriched records - publishing CloudWatch metrics per city and triggering SNS alerts the moment any city crosses AQI 150.

The **batch path** runs in parallel: Kinesis Firehose buffers the same raw records and delivers them to S3 every 60 seconds, partitioned by date. A Glue crawler runs every 15 minutes, keeping the Athena catalog current so analysts can query the full historical dataset without touching the live stream.

![Angaza platform architecture](./architecture.png)

---

## Stack

| Layer | Technology | Why |
|---|---|---|
| Ingestion | Kinesis Data Streams - 2 shards | Managed, native Firehose + Lambda integration |
| Batch delivery | Kinesis Firehose → S3 | Zero-ops delivery, dynamic date partitioning |
| Stream analytics | Kinesis Data Analytics (Flink) | Windowed aggregations without managing clusters |
| Serverless compute | AWS Lambda - Python 3.11 | Per-city metric publishing and alert fanout |
| Storage | S3 - raw / analytical / checkpoints | Medallion layout, versioning, lifecycle tiering |
| Catalog | AWS Glue + Athena | Schema-on-read, no ETL pipeline to maintain |
| Alerting | SNS - email + SQS | Decoupled, extensible subscriber model |
| Observability | CloudWatch - metrics, dashboards, alarms | Per-city AQI trend, PM2.5 levels, Lambda errors |
| Visualisation | Grafana - CloudWatch data source | Live dashboards without leaving the AWS ecosystem |
| IaC | Python + boto3 | Idempotent deploy and teardown in a single command |
| Testing | pytest - 46 tests | Unit, mock, and integration tiers |

---

## Project structure

```
angaza/
├── config.py                      # Every setting via environment variable
├── producer/
│   ├── aqi_producer.py            # Live producer - 10 cities, 1 reading/second
│   └── data_seeder.py             # Historical backfill - 30 days, 48 readings/day
├── streams/
│   ├── aqi_stream.py              # Kinesis AQI stream - 2 shards
│   └── stats_stream.py            # Kinesis Stats stream - aggregated output
├── firehose/
│   └── aqi_firehose.py            # Firehose → S3 raw/, buffered 60s / 64MB
├── processor/
│   └── handler.py                 # Lambda - CloudWatch metrics + SNS alerts
├── alerting/
│   └── sns_alerts.py              # SNS topic, email subscription, resource policy
├── monitoring/
│   └── cloudwatch_dashboard.py    # 4-widget dashboard + per-city alarms
├── catalog/
│   └── glue_crawler.py            # Glue database, crawler, sample Athena queries
├── infra/
│   ├── deploy.py                  # Full platform deploy - idempotent, coloured output
│   └── teardown.py                # Full platform teardown - prompts for confirmation
└── tests/
    └── test_pipeline.py           # 46 tests - unit, mock, and integration
```

---

## Quick start

**Requirements:** Python 3.11+, AWS credentials, boto3

```bash
git clone https://github.com/sogodongo/angaza.git
cd angaza
pip install -r requirements.txt
```

Set your environment:

```bash
export AWS_REGION=us-east-1
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export AQI_ALERT_EMAIL=you@example.com
```

Deploy everything in one command:

```bash
python3 infra/deploy.py
```

```
────────────────────────────────────────────────────────────
  Angaza - AQI Intelligence Platform
────────────────────────────────────────────────────────────
  [OK]    Bucket created: angaza-data-lake-123456789
  [OK]    Versioning enabled
  [OK]    Lifecycle policy applied (raw/ → IA@30d, Glacier@90d)
  [OK]    Stream is ACTIVE: angaza-aqi-stream
  [OK]    Stream is ACTIVE: angaza-stats-stream
  [OK]    Firehose is ACTIVE: angaza-batch-firehose
  [OK]    Topic created: angaza-alerts
  [OK]    Glue database created: angaza_analytics_db
  [OK]    Dashboard created: Angaza-AQI-Dashboard

  Angaza deployed successfully in 47s
```

Seed 30 days of historical data, then start the live producer:

```bash
python3 producer/data_seeder.py   # ~14,400 records across 10 cities
python3 producer/aqi_producer.py  # runs until Ctrl+C
```

Run the test suite - no AWS credentials required for unit and mock tests:

```bash
pytest tests/ -m "not integration" -v   # 46 tests, ~5 seconds
pytest tests/ -m integration -v         # requires deployed resources
```

Tear down cleanly when you are done:

```bash
python3 infra/teardown.py   # prompts: type 'angaza' to confirm
```

---

## Cities monitored

Ten cities across East, West, and Southern Africa - chosen to represent the range of urban air quality conditions on the continent.

| City | Country | Baseline AQI | Peak risk period |
|---|---|---|---|
| Nairobi | Kenya | 65 - 125 | Morning and evening rush hours |
| Mombasa | Kenya | 30 - 80 | Dry season, dhow traffic |
| Dar es Salaam | Tanzania | 55 - 125 | Industrial corridor, dry season |
| Kampala | Uganda | 65 - 155 | Year-round traffic congestion |
| Addis Ababa | Ethiopia | 45 - 145 | Construction season |
| Lagos | Nigeria | 80 - 200 | Harmattan winds, November - March |
| Accra | Ghana | 50 - 110 | Harmattan winds |
| Cape Town | South Africa | 25 - 65 | Berg wind events |
| Johannesburg | South Africa | 55 - 145 | Winter inversion, May - August |
| Khartoum | Sudan | 105 - 215 | Haboob dust storms |

---

## AQI reference

| Range | Category | Health guidance |
|---|---|---|
| 0 - 50 | Good | No precautions needed |
| 51 - 100 | Moderate | Unusually sensitive individuals should limit prolonged outdoor exertion |
| 101 - 150 | Unhealthy for Sensitive Groups | Sensitive groups should reduce prolonged outdoor exertion |
| 151 - 200 | Unhealthy | Everyone should limit prolonged outdoor exertion |
| 201 - 300 | Very Unhealthy | Everyone should avoid prolonged outdoor exertion |
| 301 - 500 | Hazardous | Everyone should avoid all outdoor exertion |

Angaza fires SNS alerts when any city crosses **AQI 150** - the threshold at which the general population begins to experience health effects. This is configurable via `AQI_ALERT_THRESHOLD`.

---

## Sample Athena queries

Run these in the Athena console after the Glue crawler has completed its first run.

**Hourly AQI trend - last 7 days:**

```sql
SELECT
    city_id,
    city_name,
    DATE_TRUNC('hour', from_iso8601_timestamp(timestamp)) AS hour,
    ROUND(AVG(aqi_score), 1)                              AS avg_aqi,
    MAX(aqi_score)                                        AS peak_aqi
FROM angaza_analytics_db.aqi_readings
WHERE from_iso8601_timestamp(timestamp) >= NOW() - INTERVAL '7' DAY
GROUP BY 1, 2, 3
ORDER BY hour DESC, avg_aqi DESC;
```

**Top 5 most polluted cities - last 30 days:**

```sql
SELECT
    city_name,
    country,
    ROUND(AVG(aqi_score), 1) AS avg_aqi,
    MAX(aqi_score)           AS peak_aqi,
    COUNT(*)                 AS total_readings
FROM angaza_analytics_db.aqi_readings
WHERE from_iso8601_timestamp(timestamp) >= NOW() - INTERVAL '30' DAY
GROUP BY 1, 2
ORDER BY avg_aqi DESC
LIMIT 5;
```

**Daily alert count per city:**

```sql
SELECT
    city_name,
    CAST(from_iso8601_timestamp(timestamp) AS DATE) AS date,
    COUNT(*)                                        AS alerts,
    ROUND(AVG(aqi_score), 1)                        AS avg_aqi_when_alert
FROM angaza_analytics_db.aqi_readings
WHERE alert = true
GROUP BY 1, 2
ORDER BY date DESC, alerts DESC;
```

---

## Configuration

All settings are environment variables. The platform runs with sensible defaults - only `AWS_ACCOUNT_ID` and `AQI_ALERT_EMAIL` need to be set explicitly before deploying.

| Variable | Default | Description |
|---|---|---|
| `AWS_REGION` | `us-east-1` | Deployment region |
| `AWS_ACCOUNT_ID` | - | Required - used for IAM ARNs and bucket naming |
| `AQI_STREAM_NAME` | `angaza-aqi-stream` | Kinesis stream for raw sensor data |
| `STATS_STREAM_NAME` | `angaza-stats-stream` | Kinesis stream for aggregated output |
| `AQI_S3_BUCKET` | `angaza-data-lake-{account_id}` | S3 data lake bucket |
| `AQI_ALERT_EMAIL` | - | Email address for SNS alert subscriptions |
| `AQI_ALERT_THRESHOLD` | `150` | AQI score that triggers alerts |
| `PRODUCER_INTERVAL_SEC` | `1.0` | Seconds between producer batches |
| `FIREHOSE_BUFFER_SECONDS` | `60` | Firehose flush interval |
| `FIREHOSE_BUFFER_MB` | `64` | Firehose flush size |

---

## Engineering decisions

**Kinesis over Kafka** - Fully managed, no cluster to operate, and native integrations with Firehose and Lambda mean the entire pipeline from stream to S3 to Lambda requires zero infrastructure management. For this scale - 10 cities, one reading per second - Kinesis is the right tool. Kafka would add operational overhead without adding capability.

**Newline-delimited JSON in raw/, not Parquet** - Firehose writes records as they arrive. Converting to Parquet at ingestion time would couple the write path to a schema and make the raw tier brittle to upstream changes. Parquet conversion happens at the analytical layer via Glue, keeping raw/ as a faithful, schema-free audit log.

**Idempotent deploy** - Every setup function checks for existing resources before attempting to create them. Running `deploy.py` twice produces `[SKIP]` for anything already in place and `[OK]` for anything that needed creating. This means the deploy script is safe to run in CI, after a partial failure, or after a teardown and rebuild.

**Partial-failure retry on Kinesis** - `put_records` returns partial failures silently via `FailedRecordCount`. A naive implementation would retry the entire batch - wasteful and potentially duplicating successful records. Angaza tracks which individual records failed and retries only those, with exponential backoff capped at five attempts.

**Least-privilege IAM** - Every IAM role grants only the permissions its service needs. The Firehose role can write to S3 and read from Kinesis - nothing else. The Lambda role can read from Kinesis, publish CloudWatch metrics, and publish to SNS - nothing else. No wildcard actions, no `*` resources except where AWS requires it for CloudWatch Logs.

---

## Author

**Sam Odongo** - Senior Data & AI Engineer based in Nairobi, Kenya, with seven years of experience building production data systems across East Africa. Currently a Data Engineer at Turing and an active freelance consultant on Upwork serving international clients.

Angaza is part of a broader portfolio of systems engineered in Kenya and tested against the infrastructure constraints most tutorials never mention.

[samodongo.com](https://samodongo.com) · [GitHub](https://github.com/sogodongo) · [LinkedIn](https://linkedin.com/in/sam-odongo-ba3b11122)
