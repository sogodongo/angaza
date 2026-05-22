# Angaza

> *Angaza* — Swahili for "to illuminate, to shed light"

Real-time air quality intelligence platform built on AWS. Streams AQI sensor data from 10 African cities, detects hazardous conditions, and delivers alerts within seconds of a threshold breach.

Built in Nairobi. Engineered against the constraints most tutorials never mention.

---

## Architecture

```
Boto3 Producer
     │
     ▼
AQI Stream (Kinesis)
     │
     ├──────────────────────► Firehose ──► S3 raw/
     │
     ▼
AQI Analysis (Flink)
     │
     ▼
Stats Stream (Kinesis)
     │
     ├──────────────────────► Lambda ──► CloudWatch metrics
     │                                        │
     │                                        ▼
     │                                  Grafana dashboard
     │
     └──────────────────────► SNS ──► Email alerts

S3 analytical/ ◄── Glue Crawler ──► Athena queries
```

**Data flow:**

1. Producer generates AQI readings for 10 cities every second, batched 10 at a time onto the AQI Kinesis stream
2. Firehose buffers raw records and delivers them to S3 every 60 seconds, partitioned by date
3. Kinesis Data Analytics (Flink) runs 5-minute tumbling window aggregations and forwards enriched records to the Stats stream
4. Lambda reads the Stats stream, publishes CloudWatch metrics per city, and fires SNS alerts when AQI exceeds 150
5. Glue crawler runs every 15 minutes on the analytical prefix, keeping the Athena catalog up to date

---

## Stack

| Layer | Technology |
|---|---|
| Ingestion | Kinesis Data Streams (2 shards) |
| Batch delivery | Kinesis Firehose → S3 |
| Stream analytics | Kinesis Data Analytics (Apache Flink) |
| Serverless compute | AWS Lambda (Python 3.11) |
| Storage | S3 data lake — raw / analytical / checkpoints |
| Catalog | AWS Glue + Athena |
| Alerting | SNS — email + SQS |
| Observability | CloudWatch metrics, dashboards, alarms |
| Visualisation | Grafana (CloudWatch data source) |
| IaC | Python + boto3 (idempotent deploy/teardown) |
| Testing | pytest — 46 unit + mock tests |

---

## Project structure

```
angaza/
├── config.py                     # All settings via env vars
├── producer/
│   ├── aqi_producer.py           # Live data producer → Kinesis
│   └── data_seeder.py            # Historical backfill → S3
├── streams/
│   ├── aqi_stream.py             # Kinesis AQI stream setup
│   └── stats_stream.py           # Kinesis Stats stream setup
├── firehose/
│   └── aqi_firehose.py           # Firehose → S3 raw/
├── processor/
│   └── handler.py                # Lambda — metrics + alerts
├── alerting/
│   └── sns_alerts.py             # SNS topic + subscriptions
├── monitoring/
│   └── cloudwatch_dashboard.py   # Dashboard + city alarms
├── catalog/
│   └── glue_crawler.py           # Glue DB + crawler + Athena queries
├── infra/
│   ├── deploy.py                 # Full platform deploy (idempotent)
│   └── teardown.py               # Full platform teardown
└── tests/
    └── test_pipeline.py          # 46 tests — unit + mock + integration
```

---

## Quick start

**Prerequisites:** Python 3.11+, AWS credentials configured, boto3

```bash
git clone https://github.com/sogodongo/angaza.git
cd angaza
pip install -r requirements.txt
```

**Configure:**

```bash
export AWS_REGION=us-east-1
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export AQI_ALERT_EMAIL=you@example.com
```

**Deploy the full platform:**

```bash
python3 infra/deploy.py
```

Output:
```
  [OK]    Bucket created: angaza-data-lake-123456789
  [OK]    Versioning enabled
  [OK]    Stream is ACTIVE: angaza-aqi-stream
  [OK]    Firehose is ACTIVE: angaza-batch-firehose
  [OK]    Topic created: angaza-alerts
  [OK]    Glue database created: angaza_analytics_db
  [OK]    Dashboard created: Angaza-AQI-Dashboard

  Angaza deployed successfully in 47s
```

**Seed 30 days of historical data:**

```bash
python3 producer/data_seeder.py
```

**Start the live producer:**

```bash
python3 producer/aqi_producer.py
```

**Run the test suite:**

```bash
# Unit + mock tests (no AWS required)
pytest tests/ -m "not integration" -v

# Integration tests (requires deployed AWS resources)
pytest tests/ -m integration -v
```

**Tear down everything:**

```bash
python3 infra/teardown.py
```

---

## Cities monitored

| City | Country | Typical AQI |
|---|---|---|
| Nairobi | Kenya | 65 – 125 |
| Mombasa | Kenya | 30 – 80 |
| Dar es Salaam | Tanzania | 55 – 125 |
| Kampala | Uganda | 65 – 155 |
| Addis Ababa | Ethiopia | 45 – 145 |
| Lagos | Nigeria | 80 – 200 |
| Accra | Ghana | 50 – 110 |
| Cape Town | South Africa | 25 – 65 |
| Johannesburg | South Africa | 55 – 145 |
| Khartoum | Sudan | 105 – 215 |

---

## AQI categories

| Range | Category | Severity |
|---|---|---|
| 0 – 50 | Good | 1 |
| 51 – 100 | Moderate | 2 |
| 101 – 150 | Unhealthy for Sensitive Groups | 3 |
| 151 – 200 | Unhealthy | 4 |
| 201 – 300 | Very Unhealthy | 5 |
| 301 – 500 | Hazardous | 6 |

Alerts fire when AQI exceeds **150** (configurable via `AQI_ALERT_THRESHOLD`).

---

## Sample Athena queries

**Hourly AQI trend — last 7 days:**

```sql
SELECT
    city_id,
    DATE_TRUNC('hour', from_iso8601_timestamp(timestamp)) AS hour,
    ROUND(AVG(aqi_score), 1) AS avg_aqi
FROM angaza_analytics_db.aqi_readings
WHERE from_iso8601_timestamp(timestamp) >= NOW() - INTERVAL '7' DAY
GROUP BY 1, 2
ORDER BY hour DESC;
```

**Top 5 most polluted cities:**

```sql
SELECT city_name, country, ROUND(AVG(aqi_score), 1) AS avg_aqi
FROM angaza_analytics_db.aqi_readings
GROUP BY 1, 2
ORDER BY avg_aqi DESC
LIMIT 5;
```

**Daily alert count:**

```sql
SELECT
    city_id,
    CAST(from_iso8601_timestamp(timestamp) AS DATE) AS date,
    COUNT(*) AS alert_count
FROM angaza_analytics_db.aqi_readings
WHERE alert = true
GROUP BY 1, 2
ORDER BY date DESC;
```

---

## Configuration reference

| Variable | Default | Description |
|---|---|---|
| `AWS_REGION` | `us-east-1` | AWS region |
| `AWS_ACCOUNT_ID` | — | Required at deploy time |
| `AQI_STREAM_NAME` | `angaza-aqi-stream` | Kinesis AQI stream |
| `STATS_STREAM_NAME` | `angaza-stats-stream` | Kinesis Stats stream |
| `AQI_S3_BUCKET` | `angaza-data-lake-{account}` | S3 data lake bucket |
| `AQI_ALERT_EMAIL` | — | Email for SNS alerts |
| `AQI_ALERT_THRESHOLD` | `150` | AQI score that triggers alerts |
| `PRODUCER_INTERVAL_SEC` | `1.0` | Seconds between producer batches |

---

## Engineering notes

**Why Kinesis over Kafka?** Fully managed, no cluster to operate, native AWS integrations with Firehose and Lambda. For this scale — 10 cities, 1 reading per second — it is the right tool.

**Why newline-delimited JSON over Parquet in raw/?** Firehose writes raw records as they arrive. Parquet conversion happens at the analytical layer via Glue, keeping the raw tier as a faithful audit log.

**Idempotent deploy:** Every setup function checks for existing resources before creating them. Running `deploy.py` twice is safe — you will see `[SKIP]` for anything already in place.

**Retry strategy:** Kinesis `put_records` returns partial failures silently. The producer tracks `FailedRecordCount` per batch and retries only the failed records with exponential backoff — not the whole batch.

**Test coverage:** 46 tests across 8 test classes. Unit tests cover AQI category boundaries, record shape validation, Kinesis entry formatting, and S3 key generation. Mock tests verify Lambda batch processing, alert logic, and retry behaviour without touching AWS.

---

## Author

**Sam Odongo** — Senior Data & AI Engineer, Nairobi

[samodongo.com](https://samodongo.com) · [GitHub](https://github.com/sogodongo) · [LinkedIn](https://linkedin.com/in/sam-odongo-ba3b11122)
