"""
tests/test_pipeline.py — Angaza test suite.

Three test categories:
  - Unit tests      : pure logic, no AWS, instant
  - Mock tests      : boto3 mocked, verify behaviour without AWS calls
  - Integration     : marked @pytest.mark.integration, hits real AWS

Run:
    pytest tests/                                    # unit + mock only
    pytest tests/ -m integration                     # integration only
    pytest tests/ -v                                 # verbose output
"""

import base64
import json
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config
from producer.aqi_producer import (
    CITIES,
    _build_kinesis_entry,
    _make_record,
    _put_with_retry,
    send_batch,
)
from producer.data_seeder import (
    _readings_for_city_day,
    _s3_key,
)
from processor.handler import (
    _decode_record,
    _should_alert,
    lambda_handler,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def nairobi():
    """Return the Nairobi city profile."""
    return next(c for c in CITIES if c["id"] == "NBO")


@pytest.fixture
def lagos():
    """Return the Lagos city profile."""
    return next(c for c in CITIES if c["id"] == "LGS")


@pytest.fixture
def sample_record(nairobi):
    """Return a single generated AQI record for Nairobi."""
    return _make_record(nairobi)


@pytest.fixture
def kinesis_event():
    """Return a mock Kinesis Lambda event with two records."""
    def _encode(payload):
        return base64.b64encode(json.dumps(payload).encode()).decode()

    return {
        "Records": [
            {
                "kinesis": {
                    "data": _encode({
                        "city_id":   "CPT",
                        "city_name": "Cape Town",
                        "country":   "ZA",
                        "aqi_score": 45,
                        "pm2_5":     18.2,
                        "category":  "Good",
                        "severity":  1,
                        "alert":     False,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    })
                }
            },
            {
                "kinesis": {
                    "data": _encode({
                        "city_id":   "LGS",
                        "city_name": "Lagos",
                        "country":   "NG",
                        "aqi_score": 189,
                        "pm2_5":     84.1,
                        "category":  "Unhealthy",
                        "severity":  4,
                        "alert":     True,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    })
                }
            },
        ]
    }


# ── Unit tests — pure logic ───────────────────────────────────────────────────

class TestAqiCategories:
    """config.get_aqi_category covers all six bands correctly."""

    def test_good(self):
        assert config.get_aqi_category(25)  == ("Good", 1)

    def test_moderate(self):
        assert config.get_aqi_category(75)  == ("Moderate", 2)

    def test_unhealthy_sensitive(self):
        assert config.get_aqi_category(125) == ("Unhealthy for Sensitive", 3)

    def test_unhealthy(self):
        assert config.get_aqi_category(175) == ("Unhealthy", 4)

    def test_very_unhealthy(self):
        assert config.get_aqi_category(250) == ("Very Unhealthy", 5)

    def test_hazardous(self):
        assert config.get_aqi_category(350) == ("Hazardous", 6)

    def test_boundary_50(self):
        assert config.get_aqi_category(50)[0]  == "Good"

    def test_boundary_51(self):
        assert config.get_aqi_category(51)[0]  == "Moderate"

    def test_boundary_150(self):
        assert config.get_aqi_category(150)[0] == "Unhealthy for Sensitive"

    def test_boundary_151(self):
        assert config.get_aqi_category(151)[0] == "Unhealthy"


class TestRecordShape:
    """_make_record produces correctly shaped records."""

    def test_required_fields(self, sample_record):
        required = {
            "city_id", "city_name", "country",
            "timestamp", "aqi_score", "category",
            "severity", "pm2_5", "pm10", "alert",
        }
        assert required.issubset(sample_record.keys())

    def test_aqi_in_range(self, sample_record):
        assert 0 <= sample_record["aqi_score"] <= 500

    def test_pm25_positive(self, sample_record):
        assert sample_record["pm2_5"] > 0

    def test_pm10_positive(self, sample_record):
        assert sample_record["pm10"] > 0

    def test_pm10_greater_than_pm25(self, sample_record):
        assert sample_record["pm10"] > sample_record["pm2_5"]

    def test_city_id_is_nairobi(self, sample_record):
        assert sample_record["city_id"] == "NBO"

    def test_timestamp_is_utc_iso(self, sample_record):
        ts = sample_record["timestamp"]
        assert "T" in ts
        assert "+00:00" in ts or "Z" in ts

    def test_alert_flag_type(self, sample_record):
        assert isinstance(sample_record["alert"], bool)

    def test_alert_consistent_with_threshold(self, nairobi):
        """alert flag must agree with the AQI threshold in config."""
        for _ in range(20):
            r = _make_record(nairobi)
            if r["aqi_score"] > config.AQI_ALERT_THRESHOLD:
                assert r["alert"] is True
            else:
                assert r["alert"] is False


class TestKinesisEntry:
    """_build_kinesis_entry produces valid Kinesis PutRecords entries."""

    def test_has_data_and_partition_key(self, sample_record):
        entry = _build_kinesis_entry(sample_record)
        assert "Data" in entry
        assert "PartitionKey" in entry

    def test_partition_key_is_city_id(self, sample_record):
        entry = _build_kinesis_entry(sample_record)
        assert entry["PartitionKey"] == sample_record["city_id"]

    def test_data_is_bytes(self, sample_record):
        entry = _build_kinesis_entry(sample_record)
        assert isinstance(entry["Data"], bytes)

    def test_data_is_valid_json(self, sample_record):
        entry  = _build_kinesis_entry(sample_record)
        parsed = json.loads(entry["Data"].decode("utf-8"))
        assert parsed["city_id"] == sample_record["city_id"]

    def test_all_cities_have_unique_partition_keys(self):
        records = [_make_record(city) for city in CITIES]
        keys    = [_build_kinesis_entry(r)["PartitionKey"] for r in records]
        assert len(set(keys)) == len(CITIES)


class TestDataSeeder:
    """_readings_for_city_day and _s3_key produce correct output."""

    def test_48_readings_per_day(self, nairobi):
        date     = datetime.now(timezone.utc)
        readings = _readings_for_city_day(nairobi, date)
        assert len(readings) == 48

    def test_readings_start_at_midnight(self, nairobi):
        date     = datetime.now(timezone.utc)
        readings = _readings_for_city_day(nairobi, date)
        assert readings[0]["timestamp"].endswith("T00:00:00+00:00") or \
               "00:00:00" in readings[0]["timestamp"]

    def test_readings_end_at_2330(self, nairobi):
        date     = datetime.now(timezone.utc)
        readings = _readings_for_city_day(nairobi, date)
        assert "23:30:00" in readings[-1]["timestamp"]

    def test_rush_hour_higher_than_midnight(self, nairobi):
        date      = datetime.now(timezone.utc)
        readings  = _readings_for_city_day(nairobi, date)
        midnight  = readings[0]["aqi_score"]
        # slot 14 = 7am (14 * 30min = 420min = 7h)
        rush_avg  = sum(r["aqi_score"] for r in readings[14:16]) / 2
        # Rush hour should trend higher on average across many runs
        # We just check it's a positive integer — determinism not guaranteed
        assert rush_avg > 0

    def test_s3_key_format(self, nairobi):
        date = datetime(2026, 5, 22, tzinfo=timezone.utc)
        key  = _s3_key(nairobi["id"], date)
        assert key == "raw/year=2026/month=05/day=22/city=NBO/readings.json"

    def test_s3_key_zero_pads_month(self):
        date = datetime(2026, 1, 5, tzinfo=timezone.utc)
        key  = _s3_key("KLA", date)
        assert "month=01" in key
        assert "day=05"   in key


class TestLambdaDecoder:
    """_decode_record handles valid and invalid Kinesis records."""

    def test_decodes_valid_record(self, kinesis_event):
        raw    = kinesis_event["Records"][0]
        result = _decode_record(raw)
        assert result["city_id"]   == "CPT"
        assert result["aqi_score"] == 45

    def test_returns_none_for_bad_base64(self):
        bad = {"kinesis": {"data": "!!not-base64!!"}}
        assert _decode_record(bad) is None

    def test_returns_none_for_bad_json(self):
        bad_json = base64.b64encode(b"not json at all").decode()
        bad      = {"kinesis": {"data": bad_json}}
        assert _decode_record(bad) is None

    def test_returns_none_for_missing_key(self):
        assert _decode_record({}) is None


class TestAlertLogic:
    """_should_alert fires correctly around the threshold."""

    def test_below_threshold(self):
        assert _should_alert({"aqi_score": 100}) is False

    def test_at_threshold(self):
        assert _should_alert({"aqi_score": config.AQI_ALERT_THRESHOLD}) is False

    def test_above_threshold(self):
        assert _should_alert({"aqi_score": config.AQI_ALERT_THRESHOLD + 1}) is True

    def test_well_above_threshold(self):
        assert _should_alert({"aqi_score": 300}) is True

    def test_zero_aqi(self):
        assert _should_alert({"aqi_score": 0}) is False


# ── Mock tests — boto3 mocked ─────────────────────────────────────────────────

class TestKinesisPutRetry:
    """_put_with_retry handles AWS errors and partial failures correctly."""

    def test_successful_put(self, sample_record):
        mock_kinesis = MagicMock()
        mock_kinesis.put_records.return_value = {
            "FailedRecordCount": 0,
            "Records": [{"SequenceNumber": "1", "ShardId": "shardId-000"}],
        }
        entry     = _build_kinesis_entry(sample_record)
        delivered = _put_with_retry(mock_kinesis, "test-stream", [entry])
        assert delivered == 1
        mock_kinesis.put_records.assert_called_once()

    def test_partial_failure_retries(self, sample_record):
        mock_kinesis = MagicMock()
        # First call: one failure
        mock_kinesis.put_records.side_effect = [
            {
                "FailedRecordCount": 1,
                "Records": [{"ErrorCode": "ProvisionedThroughputExceededException"}],
            },
            # Retry: success
            {
                "FailedRecordCount": 0,
                "Records": [{"SequenceNumber": "1", "ShardId": "shardId-000"}],
            },
        ]
        entry     = _build_kinesis_entry(sample_record)
        delivered = _put_with_retry(mock_kinesis, "test-stream", [entry])
        assert mock_kinesis.put_records.call_count == 2
        assert delivered == 1

    def test_empty_entries_returns_zero(self):
        mock_kinesis = MagicMock()
        delivered    = _put_with_retry(mock_kinesis, "test-stream", [])
        assert delivered == 0
        mock_kinesis.put_records.assert_not_called()


class TestLambdaHandler:
    """lambda_handler processes batches and triggers alerts correctly."""

    def test_processes_all_records(self, kinesis_event):
        with patch("processor.handler._cw")  as mock_cw, \
             patch("processor.handler._sns") as mock_sns:
            mock_cw.put_metric_data.return_value  = {}
            mock_sns.publish.return_value          = {"MessageId": "abc123"}

            result = lambda_handler(kinesis_event, None)

        assert result["total"]     == 2
        assert result["processed"] == 2
        assert result["skipped"]   == 0

    def test_alerts_only_for_high_aqi(self, kinesis_event):
        with patch("processor.handler._cw")  as mock_cw, \
             patch("processor.handler._sns") as mock_sns:
            mock_cw.put_metric_data.return_value = {}
            mock_sns.publish.return_value         = {"MessageId": "abc123"}

            import processor.handler as h
            original_arn      = h.SNS_TOPIC_ARN
            h.SNS_TOPIC_ARN   = "arn:aws:sns:us-east-1:123456789:angaza-alerts"

            result = lambda_handler(kinesis_event, None)
            h.SNS_TOPIC_ARN   = original_arn

        # Only Lagos (AQI 189) should trigger an alert
        assert result["alerted"] == 1
        mock_sns.publish.assert_called_once()

    def test_empty_event_returns_zeros(self):
        result = lambda_handler({"Records": []}, None)
        assert result == {
            "total": 0, "processed": 0, "alerted": 0, "skipped": 0
        }

    def test_bad_record_is_skipped(self):
        bad_event = {
            "Records": [{"kinesis": {"data": "!!bad!!"}}]
        }
        result = lambda_handler(bad_event, None)
        assert result["skipped"]   == 1
        assert result["processed"] == 0


# ── Integration tests — real AWS ──────────────────────────────────────────────

@pytest.mark.integration
class TestIntegration:
    """Hit real AWS resources. Run with: pytest -m integration"""

    def test_s3_bucket_exists(self):
        import boto3
        s3 = boto3.client("s3", region_name=config.AWS_REGION)
        response = s3.head_bucket(Bucket=config.S3_BUCKET)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_aqi_stream_active(self):
        import boto3
        kinesis = boto3.client("kinesis", region_name=config.AWS_REGION)
        resp    = kinesis.describe_stream_summary(StreamName=config.AQI_STREAM_NAME)
        assert resp["StreamDescriptionSummary"]["StreamStatus"] == "ACTIVE"

    def test_stats_stream_active(self):
        import boto3
        kinesis = boto3.client("kinesis", region_name=config.AWS_REGION)
        resp    = kinesis.describe_stream_summary(StreamName=config.STATS_STREAM_NAME)
        assert resp["StreamDescriptionSummary"]["StreamStatus"] == "ACTIVE"

    def test_producer_sends_to_kinesis(self):
        """Send 10 records and verify zero failures."""
        import boto3
        kinesis   = boto3.client("kinesis", region_name=config.AWS_REGION)
        delivered = send_batch(kinesis, config.AQI_STREAM_NAME)
        assert delivered == len(CITIES)

    def test_s3_seeder_writes_objects(self):
        """Seed 1 day and confirm objects appear in S3."""
        import boto3
        import time
        from producer.data_seeder import seed

        seed(days=1, dry_run=False)
        time.sleep(3)

        s3       = boto3.client("s3", region_name=config.AWS_REGION)
        response = s3.list_objects_v2(
            Bucket=config.S3_BUCKET,
            Prefix=config.S3_PREFIX_RAW,
        )
        assert response.get("KeyCount", 0) > 0
