"""
monitoring/cloudwatch_dashboard.py — Angaza CloudWatch dashboard and alarms.

Creates:
  - A dashboard with 4 widgets:
      1. AQI score trend per city (line chart)
      2. PM2.5 levels per city (line chart)
      3. Alert count (single value)
      4. Lambda errors (bar chart)
  - One alarm per city that fires SNS when AQI exceeds the threshold

Run:
    python3 monitoring/cloudwatch_dashboard.py
"""

import json
import logging
import sys
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config
from producer.aqi_producer import CITIES

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def _aqi_metric(city_id: str) -> dict:
    """Build a CloudWatch metric entry for a city's AQI score."""
    return {
        "Id":         f"aqi_{city_id.lower()}",
        "MetricStat": {
            "Metric": {
                "Namespace":  config.CW_NAMESPACE,
                "MetricName": "AQIScore",
                "Dimensions": [{"Name": "City", "Value": city_id}],
            },
            "Period": 300,   # 5-minute resolution
            "Stat":   "Average",
        },
        "ReturnData": True,
    }


def _pm25_metric(city_id: str) -> dict:
    """Build a CloudWatch metric entry for a city's PM2.5 level."""
    return {
        "Id":         f"pm25_{city_id.lower()}",
        "MetricStat": {
            "Metric": {
                "Namespace":  config.CW_NAMESPACE,
                "MetricName": "PM25Level",
                "Dimensions": [{"Name": "City", "Value": city_id}],
            },
            "Period": 300,
            "Stat":   "Average",
        },
        "ReturnData": True,
    }


def _build_dashboard_body() -> str:
    """Build the full dashboard JSON with 4 widgets.

    Layout:
      Row 1: AQI trend (left)  | PM2.5 levels (right)
      Row 2: Alert count (left)| Lambda errors (right)
    """
    city_ids = [c["id"] for c in CITIES]

    # Widget 1 — AQI score trend, all cities
    aqi_metrics = [
        [config.CW_NAMESPACE, "AQIScore", "City", cid]
        for cid in city_ids
    ]
    widget_aqi = {
        "type":   "metric",
        "x": 0,  "y": 0,  "width": 12, "height": 6,
        "properties": {
            "title":   "AQI Score — all cities",
            "view":    "timeSeries",
            "stacked": False,
            "metrics": aqi_metrics,
            "period":  300,
            "stat":    "Average",
            "region":  config.AWS_REGION,
            "annotations": {
                "horizontal": [{
                    "label": "Unhealthy threshold",
                    "value": config.AQI_ALERT_THRESHOLD,
                    "color": "#ff0000",
                }]
            },
        },
    }

    # Widget 2 — PM2.5 levels, all cities
    pm25_metrics = [
        [config.CW_NAMESPACE, "PM25Level", "City", cid]
        for cid in city_ids
    ]
    widget_pm25 = {
        "type":   "metric",
        "x": 12, "y": 0,  "width": 12, "height": 6,
        "properties": {
            "title":   "PM2.5 Level — all cities",
            "view":    "timeSeries",
            "stacked": False,
            "metrics": pm25_metrics,
            "period":  300,
            "stat":    "Average",
            "region":  config.AWS_REGION,
        },
    }

    # Widget 3 — Alert count (cities above threshold)
    widget_alerts = {
        "type":   "metric",
        "x": 0,  "y": 6,  "width": 12, "height": 6,
        "properties": {
            "title":   "Cities above AQI threshold",
            "view":    "timeSeries",
            "metrics": [
                [config.CW_NAMESPACE, "AlertCount", {"stat": "Sum", "period": 300}]
            ],
            "region":  config.AWS_REGION,
        },
    }

    # Widget 4 — Lambda errors
    widget_lambda = {
        "type":   "metric",
        "x": 12, "y": 6,  "width": 12, "height": 6,
        "properties": {
            "title":   "Lambda errors",
            "view":    "timeSeries",
            "metrics": [
                [
                    "AWS/Lambda",
                    "Errors",
                    "FunctionName", config.LAMBDA_FUNCTION_NAME,
                    {"stat": "Sum", "period": 300, "color": "#ff6b6b"},
                ]
            ],
            "region":  config.AWS_REGION,
        },
    }

    body = json.dumps({
        "widgets": [widget_aqi, widget_pm25, widget_alerts, widget_lambda]
    })
    return body


def create_dashboard(cw: boto3.client) -> None:
    """Create or update the Angaza CloudWatch dashboard."""
    name = "Angaza-AQI-Dashboard"
    body = _build_dashboard_body()

    cw.put_dashboard(DashboardName=name, DashboardBody=body)
    log.info("[OK]    Dashboard created: %s", name)


def create_alarms(cw: boto3.client, sns_topic_arn: str) -> None:
    """Create one CloudWatch alarm per city.

    Each alarm fires SNS when a city's average AQI exceeds
    the threshold for two consecutive 5-minute periods.
    """
    for city in CITIES:
        alarm_name = f"angaza-aqi-{city['id'].lower()}-high"

        try:
            cw.put_metric_alarm(
                AlarmName=alarm_name,
                AlarmDescription=(
                    f"AQI in {city['name']} exceeded "
                    f"{config.AQI_ALERT_THRESHOLD} — Angaza"
                ),
                Namespace=config.CW_NAMESPACE,
                MetricName="AQIScore",
                Dimensions=[{"Name": "City", "Value": city["id"]}],
                Period=300,            # 5-minute window
                EvaluationPeriods=2,   # must breach twice before firing
                Threshold=config.AQI_ALERT_THRESHOLD,
                ComparisonOperator="GreaterThanThreshold",
                Statistic="Average",
                TreatMissingData="notBreaching",
                AlarmActions=[sns_topic_arn] if sns_topic_arn else [],
                OKActions=[sns_topic_arn]    if sns_topic_arn else [],
            )
            log.info("[OK]    Alarm created: %s", alarm_name)
        except ClientError as exc:
            log.error("[ERR]   Alarm failed (%s): %s",
                      alarm_name, exc.response["Error"]["Message"])


def setup_monitoring(sns_topic_arn: str = "") -> None:
    """Create the dashboard and all city alarms.

    Args:
        sns_topic_arn: ARN of the angaza-alerts topic.
                       Pass empty string to create alarms without actions.
    """
    cw = boto3.client("cloudwatch", region_name=config.AWS_REGION)

    log.info("─" * 60)
    log.info("Setting up Angaza monitoring")
    log.info("  Namespace : %s", config.CW_NAMESPACE)
    log.info("  Cities    : %d alarms", len(CITIES))
    log.info("  SNS ARN   : %s", sns_topic_arn or "(none)")
    log.info("─" * 60)

    create_dashboard(cw)
    create_alarms(cw, sns_topic_arn)

    log.info("─" * 60)
    log.info("Monitoring ready")
    log.info("  Dashboard : https://%s.console.aws.amazon.com/cloudwatch/"
             "home#dashboards:name=Angaza-AQI-Dashboard", config.AWS_REGION)
    log.info("─" * 60)


if __name__ == "__main__":
    import os
    sns_arn = os.getenv("AQI_SNS_TOPIC_ARN", "")
    setup_monitoring(sns_topic_arn=sns_arn)
