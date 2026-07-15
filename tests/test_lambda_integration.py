"""Integration tests wiring Lambda handlers together with common/secrets.py and connectors/.

Scope, deliberately: these drive the real handler -> common.secrets -> connectors call chain
in-process, with AWS services mocked via moto (Secrets Manager, S3) and Amazon's HTTP APIs
(LWA token endpoint, Sponsored Ads v3 reporting API, the signed report download URL) faked by
monkeypatching `requests`. That catches wiring bugs across module boundaries -- e.g. a field
name mismatch between what report_poller returns and what report_downloader reads -- the way
tests/test_validation_rules.py's narrow unit tests can't.

What this is NOT: a `sam local invoke`/Docker-based test of the actual built SAM artifact, and
not a real call against Amazon's sandbox. connectors/ads_connector.py's REGION_HOSTS is
hardcoded per-region (not env-var-overridable), so redirecting real Ads API calls into a local
stub inside an isolated container isn't feasible without an app code change -- see README's
"Deploying (AWS SAM)" section for the rest of that tradeoff.
"""

import gzip
import io
import json
import os

os.environ.setdefault("ADS_LWA_CLIENT_ID", "test-client-id")
os.environ.setdefault("ADS_LWA_CLIENT_SECRET", "test-client-secret")
os.environ.setdefault("RAW_BUCKET", "test-raw-bucket")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("REDSHIFT_WORKGROUP_NAME", "test-workgroup")
os.environ.setdefault("REDSHIFT_DATABASE_NAME", "test-database")
os.environ.setdefault("ALERTS_TOPIC_ARN", "arn:aws:sns:us-east-1:123456789012:ads-pipeline-alerts")

# botocore resolves (and caches) credentials on a client at construction time, before moto's
# mock_aws() ever gets a chance to intercept the call -- report_downloader.py builds its S3
# client at import time, outside any test's mock_aws() context. On a machine with real AWS
# config lying around (e.g. ~/.aws/credentials) that resolution silently succeeds and moto
# take it from there; on a clean box (CI runners) there's nothing to resolve and boto3 raises
# NoCredentialsError before moto is ever involved. These dummy values are moto's own
# documented fix -- moto never validates them, they just need to exist.
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("AWS_SECURITY_TOKEN", "testing")
os.environ.setdefault("AWS_SESSION_TOKEN", "testing")

import boto3
import pytest
import requests
from moto import mock_aws

from common import secrets as secrets_module
from connectors import base as connectors_base
from lambda_handlers import (
    prepare_map_input,
    reconciliation_check,
    report_downloader,
    report_poller,
    report_requester,
)

SECRET_NAME = "ads-pipeline/brand-1-us/refresh-token"
DOWNLOAD_URL = "https://downloads.example.com/report.gz"


class FakeResponse:
    def __init__(self, status_code=200, json_data=None, raw_bytes=None):
        self.status_code = status_code
        self._json = json_data
        self.raw = io.BytesIO(raw_bytes) if raw_bytes is not None else None

    def json(self):
        return self._json

    @property
    def ok(self):
        return self.status_code < 400

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} error")

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


@pytest.fixture(autouse=True)
def _clear_access_token_cache():
    # Module-level cache in common/secrets.py -- clear so tests don't depend on run order.
    secrets_module._access_token_cache.clear()
    yield


@pytest.fixture
def aws():
    with mock_aws():
        secretsmanager = boto3.client("secretsmanager", region_name="us-east-1")
        secretsmanager.create_secret(
            Name=SECRET_NAME, SecretString=json.dumps({"refresh_token": "Atzr|fake-refresh-token"})
        )
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=os.environ["RAW_BUCKET"])
        yield


@pytest.fixture
def fake_ads_api(monkeypatch):
    def fake_post(url, data=None, timeout=None, **kwargs):
        assert url == "https://api.amazon.com/auth/o2/token"
        return FakeResponse(json_data={"access_token": "fake-access-token", "expires_in": 3600})

    def fake_request(method, url, headers=None, timeout=None, **kwargs):
        if method == "POST" and url.endswith("/reporting/reports"):
            return FakeResponse(json_data={"reportId": "rpt-123"})
        if method == "GET" and url.endswith("/reporting/reports/rpt-123"):
            return FakeResponse(json_data={"status": "COMPLETED", "url": DOWNLOAD_URL})
        raise AssertionError(f"unexpected {method} {url}")

    def fake_get(url, stream=None, timeout=None, **kwargs):
        assert url == DOWNLOAD_URL
        rows = [
            {"date": "2026-06-01", "campaignId": "c1", "impressions": 100},
            {"date": "2026-06-02", "campaignId": "c1", "impressions": 200},
        ]
        body = "\n".join(json.dumps(r) for r in rows).encode("utf-8")
        return FakeResponse(raw_bytes=gzip.compress(body))

    monkeypatch.setattr(requests, "post", fake_post)
    monkeypatch.setattr(requests, "request", fake_request)
    monkeypatch.setattr(requests, "get", fake_get)


def test_requester_poller_downloader_roundtrip(aws, fake_ads_api):
    """Drives one Map-iteration's worth of the ads_ingestion state machine end to end:
    report_requester -> report_poller -> report_downloader, each handler's output merged
    into the next's input exactly as the ASL's ResultPath/OutputPath wiring does.
    """
    item = {
        "profile_id": "1234567890",
        "region": "NA",
        "secret_name": SECRET_NAME,
        "ad_product": "SPONSORED_PRODUCTS",
        "start_date": "2026-06-01",
        "until_date": "2026-06-02",
    }

    requested = report_requester.handler(item, None)
    assert requested["report_id"] == "rpt-123"

    polled = report_poller.handler(requested, None)
    assert polled["report_status"] == "COMPLETED"
    assert polled["download_url"] == DOWNLOAD_URL

    downloaded = report_downloader.handler(polled, None)
    assert len(downloaded["bronze_keys"]) == 2  # one key per calendar day present in the report

    s3 = boto3.client("s3", region_name="us-east-1")
    for key in downloaded["bronze_keys"]:
        assert "ad_product=SPONSORED_PRODUCTS" in key
        assert "profile_id=1234567890" in key
        obj = s3.get_object(Bucket=os.environ["RAW_BUCKET"], Key=key)
        row = json.loads(obj["Body"].read().decode("utf-8"))
        assert row["campaignId"] == "c1"


def test_download_and_stream_report_flushes_in_bounded_batches(aws, monkeypatch):
    """A report whose row count crosses FLUSH_ROW_THRESHOLD must be written as multiple parts
    for the same day, rather than the whole report being buffered in memory before any of it
    reaches S3 -- see connectors/base.py's download_and_stream_report.
    """
    monkeypatch.setattr(connectors_base, "FLUSH_ROW_THRESHOLD", 3)

    rows = [{"date": "2026-06-01", "campaignId": "c1", "impressions": i} for i in range(7)]
    body = "\n".join(json.dumps(r) for r in rows).encode("utf-8")

    def fake_get(url, stream=None, timeout=None, **kwargs):
        return FakeResponse(raw_bytes=gzip.compress(body))

    monkeypatch.setattr(requests, "get", fake_get)

    def key_for_date(report_date, part):
        return f"bronze/ad_product=SPONSORED_PRODUCTS/profile_id=1234567890/report_part{part}.json"

    s3 = boto3.client("s3", region_name="us-east-1")
    keys = connectors_base.download_and_stream_report(DOWNLOAD_URL, s3, os.environ["RAW_BUCKET"], key_for_date)

    assert len(keys) == 3  # 7 rows at a flush threshold of 3 -> batches of 3, 3, then 1
    assert len(set(keys)) == 3  # each part is a distinct key, none overwrite each other

    total_rows = 0
    for key in keys:
        obj = s3.get_object(Bucket=os.environ["RAW_BUCKET"], Key=key)
        total_rows += len(obj["Body"].read().decode("utf-8").splitlines())
    assert total_rows == 7  # every row landed exactly once across the parts


def test_access_token_is_cached_across_handler_invocations(aws, fake_ads_api, monkeypatch):
    lwa_calls = []
    real_post = requests.post

    def counting_post(url, **kwargs):
        lwa_calls.append(url)
        return real_post(url, **kwargs)

    monkeypatch.setattr(requests, "post", counting_post)

    item = {
        "profile_id": "1234567890",
        "region": "NA",
        "secret_name": SECRET_NAME,
        "ad_product": "SPONSORED_PRODUCTS",
        "start_date": "2026-06-01",
        "until_date": "2026-06-02",
    }
    requested = report_requester.handler(item, None)
    report_poller.handler(requested, None)

    assert len(lwa_calls) == 1  # poller reused requester's cached access token, no 2nd LWA exchange


def test_prepare_map_input_expands_profiles_and_ad_products(tmp_path, monkeypatch):
    profiles_yaml = tmp_path / "profiles.yaml"
    profiles_yaml.write_text(
        """
profiles:
  - profile_id: "1111111111"
    region: NA
    secret_name: ads-pipeline/brand-1-us/refresh-token
    ad_products: [SPONSORED_PRODUCTS, SPONSORED_BRANDS]
  - profile_id: "2222222222"
    region: EU
    secret_name: ads-pipeline/brand-2-uk/refresh-token
    ad_products: [SPONSORED_PRODUCTS]
"""
    )
    monkeypatch.setattr(prepare_map_input, "PROFILES_CONFIG_PATH", str(profiles_yaml))

    result = prepare_map_input.handler({"time": "2026-07-01T06:00:00Z"}, None)

    assert result["until_date"] == "2026-06-30"  # anchor date minus 1 day
    assert result["start_date"] == "2026-06-01"  # 30-day rolling window ending on until_date
    assert len(result["items"]) == 3  # 2 ad products for profile 1 + 1 for profile 2

    profile_1_products = {
        item["ad_product"] for item in result["items"] if item["profile_id"] == "1111111111"
    }
    assert profile_1_products == {"SPONSORED_PRODUCTS", "SPONSORED_BRANDS"}


class FakeRedshiftData:
    """moto's redshift-data support doesn't simulate real query execution (status never reaches
    FINISHED, no result rows) -- so reconciliation_check.py's boto3 client is faked directly here,
    the same way requests.get/post are faked for the Ads API above.
    """

    def __init__(self, row: dict):
        self._row = row

    def execute_statement(self, **kwargs):
        return {"Id": "stmt-123"}

    def describe_statement(self, **kwargs):
        return {"Status": "FINISHED"}

    def get_statement_result(self, **kwargs):
        columns = list(self._row.keys())
        record = []
        for value in self._row.values():
            if isinstance(value, float):
                record.append({"doubleValue": value})
            elif isinstance(value, int):
                record.append({"longValue": value})
            else:
                record.append({"stringValue": value})
        return {
            "ColumnMetadata": [{"name": name} for name in columns],
            "Records": [record],
        }


class FakeSns:
    def __init__(self):
        self.published = []

    def publish(self, **kwargs):
        self.published.append(kwargs)
        return {"MessageId": "msg-123"}


MATCHING_RECONCILIATION_ROW = {
    "source_row_count": 100,
    "target_row_count": 100,
    "source_impressions": 5000,
    "target_impressions": 5000,
    "source_clicks": 200,
    "target_clicks": 200,
    "source_cost": 123.45,
    "target_cost": 123.45,
    "source_purchases_14d": 10,
    "target_purchases_14d": 10,
    "source_sales_14d": 999.99,
    "target_sales_14d": 999.99,
}


def test_reconciliation_check_reports_match(monkeypatch):
    fake_redshift_data = FakeRedshiftData(MATCHING_RECONCILIATION_ROW)
    fake_sns = FakeSns()
    monkeypatch.setattr(reconciliation_check, "_redshift_data", fake_redshift_data)
    monkeypatch.setattr(reconciliation_check, "_sns", fake_sns)

    result = reconciliation_check.handler({"some": "input"}, None)

    assert result["some"] == "input"  # prior state passes through
    assert result["reconciliation"]["all_match"] is True
    assert all(m["matched"] for m in result["reconciliation"]["measures"].values())

    assert len(fake_sns.published) == 1
    published = fake_sns.published[0]
    assert published["TopicArn"] == os.environ["ALERTS_TOPIC_ARN"]
    assert "Reconciliation OK" in published["Message"]


def test_reconciliation_check_reports_mismatch(monkeypatch):
    mismatched_row = dict(MATCHING_RECONCILIATION_ROW)
    mismatched_row["target_row_count"] = 95  # target lost 5 rows during load

    fake_redshift_data = FakeRedshiftData(mismatched_row)
    fake_sns = FakeSns()
    monkeypatch.setattr(reconciliation_check, "_redshift_data", fake_redshift_data)
    monkeypatch.setattr(reconciliation_check, "_sns", fake_sns)

    result = reconciliation_check.handler({}, None)

    assert result["reconciliation"]["all_match"] is False
    assert result["reconciliation"]["measures"]["row_count"]["matched"] is False
    assert result["reconciliation"]["measures"]["impressions"]["matched"] is True

    published = fake_sns.published[0]
    assert "Reconciliation MISMATCH" in published["Message"]
    assert "row_count: source=100 target=95 [MISMATCH]" in published["Message"]
