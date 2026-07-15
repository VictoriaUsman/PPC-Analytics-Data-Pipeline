"""Abstract base for Amazon Ads report connectors.

Owns the shared HTTP retry/backoff strategy (same shape as the reference POS Pipeline's
BasePOSConnector._request) and defines the create/poll/download interface every Lambda
handler drives through Step Functions' Wait/Choice poll loop (see
statemachine/ads_ingestion.asl.json).

The interface is deliberately abstract at the create_report/poll_report/download_report
boundary -- not tied to Sponsored Ads v3's specific request/response shapes -- so that
migrating to Amazon's unified reporting API later (v3 sunsets Dec 31, 2026; the unified API
is still in open beta as of this writing, see README) is a new subclass, not a pipeline
rewrite. connectors/ads_connector.py is today's concrete v3 implementation.
"""

import abc
import dataclasses
import gzip
import json
import random
import time
from datetime import date

import requests

from common.logging_config import get_logger, log_fields

logger = get_logger(__name__)

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
MAX_ATTEMPTS = 5
MAX_BACKOFF_SECONDS = 60


@dataclasses.dataclass
class ReportStatus:
    status: str  # PENDING | PROCESSING | COMPLETED | FAILED
    download_url: str | None = None
    failure_reason: str | None = None


class AdsReportConnector(abc.ABC):
    def __init__(self, profile_id: str, region: str, access_token: str, client_id: str):
        self.profile_id = profile_id
        self.region = region
        self.access_token = access_token
        self.client_id = client_id

    @abc.abstractmethod
    def create_report(self, ad_product: str, start_date: date, end_date: date) -> str:
        """Request a report for [start_date, end_date] (inclusive); returns a report_id."""

    @abc.abstractmethod
    def poll_report(self, report_id: str) -> ReportStatus:
        """Check a previously requested report's status."""

    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        headers = kwargs.pop("headers", {})
        headers.setdefault("Amazon-Advertising-API-ClientId", self.client_id)
        headers.setdefault("Amazon-Advertising-API-Scope", self.profile_id)
        headers.setdefault("Authorization", f"Bearer {self.access_token}")

        last_exc = None
        for attempt in range(MAX_ATTEMPTS):
            try:
                response = requests.request(method, url, headers=headers, timeout=30, **kwargs)
            except (requests.ConnectionError, requests.Timeout) as exc:
                last_exc = exc
                delay = self._backoff_delay(attempt)
                logger.warning(
                    "connection error, retrying",
                    extra=log_fields(profile_id=self.profile_id, attempt=attempt, delay_seconds=delay),
                )
                time.sleep(delay)
                continue

            if response.status_code not in RETRYABLE_STATUS_CODES:
                return response

            delay = self._retry_after(response)
            if delay is None:
                # Amazon's own docs-repo tracker documents Retry-After sometimes missing
                # specifically on POST /reporting/reports -- don't assume it's always present.
                delay = self._backoff_delay(attempt)
            logger.warning(
                "retryable response, backing off",
                extra=log_fields(
                    profile_id=self.profile_id,
                    status_code=response.status_code,
                    attempt=attempt,
                    delay_seconds=delay,
                ),
            )
            time.sleep(delay)

        if last_exc:
            raise last_exc
        return response  # last (still-retryable) response, let the caller decide

    @staticmethod
    def _backoff_delay(attempt: int) -> float:
        return random.uniform(0, min(MAX_BACKOFF_SECONDS, 1 * (2**attempt)))

    @staticmethod
    def _retry_after(response: requests.Response) -> float | None:
        value = response.headers.get("Retry-After")
        if value is None:
            return None
        try:
            return float(value)
        except ValueError:
            return None


FLUSH_ROW_THRESHOLD = 50_000
"""Bounds peak memory to ~one flush-batch's worth of rows, independent of how large a
report gets overall -- previously the entire report was buffered as parsed Python objects
before anything was written, which for a big-enough profile/ad_product risks exceeding
Lambda's configured memory well before Lambda's timeout is ever a factor.
"""


def download_and_stream_report(download_url: str, s3_client, bucket: str, key_for_date) -> list[str]:
    """Stream a completed report's signed URL straight into S3, split per calendar day.

    Standalone (not connector-instance-bound) since downloading from Amazon's signed URL
    needs no LWA auth headers -- only the URL itself, which is itself the bearer credential
    and time-limited, so this must run immediately on COMPLETED status, not be queued.
    Downloads and decompresses in a single streaming pass, flushing whatever's buffered to
    S3 every FLUSH_ROW_THRESHOLD rows rather than accumulating the whole report in memory --
    reports across 26 profiles can be large, and the rolling reprocessing window (see
    common/scheduling.py) means a report already covers up to 30 days per call.

    Splits rows by their `date` field so each day lands under its own Hive partition (see
    common.s3_paths.object_key), which is what lets a rerun of an overlapping window
    overwrite exactly the affected days rather than the whole window. Rows for a given day
    aren't assumed to arrive contiguously, so a day's rows aren't flushed until either the
    row-count threshold is hit (flushing everything buffered so far, across all days) or the
    stream ends -- a day can end up split across more than one part as a result (see
    common.s3_paths.object_key's `part` argument).

    `key_for_date(report_date, part) -> str` is the caller-supplied key builder (typically
    common.s3_paths.object_key bound to ad_product/profile_id/report_id).
    """
    rows_by_date: dict[str, list[dict]] = {}
    parts_written: dict[str, int] = {}
    written_keys: list[str] = []
    buffered_row_count = 0

    def flush() -> None:
        nonlocal buffered_row_count
        for report_date_str, rows in rows_by_date.items():
            if not rows:
                continue
            report_date = date.fromisoformat(report_date_str)
            part = parts_written.get(report_date_str, 0)
            key = key_for_date(report_date, part)
            body = "\n".join(json.dumps(r) for r in rows).encode("utf-8")
            s3_client.put_object(Bucket=bucket, Key=key, Body=body, ServerSideEncryption="aws:kms")
            written_keys.append(key)
            parts_written[report_date_str] = part + 1
        rows_by_date.clear()
        buffered_row_count = 0

    with requests.get(download_url, stream=True, timeout=60) as response:
        response.raise_for_status()
        with gzip.GzipFile(fileobj=response.raw) as decompressed:
            for line in decompressed:
                if not line.strip():
                    continue
                record = json.loads(line)
                rows_by_date.setdefault(record["date"], []).append(record)
                buffered_row_count += 1
                if buffered_row_count >= FLUSH_ROW_THRESHOLD:
                    flush()

    flush()  # remaining rows that never hit the threshold
    return written_keys
