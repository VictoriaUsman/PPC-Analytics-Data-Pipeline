"""Step Functions entrypoint: stream a COMPLETED report straight into S3 bronze.

Invoked exactly once per Map branch, right after the poll loop observes status=COMPLETED.
Streams and decompresses in one pass, flushing to S3 in bounded batches rather than
buffering the whole report in memory (see connectors.base.download_and_stream_report), and
runs immediately since Amazon's signed download URL is short-lived.

Input: prior state merged with {"download_url", "report_id", "ad_product", "profile_id"}
Output: input merged with {"bronze_keys": [...]}
"""

import os
from datetime import date

import boto3

from common.logging_config import get_logger, log_fields
from common.s3_paths import object_key
from connectors.base import download_and_stream_report

logger = get_logger(__name__)

BUCKET = os.environ["RAW_BUCKET"]

_s3 = boto3.client("s3")


def handler(event, context):
    ad_product = event["ad_product"]
    profile_id = event["profile_id"]
    report_id = event["report_id"]

    def key_for_date(report_date: date, part: int) -> str:
        return object_key("bronze", ad_product, profile_id, report_date, report_id, part=part)

    bronze_keys = download_and_stream_report(event["download_url"], _s3, BUCKET, key_for_date)

    logger.info(
        "landed report to bronze",
        extra=log_fields(
            profile_id=profile_id,
            ad_product=ad_product,
            report_id=report_id,
            object_count=len(bronze_keys),
        ),
    )
    return {**event, "bronze_keys": bronze_keys}
