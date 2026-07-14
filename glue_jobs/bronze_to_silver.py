"""AWS Glue Python Shell job: bronze -> silver/rejected.

Same shape as the reference POS Pipeline's glue_jobs/bronze_to_silver.py -- a boto3
list+validate loop, not Spark. At Amazon Ads' per-run volume (26 profiles x up to 3 ad
products x 30-day rolling window, all campaign-level aggregates, no per-click/per-impression
rows) this is nowhere near Spark territory, same "Python Shell, not Spark" reasoning as the
reference at 500-700MB/day.

Invoked from statemachine/ads_ingestion.asl.json via glue:startJobRun.sync (Step Functions'
native `.sync` integration waits for job completion -- no custom poll loop needed here,
unlike the Redshift load step).

CLI: python bronze_to_silver.py --bucket <bucket> --start-date <YYYY-MM-DD> --until-date <YYYY-MM-DD>
"""

import argparse
import json
import re
from datetime import date, timedelta

import boto3

from common.logging_config import get_logger, log_fields
from common.s3_paths import swap_zone
from validation.rules import detect_new_fields, validate_record

logger = get_logger(__name__)

METRIC_NAMESPACE = "AdsPipeline/Validation"
# Diagnostic threshold only -- the actual alert trigger is the S3 Event Notification on the
# rejected/ prefix (fires on any single rejected record), not this ratio. Kept as a
# trend/severity signal surfaced once you're already alerted, same design choice as the
# reference pipeline.
REJECTED_RATIO_WARNING_THRESHOLD = 0.10

AD_PRODUCT_RE = re.compile(r"ad_product=([^/]+)/")
DATE_PARTITION_RE = re.compile(r"year=(\d{4})/month=(\d{2})/day=(\d{2})/")


def _parse_args(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--start-date")
    parser.add_argument("--until-date")
    args = parser.parse_args(argv)

    if args.start_date and args.until_date:
        start = date.fromisoformat(args.start_date)
        until = date.fromisoformat(args.until_date)
    else:
        until = date.today() - timedelta(days=1)
        start = until
    return args.bucket, start, until


def _key_in_window(key: str, start: date, until: date) -> bool:
    match = DATE_PARTITION_RE.search(key)
    if not match:
        return False
    key_date = date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
    return start <= key_date <= until


def _list_bronze_keys(s3, bucket: str, start: date, until: date) -> list:
    """List bronze objects whose date partition falls in [start, until].

    Bronze keys don't share a fixed-depth prefix (profile_id sits between ad_product and
    date), so this lists everything under bronze/ and filters client-side -- fine at this
    pipeline's volume, same caveat the reference pipeline documents for its own
    _list_bronze_keys (wouldn't scale as-is to a much larger bronze zone).
    """
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix="bronze/"):
        for obj in page.get("Contents", []):
            if _key_in_window(obj["Key"], start, until):
                keys.append(obj["Key"])
    return keys


def _process_key(s3, bucket: str, key: str) -> dict:
    ad_product_match = AD_PRODUCT_RE.search(key)
    ad_product = ad_product_match.group(1) if ad_product_match else None

    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()

    valid_lines, rejected_lines = [], []
    reasons: dict[str, int] = {}
    new_fields: set = set()

    for line in body.decode("utf-8").splitlines():
        if not line.strip():
            continue
        record = json.loads(line)
        reason = validate_record(record, ad_product)
        if reason is None:
            valid_lines.append(json.dumps(record))
        else:
            rejected_lines.append(json.dumps({**record, "_validation_error": reason}))
            reasons[reason] = reasons.get(reason, 0) + 1
        new_fields |= detect_new_fields(record, ad_product)

    if valid_lines:
        s3.put_object(
            Bucket=bucket,
            Key=swap_zone(key, "silver"),
            Body="\n".join(valid_lines).encode("utf-8"),
            ServerSideEncryption="aws:kms",
        )
    if rejected_lines:
        # This PUT is the alert trigger -- an S3 Event Notification on rejected/ fires
        # immediately, publishing to the shared SNS topic (see infra/configure_alerting.py
        # and README) -- not a batch ratio threshold, so even one bad row surfaces right away.
        s3.put_object(
            Bucket=bucket,
            Key=swap_zone(key, "rejected"),
            Body="\n".join(rejected_lines).encode("utf-8"),
            ServerSideEncryption="aws:kms",
        )

    return {
        "ad_product": ad_product,
        "valid": len(valid_lines),
        "rejected": len(rejected_lines),
        "reasons": reasons,
        "new_fields": new_fields,
    }


def _report_ad_product_summary(cloudwatch, ad_product: str, agg: dict) -> dict:
    total = agg["valid"] + agg["rejected"]
    ratio = (agg["rejected"] / total) if total else 0.0

    cloudwatch.put_metric_data(
        Namespace=METRIC_NAMESPACE,
        MetricData=[
            {
                "MetricName": "RejectedRatio",
                "Dimensions": [{"Name": "AdProduct", "Value": ad_product}],
                "Value": ratio * 100,
                "Unit": "Percent",
            },
            {
                "MetricName": "NewFieldCount",
                "Dimensions": [{"Name": "AdProduct", "Value": ad_product}],
                "Value": len(agg["new_fields"]),
                "Unit": "Count",
            },
        ],
    )

    logger.info(
        "validation summary",
        extra=log_fields(ad_product=ad_product, valid=agg["valid"], rejected=agg["rejected"], ratio=ratio),
    )
    if ratio > REJECTED_RATIO_WARNING_THRESHOLD:
        logger.warning(
            "rejected ratio exceeds diagnostic threshold",
            extra=log_fields(ad_product=ad_product, ratio=ratio, reasons=agg["reasons"]),
        )
    if agg["new_fields"]:
        logger.warning(
            "new fields detected -- update validation/rules.py KNOWN_FIELDS if this is a real schema change",
            extra=log_fields(ad_product=ad_product, new_fields=sorted(agg["new_fields"])),
        )

    return {
        "ad_product": ad_product,
        "valid": agg["valid"],
        "rejected": agg["rejected"],
        "ratio": ratio,
        "new_fields": sorted(agg["new_fields"]),
    }


def main(argv=None) -> list:
    bucket, start, until = _parse_args(argv)
    s3 = boto3.client("s3")
    cloudwatch = boto3.client("cloudwatch")

    keys = _list_bronze_keys(s3, bucket, start, until)
    logger.info(
        "listed bronze keys",
        extra=log_fields(count=len(keys), start_date=start.isoformat(), until_date=until.isoformat()),
    )

    aggregates: dict[str, dict] = {}
    for key in keys:
        result = _process_key(s3, bucket, key)
        ad_product = result["ad_product"]
        agg = aggregates.setdefault(
            ad_product, {"valid": 0, "rejected": 0, "reasons": {}, "new_fields": set()}
        )
        agg["valid"] += result["valid"]
        agg["rejected"] += result["rejected"]
        agg["new_fields"] |= result["new_fields"]
        for reason, count in result["reasons"].items():
            agg["reasons"][reason] = agg["reasons"].get(reason, 0) + count

    return [_report_ad_product_summary(cloudwatch, ad_product, agg) for ad_product, agg in aggregates.items()]


if __name__ == "__main__":
    main()
