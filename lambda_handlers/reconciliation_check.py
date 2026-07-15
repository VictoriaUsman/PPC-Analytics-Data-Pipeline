"""Step Functions entrypoint: reconcile staging_campaign_performance (source) against
fct_campaign_performance (target) and post the result to the pipeline's Teams alert channel on
every run, not just on mismatch -- see README's Data Validation section for the reasoning and
redshift/reconciliation_check.sql for the query itself.

Runs as the ReconciliationCheck task in statemachine/redshift_load.asl.json, immediately after
MergeIntoFact/IsMergeDone confirms FINISHED. Uses the Redshift Data API directly via boto3 (not
the executeStatement/Wait/Describe polling idiom the other redshift_load tasks use natively in
ASL) since this is a single fast aggregate query well within one Lambda invocation, and the
match/mismatch comparison itself needs real code -- classic Step Functions Choice states can only
compare a field to a fixed literal, not two dynamic query results against each other.

A mismatch here doesn't fail the state machine -- MergeIntoFact already committed, so there's
nothing left to roll back at this point, and the whole point of this task is to surface the
mismatch for a human to investigate (via the SNS-backed Teams channel), the same way a CloudWatch
alarm notifies without blocking whatever it's watching.

Input: prior state (untouched; nothing from the event is needed beyond what's already in the
    environment)
Output: input merged with {"reconciliation": {"all_match": bool, "measures": {...}}}
"""

import os
import time
from pathlib import Path

import boto3

from common.logging_config import get_logger, log_fields

logger = get_logger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
RECONCILIATION_SQL_PATH = os.environ.get(
    "RECONCILIATION_SQL_PATH", str(REPO_ROOT / "redshift" / "reconciliation_check.sql")
)
RECONCILIATION_SQL = Path(RECONCILIATION_SQL_PATH).read_text()

WORKGROUP_NAME = os.environ["REDSHIFT_WORKGROUP_NAME"]
DATABASE_NAME = os.environ["REDSHIFT_DATABASE_NAME"]
ALERTS_TOPIC_ARN = os.environ["ALERTS_TOPIC_ARN"]

POLL_INTERVAL_SECONDS = 2
MAX_POLL_ATTEMPTS = 30  # 2s * 30 = 60s -- well under this function's Timeout

MEASURES = ("row_count", "impressions", "clicks", "cost", "purchases_14d", "sales_14d")

_redshift_data = boto3.client("redshift-data")
_sns = boto3.client("sns")


def _run_reconciliation_query() -> dict:
    statement_id = _redshift_data.execute_statement(
        WorkgroupName=WORKGROUP_NAME, Database=DATABASE_NAME, Sql=RECONCILIATION_SQL
    )["Id"]

    for _ in range(MAX_POLL_ATTEMPTS):
        status = _redshift_data.describe_statement(Id=statement_id)
        if status["Status"] == "FINISHED":
            break
        if status["Status"] in ("FAILED", "ABORTED"):
            raise RuntimeError(f"reconciliation query {status['Status']}: {status.get('Error')}")
        time.sleep(POLL_INTERVAL_SECONDS)
    else:
        raise TimeoutError("reconciliation query did not reach FINISHED in time")

    result = _redshift_data.get_statement_result(Id=statement_id)
    columns = [c["name"] for c in result["ColumnMetadata"]]
    row = result["Records"][0]
    return {column: _field_value(field) for column, field in zip(columns, row)}


def _field_value(field: dict):
    for key in ("longValue", "doubleValue", "stringValue"):
        if key in field:
            return field[key]
    return None  # isNull, or an unexpected/empty field shape


def _compare(values: dict) -> dict:
    measures = {}
    for measure in MEASURES:
        source_value = values[f"source_{measure}"]
        target_value = values[f"target_{measure}"]
        measures[measure] = {
            "source": source_value,
            "target": target_value,
            "matched": source_value == target_value,
        }
    return measures


def _format_message(measures: dict, all_match: bool) -> str:
    header = "Reconciliation OK" if all_match else "Reconciliation MISMATCH"
    lines = [header, ""]
    for measure, values in measures.items():
        flag = "OK" if values["matched"] else "MISMATCH"
        lines.append(f"{measure}: source={values['source']} target={values['target']} [{flag}]")
    return "\n".join(lines)


def handler(event, context):
    values = _run_reconciliation_query()
    measures = _compare(values)
    all_match = all(m["matched"] for m in measures.values())

    _sns.publish(
        TopicArn=ALERTS_TOPIC_ARN,
        Subject="Ads Pipeline: reconciliation report",
        Message=_format_message(measures, all_match),
    )

    logger.info("reconciliation complete", extra=log_fields(all_match=all_match))

    return {**event, "reconciliation": {"all_match": all_match, "measures": measures}}
