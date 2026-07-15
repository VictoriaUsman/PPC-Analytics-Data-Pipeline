"""One-off/idempotent setup script: schedule recurring Redshift Serverless snapshots.

Redshift Serverless auto-generates free "recovery points" with a ~24-hour retention -- fine for
undoing a same-day mistake, but not a real disaster-recovery posture (a bad deploy or a corrupted
MERGE caught a few days later has nothing to restore to). This adds a scheduled snapshot action on
top of that, with a retention period longer than LOOKBACK_DAYS (see common/scheduling.py) -- so a
restore is always followed by a recovery window the pipeline's own idempotent re-ingestion can
fully close: restore the namespace from the latest snapshot, then let the next scheduled run's
rolling window (and its `MERGE`) re-pull and reconcile anything since the snapshot. No custom
point-in-time replay logic needed.

This does NOT cover a full AWS region outage (that would need cross-region snapshot copy, an added
storage/transfer cost this pipeline hasn't been asked to take on -- see README's Disaster Recovery
runbook for that tradeoff).

Usage: python configure_redshift_backup.py --namespace <namespace> --role-arn <role-arn>
       [--schedule 'cron(0 6 * * ? *)'] [--retention-days 35] [--snapshot-prefix ads-pipeline]
"""
import argparse

import boto3

SCHEDULED_ACTION_NAME = "ads-pipeline-scheduled-snapshot"


def _parse_args(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--namespace", required=True, help="Redshift Serverless namespace name")
    parser.add_argument(
        "--role-arn",
        required=True,
        help="IAM role redshift-serverless.amazonaws.com assumes to create the snapshot -- needs "
        "redshift-serverless:CreateSnapshot scoped to this namespace. Not created by this script, "
        "same reasoning as configure_bucket_security.py's --kms-key-id: a stateful IAM resource "
        "provisioned once, out of band.",
    )
    parser.add_argument(
        "--schedule",
        default="cron(0 6 * * ? *)",
        help="EventBridge Scheduler cron/rate expression (default: daily at 06:00 UTC)",
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=35,
        help="Snapshot retention. Default (35) is LOOKBACK_DAYS (30) plus a margin, so the oldest "
        "snapshot still in retention is always old enough that the rolling window's re-ingestion "
        "fully covers the gap between it and now.",
    )
    parser.add_argument("--snapshot-prefix", default="ads-pipeline")
    return parser.parse_args(argv)


def configure_scheduled_snapshot(
    client, namespace: str, role_arn: str, schedule: str, retention_days: int, snapshot_prefix: str
) -> None:
    target_action = {
        "createSnapshot": {
            "namespaceName": namespace,
            "retentionPeriod": retention_days,
            "snapshotNamePrefix": snapshot_prefix,
        }
    }

    try:
        client.get_scheduled_action(scheduledActionName=SCHEDULED_ACTION_NAME)
    except client.exceptions.ResourceNotFoundException:
        client.create_scheduled_action(
            scheduledActionName=SCHEDULED_ACTION_NAME,
            namespaceName=namespace,
            roleArn=role_arn,
            schedule={"cron": schedule},
            targetAction=target_action,
            enabled=True,
            scheduledActionDescription=(
                "Recurring snapshot for disaster recovery -- see README's Disaster Recovery runbook."
            ),
        )
        print(f"{SCHEDULED_ACTION_NAME}: created ({schedule}, {retention_days}-day retention)")
    else:
        client.update_scheduled_action(
            scheduledActionName=SCHEDULED_ACTION_NAME,
            roleArn=role_arn,
            schedule={"cron": schedule},
            targetAction=target_action,
            enabled=True,
        )
        print(f"{SCHEDULED_ACTION_NAME}: updated ({schedule}, {retention_days}-day retention)")


def main(argv=None) -> None:
    args = _parse_args(argv)
    client = boto3.client("redshift-serverless")
    configure_scheduled_snapshot(
        client, args.namespace, args.role_arn, args.schedule, args.retention_days, args.snapshot_prefix
    )


if __name__ == "__main__":
    main()
