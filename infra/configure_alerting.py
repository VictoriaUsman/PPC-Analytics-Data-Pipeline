"""One-off/idempotent setup script: wire every native AWS failure signal into one shared SNS
topic, which AWS Chatbot forwards to the team's Microsoft Teams channel.

Same "native signal first, no custom bridge code" philosophy as the reference POS Pipeline's
alerting design (README "Alerting & Failure Notifications") -- simpler here, since this pipeline
is entirely AWS-native and has no GCP leg, so there's no Pub/Sub -> Cloud Function -> Teams-webhook
bridge to build. AWS Chatbot subscribes directly to the SNS topic this script creates.

Signals wired up:
  - Step Functions: EventBridge rule, Execution Status Change = FAILED | TIMED_OUT | ABORTED,
    for both state machines (ingestion, redshift load)
  - Glue: EventBridge rule, Glue Job State Change = FAILED, for the bronze_to_silver job
  - Lambda: CloudWatch Alarm on the Errors metric, per function (report_requester, report_poller,
    report_downloader)
  - Tolerated per-branch ingestion failures: CloudWatch Alarm on the custom
    AdsPipeline/Ingestion BranchFailureCount metric -- the ingestion Map state tolerates up to
    10% branch failures (see statemachine/ads_ingestion.asl.json ToleratedFailurePercentage), so
    a single bad profile/ad-product never fails the overall execution and would otherwise be
    invisible to the Step Functions EventBridge rule above. The ReportTimedOut/IngestionBranchFailed
    branch states emit this metric directly via a putMetricData Task before ending.
  - Rejected records: S3 Event Notification, ObjectCreated on the rejected/ prefix -- fires on any
    single rejected record, not a batch ratio (see README "Schema Drift Detection & Alerting").

NOT scriptable: authorizing AWS Chatbot's one-time OAuth connection to the Microsoft Teams
channel. That's an interactive step done once in the Chatbot console/Teams app -- this script
only creates the SNS topic and points to it; run through that console step separately and note
the resulting Chatbot configuration ARN in your deployment notes.

Usage:
  python configure_alerting.py --bucket <raw-bucket> \\
      --ingestion-state-machine-arn <arn> --redshift-load-state-machine-arn <arn> \\
      --glue-job-name bronze_to_silver \\
      --lambda-function-names ads-report-requester ads-report-poller ads-report-downloader \\
      [--email alerts@example.com]
"""
import argparse
import json

import boto3

TOPIC_NAME = "ads-pipeline-alerts"
SFN_RULE_NAME = "ads-pipeline-sfn-failures"
GLUE_RULE_NAME = "ads-pipeline-glue-failures"
REJECTED_NOTIFICATION_ID = "ads-pipeline-rejected-zone-alert"
BRANCH_FAILURE_ALARM_NAME = "ads-pipeline-ingestion-branch-failures"


def _parse_args(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True, help="Raw data bucket (for the rejected/ S3 event notification)")
    parser.add_argument("--ingestion-state-machine-arn", required=True)
    parser.add_argument("--redshift-load-state-machine-arn", required=True)
    parser.add_argument("--glue-job-name", required=True)
    parser.add_argument("--lambda-function-names", nargs="+", required=True)
    parser.add_argument("--email", help="Optional email address for a backup SNS subscription")
    return parser.parse_args(argv)


def ensure_sns_topic(sns) -> str:
    topic_arn = sns.create_topic(Name=TOPIC_NAME)["TopicArn"]
    print(f"SNS topic ready: {topic_arn}")
    return topic_arn


def ensure_email_subscription(sns, topic_arn: str, email: str) -> None:
    existing = sns.list_subscriptions_by_topic(TopicArn=topic_arn)["Subscriptions"]
    if any(s["Protocol"] == "email" and s["Endpoint"] == email for s in existing):
        print(f"email subscription already present for {email}")
        return
    sns.subscribe(TopicArn=topic_arn, Protocol="email", Endpoint=email)
    print(f"email subscription requested for {email} -- confirmation email will be sent")


def _allow_eventbridge_to_publish(sns, topic_arn: str) -> None:
    sns.set_topic_attributes(
        TopicArn=topic_arn,
        AttributeName="Policy",
        AttributeValue=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "AllowEventBridgePublish",
                        "Effect": "Allow",
                        "Principal": {"Service": "events.amazonaws.com"},
                        "Action": "sns:Publish",
                        "Resource": topic_arn,
                    }
                ],
            }
        ),
    )


def ensure_sfn_failure_rule(events, topic_arn: str, state_machine_arns: list) -> None:
    events.put_rule(
        Name=SFN_RULE_NAME,
        EventPattern=json.dumps(
            {
                "source": ["aws.states"],
                "detail-type": ["Step Functions Execution Status Change"],
                "detail": {
                    "status": ["FAILED", "TIMED_OUT", "ABORTED"],
                    "stateMachineArn": state_machine_arns,
                },
            }
        ),
        State="ENABLED",
    )
    events.put_targets(Rule=SFN_RULE_NAME, Targets=[{"Id": "ads-pipeline-alerts-sns", "Arn": topic_arn}])
    print(f"EventBridge rule {SFN_RULE_NAME!r} -> SNS for {len(state_machine_arns)} state machine(s)")


def ensure_glue_failure_rule(events, topic_arn: str, job_name: str) -> None:
    events.put_rule(
        Name=GLUE_RULE_NAME,
        EventPattern=json.dumps(
            {
                "source": ["aws.glue"],
                "detail-type": ["Glue Job State Change"],
                "detail": {"jobName": [job_name], "state": ["FAILED", "TIMEOUT"]},
            }
        ),
        State="ENABLED",
    )
    events.put_targets(Rule=GLUE_RULE_NAME, Targets=[{"Id": "ads-pipeline-alerts-sns", "Arn": topic_arn}])
    print(f"EventBridge rule {GLUE_RULE_NAME!r} -> SNS for Glue job {job_name!r}")


def ensure_lambda_error_alarms(cloudwatch, topic_arn: str, function_names: list) -> None:
    for name in function_names:
        alarm_name = f"ads-pipeline-lambda-errors-{name}"
        cloudwatch.put_metric_alarm(
            AlarmName=alarm_name,
            Namespace="AWS/Lambda",
            MetricName="Errors",
            Dimensions=[{"Name": "FunctionName", "Value": name}],
            Statistic="Sum",
            Period=300,
            EvaluationPeriods=1,
            Threshold=0,
            ComparisonOperator="GreaterThanThreshold",
            TreatMissingData="notBreaching",
            AlarmActions=[topic_arn],
        )
        print(f"CloudWatch alarm {alarm_name!r} -> SNS on {name} Errors")


def ensure_branch_failure_alarm(cloudwatch, topic_arn: str) -> None:
    cloudwatch.put_metric_alarm(
        AlarmName=BRANCH_FAILURE_ALARM_NAME,
        Namespace="AdsPipeline/Ingestion",
        MetricName="BranchFailureCount",
        Statistic="Sum",
        Period=300,
        EvaluationPeriods=1,
        Threshold=0,
        ComparisonOperator="GreaterThanThreshold",
        TreatMissingData="notBreaching",
        AlarmActions=[topic_arn],
    )
    print(f"CloudWatch alarm {BRANCH_FAILURE_ALARM_NAME!r} -> SNS on any tolerated branch failure")


def ensure_rejected_zone_notification(s3, sns, bucket: str, topic_arn: str) -> None:
    """Merge an SNS-destined ObjectCreated notification for rejected/ into whatever bucket
    notification configuration already exists, keyed by a fixed notification ID.
    """
    sns.set_topic_attributes(
        TopicArn=topic_arn,
        AttributeName="Policy",
        AttributeValue=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "AllowS3Publish",
                        "Effect": "Allow",
                        "Principal": {"Service": "s3.amazonaws.com"},
                        "Action": "sns:Publish",
                        "Resource": topic_arn,
                        "Condition": {"ArnLike": {"aws:SourceArn": f"arn:aws:s3:::{bucket}"}},
                    }
                ],
            }
        ),
    )

    existing = s3.get_bucket_notification_configuration(Bucket=bucket)
    existing.pop("ResponseMetadata", None)
    topic_configs = [c for c in existing.get("TopicConfigurations", []) if c.get("Id") != REJECTED_NOTIFICATION_ID]
    topic_configs.append(
        {
            "Id": REJECTED_NOTIFICATION_ID,
            "TopicArn": topic_arn,
            "Events": ["s3:ObjectCreated:*"],
            "Filter": {"Key": {"FilterRules": [{"Name": "prefix", "Value": "rejected/"}]}},
        }
    )
    existing["TopicConfigurations"] = topic_configs
    s3.put_bucket_notification_configuration(Bucket=bucket, NotificationConfiguration=existing)
    print(f"s3://{bucket}: rejected/ ObjectCreated events -> SNS")


def main(argv=None) -> None:
    args = _parse_args(argv)
    sns = boto3.client("sns")
    events = boto3.client("events")
    cloudwatch = boto3.client("cloudwatch")
    s3 = boto3.client("s3")

    topic_arn = ensure_sns_topic(sns)
    _allow_eventbridge_to_publish(sns, topic_arn)

    if args.email:
        ensure_email_subscription(sns, topic_arn, args.email)

    ensure_sfn_failure_rule(
        events, topic_arn, [args.ingestion_state_machine_arn, args.redshift_load_state_machine_arn]
    )
    ensure_glue_failure_rule(events, topic_arn, args.glue_job_name)
    ensure_lambda_error_alarms(cloudwatch, topic_arn, args.lambda_function_names)
    ensure_branch_failure_alarm(cloudwatch, topic_arn)
    ensure_rejected_zone_notification(s3, sns, args.bucket, topic_arn)

    print(
        "\nRemaining manual step (not scriptable): in the AWS Chatbot console, create a Microsoft "
        f"Teams channel configuration subscribed to {topic_arn}, authorizing the one-time OAuth "
        "connection to your Teams tenant. Record the resulting Chatbot configuration ARN in your "
        "deployment notes."
    )


if __name__ == "__main__":
    main()
