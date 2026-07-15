"""Deploy the compute/orchestration stack defined in template.yaml (AWS SAM).

Two things `sam deploy` can't do on its own, so this script does them first:

1. Glue Python Shell jobs have no CodeUri-style packaging the way Lambda does -- the script
   and its dependencies (common/, validation/) must already be sitting in S3 before the stack
   references them. This uploads glue_jobs/bronze_to_silver.py and a zip of common/+validation/
   to s3://<deploy-artifacts-bucket>/glue/.
2. The Redshift Data API state machine tasks that run more than one statement per call
   (ScdDimCampaign, ScdFctCampaignPerformanceHistory) or a large single statement (MergeIntoFact)
   get their SQL text from redshift/*.sql at deploy time via CloudFormation Parameters,
   substituted into the ASL via template.yaml's DefinitionSubstitutions -- see that file's
   Parameters block. This script reads those files and passes their contents through
   `sam deploy --parameter-overrides`.

subprocess.run is called with argv as a list (no shell=True), so multi-line SQL text and
anything else in these parameters reaches `sam` as a single argument each, with no shell
quoting/escaping involved.

The Amazon Ads LWA client secret is read from the ADS_LWA_CLIENT_SECRET environment variable
(never a CLI flag, so it never lands in shell history or `ps`), matching the env var name
common/secrets.py already reads at runtime -- see template.yaml's AdsLwaClientSecret parameter
for the NoEcho tradeoff this implies.

Usage: python infra/deploy.py --stack-name ads-pipeline --region us-east-1 \\
           --raw-bucket <bucket> --kms-key-arn <arn> --deploy-artifacts-bucket <bucket> \\
           --redshift-workgroup <name> --redshift-database <name> \\
           --ads-lwa-client-id <id> --alerts-topic-arn <arn> \\
           [--sam-artifacts-bucket <bucket>] [--guided]
"""

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import boto3

REPO_ROOT = Path(__file__).resolve().parent.parent

SQL_FILES = {
    "MergeFactSql": REPO_ROOT / "redshift" / "merge_fct_campaign_performance.sql",
    "ScdDimCampaignCloseSql": REPO_ROOT / "redshift" / "scd2_dim_campaign_close.sql",
    "ScdDimCampaignInsertSql": REPO_ROOT / "redshift" / "scd2_dim_campaign_insert.sql",
    "ScdFctCampaignPerformanceCloseSql": REPO_ROOT / "redshift" / "scd2_fct_campaign_performance_close.sql",
    "ScdFctCampaignPerformanceInsertSql": REPO_ROOT / "redshift" / "scd2_fct_campaign_performance_insert.sql",
}


def _parse_args(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--stack-name", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--raw-bucket", required=True, help="Pre-existing raw S3 bucket name")
    parser.add_argument("--kms-key-arn", required=True, help="Pre-existing KMS key ARN used for the bucket's SSE")
    parser.add_argument(
        "--deploy-artifacts-bucket", required=True, help="Bucket to upload the Glue script/dependencies zip to"
    )
    parser.add_argument("--redshift-workgroup", required=True)
    parser.add_argument("--redshift-database", required=True)
    parser.add_argument("--ads-lwa-client-id", required=True)
    parser.add_argument(
        "--alerts-topic-arn",
        required=True,
        help="ARN of the SNS topic infra/configure_alerting.py creates (ads-pipeline-alerts)",
    )
    parser.add_argument("--lookback-days", default="30")
    parser.add_argument(
        "--sam-artifacts-bucket",
        help="Bucket sam deploy uses for its own packaged Lambda/state-machine artifacts. "
        "Omit to let sam manage a bootstrap bucket via --resolve-s3.",
    )
    parser.add_argument("--guided", action="store_true", help="Run `sam deploy --guided` instead of a scripted deploy")
    return parser.parse_args(argv)


def upload_glue_artifacts(deploy_artifacts_bucket: str, region: str) -> None:
    s3 = boto3.client("s3", region_name=region)

    s3.upload_file(
        str(REPO_ROOT / "glue_jobs" / "bronze_to_silver.py"), deploy_artifacts_bucket, "glue/bronze_to_silver.py"
    )

    with tempfile.TemporaryDirectory() as tmp_dir:
        zip_base = Path(tmp_dir) / "common_and_validation"
        shutil.copytree(REPO_ROOT / "common", Path(tmp_dir) / "build" / "common")
        shutil.copytree(REPO_ROOT / "validation", Path(tmp_dir) / "build" / "validation")
        zip_path = shutil.make_archive(str(zip_base), "zip", root_dir=str(Path(tmp_dir) / "build"))
        s3.upload_file(zip_path, deploy_artifacts_bucket, "glue/common_and_validation.zip")


def read_sql_params() -> dict:
    return {key: path.read_text() for key, path in SQL_FILES.items()}


def run_sam_deploy(args, sql_params: dict) -> None:
    client_secret = os.environ.get("ADS_LWA_CLIENT_SECRET")
    if not client_secret:
        sys.exit("ADS_LWA_CLIENT_SECRET must be set in the environment (never passed as a CLI flag).")

    subprocess.run(["sam", "build", "--template-file", str(REPO_ROOT / "template.yaml")], cwd=REPO_ROOT, check=True)

    if args.guided:
        subprocess.run(["sam", "deploy", "--guided"], cwd=REPO_ROOT, check=True)
        return

    parameter_overrides = {
        "RawBucketName": args.raw_bucket,
        "KmsKeyArn": args.kms_key_arn,
        "DeployArtifactsBucket": args.deploy_artifacts_bucket,
        "RedshiftWorkgroupName": args.redshift_workgroup,
        "RedshiftDatabaseName": args.redshift_database,
        "AdsLwaClientId": args.ads_lwa_client_id,
        "AdsLwaClientSecret": client_secret,
        "AlertsTopicArn": args.alerts_topic_arn,
        "LookbackDays": args.lookback_days,
        **sql_params,
    }

    deploy_cmd = [
        "sam",
        "deploy",
        "--stack-name",
        args.stack_name,
        "--region",
        args.region,
        "--capabilities",
        "CAPABILITY_IAM",
        "--no-confirm-changeset",
        "--no-fail-on-empty-changeset",
    ]
    if args.sam_artifacts_bucket:
        deploy_cmd += ["--s3-bucket", args.sam_artifacts_bucket]
    else:
        deploy_cmd += ["--resolve-s3"]

    deploy_cmd.append("--parameter-overrides")
    for key, value in parameter_overrides.items():
        deploy_cmd.append(f"{key}={value}")

    subprocess.run(deploy_cmd, cwd=REPO_ROOT, check=True)


def main(argv=None) -> None:
    args = _parse_args(argv)
    upload_glue_artifacts(args.deploy_artifacts_bucket, args.region)
    sql_params = read_sql_params()
    run_sam_deploy(args, sql_params)


if __name__ == "__main__":
    main()
