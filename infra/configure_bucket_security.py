"""One-off/idempotent setup script: harden the raw-data bucket (bronze/silver/rejected all live
in one bucket, partitioned by zone prefix -- see common/s3_paths.py).

The reference POS Pipeline explicitly flagged this layer as thin ("needs hardening before
production"); the user asked for "similar security" as a first-class goal here, not a deferred
TODO, so this ships from the start:

- SSE-KMS default encryption (a customer-managed key, not the S3-managed default -- lets you
  control/rotate/revoke access to the key independently of bucket permissions)
- Block Public Access, all four settings
- Versioning (protects against accidental overwrite -- object keys are deterministic and
  idempotently overwritten by design, so versioning is a safety net, not the primary mechanism),
  paired with a NoncurrentVersionExpiration lifecycle rule -- without it, versioning alone keeps
  every overwritten prior version forever, since these keys are overwritten routinely by design
  (see common/scheduling.py's rolling window), not as a rare exception
- A bucket policy denying any request that isn't TLS (aws:SecureTransport)
- CloudTrail data events for the bucket's Object-level API calls, so every read/write is
  independently auditable

Usage: python configure_bucket_security.py --bucket <bucket> --kms-key-id <key-id-or-arn>
       [--trail-name <existing-cloudtrail-trail-name>]
"""
import argparse
import json

import boto3


def _parse_args(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--kms-key-id", required=True, help="KMS key ID or ARN used for SSE-KMS default encryption")
    parser.add_argument(
        "--trail-name",
        help="Existing CloudTrail trail to attach a data-event selector to for this bucket. "
        "Skipped if omitted -- CloudTrail trails are account-wide singletons, not something this "
        "script should create on its own.",
    )
    parser.add_argument(
        "--noncurrent-version-expiration-days",
        type=int,
        default=30,
        help="How long a superseded object version is kept before expiring (bounds versioning's "
        "storage cost). Default matches LOOKBACK_DAYS -- a version older than the rolling "
        "reprocessing window is no longer needed as an undo target.",
    )
    return parser.parse_args(argv)


def configure_encryption(s3, bucket: str, kms_key_id: str) -> None:
    s3.put_bucket_encryption(
        Bucket=bucket,
        ServerSideEncryptionConfiguration={
            "Rules": [
                {
                    "ApplyServerSideEncryptionByDefault": {
                        "SSEAlgorithm": "aws:kms",
                        "KMSMasterKeyID": kms_key_id,
                    },
                    "BucketKeyEnabled": True,
                }
            ]
        },
    )
    print(f"s3://{bucket}: default encryption set to SSE-KMS ({kms_key_id})")


def configure_public_access_block(s3, bucket: str) -> None:
    s3.put_public_access_block(
        Bucket=bucket,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True,
        },
    )
    print(f"s3://{bucket}: all four Block Public Access settings enabled")


def configure_versioning(s3, bucket: str) -> None:
    s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    print(f"s3://{bucket}: versioning enabled")


def configure_noncurrent_version_expiration(s3, bucket: str, days: int) -> None:
    """Merges a NoncurrentVersionExpiration rule into whatever lifecycle configuration already
    exists on the bucket, keyed by a fixed rule ID -- same additive-merge discipline as
    configure_rejected_lifecycle.py, so the two scripts' rules coexist regardless of run order.
    """
    rule_id = "expire-noncurrent-versions"
    try:
        existing_rules = s3.get_bucket_lifecycle_configuration(Bucket=bucket)["Rules"]
    except s3.exceptions.ClientError as exc:
        if exc.response["Error"]["Code"] != "NoSuchLifecycleConfiguration":
            raise
        existing_rules = []

    other_rules = [r for r in existing_rules if r["ID"] != rule_id]
    version_rule = {
        "ID": rule_id,
        "Status": "Enabled",
        "Filter": {},
        "NoncurrentVersionExpiration": {"NoncurrentDays": days},
    }

    s3.put_bucket_lifecycle_configuration(
        Bucket=bucket,
        LifecycleConfiguration={"Rules": other_rules + [version_rule]},
    )
    print(f"s3://{bucket}: noncurrent object versions will expire after {days} days")


def configure_tls_only_policy(s3, bucket: str) -> None:
    """Merge a TLS-only Deny statement into whatever bucket policy already exists, keyed by a
    fixed Sid, rather than overwriting it -- same additive-merge discipline as the reference
    pipeline's configure_rejected_lifecycle.py.
    """
    sid = "DenyInsecureTransport"
    try:
        existing_policy = json.loads(s3.get_bucket_policy(Bucket=bucket)["Policy"])
    except s3.exceptions.ClientError as exc:
        if exc.response["Error"]["Code"] != "NoSuchBucketPolicy":
            raise
        existing_policy = {"Version": "2012-10-17", "Statement": []}

    other_statements = [s for s in existing_policy["Statement"] if s.get("Sid") != sid]
    deny_statement = {
        "Sid": sid,
        "Effect": "Deny",
        "Principal": "*",
        "Action": "s3:*",
        "Resource": [f"arn:aws:s3:::{bucket}", f"arn:aws:s3:::{bucket}/*"],
        "Condition": {"Bool": {"aws:SecureTransport": "false"}},
    }
    existing_policy["Statement"] = other_statements + [deny_statement]

    s3.put_bucket_policy(Bucket=bucket, Policy=json.dumps(existing_policy))
    print(f"s3://{bucket}: TLS-only bucket policy merged in")


def configure_cloudtrail_data_events(cloudtrail, bucket: str, trail_name: str) -> None:
    event_selectors = cloudtrail.get_event_selectors(TrailName=trail_name).get("EventSelectors", [])
    data_resources = []
    for selector in event_selectors:
        data_resources.extend(selector.get("DataResources", []))

    bucket_arn = f"arn:aws:s3:::{bucket}/"
    already_covered = any(
        dr["Type"] == "AWS::S3::Object" and bucket_arn in dr.get("Values", []) for dr in data_resources
    )
    if already_covered:
        print(f"s3://{bucket}: CloudTrail data events already configured on trail {trail_name!r}")
        return

    event_selectors.append(
        {
            "ReadWriteType": "All",
            "IncludeManagementEvents": False,
            "DataResources": [{"Type": "AWS::S3::Object", "Values": [bucket_arn]}],
        }
    )
    cloudtrail.put_event_selectors(TrailName=trail_name, EventSelectors=event_selectors)
    print(f"s3://{bucket}: CloudTrail data events added to trail {trail_name!r}")


def main(argv=None) -> None:
    args = _parse_args(argv)
    s3 = boto3.client("s3")

    configure_encryption(s3, args.bucket, args.kms_key_id)
    configure_public_access_block(s3, args.bucket)
    configure_versioning(s3, args.bucket)
    configure_noncurrent_version_expiration(s3, args.bucket, args.noncurrent_version_expiration_days)
    configure_tls_only_policy(s3, args.bucket)

    if args.trail_name:
        configure_cloudtrail_data_events(boto3.client("cloudtrail"), args.bucket, args.trail_name)
    else:
        print("no --trail-name given: skipping CloudTrail data-event configuration")


if __name__ == "__main__":
    main()
