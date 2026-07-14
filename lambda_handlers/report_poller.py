"""Step Functions poll-loop entrypoint: check one report's current status.

Invoked repeatedly by the Wait/Choice loop in statemachine/ads_ingestion.asl.json until
status is COMPLETED or FAILED, or the state machine's own elapsed-time guard trips (Amazon's
reports have been observed stuck in PENDING indefinitely with no confirmed root cause --
see README's Open Items -- so the timeout lives in the state machine, not here).

Input: prior state merged with {"report_id"}
Output: input merged with {"report_status", "download_url", "failure_reason"}
"""

import os

from common.logging_config import get_logger, log_fields
from common.secrets import get_access_token
from connectors.ads_connector import SponsoredAdsConnector

logger = get_logger(__name__)

CLIENT_ID = os.environ["ADS_LWA_CLIENT_ID"]


def handler(event, context):
    access_token = get_access_token(event["secret_name"])
    connector = SponsoredAdsConnector(event["profile_id"], event["region"], access_token, CLIENT_ID)
    status = connector.poll_report(event["report_id"])

    logger.info(
        "polled report status",
        extra=log_fields(
            profile_id=event["profile_id"],
            report_id=event["report_id"],
            status=status.status,
        ),
    )
    return {
        **event,
        "report_status": status.status,
        "download_url": status.download_url,
        "failure_reason": status.failure_reason,
    }
