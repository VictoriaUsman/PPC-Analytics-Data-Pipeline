"""Step Functions Map-branch entrypoint: request one (profile, ad_product) report.

Thin wrapper, same shape as the reference pipeline's per-vendor Lambda handlers -- resolves
credentials, constructs the connector, calls one connector method, returns. All fan-out
(26 profiles x up to 3 ad products) and the poll loop live in
statemachine/ads_ingestion.asl.json, not in this Lambda.

Input (one Map iteration item):
  {"profile_id", "region", "secret_name", "ad_product", "start_date", "until_date"}
Output: input merged with {"report_id"}
"""

import os
from datetime import date

from common.logging_config import get_logger, log_fields
from common.secrets import get_access_token
from connectors.ads_connector import SponsoredAdsConnector

logger = get_logger(__name__)

CLIENT_ID = os.environ["ADS_LWA_CLIENT_ID"]


def handler(event, context):
    profile_id = event["profile_id"]
    ad_product = event["ad_product"]
    start_date = date.fromisoformat(event["start_date"])
    until_date = date.fromisoformat(event["until_date"])

    access_token = get_access_token(event["secret_name"])
    connector = SponsoredAdsConnector(profile_id, event["region"], access_token, CLIENT_ID)
    report_id = connector.create_report(ad_product, start_date, until_date)

    logger.info(
        "requested report",
        extra=log_fields(profile_id=profile_id, ad_product=ad_product, report_id=report_id),
    )
    return {**event, "report_id": report_id}
