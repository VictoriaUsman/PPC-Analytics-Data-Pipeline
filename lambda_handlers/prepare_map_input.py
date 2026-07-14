"""Step Functions entrypoint: the state machine's first state.

Expands config/profiles.yaml into the flat list of {profile_id, ad_product, ...} items the
Map state in statemachine/ads_ingestion.asl.json fans out over, and computes the shared
rolling report-date window (see common/scheduling.py) once, anchored on the EventBridge
event that triggered this execution.

Not every profile runs all three ad products (see README's Open Items) -- this expands
per-profile ad_products lists rather than assuming a fixed 26x3 grid.

Input: the raw EventBridge scheduled event.
Output: {"start_date", "until_date", "items": [{"profile_id", "region", "secret_name",
         "ad_product", "start_date", "until_date"}, ...]}
"""

import os

import yaml

from common.logging_config import get_logger, log_fields
from common.scheduling import scheduled_window

logger = get_logger(__name__)

PROFILES_CONFIG_PATH = os.environ.get("PROFILES_CONFIG_PATH", "config/profiles.yaml")
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "30"))


def _load_profiles() -> list[dict]:
    with open(PROFILES_CONFIG_PATH) as f:
        return yaml.safe_load(f)["profiles"]


def handler(event, context):
    start_date, until_date = scheduled_window(event, lookback_days=LOOKBACK_DAYS)

    items = [
        {
            "profile_id": profile["profile_id"],
            "region": profile["region"],
            "secret_name": profile["secret_name"],
            "ad_product": ad_product,
            "start_date": start_date.isoformat(),
            "until_date": until_date.isoformat(),
        }
        for profile in _load_profiles()
        for ad_product in profile["ad_products"]
    ]

    logger.info(
        "prepared ingestion map input",
        extra=log_fields(item_count=len(items), start_date=start_date.isoformat(), until_date=until_date.isoformat()),
    )
    return {"start_date": start_date.isoformat(), "until_date": until_date.isoformat(), "items": items}
