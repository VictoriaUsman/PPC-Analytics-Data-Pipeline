"""Single source of truth for per-ad-product record validation.

Same role as the reference POS Pipeline's validation/rules.py: `validate_record()` is a
pure function (no exceptions, no side effects -- returns a reason string or None), and
`detect_new_fields()` is a separate concern from validity -- schema drift is tracked, not
gated on.

Column names below match connectors/ads_connector.py's COLUMNS_BY_AD_PRODUCT request, which
is itself flagged there as assembled from Amazon's v3 migration guide rather than verified
against live sandbox data -- confirm both together before relying on this in production.
"""

from datetime import date

REQUIRED_FIELDS = {
    "SPONSORED_PRODUCTS": ("date", "campaignId", "impressions", "clicks", "cost"),
    "SPONSORED_BRANDS": ("date", "campaignId", "impressions", "clicks", "cost"),
    "SPONSORED_DISPLAY": ("date", "campaignId", "impressions", "clicks", "cost"),
}

KNOWN_FIELDS = {
    "SPONSORED_PRODUCTS": {
        "date", "campaignId", "campaignName", "impressions", "clicks", "cost",
        "purchases14d", "sales14d",
    },
    "SPONSORED_BRANDS": {
        "date", "campaignId", "campaignName", "impressions", "clicks", "cost",
        "purchases14d", "sales14d",
    },
    "SPONSORED_DISPLAY": {
        "date", "campaignId", "campaignName", "impressions", "clicks", "cost",
        "purchases14d", "sales14d",
    },
}

# Fields needing more than presence/non-emptiness -- type/range checked so a malformed value
# (a negative cost, a non-numeric impressions count) is rejected rather than reaching silver/.
NUMERIC_FIELD_CHECKS = {
    "SPONSORED_PRODUCTS": ("impressions", "clicks", "cost"),
    "SPONSORED_BRANDS": ("impressions", "clicks", "cost"),
    "SPONSORED_DISPLAY": ("impressions", "clicks", "cost"),
}


def _is_non_negative_number(value) -> bool:
    try:
        return float(value) >= 0
    except (TypeError, ValueError):
        return False


def _is_valid_report_date(value) -> bool:
    try:
        date.fromisoformat(value)
        return True
    except (TypeError, ValueError):
        return False


def validate_record(record: dict, ad_product: str) -> str | None:
    """Return a validation-failure reason, or None if the record is valid.

    Beyond presence, `date` gets an explicit format check and the numeric fields in
    NUMERIC_FIELD_CHECKS get a non-negative-number check -- a present-but-malformed value
    (e.g. a negative cost, or a date that isn't real ISO-8601) is rejected rather than
    silently reaching silver/. No referential checks, and no type/range checks on
    non-required fields.
    """
    for field in REQUIRED_FIELDS[ad_product]:
        if record.get(field) in (None, ""):
            return f"missing required field: {field}"

    if not _is_valid_report_date(record["date"]):
        return f"invalid date: {record.get('date')!r}"

    for field in NUMERIC_FIELD_CHECKS[ad_product]:
        if field == "date":
            continue
        if not _is_non_negative_number(record[field]):
            return f"invalid non-negative numeric field {field}: {record.get(field)!r}"

    return None


def detect_new_fields(record: dict, ad_product: str) -> set:
    """Diff a record's top-level keys against the KNOWN_FIELDS baseline for this ad product.

    Not a validation gate -- an unrecognized field doesn't reject the record, it's a
    drift signal surfaced via the RejectedRatio/NewFieldCount CloudWatch metrics in
    glue_jobs/bronze_to_silver.py. Catches unknown fields *appearing*; it cannot catch a
    field keeping its name while silently changing meaning/scale (would need explicit
    type/range checks added here once observed).
    """
    return set(record.keys()) - KNOWN_FIELDS[ad_product]
