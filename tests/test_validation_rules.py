from validation.rules import detect_new_fields, validate_record

AD_PRODUCT = "SPONSORED_PRODUCTS"

VALID_RECORD = {
    "date": "2026-07-01",
    "campaignId": "111222333",
    "campaignName": "brand-1-us-sp-auto",
    "impressions": 1000,
    "clicks": 12,
    "cost": "4.50",
    "purchases14d": 2,
    "sales14d": 39.98,
}


def test_valid_record_passes():
    assert validate_record(VALID_RECORD, AD_PRODUCT) is None


def test_missing_required_field_rejected():
    record = {**VALID_RECORD}
    del record["impressions"]
    reason = validate_record(record, AD_PRODUCT)
    assert reason == "missing required field: impressions"


def test_empty_string_required_field_rejected():
    record = {**VALID_RECORD, "campaignId": ""}
    reason = validate_record(record, AD_PRODUCT)
    assert reason == "missing required field: campaignId"


def test_invalid_date_rejected():
    record = {**VALID_RECORD, "date": "07/01/2026"}
    reason = validate_record(record, AD_PRODUCT)
    assert reason is not None
    assert "invalid date" in reason


def test_negative_cost_rejected():
    record = {**VALID_RECORD, "cost": "-1.00"}
    reason = validate_record(record, AD_PRODUCT)
    assert reason is not None
    assert "cost" in reason


def test_non_numeric_clicks_rejected():
    record = {**VALID_RECORD, "clicks": "not-a-number"}
    reason = validate_record(record, AD_PRODUCT)
    assert reason is not None
    assert "clicks" in reason


def test_detect_new_fields_empty_for_known_schema():
    assert detect_new_fields(VALID_RECORD, AD_PRODUCT) == set()


def test_detect_new_fields_flags_unknown_key():
    record = {**VALID_RECORD, "newAttributionMetric": 5}
    assert detect_new_fields(record, AD_PRODUCT) == {"newAttributionMetric"}
