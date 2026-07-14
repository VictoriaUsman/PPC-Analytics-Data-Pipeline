"""Derives the report date window from the triggering EventBridge event.

This is the crux of ingestion idempotency, same role as the reference POS Pipeline's
common/scheduling.py -- but a different shape. The reference computes a delta window
(since the last run) because POS orders only ever get created/updated once and settle
quickly. Amazon Ads attribution does not settle that fast: click-attribution windows run
7-14 days, with restatement checkpoints out to ~28 days after that (see README's
"Attribution lookback" section) -- so a "since last run" delta would silently miss
conversions that get attributed to a campaign days after the click. Instead, every run
re-pulls the same rolling `lookback_days`-day window and relies on the curated layer's
MERGE (keyed on profile_id/ad_product/campaign_id/report_date) to update revised rows in
place -- see redshift/merge_fct_campaign_performance.sql.
"""

from datetime import date, datetime, timedelta, timezone

DEFAULT_LOOKBACK_DAYS = 30


def scheduled_window(event: dict, lookback_days: int = DEFAULT_LOOKBACK_DAYS) -> tuple[date, date]:
    """Return (start, until) inclusive date bounds for the reporting window.

    Anchored on the EventBridge schedule's fixed `event["time"]`, not wall-clock
    `datetime.now()`, so a Step Functions Task retry re-invoking with the same triggering
    event recomputes the identical window instead of a shifted one -- same idempotency
    reasoning as the reference pipeline's scheduled_window().

    `until` is yesterday (UTC) relative to the anchor: the current day is still
    accumulating clicks/conversions and isn't a meaningful report day yet.

    Caveat: a manual/test invocation with no `event["time"]` falls back to
    `datetime.now(timezone.utc)` and is NOT retry-safe -- fine for local testing, not for a
    production trigger path.
    """
    anchor = _anchor_time(event)
    until = anchor.date() - timedelta(days=1)
    start = until - timedelta(days=lookback_days - 1)
    return start, until


def _anchor_time(event: dict) -> datetime:
    raw = event.get("time") if isinstance(event, dict) else None
    if raw:
        return datetime.strptime(raw, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc)


def dates_in_window(start: date, until: date):
    """Yield every date in [start, until], inclusive -- one per bronze/silver partition."""
    if until < start:
        raise ValueError(f"until ({until}) precedes start ({start})")
    current = start
    while current <= until:
        yield current
        current += timedelta(days=1)
