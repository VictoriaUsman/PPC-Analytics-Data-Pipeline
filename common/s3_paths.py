"""Single shared S3 key/partition-scheme builder.

Mirrors the reference POS Pipeline's common/s3_paths.py: every writer (Lambda downloader,
Glue validation job, Redshift load step) routes through object_key() so bronze/silver/
rejected line up under identical partitions and the Glue Data Catalog / Redshift COPY can
rely on one fixed layout. Unlike the reference, bronze_to_silver here also calls this
function directly (rather than a raw zone-prefix string .replace()) to avoid the drift the
reference repo's own report flagged as a shortcut.
"""

from datetime import date

ZONES = ("bronze", "silver", "rejected")
AD_PRODUCTS = ("SPONSORED_PRODUCTS", "SPONSORED_BRANDS", "SPONSORED_DISPLAY")


def object_key(
    zone: str, ad_product: str, profile_id: str, report_date: date, report_id: str, part: int
) -> str:
    """Build a Hive-style partitioned S3 key.

    Partitioned by ad_product/profile_id/year/month/day so Glue crawlers auto-sync schema
    per partition and Athena/Redshift Spectrum-style pruning works on any of those
    dimensions. Keyed by report_date (the logical day the report covers), not wall-clock
    write time, so a rerun of the same day's report overwrites the same key rather than
    writing a duplicate — see common/scheduling.py for why report_date is the right anchor
    given Amazon's attribution-lookback re-pull window.

    `part` is a flush-batch index (see connectors.base.download_and_stream_report, which
    flushes rows to S3 in bounded batches rather than buffering a whole report in memory) --
    always present, even when a report_date ends up with only one part, since whether a
    given day needs more than one part isn't known until the whole report has been read.
    Retrying the same report download reproduces the same batches in the same order, so
    parts stay 1:1 with the same keys rather than drifting into duplicates.
    """
    if zone not in ZONES:
        raise ValueError(f"unknown zone: {zone!r} (expected one of {ZONES})")
    if ad_product not in AD_PRODUCTS:
        raise ValueError(f"unknown ad_product: {ad_product!r} (expected one of {AD_PRODUCTS})")

    return (
        f"{zone}/ad_product={ad_product}/profile_id={profile_id}/"
        f"year={report_date.year:04d}/month={report_date.month:02d}/day={report_date.day:02d}/"
        f"report_{report_id}_part{part:04d}.json"
    )


def swap_zone(key: str, new_zone: str) -> str:
    """Derive a silver/rejected key from a bronze key (or vice versa) by swapping the
    leading zone segment.

    The reference pipeline's bronze_to_silver.py did this as an ad hoc `key.replace(...)`
    rather than through its shared s3_paths helper -- its own retrospective flagged that as
    a discipline gap. This function is the one sanctioned way to do that swap here, so every
    caller (Glue validation job, any future reprocessing tool) stays consistent even though
    the transformation is simple.
    """
    if new_zone not in ZONES:
        raise ValueError(f"unknown zone: {new_zone!r} (expected one of {ZONES})")
    current_zone = key.split("/", 1)[0]
    if current_zone not in ZONES:
        raise ValueError(f"key {key!r} doesn't start with a known zone segment")
    return new_zone + key[len(current_zone):]


def zone_prefix(zone: str, ad_product: str | None = None, profile_id: str | None = None) -> str:
    """Prefix for listing a zone (optionally scoped to one ad_product/profile) — used by the
    Glue validation job to enumerate a day's bronze objects without a fixed-depth assumption.
    """
    if zone not in ZONES:
        raise ValueError(f"unknown zone: {zone!r} (expected one of {ZONES})")
    parts = [zone]
    if ad_product is not None:
        parts.append(f"ad_product={ad_product}")
    if profile_id is not None:
        parts.append(f"profile_id={profile_id}")
    return "/".join(parts) + "/"
