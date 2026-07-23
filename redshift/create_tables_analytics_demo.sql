-- Companion tables for the ads-performance analytics dashboard, additive to create_tables.sql.
--
-- Neither table is written by the production pipeline (ads_ingestion / redshift_load) -- the
-- Sponsored Ads reporting API has no bid or total-account-sales fields (see
-- lambda_handlers/*, connectors/ads_connector.py), so both stay demo/seed-only until a real
-- source for them exists:
--   - dim_campaign_bid: per-campaign target CPC, needed for a bid-vs-actual-CPC efficiency view.
--     Kept as a separate table rather than a new dim_campaign column because a bid isn't part of
--     dim_campaign's existing SCD Type 2 tracking (campaign_name only) -- adding it there would
--     mean versioning a new dim_campaign row on every bid change, which nothing here needs yet.
--   - fct_account_daily_sales: total account sales (organic + all ad channels), needed for TACoS
--     (ad spend / total sales) as distinct from ACoS (ad spend / ad-attributed sales_14d).
--     fct_campaign_performance only carries the latter.
--
-- Populated by seed/generate_seed_data.py -> redshift/seed_data.sql for local/dev use.

CREATE TABLE IF NOT EXISTS dim_campaign_bid (
    profile_id      VARCHAR(32)   NOT NULL,
    ad_product      VARCHAR(32)   NOT NULL,
    campaign_id     VARCHAR(64)   NOT NULL,
    target_cpc      DECIMAL(10,2) NOT NULL,
    PRIMARY KEY (profile_id, ad_product, campaign_id)
)
DISTSTYLE ALL
SORTKEY (profile_id, ad_product, campaign_id);

CREATE TABLE IF NOT EXISTS fct_account_daily_sales (
    profile_id      VARCHAR(32)   NOT NULL,
    report_date     DATE          NOT NULL REFERENCES dim_date(date_key),
    total_sales     DECIMAL(18,4) NOT NULL,
    PRIMARY KEY (profile_id, report_date)
)
DISTSTYLE KEY
DISTKEY (profile_id)
SORTKEY (report_date, profile_id);
