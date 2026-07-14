-- Redshift Serverless curated schema for Amazon Ads campaign performance.
--
-- staging_campaign_performance is TRUNCATE+COPY'd from s3://<bucket>/silver/ on every run of
-- redshift_load.asl.json (see statemachine/redshift_load.asl.json) -- silver/ itself holds the
-- full history (each report_date's file is idempotently overwritten in place by
-- glue_jobs/bronze_to_silver.py via common/s3_paths.swap_zone, not appended), so staging always
-- mirrors the complete known-truth state, including any attribution restatements picked up by the
-- rolling 30-day re-pull window. See merge_fct_campaign_performance.sql for why the fact merge
-- updates unconditionally on match rather than checking a recency column, unlike the reference
-- POS Pipeline's BigQuery merges (bigquery/merge_fct_orders.sql etc.).
--
-- Run once (or via a migration tool) before the pipeline's first load. dim_profile must be seeded
-- separately from config/profiles.yaml (account_name/marketplace/region aren't present in ad
-- report data) -- see README. Redshift primary/foreign keys are informational only (not enforced
-- at write time), so an unseeded dim_profile row never blocks a load -- it only affects downstream
-- joins/BI tooling until seeded.

CREATE TABLE IF NOT EXISTS dim_profile (
    profile_id      VARCHAR(32)  NOT NULL,
    account_name    VARCHAR(256) NOT NULL,
    marketplace     VARCHAR(16)  NOT NULL,
    region          VARCHAR(4)   NOT NULL,
    PRIMARY KEY (profile_id)
);

CREATE TABLE IF NOT EXISTS dim_ad_product (
    ad_product      VARCHAR(32) NOT NULL,
    PRIMARY KEY (ad_product)
);

INSERT INTO dim_ad_product (ad_product)
SELECT v.ad_product
FROM (VALUES ('SPONSORED_PRODUCTS'), ('SPONSORED_BRANDS'), ('SPONSORED_DISPLAY')) AS v(ad_product)
WHERE NOT EXISTS (SELECT 1 FROM dim_ad_product WHERE dim_ad_product.ad_product = v.ad_product);

-- Populated incrementally by merge_fct_campaign_performance.sql from the report_dates it finds in
-- staging -- no separate maintenance job needed.
CREATE TABLE IF NOT EXISTS dim_date (
    date_key    DATE     NOT NULL,
    year        SMALLINT NOT NULL,
    month       SMALLINT NOT NULL,
    day         SMALLINT NOT NULL,
    PRIMARY KEY (date_key)
);

CREATE TABLE IF NOT EXISTS staging_campaign_performance (
    profile_id      VARCHAR(32)   NOT NULL,
    ad_product      VARCHAR(32)   NOT NULL,
    campaign_id     VARCHAR(64)   NOT NULL,
    campaign_name   VARCHAR(512),
    report_date     DATE          NOT NULL,
    impressions     BIGINT        NOT NULL,
    clicks          BIGINT        NOT NULL,
    cost            DECIMAL(18,4) NOT NULL,
    purchases_14d   BIGINT,
    sales_14d       DECIMAL(18,4)
);

CREATE TABLE IF NOT EXISTS fct_campaign_performance (
    profile_id      VARCHAR(32)   NOT NULL REFERENCES dim_profile(profile_id),
    ad_product      VARCHAR(32)   NOT NULL REFERENCES dim_ad_product(ad_product),
    campaign_id     VARCHAR(64)   NOT NULL,
    campaign_name   VARCHAR(512),
    report_date     DATE          NOT NULL REFERENCES dim_date(date_key),
    impressions     BIGINT        NOT NULL,
    clicks          BIGINT        NOT NULL,
    cost            DECIMAL(18,4) NOT NULL,
    purchases_14d   BIGINT,
    sales_14d       DECIMAL(18,4),
    PRIMARY KEY (profile_id, ad_product, campaign_id, report_date)
)
DISTSTYLE KEY
DISTKEY (profile_id)
SORTKEY (report_date, profile_id, ad_product);
