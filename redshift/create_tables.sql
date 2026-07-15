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
--
-- dim_profile and dim_campaign are both SCD Type 2 (see scd2_dim_profile.sql /
-- scd2_dim_campaign.sql and README's "Slowly Changing Dimensions" section for why this is
-- hand-rolled SQL rather than a dbt snapshot): a rename/reassignment produces a new *_current
-- row rather than overwriting history in place. Because a profile_id/campaign_id can therefore
-- appear on more than one dim_profile/dim_campaign row over time, neither table's natural key is
-- declared PRIMARY KEY/UNIQUE, and fct_campaign_performance's FK below targets dim_ad_product
-- only, not dim_profile -- joins against current attributes should go through the
-- dim_profile_current/dim_campaign_current views instead.

CREATE TABLE IF NOT EXISTS dim_profile (
    dim_profile_key BIGINT IDENTITY(1,1),
    profile_id      VARCHAR(32)  NOT NULL,
    account_name    VARCHAR(256) NOT NULL,
    marketplace     VARCHAR(16)  NOT NULL,
    region          VARCHAR(4)   NOT NULL,
    valid_from      DATE         NOT NULL,
    valid_to        DATE,
    is_current      BOOLEAN      NOT NULL,
    PRIMARY KEY (dim_profile_key)
)
DISTSTYLE ALL
SORTKEY (profile_id);

-- Scratch table an operator (re-)populates from config/profiles.yaml before running
-- scd2_dim_profile.sql -- see README's "Slowly Changing Dimensions" section for why this seed
-- step stays manual rather than joining the automated redshift_load state machine.
CREATE TABLE IF NOT EXISTS staging_profile (
    profile_id      VARCHAR(32)  NOT NULL,
    account_name    VARCHAR(256) NOT NULL,
    marketplace     VARCHAR(16)  NOT NULL,
    region          VARCHAR(4)   NOT NULL
);

CREATE VIEW dim_profile_current AS
SELECT dim_profile_key, profile_id, account_name, marketplace, region, valid_from
FROM dim_profile
WHERE is_current = TRUE;

-- Populated by scd2_dim_campaign.sql from staging_campaign_performance on every
-- redshift_load run, ahead of MergeIntoFact -- see statemachine/redshift_load.asl.json.
CREATE TABLE IF NOT EXISTS dim_campaign (
    dim_campaign_key BIGINT IDENTITY(1,1),
    profile_id       VARCHAR(32)   NOT NULL,
    ad_product       VARCHAR(32)   NOT NULL,
    campaign_id      VARCHAR(64)   NOT NULL,
    campaign_name    VARCHAR(512),
    valid_from       DATE          NOT NULL,
    valid_to         DATE,
    is_current       BOOLEAN       NOT NULL,
    PRIMARY KEY (dim_campaign_key)
)
DISTSTYLE ALL
SORTKEY (profile_id, ad_product, campaign_id);

CREATE VIEW dim_campaign_current AS
SELECT dim_campaign_key, profile_id, ad_product, campaign_id, campaign_name, valid_from
FROM dim_campaign
WHERE is_current = TRUE;

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
    profile_id      VARCHAR(32)   NOT NULL,
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

-- Companion to fct_campaign_performance: the fact table above is overwritten unconditionally
-- on match (see merge_fct_campaign_performance.sql), so a rolling-window re-pull that revises a
-- day's measures (e.g. an Amazon attribution-window restatement) leaves no trace of what was
-- reported before. This table keeps every prior version of a (profile_id, ad_product,
-- campaign_id, report_date) row whenever its measures actually change -- see
-- scd2_fct_campaign_performance_close.sql / scd2_fct_campaign_performance_insert.sql -- so BI can
-- answer "what did we report for this campaign-day at different points in time," not just "what's
-- true now." Change-detected rather than populated on every run: an unchanged day (the common
-- case for most of the 30-day rolling window on most runs) accumulates no history rows.
CREATE TABLE IF NOT EXISTS fct_campaign_performance_history (
    profile_id      VARCHAR(32)   NOT NULL,
    ad_product      VARCHAR(32)   NOT NULL REFERENCES dim_ad_product(ad_product),
    campaign_id     VARCHAR(64)   NOT NULL,
    campaign_name   VARCHAR(512),
    report_date     DATE          NOT NULL REFERENCES dim_date(date_key),
    impressions     BIGINT        NOT NULL,
    clicks          BIGINT        NOT NULL,
    cost            DECIMAL(18,4) NOT NULL,
    purchases_14d   BIGINT,
    sales_14d       DECIMAL(18,4),
    valid_from      DATE          NOT NULL,
    valid_to        DATE,
    is_current      BOOLEAN       NOT NULL,
    PRIMARY KEY (profile_id, ad_product, campaign_id, report_date, valid_from)
)
DISTSTYLE KEY
DISTKEY (profile_id)
SORTKEY (report_date, profile_id, ad_product);
