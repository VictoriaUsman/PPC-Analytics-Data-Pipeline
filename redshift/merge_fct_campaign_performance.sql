-- Refreshes dim_date from any new report_dates in the current staging load, then upserts
-- staging_campaign_performance into fct_campaign_performance.
--
-- Runs as the MergeIntoFact task in statemachine/redshift_load.asl.json (redshift-data
-- executeStatement), immediately after CopyIntoStaging (batchExecuteStatement: TRUNCATE +
-- COPY from s3://<bucket>/silver/) succeeds.
--
-- staging_campaign_performance is TRUNCATE+reloaded from the *entire* silver/ zone every run, not
-- just the current run's 30-day window -- silver/ is idempotently overwritten per report_date (see
-- glue_jobs/bronze_to_silver.py + common/s3_paths.swap_zone), so staging always reflects the
-- complete known-truth state, including any attribution restatements. That's why this merge
-- updates unconditionally on match, unlike the reference POS Pipeline's BigQuery merges
-- (bigquery/merge_fct_orders.sql etc.), which gate the update on an `updated_at` recency column --
-- there's no "loaded more than once, keep the newest" scenario here since staging isn't
-- append-only.

INSERT INTO dim_date (date_key, year, month, day)
SELECT DISTINCT
    s.report_date,
    DATE_PART(year, s.report_date)::SMALLINT,
    DATE_PART(month, s.report_date)::SMALLINT,
    DATE_PART(day, s.report_date)::SMALLINT
FROM staging_campaign_performance s
WHERE NOT EXISTS (
    SELECT 1 FROM dim_date d WHERE d.date_key = s.report_date
);

MERGE INTO fct_campaign_performance
USING staging_campaign_performance AS source
ON fct_campaign_performance.profile_id = source.profile_id
   AND fct_campaign_performance.ad_product = source.ad_product
   AND fct_campaign_performance.campaign_id = source.campaign_id
   AND fct_campaign_performance.report_date = source.report_date
WHEN MATCHED THEN
    UPDATE SET
        campaign_name = source.campaign_name,
        impressions   = source.impressions,
        clicks        = source.clicks,
        cost          = source.cost,
        purchases_14d = source.purchases_14d,
        sales_14d     = source.sales_14d
WHEN NOT MATCHED THEN
    INSERT (
        profile_id, ad_product, campaign_id, campaign_name, report_date,
        impressions, clicks, cost, purchases_14d, sales_14d
    )
    VALUES (
        source.profile_id, source.ad_product, source.campaign_id, source.campaign_name,
        source.report_date, source.impressions, source.clicks, source.cost,
        source.purchases_14d, source.sales_14d
    );
