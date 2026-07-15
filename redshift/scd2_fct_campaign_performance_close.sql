-- SCD Type 2 close, step 1 of 2, for fct_campaign_performance_history: close out the current
-- history row for any (profile_id, ad_product, campaign_id, report_date) whose measures this
-- run's staging data disagrees with -- i.e. an attribution-window revision. Paired with
-- scd2_fct_campaign_performance_insert.sql; see that file for why this is two statements and not
-- a single MERGE.
--
-- Runs as the ScdFctCampaignPerformanceHistory task in statemachine/redshift_load.asl.json,
-- after ScdDimCampaign and before MergeIntoFact -- the "old" value must be captured here before
-- MergeIntoFact overwrites fct_campaign_performance in place.
--
-- Change-detected (IS DISTINCT FROM across every measure), unlike merge_fct_campaign_performance
-- .sql's unconditional-on-match fact upsert -- staging_campaign_performance reflects the full
-- 30-day rolling window on every run (see that file), so an unconditional close+insert here would
-- version a new no-op history row for every campaign-day it revisits on every run, even when
-- nothing actually changed.

UPDATE fct_campaign_performance_history
SET is_current = FALSE,
    valid_to   = CURRENT_DATE
FROM (
    SELECT DISTINCT
        profile_id, ad_product, campaign_id, campaign_name, report_date,
        impressions, clicks, cost, purchases_14d, sales_14d
    FROM staging_campaign_performance
) s
WHERE fct_campaign_performance_history.profile_id  = s.profile_id
  AND fct_campaign_performance_history.ad_product  = s.ad_product
  AND fct_campaign_performance_history.campaign_id = s.campaign_id
  AND fct_campaign_performance_history.report_date = s.report_date
  AND fct_campaign_performance_history.is_current  = TRUE
  AND (
        fct_campaign_performance_history.campaign_name IS DISTINCT FROM s.campaign_name
     OR fct_campaign_performance_history.impressions   IS DISTINCT FROM s.impressions
     OR fct_campaign_performance_history.clicks        IS DISTINCT FROM s.clicks
     OR fct_campaign_performance_history.cost          IS DISTINCT FROM s.cost
     OR fct_campaign_performance_history.purchases_14d IS DISTINCT FROM s.purchases_14d
     OR fct_campaign_performance_history.sales_14d     IS DISTINCT FROM s.sales_14d
      );
