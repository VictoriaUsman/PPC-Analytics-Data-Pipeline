-- SCD Type 2 upsert of dim_campaign, step 1 of 2: close out stale current rows.
--
-- Runs as the ScdDimCampaign task in statemachine/redshift_load.asl.json (redshift-data
-- batchExecuteStatement, first element of Sqls), after CopyIntoStaging succeeds and before
-- MergeIntoFact -- dimensions load before the fact that references them. Paired with
-- scd2_dim_campaign_insert.sql (second element of Sqls); see that file for why this is two
-- statements and not a single MERGE.
--
-- staging_campaign_performance is TRUNCATE+reloaded from the entire silver/ zone every run (see
-- merge_fct_campaign_performance.sql), so it always reflects the complete known-truth state of
-- every campaign_id's current name.

UPDATE dim_campaign
SET is_current = FALSE,
    valid_to   = CURRENT_DATE
FROM (
    SELECT DISTINCT profile_id, ad_product, campaign_id, campaign_name
    FROM staging_campaign_performance
) s
WHERE dim_campaign.profile_id    = s.profile_id
  AND dim_campaign.ad_product    = s.ad_product
  AND dim_campaign.campaign_id   = s.campaign_id
  AND dim_campaign.is_current    = TRUE
  AND dim_campaign.campaign_name IS DISTINCT FROM s.campaign_name;
