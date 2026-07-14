-- SCD Type 2 upsert of dim_campaign from staging_campaign_performance.
--
-- Runs as the ScdDimCampaign task in statemachine/redshift_load.asl.json (redshift-data
-- batchExecuteStatement), after CopyIntoStaging succeeds and before MergeIntoFact -- dimensions
-- load before the fact that references them. staging_campaign_performance is TRUNCATE+reloaded
-- from the entire silver/ zone every run (see merge_fct_campaign_performance.sql), so it always
-- reflects the complete known-truth state of every campaign_id's current name.
--
-- Two statements, not a single MERGE: Redshift's MERGE can only express "update in place" or
-- "insert new row" per matched key, not "close the old row and insert a fresh one," which is what
-- SCD Type 2 needs whenever campaign_name has changed. Step 1 closes out any current row whose
-- name no longer matches staging; step 2 inserts a fresh current row for anything that has no
-- open row left after step 1 -- either a brand-new campaign_id, or one just closed for a rename.

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

INSERT INTO dim_campaign (
    profile_id, ad_product, campaign_id, campaign_name, valid_from, valid_to, is_current
)
SELECT DISTINCT
    s.profile_id, s.ad_product, s.campaign_id, s.campaign_name, CURRENT_DATE, NULL, TRUE
FROM staging_campaign_performance s
LEFT JOIN dim_campaign d
       ON d.profile_id  = s.profile_id
      AND d.ad_product  = s.ad_product
      AND d.campaign_id = s.campaign_id
      AND d.is_current  = TRUE
WHERE d.campaign_id IS NULL;
