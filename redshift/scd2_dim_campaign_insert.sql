-- SCD Type 2 upsert of dim_campaign, step 2 of 2: insert fresh current rows.
--
-- Runs as the ScdDimCampaign task in statemachine/redshift_load.asl.json (redshift-data
-- batchExecuteStatement, second element of Sqls), immediately after
-- scd2_dim_campaign_close.sql in the same call -- must run second so a just-renamed
-- campaign_id has no open row left when this INSERT's LEFT JOIN runs, otherwise it would be
-- skipped instead of getting a new current row.
--
-- Picks up anything with no open (is_current = TRUE) row: either a brand-new campaign_id, or
-- one just closed by the companion UPDATE for a rename.

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
