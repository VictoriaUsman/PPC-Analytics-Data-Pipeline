-- SCD Type 2 insert, step 2 of 2, for fct_campaign_performance_history: insert a fresh current
-- row for any (profile_id, ad_product, campaign_id, report_date) with no open row -- either the
-- first time that campaign-day has ever been loaded, or one just closed by the companion UPDATE
-- because this run's measures disagree with what was previously recorded.
--
-- Must run second, after scd2_fct_campaign_performance_close.sql in the same call -- a
-- just-closed row must have no open row left when this INSERT's LEFT JOIN runs, otherwise the
-- revision would be skipped instead of getting a new current row.

INSERT INTO fct_campaign_performance_history (
    profile_id, ad_product, campaign_id, campaign_name, report_date,
    impressions, clicks, cost, purchases_14d, sales_14d,
    valid_from, valid_to, is_current
)
SELECT DISTINCT
    s.profile_id, s.ad_product, s.campaign_id, s.campaign_name, s.report_date,
    s.impressions, s.clicks, s.cost, s.purchases_14d, s.sales_14d,
    CURRENT_DATE, NULL, TRUE
FROM staging_campaign_performance s
LEFT JOIN fct_campaign_performance_history h
       ON h.profile_id  = s.profile_id
      AND h.ad_product  = s.ad_product
      AND h.campaign_id = s.campaign_id
      AND h.report_date = s.report_date
      AND h.is_current  = TRUE
WHERE h.campaign_id IS NULL;
