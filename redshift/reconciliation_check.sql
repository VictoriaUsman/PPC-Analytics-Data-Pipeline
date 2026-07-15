-- Data validation: compares staging_campaign_performance (source -- freshly TRUNCATE+COPY'd from
-- the full silver/ zone this run, see merge_fct_campaign_performance.sql) against
-- fct_campaign_performance (target), scoped to the date range staging actually covers.
--
-- Runs as the ReconciliationCheckFunction Lambda's query, invoked as the ReconciliationCheck task
-- in statemachine/redshift_load.asl.json immediately after MergeIntoFact/IsMergeDone confirms
-- FINISHED. Because MergeIntoFact updates unconditionally on match (no recency check -- see that
-- file's header comment), a clean run means these two sides come out byte-for-byte equal on every
-- measure; any mismatch means the load silently lost or duplicated rows and is worth a human
-- looking at, not a normal day-to-day occurrence to expect.
--
-- One row out, with a source_/target_ pair of columns per measure -- lambda_handlers/
-- reconciliation_check.py parses this by column name rather than position.

SELECT
    (SELECT COUNT(*) FROM staging_campaign_performance) AS source_row_count,
    (SELECT COUNT(*) FROM fct_campaign_performance f
        WHERE f.report_date BETWEEN (SELECT MIN(report_date) FROM staging_campaign_performance)
                                 AND (SELECT MAX(report_date) FROM staging_campaign_performance)
    ) AS target_row_count,
    (SELECT COALESCE(SUM(impressions), 0) FROM staging_campaign_performance) AS source_impressions,
    (SELECT COALESCE(SUM(f.impressions), 0) FROM fct_campaign_performance f
        WHERE f.report_date BETWEEN (SELECT MIN(report_date) FROM staging_campaign_performance)
                                 AND (SELECT MAX(report_date) FROM staging_campaign_performance)
    ) AS target_impressions,
    (SELECT COALESCE(SUM(clicks), 0) FROM staging_campaign_performance) AS source_clicks,
    (SELECT COALESCE(SUM(f.clicks), 0) FROM fct_campaign_performance f
        WHERE f.report_date BETWEEN (SELECT MIN(report_date) FROM staging_campaign_performance)
                                 AND (SELECT MAX(report_date) FROM staging_campaign_performance)
    ) AS target_clicks,
    (SELECT COALESCE(SUM(cost), 0) FROM staging_campaign_performance) AS source_cost,
    (SELECT COALESCE(SUM(f.cost), 0) FROM fct_campaign_performance f
        WHERE f.report_date BETWEEN (SELECT MIN(report_date) FROM staging_campaign_performance)
                                 AND (SELECT MAX(report_date) FROM staging_campaign_performance)
    ) AS target_cost,
    (SELECT COALESCE(SUM(purchases_14d), 0) FROM staging_campaign_performance) AS source_purchases_14d,
    (SELECT COALESCE(SUM(f.purchases_14d), 0) FROM fct_campaign_performance f
        WHERE f.report_date BETWEEN (SELECT MIN(report_date) FROM staging_campaign_performance)
                                 AND (SELECT MAX(report_date) FROM staging_campaign_performance)
    ) AS target_purchases_14d,
    (SELECT COALESCE(SUM(sales_14d), 0) FROM staging_campaign_performance) AS source_sales_14d,
    (SELECT COALESCE(SUM(f.sales_14d), 0) FROM fct_campaign_performance f
        WHERE f.report_date BETWEEN (SELECT MIN(report_date) FROM staging_campaign_performance)
                                 AND (SELECT MAX(report_date) FROM staging_campaign_performance)
    ) AS target_sales_14d;
