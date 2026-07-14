-- SCD Type 2 upsert of dim_profile from staging_profile.
--
-- Unlike scd2_dim_campaign.sql, this is a manual/operator-run step, not a task in
-- statemachine/redshift_load.asl.json: account_name/marketplace/region come from
-- config/profiles.yaml, which nothing in the automated pipeline currently loads into Redshift
-- (see README's "dim_profile must be seeded separately" note on create_tables.sql). Run this
-- after re-populating staging_profile from the current profiles.yaml, whenever an account is
-- renamed, reassigned to a different marketplace/region, or newly onboarded.
--
-- Same two-statement shape as scd2_dim_campaign.sql and for the same reason: Redshift's MERGE
-- can't express "close the old row and insert a fresh one" in a single statement.

UPDATE dim_profile
SET is_current = FALSE,
    valid_to   = CURRENT_DATE
FROM staging_profile s
WHERE dim_profile.profile_id = s.profile_id
  AND dim_profile.is_current = TRUE
  AND (
        dim_profile.account_name IS DISTINCT FROM s.account_name
     OR dim_profile.marketplace  IS DISTINCT FROM s.marketplace
     OR dim_profile.region       IS DISTINCT FROM s.region
  );

INSERT INTO dim_profile (
    profile_id, account_name, marketplace, region, valid_from, valid_to, is_current
)
SELECT
    s.profile_id, s.account_name, s.marketplace, s.region, CURRENT_DATE, NULL, TRUE
FROM staging_profile s
LEFT JOIN dim_profile d
       ON d.profile_id = s.profile_id
      AND d.is_current = TRUE
WHERE d.profile_id IS NULL;
