"""Generates synthetic Amazon Ads campaign-performance data for local development and the
analytics dashboard -- nothing is deployed to a real AWS/Redshift account yet (see README's
Open Questions), so this stands in for a real 30-day-window ingestion run.

Produces two outputs from one in-memory dataset, so the SQL and JSON never drift apart:
  - seed/seed_data.json      -- compact array-encoded dataset the dashboard artifact embeds directly
  - redshift/seed_data.sql   -- INSERT statements for dim_profile/dim_campaign/dim_date/
                                fct_campaign_performance (the existing schema) plus
                                dim_campaign_bid/fct_account_daily_sales (see
                                redshift/create_tables_analytics_demo.sql) for the bid and
                                TACoS metrics the core schema doesn't carry.

Deterministic (fixed RANDOM_SEED) so re-running reproduces byte-identical output.
"""

import json
import random
from datetime import date, timedelta

RANDOM_SEED = 42
NUM_DAYS = 60
END_DATE = date(2026, 7, 22)  # day before "today" (2026-07-23) -- last complete report day
START_DATE = END_DATE - timedelta(days=NUM_DAYS - 1)

AD_PRODUCTS = ["SPONSORED_PRODUCTS", "SPONSORED_BRANDS", "SPONSORED_DISPLAY"]

# 26 independent advertiser profiles (see README: "26 separate OAuth grants").
BRANDS = [
    ("brand-1-us", "US", "NA"), ("brand-2-uk", "UK", "EU"), ("brand-3-de", "DE", "EU"),
    ("brand-4-us", "US", "NA"), ("brand-5-jp", "JP", "FE"), ("brand-6-us", "US", "NA"),
    ("brand-7-ca", "CA", "NA"), ("brand-8-fr", "FR", "EU"), ("brand-9-us", "US", "NA"),
    ("brand-10-uk", "UK", "EU"), ("brand-11-it", "IT", "EU"), ("brand-12-us", "US", "NA"),
    ("brand-13-es", "ES", "EU"), ("brand-14-de", "DE", "EU"), ("brand-15-us", "US", "NA"),
    ("brand-16-jp", "JP", "FE"), ("brand-17-uk", "UK", "EU"), ("brand-18-us", "US", "NA"),
    ("brand-19-ca", "CA", "NA"), ("brand-20-us", "US", "NA"), ("brand-21-de", "DE", "EU"),
    ("brand-22-us", "US", "NA"), ("brand-23-uk", "UK", "EU"), ("brand-24-us", "US", "NA"),
    ("brand-25-fr", "FR", "EU"), ("brand-26-us", "US", "NA"),
]

# archetype -> (impressions/day base range, ctr range, cvr range, roas range, cpc bid range)
ARCHETYPES = {
    "brand_defense": dict(impr=(9_000, 42_000), ctr=(0.008, 0.013), cvr=(0.15, 0.21), roas=(6.0, 10.5), bid=(0.35, 0.75)),
    "category_conquest": dict(impr=(20_000, 90_000), ctr=(0.003, 0.005), cvr=(0.05, 0.08), roas=(2.0, 3.6), bid=(0.90, 1.65)),
    "competitor_targeting": dict(impr=(15_000, 70_000), ctr=(0.0025, 0.0045), cvr=(0.04, 0.07), roas=(1.6, 3.0), bid=(1.05, 1.95)),
    "auto_discovery": dict(impr=(12_000, 55_000), ctr=(0.004, 0.006), cvr=(0.08, 0.12), roas=(3.2, 5.2), bid=(0.55, 1.05)),
    "sb_video": dict(impr=(25_000, 110_000), ctr=(0.0035, 0.005), cvr=(0.06, 0.10), roas=(3.0, 5.5), bid=(0.65, 1.20)),
    "sb_store_spotlight": dict(impr=(8_000, 30_000), ctr=(0.003, 0.0045), cvr=(0.07, 0.11), roas=(3.5, 6.0), bid=(0.60, 1.10)),
    "sd_retargeting": dict(impr=(30_000, 140_000), ctr=(0.0015, 0.0028), cvr=(0.03, 0.055), roas=(2.2, 4.0), bid=(0.35, 0.65)),
    "sd_audience_expansion": dict(impr=(20_000, 95_000), ctr=(0.0012, 0.0022), cvr=(0.02, 0.04), roas=(1.5, 2.8), bid=(0.40, 0.75)),
}

CAMPAIGN_TEMPLATES = {
    "SPONSORED_PRODUCTS": [
        ("SP | Brand Defense | Exact", "brand_defense"),
        ("SP | Category Conquest | Broad", "category_conquest"),
        ("SP | Competitor Targeting | Product", "competitor_targeting"),
        ("SP | Auto Discovery", "auto_discovery"),
    ],
    "SPONSORED_BRANDS": [
        ("SB | Brand Awareness | Video", "sb_video"),
        ("SB | Store Spotlight", "sb_store_spotlight"),
    ],
    "SPONSORED_DISPLAY": [
        ("SD | Retargeting | Views", "sd_retargeting"),
        ("SD | Audience Expansion", "sd_audience_expansion"),
    ],
}


def daterange(start, end):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def build_dataset():
    rng = random.Random(RANDOM_SEED)
    dates = list(daterange(START_DATE, END_DATE))

    profiles = []
    for i, (account_name, marketplace, region) in enumerate(BRANDS):
        profile_id = f"{1000000000000000 + i * 37}"
        # Most profiles run all 3 ad products; a handful only run 1-2 (see README Open Question).
        if rng.random() < 0.15:
            ad_products = rng.sample(AD_PRODUCTS, k=2)
        else:
            ad_products = list(AD_PRODUCTS)
        profiles.append({
            "id": profile_id,
            "account_name": account_name,
            "marketplace": marketplace,
            "region": region,
            "ad_products": ad_products,
            "size_factor": round(rng.uniform(0.5, 2.2), 3),
        })

    campaigns = []
    for p_idx, profile in enumerate(profiles):
        for ad_product in profile["ad_products"]:
            for name, archetype in CAMPAIGN_TEMPLATES[ad_product]:
                spec = ARCHETYPES[archetype]
                campaigns.append({
                    "id": f"camp_{p_idx:02d}_{len(campaigns):04d}",
                    "profile_idx": p_idx,
                    "ad_product": ad_product,
                    "name": f"{profile['account_name']} - {name}",
                    "archetype": archetype,
                    "bid": round(rng.uniform(*spec["bid"]), 2),
                    # per-campaign multiplier so sibling campaigns of the same archetype aren't identical
                    "campaign_factor": round(rng.uniform(0.75, 1.3), 3),
                })

    facts = []  # [campaign_idx, date_idx, impressions, clicks, cost, purchases, sales]
    for c_idx, camp in enumerate(campaigns):
        spec = ARCHETYPES[camp["archetype"]]
        profile = profiles[camp["profile_idx"]]
        trend_drift = rng.uniform(-0.0015, 0.0025)  # slow day-over-day drift, up or down
        level = 1.0
        for d_idx, d in enumerate(dates):
            weekday_factor = 0.78 if d.weekday() >= 5 else 1.0  # lighter weekend spend, B2C pattern
            level *= (1 + trend_drift)
            noise = rng.uniform(0.85, 1.15)

            impressions = max(50, round(
                rng.uniform(*spec["impr"]) * profile["size_factor"] * camp["campaign_factor"]
                * weekday_factor * level * noise
            ))
            ctr = rng.uniform(*spec["ctr"]) * rng.uniform(0.92, 1.08)
            clicks = max(0, round(impressions * ctr))
            cpc = camp["bid"] * rng.uniform(0.55, 0.95)  # actual CPC typically undercuts bid
            cost = round(clicks * cpc, 2)
            cvr = rng.uniform(*spec["cvr"]) * rng.uniform(0.9, 1.1)
            purchases = round(clicks * cvr)
            roas = rng.uniform(*spec["roas"]) * rng.uniform(0.9, 1.1)
            sales = round(cost * roas, 2) if cost > 0 else 0.0

            facts.append([c_idx, d_idx, impressions, clicks, cost, purchases, sales])

    # Account-level total sales (organic + all ad channels) for TACoS -- ads make up a plausible
    # 12-35% share per account, so TACoS always comes in below that account's ACoS.
    account_sales = []
    ad_sales_by_profile_day = {}
    for row in facts:
        c_idx, d_idx, *_impr_clicks_cost_purch, sales = row
        p_idx = campaigns[c_idx]["profile_idx"]
        key = (p_idx, d_idx)
        ad_sales_by_profile_day[key] = ad_sales_by_profile_day.get(key, 0.0) + sales

    ad_share_by_profile = {p_idx: rng.uniform(0.12, 0.35) for p_idx in range(len(profiles))}
    for (p_idx, d_idx), ad_sales in sorted(ad_sales_by_profile_day.items()):
        share = ad_share_by_profile[p_idx] * rng.uniform(0.9, 1.1)
        total_sales = round(ad_sales / max(share, 0.05), 2)
        account_sales.append([p_idx, d_idx, total_sales])

    return {
        "generated_at": END_DATE.isoformat(),
        "date_range": {"start": START_DATE.isoformat(), "end": END_DATE.isoformat()},
        "dims": {
            "profiles": [
                {"id": p["id"], "account_name": p["account_name"], "marketplace": p["marketplace"], "region": p["region"]}
                for p in profiles
            ],
            "ad_products": AD_PRODUCTS,
            "campaigns": [
                {"id": c["id"], "profile_idx": c["profile_idx"], "ad_product": c["ad_product"],
                 "name": c["name"], "archetype": c["archetype"], "bid": c["bid"]}
                for c in campaigns
            ],
            "dates": [d.isoformat() for d in dates],
        },
        "facts": facts,
        "account_sales": account_sales,
        "_profiles_full": profiles,
        "_campaigns_full": campaigns,
    }


def sql_str(s):
    return "'" + s.replace("'", "''") + "'"


def write_sql(dataset, path):
    profiles = dataset["dims"]["profiles"]
    campaigns = dataset["dims"]["campaigns"]
    dates = dataset["dims"]["dates"]
    facts = dataset["facts"]
    account_sales = dataset["account_sales"]

    lines = []
    lines.append("-- Synthetic seed data for local/dev use -- generated by seed/generate_seed_data.py.")
    lines.append("-- Deterministic (RANDOM_SEED=42): re-running the generator reproduces this file byte-for-byte.")
    lines.append("-- Run after redshift/create_tables.sql and redshift/create_tables_analytics_demo.sql.")
    lines.append("")

    lines.append("-- dim_profile (seeded directly as current rows; see README's SCD2 section --")
    lines.append("-- a real deploy populates this via staging_profile + scd2_dim_profile.sql instead).")
    lines.append("INSERT INTO dim_profile (profile_id, account_name, marketplace, region, valid_from, is_current) VALUES")
    rows = [
        f"({sql_str(p['id'])}, {sql_str(p['account_name'])}, {sql_str(p['marketplace'])}, {sql_str(p['region'])}, {sql_str(dataset['date_range']['start'])}, TRUE)"
        for p in profiles
    ]
    lines.append(",\n".join(rows) + ";")
    lines.append("")

    lines.append("-- dim_campaign (seeded directly as current rows; a real deploy populates this via")
    lines.append("-- scd2_dim_campaign_close.sql/scd2_dim_campaign_insert.sql instead).")
    lines.append("INSERT INTO dim_campaign (profile_id, ad_product, campaign_id, campaign_name, valid_from, is_current) VALUES")
    rows = [
        f"({sql_str(profiles[c['profile_idx']]['id'])}, {sql_str(c['ad_product'])}, {sql_str(c['id'])}, {sql_str(c['name'])}, {sql_str(dataset['date_range']['start'])}, TRUE)"
        for c in campaigns
    ]
    lines.append(",\n".join(rows) + ";")
    lines.append("")

    lines.append("INSERT INTO dim_date (date_key, year, month, day) VALUES")
    rows = [f"({sql_str(d)}, {d[0:4]}, {int(d[5:7])}, {int(d[8:10])})" for d in dates]
    lines.append(",\n".join(rows) + ";")
    lines.append("")

    lines.append("-- Per-campaign target CPC bid -- demo-only companion to dim_campaign, see")
    lines.append("-- redshift/create_tables_analytics_demo.sql for why this isn't part of the core schema.")
    lines.append("INSERT INTO dim_campaign_bid (profile_id, ad_product, campaign_id, target_cpc) VALUES")
    rows = [
        f"({sql_str(profiles[c['profile_idx']]['id'])}, {sql_str(c['ad_product'])}, {sql_str(c['id'])}, {c['bid']})"
        for c in campaigns
    ]
    lines.append(",\n".join(rows) + ";")
    lines.append("")

    lines.append("-- fct_campaign_performance -- chunked INSERTs, 500 rows each, for readability/safety.")
    chunk_size = 500
    for i in range(0, len(facts), chunk_size):
        chunk = facts[i:i + chunk_size]
        lines.append(
            "INSERT INTO fct_campaign_performance "
            "(profile_id, ad_product, campaign_id, campaign_name, report_date, impressions, clicks, cost, purchases_14d, sales_14d) VALUES"
        )
        rows = []
        for c_idx, d_idx, impressions, clicks, cost, purchases, sales in chunk:
            camp = campaigns[c_idx]
            profile = profiles[camp["profile_idx"]]
            rows.append(
                f"({sql_str(profile['id'])}, {sql_str(camp['ad_product'])}, {sql_str(camp['id'])}, "
                f"{sql_str(camp['name'])}, {sql_str(dates[d_idx])}, {impressions}, {clicks}, {cost}, {purchases}, {sales})"
            )
        lines.append(",\n".join(rows) + ";")
        lines.append("")

    lines.append("-- fct_account_daily_sales -- demo-only companion table for TACoS (total account sales,")
    lines.append("-- not just ad-attributed sales_14d). See redshift/create_tables_analytics_demo.sql.")
    for i in range(0, len(account_sales), chunk_size):
        chunk = account_sales[i:i + chunk_size]
        lines.append("INSERT INTO fct_account_daily_sales (profile_id, report_date, total_sales) VALUES")
        rows = [
            f"({sql_str(profiles[p_idx]['id'])}, {sql_str(dates[d_idx])}, {total_sales})"
            for p_idx, d_idx, total_sales in chunk
        ]
        lines.append(",\n".join(rows) + ";")
        lines.append("")

    with open(path, "w") as f:
        f.write("\n".join(lines))


def write_json(dataset, path):
    out = {k: v for k, v in dataset.items() if not k.startswith("_")}
    with open(path, "w") as f:
        json.dump(out, f, separators=(",", ":"))


if __name__ == "__main__":
    dataset = build_dataset()
    write_json(dataset, "seed/seed_data.json")
    write_sql(dataset, "redshift/seed_data.sql")
    n_facts = len(dataset["facts"])
    n_campaigns = len(dataset["dims"]["campaigns"])
    print(f"profiles={len(dataset['dims']['profiles'])} campaigns={n_campaigns} "
          f"dates={len(dataset['dims']['dates'])} fact_rows={n_facts} "
          f"account_sales_rows={len(dataset['account_sales'])}")
