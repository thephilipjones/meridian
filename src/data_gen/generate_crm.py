# Databricks notebook source
# MAGIC %pip install faker -q

# COMMAND ----------

"""Generate synthetic CRM / sales pipeline data for Meridian Internal.

Uses curated accounts aligned to Meridian's customer verticals (financial
services, life sciences, legal, government, healthcare) with realistic ARR
distributions. Account names are shared with generate_web_events.py to ensure
the customer_health gold table join works correctly.
"""

import csv
import io
import os
import random
import uuid
from datetime import datetime, timedelta

from faker import Faker

dbutils.widgets.text("catalog_name", "serverless_stable_k2zkdm_catalog")
_catalog = dbutils.widgets.get("catalog_name")

PRODUCT_NAMES = ["Regulatory Feed", "Research Platform", "Patent Monitor", "Custom Analytics"]
STAGING_PATH = f"/Volumes/{_catalog}/meridian_staging/crm"

fake = Faker()
Faker.seed(42)
random.seed(42)

STAGES = ["Prospecting", "Qualification", "Proposal", "Negotiation", "Closed Won", "Closed Lost"]
REGIONS = ["North America", "EMEA", "APAC", "LATAM"]
OWNERS = [
    "Sarah Chen", "Marcus Johnson", "Elena Vasquez", "David Kim",
    "Rachel Torres", "James Mitchell", "Priya Sharma", "Tom Anderson",
    "Lisa Park", "Michael O'Brien", "Ana Rodriguez", "Chris Walker",
]

# fmt: off
CURATED_ACCOUNTS = [
    # Enterprise — $300K–$1.5M ARR, 2-4 products, major institutions
    {"name": "Apex Capital Partners",           "id": "acct0001", "region": "North America", "products": ["Regulatory Feed", "Custom Analytics", "Patent Monitor"], "tier": "enterprise"},
    {"name": "NovaCure Therapeutics",            "id": "acct0002", "region": "North America", "products": ["Research Platform", "Patent Monitor", "Custom Analytics"], "tier": "enterprise"},
    {"name": "Sterling Financial Group",         "id": "acct0003", "region": "North America", "products": ["Regulatory Feed", "Research Platform", "Custom Analytics"], "tier": "enterprise"},
    {"name": "Morrison Keller LLP",              "id": "acct0004", "region": "North America", "products": ["Regulatory Feed", "Patent Monitor", "Custom Analytics"], "tier": "enterprise"},
    {"name": "Zenith Pharmaceuticals",           "id": "acct0005", "region": "EMEA",          "products": ["Research Platform", "Patent Monitor"], "tier": "enterprise"},
    {"name": "Cascade Health Systems",           "id": "acct0006", "region": "North America", "products": ["Research Platform", "Regulatory Feed", "Custom Analytics"], "tier": "enterprise"},
    {"name": "Blackridge Investment Bank",        "id": "acct0007", "region": "EMEA",          "products": ["Regulatory Feed", "Custom Analytics"], "tier": "enterprise"},
    {"name": "National Policy Research Center",  "id": "acct0008", "region": "North America", "products": ["Research Platform", "Custom Analytics"], "tier": "enterprise"},

    # Growth — $80K–$300K ARR, 1-3 products, expanding accounts
    {"name": "Cornerstone Capital Group",        "id": "acct0009", "region": "North America", "products": ["Regulatory Feed", "Custom Analytics"], "tier": "growth"},
    {"name": "Helix BioSciences",                "id": "acct0010", "region": "North America", "products": ["Research Platform", "Patent Monitor"], "tier": "growth"},
    {"name": "Whitfield Securities",             "id": "acct0011", "region": "EMEA",          "products": ["Regulatory Feed", "Patent Monitor"], "tier": "growth"},
    {"name": "Chambers & Whitmore",              "id": "acct0012", "region": "North America", "products": ["Regulatory Feed", "Patent Monitor"], "tier": "growth"},
    {"name": "Atlas Banking Corporation",        "id": "acct0013", "region": "APAC",          "products": ["Regulatory Feed", "Custom Analytics"], "tier": "growth"},
    {"name": "Quantum Life Sciences",            "id": "acct0014", "region": "North America", "products": ["Research Platform", "Patent Monitor", "Custom Analytics"], "tier": "growth"},
    {"name": "Federal Policy Analytics",         "id": "acct0015", "region": "North America", "products": ["Regulatory Feed", "Research Platform"], "tier": "growth"},
    {"name": "Evergreen Energy Corp",            "id": "acct0016", "region": "North America", "products": ["Regulatory Feed", "Custom Analytics"], "tier": "growth"},
    {"name": "Sentinel Insurance Group",         "id": "acct0017", "region": "North America", "products": ["Regulatory Feed", "Custom Analytics"], "tier": "growth"},
    {"name": "Pacific Research Consortium",      "id": "acct0018", "region": "APAC",          "products": ["Research Platform"], "tier": "growth"},
    {"name": "Nextera Genomics",                 "id": "acct0019", "region": "North America", "products": ["Research Platform", "Patent Monitor"], "tier": "growth"},
    {"name": "Hargrove Compliance Partners",     "id": "acct0020", "region": "North America", "products": ["Regulatory Feed"], "tier": "growth"},
    {"name": "DataStream Analytics",             "id": "acct0021", "region": "North America", "products": ["Custom Analytics", "Research Platform"], "tier": "growth"},
    {"name": "Lakewood Investment Group",        "id": "acct0022", "region": "North America", "products": ["Regulatory Feed", "Custom Analytics"], "tier": "growth"},
    {"name": "Pacific Northwest Medical",        "id": "acct0023", "region": "North America", "products": ["Research Platform"], "tier": "growth"},
    {"name": "Verdant Biosystems",               "id": "acct0024", "region": "EMEA",          "products": ["Research Platform", "Patent Monitor"], "tier": "growth"},
    {"name": "Pinnacle Trust Company",           "id": "acct0025", "region": "North America", "products": ["Regulatory Feed"], "tier": "growth"},

    # Standard — $20K–$80K ARR, 1-2 products, core customer base
    {"name": "Barrett Shaw Associates",          "id": "acct0026", "region": "North America", "products": ["Regulatory Feed", "Patent Monitor"], "tier": "standard"},
    {"name": "Coastal Health Network",           "id": "acct0027", "region": "North America", "products": ["Research Platform"], "tier": "standard"},
    {"name": "Iron Gate Partners",               "id": "acct0028", "region": "EMEA",          "products": ["Regulatory Feed"], "tier": "standard"},
    {"name": "TechNova Solutions",               "id": "acct0029", "region": "North America", "products": ["Custom Analytics"], "tier": "standard"},
    {"name": "Aethon Pharma Group",              "id": "acct0030", "region": "LATAM",         "products": ["Research Platform"], "tier": "standard"},
    {"name": "Summit Industrial Partners",       "id": "acct0031", "region": "North America", "products": ["Regulatory Feed", "Patent Monitor"], "tier": "standard"},
    {"name": "Blue Ridge Capital",               "id": "acct0032", "region": "North America", "products": ["Custom Analytics"], "tier": "standard"},
    {"name": "Brookhaven Research Labs",         "id": "acct0033", "region": "North America", "products": ["Research Platform", "Patent Monitor"], "tier": "standard"},
    {"name": "Thornton Legal Group",             "id": "acct0034", "region": "EMEA",          "products": ["Regulatory Feed"], "tier": "standard"},
    {"name": "Luminos Health Sciences",          "id": "acct0035", "region": "APAC",          "products": ["Research Platform"], "tier": "standard"},
    {"name": "Sovereign Wealth Advisors",        "id": "acct0036", "region": "EMEA",          "products": ["Regulatory Feed"], "tier": "standard"},
    {"name": "Pacific Coast Financial",          "id": "acct0037", "region": "North America", "products": ["Regulatory Feed", "Custom Analytics"], "tier": "standard"},
    {"name": "Harbor Point Capital",             "id": "acct0038", "region": "North America", "products": ["Custom Analytics"], "tier": "standard"},
    {"name": "Titan Manufacturing Group",        "id": "acct0039", "region": "North America", "products": ["Regulatory Feed"], "tier": "standard"},
    {"name": "Quantum Insights Corp",            "id": "acct0040", "region": "North America", "products": ["Custom Analytics", "Regulatory Feed"], "tier": "standard"},
    {"name": "Celaris Biotech",                  "id": "acct0041", "region": "North America", "products": ["Research Platform"], "tier": "standard"},
    {"name": "State University Medical Center",  "id": "acct0042", "region": "North America", "products": ["Research Platform"], "tier": "standard"},
    {"name": "Northwind Data Services",          "id": "acct0043", "region": "EMEA",          "products": ["Custom Analytics"], "tier": "standard"},
    {"name": "Acme Bank",                        "id": "acct0044", "region": "North America", "products": ["Regulatory Feed"], "tier": "standard"},
    {"name": "Redwood Analytics Group",          "id": "acct0045", "region": "North America", "products": ["Custom Analytics", "Research Platform"], "tier": "standard"},

    # Starter — $5K–$20K ARR, 1 product, new or small customers
    {"name": "Granite State Financial",          "id": "acct0046", "region": "North America", "products": ["Regulatory Feed"], "tier": "starter"},
    {"name": "Caldwell & Hughes LLP",            "id": "acct0047", "region": "North America", "products": ["Regulatory Feed"], "tier": "starter"},
    {"name": "Catalyst Drug Development",        "id": "acct0048", "region": "North America", "products": ["Research Platform"], "tier": "starter"},
    {"name": "Alpine Regulatory Consulting",     "id": "acct0049", "region": "EMEA",          "products": ["Regulatory Feed"], "tier": "starter"},
    {"name": "Sequoia Compliance Corp",          "id": "acct0050", "region": "North America", "products": ["Regulatory Feed"], "tier": "starter"},
    {"name": "Willow Creek Research",            "id": "acct0051", "region": "North America", "products": ["Research Platform"], "tier": "starter"},
    {"name": "Golden Gate Biotech",              "id": "acct0052", "region": "North America", "products": ["Patent Monitor"], "tier": "starter"},
    {"name": "Eastway Financial Services",       "id": "acct0053", "region": "APAC",          "products": ["Regulatory Feed"], "tier": "starter"},
    {"name": "Clearwater IP Advisors",           "id": "acct0054", "region": "North America", "products": ["Patent Monitor"], "tier": "starter"},
    {"name": "Oakmont Investment Partners",      "id": "acct0055", "region": "North America", "products": ["Custom Analytics"], "tier": "starter"},
    {"name": "Keystone Health Analytics",        "id": "acct0056", "region": "North America", "products": ["Research Platform"], "tier": "starter"},
    {"name": "Magnolia Life Sciences",           "id": "acct0057", "region": "LATAM",         "products": ["Research Platform"], "tier": "starter"},
    {"name": "River City Legal Associates",      "id": "acct0058", "region": "North America", "products": ["Regulatory Feed"], "tier": "starter"},
    {"name": "Crestview Capital Advisors",       "id": "acct0059", "region": "EMEA",          "products": ["Regulatory Feed"], "tier": "starter"},
    {"name": "Emerald Research Partners",        "id": "acct0060", "region": "APAC",          "products": ["Research Platform"], "tier": "starter"},
]
# fmt: on

TIER_CONFIG = {
    "enterprise": {"arr_range": (300_000, 1_500_000), "won_deals": (3, 6), "pipeline_deals": (4, 10)},
    "growth":     {"arr_range": (80_000, 300_000),    "won_deals": (2, 4), "pipeline_deals": (3, 7)},
    "standard":   {"arr_range": (20_000, 80_000),     "won_deals": (1, 3), "pipeline_deals": (2, 5)},
    "starter":    {"arr_range": (5_000, 20_000),      "won_deals": (1, 2), "pipeline_deals": (1, 3)},
}


def _generate_deals_for_account(account: dict) -> list[dict]:
    """Generate Closed Won deals (creating ARR) + pipeline deals for one account."""
    cfg = TIER_CONFIG[account["tier"]]
    deals = []
    base_date = datetime(2024, 2, 1)

    # Closed Won deals — split target ARR across products
    target_arr = random.randint(*cfg["arr_range"])
    num_won = random.randint(*cfg["won_deals"])
    products_for_deals = []
    for _ in range(num_won):
        products_for_deals.append(random.choice(account["products"]))

    arr_splits = [random.random() for _ in range(num_won)]
    arr_total = sum(arr_splits)
    arr_splits = [s / arr_total * target_arr for s in arr_splits]

    for i, arr_amount in enumerate(arr_splits):
        arr_amount = round(arr_amount, 2)
        amount = round(arr_amount / random.uniform(0.65, 0.90), 2)
        created = base_date + timedelta(days=random.randint(0, 500))
        close_offset = random.randint(30, 150)

        deals.append({
            "deal_id": str(uuid.uuid4())[:12],
            "account_name": account["name"],
            "account_id": account["id"],
            "deal_name": f"{account['name']} - {products_for_deals[i]}",
            "stage": "Closed Won",
            "amount": amount,
            "arr": arr_amount,
            "close_date": (created + timedelta(days=close_offset)).strftime("%Y-%m-%d"),
            "created_date": created.strftime("%Y-%m-%d"),
            "owner": random.choice(OWNERS),
            "product_line": products_for_deals[i],
            "region": account["region"],
        })

    # Pipeline deals at other stages
    pipeline_stages = ["Prospecting", "Qualification", "Proposal", "Negotiation"]
    stage_weights = [0.30, 0.30, 0.25, 0.15]
    num_pipeline = random.randint(*cfg["pipeline_deals"])

    for _ in range(num_pipeline):
        stage = random.choices(pipeline_stages, weights=stage_weights, k=1)[0]
        product = random.choice(account["products"])
        amount = round(random.uniform(cfg["arr_range"][0] * 0.2, cfg["arr_range"][1] * 0.4), 2)
        created = base_date + timedelta(days=random.randint(300, 730))
        close_offset = random.randint(30, 180)

        deals.append({
            "deal_id": str(uuid.uuid4())[:12],
            "account_name": account["name"],
            "account_id": account["id"],
            "deal_name": f"{account['name']} - {product}",
            "stage": stage,
            "amount": amount,
            "arr": 0.0,
            "close_date": (created + timedelta(days=close_offset)).strftime("%Y-%m-%d"),
            "created_date": created.strftime("%Y-%m-%d"),
            "owner": random.choice(OWNERS),
            "product_line": product,
            "region": account["region"],
        })

    # Some Closed Lost deals for realism
    if random.random() < 0.4:
        product = random.choice(account["products"])
        amount = round(random.uniform(cfg["arr_range"][0] * 0.15, cfg["arr_range"][1] * 0.3), 2)
        created = base_date + timedelta(days=random.randint(0, 600))
        deals.append({
            "deal_id": str(uuid.uuid4())[:12],
            "account_name": account["name"],
            "account_id": account["id"],
            "deal_name": f"{account['name']} - {product}",
            "stage": "Closed Lost",
            "amount": amount,
            "arr": 0.0,
            "close_date": (created + timedelta(days=random.randint(60, 180))).strftime("%Y-%m-%d"),
            "created_date": created.strftime("%Y-%m-%d"),
            "owner": random.choice(OWNERS),
            "product_line": product,
            "region": account["region"],
        })

    return deals


def write_csv(deals: list[dict], output_path: str) -> str:
    os.makedirs(output_path, exist_ok=True)
    filepath = os.path.join(output_path, "crm_deals.csv")

    fieldnames = list(deals[0].keys())
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(deals)

    with open(filepath, "w") as f:
        f.write(buf.getvalue())

    return filepath


def main(output_path: str | None = None):
    path = output_path or STAGING_PATH
    all_deals = []
    for account in CURATED_ACCOUNTS:
        all_deals.extend(_generate_deals_for_account(account))

    filepath = write_csv(all_deals, path)
    total_arr = sum(d["arr"] for d in all_deals)
    won_deals = sum(1 for d in all_deals if d["stage"] == "Closed Won")
    print(f"Generated {len(all_deals)} CRM deals ({won_deals} Closed Won, ${total_arr:,.0f} total ARR) "
          f"across {len(CURATED_ACCOUNTS)} accounts -> {filepath}")
    return all_deals


if __name__ == "__main__":
    main()
