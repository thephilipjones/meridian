# Databricks notebook source
# MAGIC %pip install faker -q

# COMMAND ----------

"""Generate synthetic web analytics / product usage events for Meridian Internal.

Uses the same curated account list as generate_crm.py with usage intensity
correlated to account tier and health profile. This ensures the customer_health
gold table join on account_name produces realistic health scores.
"""

import json
import math
import os
import random
import uuid
from datetime import datetime, timedelta

from faker import Faker

dbutils.widgets.text("catalog_name", "serverless_stable_k2zkdm_catalog")
_catalog = dbutils.widgets.get("catalog_name")

STAGING_PATH = f"/Volumes/{_catalog}/meridian_staging/web_events"

fake = Faker()
Faker.seed(42)
random.seed(42)

EVENT_TYPES = ["api_call", "query", "download", "export", "search"]
EVENT_TYPE_WEIGHTS = [0.45, 0.25, 0.10, 0.05, 0.15]

ENDPOINTS = {
    "Regulatory Feed": ["/api/v1/filings", "/api/v1/actions", "/api/v1/entities", "/api/v1/risks"],
    "Research Platform": ["/api/v1/articles", "/api/v1/search", "/api/v1/authors", "/api/v1/citations"],
    "Patent Monitor": ["/api/v1/patents", "/api/v1/landscape", "/api/v1/alerts"],
    "Custom Analytics": ["/api/v1/query", "/api/v1/reports", "/api/v1/dashboards"],
}

STATUS_CODES_NORMAL = [200, 200, 200, 200, 200, 200, 200, 200, 201, 400, 404, 500]
STATUS_CODES_PROBLEMATIC = [200, 200, 200, 200, 200, 400, 404, 500, 500, 503]

EVENTS_PER_FILE = 2500

# Usage profiles determine event volume and recency distribution.
# "recent_weight" controls what fraction of events fall in the last 30 days
# of the data range (higher = more recent activity).
USAGE_PROFILES = {
    "very_high": {"total_events": (4000, 6000), "recent_weight": 0.12, "error_profile": "normal",    "response_profile": "fast"},
    "high":      {"total_events": (2000, 3500), "recent_weight": 0.10, "error_profile": "normal",    "response_profile": "fast"},
    "medium":    {"total_events": (800, 1800),  "recent_weight": 0.08, "error_profile": "normal",    "response_profile": "normal"},
    "low":       {"total_events": (200, 600),   "recent_weight": 0.06, "error_profile": "normal",    "response_profile": "normal"},
    "declining": {"total_events": (1500, 3000), "recent_weight": 0.01, "error_profile": "normal",    "response_profile": "normal"},
    "minimal":   {"total_events": (30, 100),    "recent_weight": 0.04, "error_profile": "elevated",  "response_profile": "slow"},
}

# fmt: off
CURATED_ACCOUNTS = [
    # Enterprise — high ARR, varied usage profiles for demo storytelling
    {"name": "Apex Capital Partners",           "id": "acct0001", "products": ["Regulatory Feed", "Custom Analytics", "Patent Monitor"], "usage": "high"},
    {"name": "NovaCure Therapeutics",            "id": "acct0002", "products": ["Research Platform", "Patent Monitor", "Custom Analytics"], "usage": "very_high"},
    {"name": "Sterling Financial Group",         "id": "acct0003", "products": ["Regulatory Feed", "Research Platform", "Custom Analytics"], "usage": "declining"},
    {"name": "Morrison Keller LLP",              "id": "acct0004", "products": ["Regulatory Feed", "Patent Monitor", "Custom Analytics"], "usage": "high"},
    {"name": "Zenith Pharmaceuticals",           "id": "acct0005", "products": ["Research Platform", "Patent Monitor"], "usage": "high"},
    {"name": "Cascade Health Systems",           "id": "acct0006", "products": ["Research Platform", "Regulatory Feed", "Custom Analytics"], "usage": "low"},
    {"name": "Blackridge Investment Bank",        "id": "acct0007", "products": ["Regulatory Feed", "Custom Analytics"], "usage": "minimal"},
    {"name": "National Policy Research Center",  "id": "acct0008", "products": ["Research Platform", "Custom Analytics"], "usage": "medium"},

    # Growth — moderate ARR, some growing fast
    {"name": "Cornerstone Capital Group",        "id": "acct0009", "products": ["Regulatory Feed", "Custom Analytics"], "usage": "high"},
    {"name": "Helix BioSciences",                "id": "acct0010", "products": ["Research Platform", "Patent Monitor"], "usage": "very_high"},
    {"name": "Whitfield Securities",             "id": "acct0011", "products": ["Regulatory Feed", "Patent Monitor"], "usage": "medium"},
    {"name": "Chambers & Whitmore",              "id": "acct0012", "products": ["Regulatory Feed", "Patent Monitor"], "usage": "high"},
    {"name": "Atlas Banking Corporation",        "id": "acct0013", "products": ["Regulatory Feed", "Custom Analytics"], "usage": "medium"},
    {"name": "Quantum Life Sciences",            "id": "acct0014", "products": ["Research Platform", "Patent Monitor", "Custom Analytics"], "usage": "high"},
    {"name": "Federal Policy Analytics",         "id": "acct0015", "products": ["Regulatory Feed", "Research Platform"], "usage": "medium"},
    {"name": "Evergreen Energy Corp",            "id": "acct0016", "products": ["Regulatory Feed", "Custom Analytics"], "usage": "declining"},
    {"name": "Sentinel Insurance Group",         "id": "acct0017", "products": ["Regulatory Feed", "Custom Analytics"], "usage": "medium"},
    {"name": "Pacific Research Consortium",      "id": "acct0018", "products": ["Research Platform"], "usage": "high"},
    {"name": "Nextera Genomics",                 "id": "acct0019", "products": ["Research Platform", "Patent Monitor"], "usage": "very_high"},
    {"name": "Hargrove Compliance Partners",     "id": "acct0020", "products": ["Regulatory Feed"], "usage": "medium"},
    {"name": "DataStream Analytics",             "id": "acct0021", "products": ["Custom Analytics", "Research Platform"], "usage": "high"},
    {"name": "Lakewood Investment Group",        "id": "acct0022", "products": ["Regulatory Feed", "Custom Analytics"], "usage": "declining"},
    {"name": "Pacific Northwest Medical",        "id": "acct0023", "products": ["Research Platform"], "usage": "medium"},
    {"name": "Verdant Biosystems",               "id": "acct0024", "products": ["Research Platform", "Patent Monitor"], "usage": "medium"},
    {"name": "Pinnacle Trust Company",           "id": "acct0025", "products": ["Regulatory Feed"], "usage": "low"},

    # Standard — moderate ARR, mixed usage
    {"name": "Barrett Shaw Associates",          "id": "acct0026", "products": ["Regulatory Feed", "Patent Monitor"], "usage": "medium"},
    {"name": "Coastal Health Network",           "id": "acct0027", "products": ["Research Platform"], "usage": "medium"},
    {"name": "Iron Gate Partners",               "id": "acct0028", "products": ["Regulatory Feed"], "usage": "low"},
    {"name": "TechNova Solutions",               "id": "acct0029", "products": ["Custom Analytics"], "usage": "high"},
    {"name": "Aethon Pharma Group",              "id": "acct0030", "products": ["Research Platform"], "usage": "medium"},
    {"name": "Summit Industrial Partners",       "id": "acct0031", "products": ["Regulatory Feed", "Patent Monitor"], "usage": "low"},
    {"name": "Blue Ridge Capital",               "id": "acct0032", "products": ["Custom Analytics"], "usage": "medium"},
    {"name": "Brookhaven Research Labs",         "id": "acct0033", "products": ["Research Platform", "Patent Monitor"], "usage": "high"},
    {"name": "Thornton Legal Group",             "id": "acct0034", "products": ["Regulatory Feed"], "usage": "low"},
    {"name": "Luminos Health Sciences",          "id": "acct0035", "products": ["Research Platform"], "usage": "medium"},
    {"name": "Sovereign Wealth Advisors",        "id": "acct0036", "products": ["Regulatory Feed"], "usage": "minimal"},
    {"name": "Pacific Coast Financial",          "id": "acct0037", "products": ["Regulatory Feed", "Custom Analytics"], "usage": "medium"},
    {"name": "Harbor Point Capital",             "id": "acct0038", "products": ["Custom Analytics"], "usage": "low"},
    {"name": "Titan Manufacturing Group",        "id": "acct0039", "products": ["Regulatory Feed"], "usage": "minimal"},
    {"name": "Quantum Insights Corp",            "id": "acct0040", "products": ["Custom Analytics", "Regulatory Feed"], "usage": "high"},
    {"name": "Celaris Biotech",                  "id": "acct0041", "products": ["Research Platform"], "usage": "medium"},
    {"name": "State University Medical Center",  "id": "acct0042", "products": ["Research Platform"], "usage": "high"},
    {"name": "Northwind Data Services",          "id": "acct0043", "products": ["Custom Analytics"], "usage": "declining"},
    {"name": "Acme Bank",                        "id": "acct0044", "products": ["Regulatory Feed"], "usage": "medium"},
    {"name": "Redwood Analytics Group",          "id": "acct0045", "products": ["Custom Analytics", "Research Platform"], "usage": "medium"},

    # Starter — low ARR, lower usage
    {"name": "Granite State Financial",          "id": "acct0046", "products": ["Regulatory Feed"], "usage": "low"},
    {"name": "Caldwell & Hughes LLP",            "id": "acct0047", "products": ["Regulatory Feed"], "usage": "medium"},
    {"name": "Catalyst Drug Development",        "id": "acct0048", "products": ["Research Platform"], "usage": "low"},
    {"name": "Alpine Regulatory Consulting",     "id": "acct0049", "products": ["Regulatory Feed"], "usage": "minimal"},
    {"name": "Sequoia Compliance Corp",          "id": "acct0050", "products": ["Regulatory Feed"], "usage": "low"},
    {"name": "Willow Creek Research",            "id": "acct0051", "products": ["Research Platform"], "usage": "medium"},
    {"name": "Golden Gate Biotech",              "id": "acct0052", "products": ["Patent Monitor"], "usage": "low"},
    {"name": "Eastway Financial Services",       "id": "acct0053", "products": ["Regulatory Feed"], "usage": "minimal"},
    {"name": "Clearwater IP Advisors",           "id": "acct0054", "products": ["Patent Monitor"], "usage": "medium"},
    {"name": "Oakmont Investment Partners",      "id": "acct0055", "products": ["Custom Analytics"], "usage": "low"},
    {"name": "Keystone Health Analytics",        "id": "acct0056", "products": ["Research Platform"], "usage": "high"},
    {"name": "Magnolia Life Sciences",           "id": "acct0057", "products": ["Research Platform"], "usage": "low"},
    {"name": "River City Legal Associates",      "id": "acct0058", "products": ["Regulatory Feed"], "usage": "minimal"},
    {"name": "Crestview Capital Advisors",       "id": "acct0059", "products": ["Regulatory Feed"], "usage": "low"},
    {"name": "Emerald Research Partners",        "id": "acct0060", "products": ["Research Platform"], "usage": "medium"},
]
# fmt: on


def _seasonal_multiplier(day_of_year: int) -> float:
    """Business seasonality — higher usage in Q1/Q4, dip in summer."""
    return 1.0 + 0.3 * math.cos(2 * math.pi * (day_of_year - 30) / 365)


def _generate_events_for_account(account: dict) -> list[dict]:
    """Generate events for one account based on its usage profile."""
    profile = USAGE_PROFILES[account["usage"]]
    total_events = random.randint(*profile["total_events"])
    recent_weight = profile["recent_weight"]
    status_codes = STATUS_CODES_PROBLEMATIC if profile["error_profile"] == "elevated" else STATUS_CODES_NORMAL

    base_date = datetime(2024, 2, 1)
    data_end = base_date + timedelta(days=730)  # ~2026-02-01
    recent_cutoff = data_end - timedelta(days=30)

    num_recent = max(1, int(total_events * recent_weight))
    num_historical = total_events - num_recent

    if account["usage"] == "declining":
        num_recent = random.randint(5, 25)
        num_historical = total_events - num_recent

    events = []

    for _ in range(num_historical):
        day_offset = random.randint(0, 700)
        ts = base_date + timedelta(
            days=day_offset,
            hours=random.randint(6, 22),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59),
        )
        events.append(_make_event(account, ts, status_codes, profile))

    for _ in range(num_recent):
        day_offset = random.randint(0, 29)
        ts = recent_cutoff + timedelta(
            days=day_offset,
            hours=random.randint(6, 22),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59),
        )
        events.append(_make_event(account, ts, status_codes, profile))

    return events


def _make_event(account: dict, ts: datetime, status_codes: list, profile: dict) -> dict:
    product = random.choice(account["products"])
    status = random.choice(status_codes)

    if profile["response_profile"] == "fast":
        response_ms = int(random.lognormvariate(4.0, 0.6))
    elif profile["response_profile"] == "slow":
        response_ms = int(random.lognormvariate(6.0, 0.8))
    else:
        response_ms = int(random.lognormvariate(4.5, 0.8))

    if status >= 400:
        response_ms = int(response_ms * random.uniform(2.0, 5.0))

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": random.choices(EVENT_TYPES, weights=EVENT_TYPE_WEIGHTS, k=1)[0],
        "event_timestamp": ts.isoformat(),
        "customer_id": account["id"],
        "account_name": account["name"],
        "product": product,
        "endpoint": random.choice(ENDPOINTS[product]),
        "response_ms": response_ms,
        "status_code": status,
        "bytes_returned": random.randint(500, 5_000_000) if status < 400 else 0,
    }


def write_json_files(events: list[dict], output_path: str) -> list[str]:
    """Write events as chunked JSON files for Auto Loader."""
    os.makedirs(output_path, exist_ok=True)
    filepaths = []

    for i in range(0, len(events), EVENTS_PER_FILE):
        chunk = events[i : i + EVENTS_PER_FILE]
        filename = f"web_events_{i // EVENTS_PER_FILE:04d}.json"
        filepath = os.path.join(output_path, filename)
        with open(filepath, "w") as f:
            for event in chunk:
                f.write(json.dumps(event) + "\n")
        filepaths.append(filepath)

    return filepaths


def main(output_path: str | None = None):
    path = output_path or STAGING_PATH
    all_events = []
    for account in CURATED_ACCOUNTS:
        all_events.extend(_generate_events_for_account(account))

    all_events.sort(key=lambda e: e["event_timestamp"])
    filepaths = write_json_files(all_events, path)
    print(f"Generated {len(all_events)} web events across {len(filepaths)} files "
          f"for {len(CURATED_ACCOUNTS)} accounts -> {path}")
    return all_events


if __name__ == "__main__":
    main()
