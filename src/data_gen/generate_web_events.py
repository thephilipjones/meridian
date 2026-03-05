# Databricks notebook source
# MAGIC %pip install faker -q

# COMMAND ----------

"""Generate synthetic web analytics / product usage events for Meridian Internal."""

import json
import math
import os
import random
import uuid
from datetime import datetime, timedelta

from faker import Faker

dbutils.widgets.text("catalog_name", "serverless_stable_k2zkdm_catalog")
_catalog = dbutils.widgets.get("catalog_name")

PRODUCT_NAMES = ["Regulatory Feed", "Research Platform", "Patent Monitor", "Custom Analytics"]
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

STATUS_CODES = [200, 200, 200, 200, 200, 200, 200, 200, 201, 400, 404, 500]

NUM_CUSTOMERS = 80
NUM_EVENTS = 50000
EVENTS_PER_FILE = 2500


def _seasonal_multiplier(day_of_year: int) -> float:
    """Business seasonality — higher usage in Q1/Q4, dip in summer."""
    return 1.0 + 0.3 * math.cos(2 * math.pi * (day_of_year - 30) / 365)


def generate_customers(n: int) -> list[dict]:
    customers = []
    for _ in range(n):
        products = random.sample(PRODUCT_NAMES, k=random.randint(1, 3))
        customers.append({
            "customer_id": str(uuid.uuid4())[:8],
            "account_name": fake.company(),
            "products": products,
        })
    return customers


def generate_events(customers: list[dict], n: int) -> list[dict]:
    events = []
    base_date = datetime(2024, 2, 1)  # Aligns with FY2024 start

    for _ in range(n):
        customer = random.choice(customers)
        day_offset = random.randint(0, 730)
        ts = base_date + timedelta(
            days=day_offset,
            hours=random.randint(6, 22),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59),
        )

        multiplier = _seasonal_multiplier(day_offset)
        if random.random() > multiplier * 0.5:
            continue

        product = random.choice(customer["products"])
        status = random.choice(STATUS_CODES)
        response_ms = int(random.lognormvariate(4.5, 0.8))
        if status >= 400:
            response_ms = int(response_ms * random.uniform(2.0, 5.0))

        events.append({
            "event_id": str(uuid.uuid4()),
            "event_type": random.choices(EVENT_TYPES, weights=EVENT_TYPE_WEIGHTS, k=1)[0],
            "event_timestamp": ts.isoformat(),
            "customer_id": customer["customer_id"],
            "account_name": customer["account_name"],
            "product": product,
            "endpoint": random.choice(ENDPOINTS[product]),
            "response_ms": response_ms,
            "status_code": status,
            "bytes_returned": random.randint(500, 5_000_000) if status < 400 else 0,
        })

    events.sort(key=lambda e: e["event_timestamp"])
    return events[:n]


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
    customers = generate_customers(NUM_CUSTOMERS)
    events = generate_events(customers, NUM_EVENTS)
    filepaths = write_json_files(events, path)
    print(f"Generated {len(events)} web events across {len(filepaths)} files -> {path}")
    return events


if __name__ == "__main__":
    main()
