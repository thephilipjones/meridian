"""Generate synthetic CRM / sales pipeline data for Meridian Internal.

Produces ~500 accounts and ~2000 opportunities with realistic stage
distributions, ARR values, and close dates. Writes CSV files to the
CRM staging volume for COPY INTO ingestion.
"""

import csv
import io
import os
import random
import uuid
from datetime import datetime, timedelta

from faker import Faker

from src.common.config import PRODUCT_NAMES, STAGING_PATHS

fake = Faker()
Faker.seed(42)
random.seed(42)

STAGES = ["Prospecting", "Qualification", "Proposal", "Negotiation", "Closed Won", "Closed Lost"]
STAGE_WEIGHTS = [0.20, 0.25, 0.20, 0.15, 0.12, 0.08]

REGIONS = ["North America", "EMEA", "APAC", "LATAM"]
REGION_WEIGHTS = [0.45, 0.30, 0.15, 0.10]

OWNERS = [fake.name() for _ in range(12)]

NUM_ACCOUNTS = 500
NUM_DEALS = 2000


def generate_accounts(n: int) -> list[dict]:
    accounts = []
    for _ in range(n):
        accounts.append({
            "account_id": str(uuid.uuid4())[:8],
            "account_name": fake.company(),
            "region": random.choices(REGIONS, weights=REGION_WEIGHTS, k=1)[0],
        })
    return accounts


def generate_deals(accounts: list[dict], n: int) -> list[dict]:
    deals = []
    base_date = datetime(2024, 2, 1)  # Aligns with FY2024 start

    for _ in range(n):
        account = random.choice(accounts)
        stage = random.choices(STAGES, weights=STAGE_WEIGHTS, k=1)[0]
        created = base_date + timedelta(days=random.randint(0, 730))
        close_offset = random.randint(30, 180)

        is_closed = stage.startswith("Closed")
        amount = round(random.lognormvariate(10, 1.2), 2)
        arr = round(amount * random.uniform(0.6, 0.95), 2) if is_closed and "Won" in stage else 0.0

        deals.append({
            "deal_id": str(uuid.uuid4())[:12],
            "account_name": account["account_name"],
            "account_id": account["account_id"],
            "deal_name": f"{account['account_name']} - {random.choice(PRODUCT_NAMES)}",
            "stage": stage,
            "amount": amount,
            "arr": arr,
            "close_date": (created + timedelta(days=close_offset)).strftime("%Y-%m-%d"),
            "created_date": created.strftime("%Y-%m-%d"),
            "owner": random.choice(OWNERS),
            "product_line": random.choice(PRODUCT_NAMES),
            "region": account["region"],
        })
    return deals


def write_csv(deals: list[dict], output_path: str) -> str:
    """Write deals to CSV and return the file path."""
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
    path = output_path or STAGING_PATHS["crm"]
    accounts = generate_accounts(NUM_ACCOUNTS)
    deals = generate_deals(accounts, NUM_DEALS)
    filepath = write_csv(deals, path)
    print(f"Generated {len(deals)} CRM deals across {len(accounts)} accounts -> {filepath}")
    return deals


if __name__ == "__main__":
    main()
