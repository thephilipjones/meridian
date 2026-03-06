# Databricks notebook source
# MAGIC %pip install faker -q

# COMMAND ----------

"""Generate synthetic FDA enforcement / recall actions for Meridian Regulatory."""

import json
import os
import random
import uuid
from datetime import datetime, timedelta

from faker import Faker

dbutils.widgets.text("catalog_name", "serverless_stable_k2zkdm_catalog")
_catalog = dbutils.widgets.get("catalog_name")

STAGING_PATH = f"/Volumes/{_catalog}/meridian_staging/fda_actions"

fake = Faker()
Faker.seed(43)
random.seed(43)

NUM_ACTIONS = 1500
RECORDS_PER_FILE = 500

# Shared company pool — overlaps with SEC and patent generators for
# cross-source entity matching in the gold company_entities table.
COMPANIES = [
    "Pfizer Inc.", "Johnson & Johnson", "Merck & Co.", "AbbVie Inc.",
    "Bristol-Myers Squibb", "Amgen Inc.", "Gilead Sciences", "Eli Lilly",
    "Regeneron Pharmaceuticals", "Moderna Inc.", "Biogen Inc.",
    "Vertex Pharmaceuticals", "Illumina Inc.", "Thermo Fisher Scientific",
    "Danaher Corporation", "Becton Dickinson", "Edwards Lifesciences",
    "Intuitive Surgical", "Stryker Corporation", "Medtronic PLC",
    "Abbott Laboratories", "Baxter International", "Boston Scientific",
    "Zimmer Biomet", "Agilent Technologies", "Bio-Rad Laboratories",
    "Hologic Inc.", "ResMed Inc.", "West Pharmaceutical Services",
    "Catalent Inc.", "Exact Sciences", "Guardant Health",
    "Natera Inc.", "10x Genomics", "Twist Bioscience",
    "Novavax Inc.", "BioNTech SE", "CureVac NV", "Arctus Therapeutics",
    "Alnylam Pharmaceuticals", "Sarepta Therapeutics", "Blueprint Medicines",
    "Relay Therapeutics", "Revolution Medicines", "Arcus Biosciences",
    "Vir Biotechnology", "Krystal Biotech", "Disc Medicine",
    "Nuvalent Inc.", "Recursion Pharmaceuticals",
]

STATUSES = ["Ongoing", "Completed", "Terminated"]
STATUS_WEIGHTS = [0.30, 0.55, 0.15]

CLASSIFICATIONS = ["Class I", "Class II", "Class III"]
CLASSIFICATION_WEIGHTS = [0.15, 0.55, 0.30]

PRODUCT_CATEGORIES = [
    "Cardiovascular Drug", "Oncology Drug", "Immunosuppressant",
    "Antiviral Agent", "Antibacterial Agent", "Insulin Product",
    "Biologic Product", "Blood Glucose Monitor", "Surgical Implant",
    "Infusion Pump", "Ventilator System", "Diagnostic Reagent Kit",
    "Ophthalmic Solution", "Dermatological Cream", "Vaccine",
    "Analgesic", "Anti-Inflammatory", "Anticoagulant",
    "Respiratory Therapy Device", "Orthopedic Implant",
]

RECALL_REASONS = [
    "cGMP Deviations; product may not meet established specifications",
    "Cross-contamination: product may contain trace amounts of another drug substance",
    "Labeling: mislabeled strength on product packaging",
    "Sterility assurance: deviation from validated sterilization process",
    "Subpotent: product does not meet dissolution specifications",
    "Superpotent: active ingredient exceeds acceptable limits",
    "Particulate matter observed in injectable product",
    "Failed stability testing: product may not maintain potency through expiration",
    "Defective device component may malfunction during clinical use",
    "Software defect may cause incorrect dosage calculation",
    "Packaging defect: blister seal integrity compromised",
    "Impurity above specification limit detected during routine testing",
    "Incorrect expiration date printed on product label",
    "Foreign material found during manufacturing quality review",
    "Temperature excursion during storage may affect product integrity",
]

CITIES = [
    "New York", "San Francisco", "Indianapolis", "Cambridge", "Thousand Oaks",
    "Foster City", "Tarrytown", "Kenilworth", "North Chicago", "Princeton",
    "San Diego", "Carlsbad", "Wilmington", "Minneapolis", "Lake Forest",
    "Deerfield", "Marlborough", "Wayne", "Kalamazoo", "Santa Clara",
]

STATES = [
    "NY", "CA", "IN", "MA", "CA", "CA", "NY", "NJ", "IL", "NJ",
    "CA", "CA", "DE", "MN", "IL", "IL", "MA", "NJ", "MI", "CA",
]


def generate_actions(n: int) -> list[dict]:
    actions = []
    yesterday = datetime.today() - timedelta(days=1)
    base_date = yesterday - timedelta(days=3 * 365)
    span_days = (yesterday - base_date).days

    for _ in range(n):
        company = random.choice(COMPANIES)
        city_idx = random.randint(0, len(CITIES) - 1)
        recall_date = base_date + timedelta(days=random.randint(0, span_days))

        actions.append({
            "action_id": f"fda-{uuid.uuid4().hex[:12]}",
            "product_description": random.choice(PRODUCT_CATEGORIES),
            "reason_for_recall": random.choice(RECALL_REASONS),
            "status": random.choices(STATUSES, weights=STATUS_WEIGHTS, k=1)[0],
            "classification": random.choices(CLASSIFICATIONS, weights=CLASSIFICATION_WEIGHTS, k=1)[0],
            "recall_initiation_date": recall_date.strftime("%Y-%m-%d"),
            "company_name": company,
            "city": CITIES[city_idx],
            "state": STATES[city_idx],
        })

    return actions


def write_json_files(actions: list[dict], output_path: str) -> list[str]:
    os.makedirs(output_path, exist_ok=True)
    filepaths = []

    for i in range(0, len(actions), RECORDS_PER_FILE):
        chunk = actions[i : i + RECORDS_PER_FILE]
        filename = f"fda_actions_{i // RECORDS_PER_FILE:04d}.json"
        filepath = os.path.join(output_path, filename)
        with open(filepath, "w") as f:
            for record in chunk:
                f.write(json.dumps(record) + "\n")
        filepaths.append(filepath)

    return filepaths


def main(output_path: str | None = None):
    path = output_path or STAGING_PATH
    actions = generate_actions(NUM_ACTIONS)
    filepaths = write_json_files(actions, path)
    print(f"Generated {len(actions)} FDA actions across {len(filepaths)} files -> {path}")
    return actions


if __name__ == "__main__":
    main()
