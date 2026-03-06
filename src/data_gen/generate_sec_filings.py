# Databricks notebook source
# MAGIC %pip install faker -q

# COMMAND ----------

"""Generate synthetic SEC EDGAR filing metadata for Meridian Regulatory."""

import json
import os
import random
import uuid
from datetime import datetime, timedelta

from faker import Faker

dbutils.widgets.text("catalog_name", "serverless_stable_k2zkdm_catalog")
_catalog = dbutils.widgets.get("catalog_name")

STAGING_PATH = f"/Volumes/{_catalog}/meridian_staging/sec_filings"

fake = Faker()
Faker.seed(42)
random.seed(42)

NUM_FILINGS = 2000
RECORDS_PER_FILE = 500

# Shared company pool — reused across SEC, FDA, and patent generators so
# the gold company_entities table can demonstrate cross-source entity matching.
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

FILING_TYPES = ["10-K", "10-Q", "8-K"]
FILING_TYPE_WEIGHTS = [0.20, 0.45, 0.35]

SIC_CODES = {
    "2830": "Industrial Chemicals",
    "2834": "Pharmaceutical Preparations",
    "2835": "In Vitro Diagnostic Substances",
    "2836": "Biological Products",
    "3841": "Surgical & Medical Instruments",
    "3845": "Electromedical Equipment",
    "5047": "Medical & Hospital Equipment (Wholesale)",
    "5122": "Drugs, Drug Proprietaries & Sundries",
    "7372": "Prepackaged Software",
    "8731": "Commercial Physical & Biological Research",
}

STATES = [
    "NY", "NJ", "CA", "PA", "IL", "MA", "CT", "IN",
    "MN", "MD", "NC", "TX", "WI", "DE", "FL",
]

DESCRIPTION_TEMPLATES = [
    "Annual report for fiscal year ended {date}.",
    "Quarterly report for the period ended {date}.",
    "Current report: {event}",
]

_8K_EVENTS = [
    "Entry into a Material Definitive Agreement",
    "Completion of Acquisition or Disposition of Assets",
    "Results of Operations and Financial Condition",
    "Changes in Registrant's Certifying Accountant",
    "Other Events — Clinical Trial Results",
    "Other Events — Product Approval",
    "Other Events — Strategic Partnership",
    "Regulation FD Disclosure",
    "Financial Statements and Exhibits",
    "Departure of Directors or Principal Officers",
    "Submission of Matters to a Vote of Security Holders",
]


def _gen_cik() -> str:
    return str(random.randint(700000, 1999999))


def _company_ciks(companies: list[str]) -> dict[str, str]:
    """Assign a stable CIK to each company (deterministic given seed)."""
    return {c: _gen_cik() for c in companies}


def generate_filings(n: int) -> list[dict]:
    cik_map = _company_ciks(COMPANIES)
    filings = []
    yesterday = datetime.today() - timedelta(days=1)
    base_date = yesterday - timedelta(days=3 * 365)
    span_days = (yesterday - base_date).days

    for _ in range(n):
        company = random.choice(COMPANIES)
        filing_type = random.choices(FILING_TYPES, weights=FILING_TYPE_WEIGHTS, k=1)[0]
        filing_date = base_date + timedelta(days=random.randint(0, span_days))
        sic_code = random.choice(list(SIC_CODES.keys()))

        if filing_type == "8-K":
            description = f"Current report: {random.choice(_8K_EVENTS)}"
        elif filing_type == "10-K":
            description = f"Annual report for fiscal year ended {filing_date.strftime('%B %d, %Y')}."
        else:
            description = f"Quarterly report for the period ended {filing_date.strftime('%B %d, %Y')}."

        filings.append({
            "filing_id": f"edgar-{uuid.uuid4().hex[:12]}",
            "cik": cik_map[company],
            "company_name": company,
            "filing_type": filing_type,
            "filing_date": filing_date.strftime("%Y-%m-%d"),
            "description": description,
            "sic_code": sic_code,
            "state": random.choice(STATES),
        })

    return filings


def write_json_files(filings: list[dict], output_path: str) -> list[str]:
    os.makedirs(output_path, exist_ok=True)
    filepaths = []

    for i in range(0, len(filings), RECORDS_PER_FILE):
        chunk = filings[i : i + RECORDS_PER_FILE]
        filename = f"sec_filings_{i // RECORDS_PER_FILE:04d}.json"
        filepath = os.path.join(output_path, filename)
        with open(filepath, "w") as f:
            for record in chunk:
                f.write(json.dumps(record) + "\n")
        filepaths.append(filepath)

    return filepaths


def main(output_path: str | None = None):
    path = output_path or STAGING_PATH
    filings = generate_filings(NUM_FILINGS)
    filepaths = write_json_files(filings, path)
    print(f"Generated {len(filings)} SEC filings across {len(filepaths)} files -> {path}")
    return filings


if __name__ == "__main__":
    main()
