"""Generate synthetic financial summaries for Meridian Internal.

Produces quarterly revenue, cost, and margin data by product line,
consistent with CRM account names and product lines. Writes CSV files
to the financials staging volume for COPY INTO ingestion.
"""

import csv
import io
import os
import random

from src.common.config import FISCAL_YEAR_START_MONTH, PRODUCT_NAMES, STAGING_PATHS

random.seed(42)

FISCAL_YEARS = [2024, 2025, 2026]
QUARTERS = ["Q1", "Q2", "Q3", "Q4"]

PRODUCT_BASE_REVENUE = {
    "Regulatory Feed": 4_200_000,
    "Research Platform": 3_100_000,
    "Patent Monitor": 1_800_000,
    "Custom Analytics": 2_500_000,
}

PRODUCT_MARGIN_RANGE = {
    "Regulatory Feed": (0.62, 0.72),
    "Research Platform": (0.68, 0.78),
    "Patent Monitor": (0.55, 0.65),
    "Custom Analytics": (0.70, 0.82),
}

PRODUCT_CUSTOMER_BASE = {
    "Regulatory Feed": 120,
    "Research Platform": 85,
    "Patent Monitor": 45,
    "Custom Analytics": 60,
}


def generate_financials() -> list[dict]:
    rows = []
    for fy in FISCAL_YEARS:
        for qi, q in enumerate(QUARTERS):
            for product in PRODUCT_NAMES:
                base = PRODUCT_BASE_REVENUE[product]
                year_growth = 1.0 + 0.12 * (fy - FISCAL_YEARS[0])
                q_seasonality = [0.22, 0.24, 0.23, 0.31][qi]
                revenue = round(base * year_growth * q_seasonality * random.uniform(0.92, 1.08), 2)

                margin_lo, margin_hi = PRODUCT_MARGIN_RANGE[product]
                gross_margin_pct = random.uniform(margin_lo, margin_hi)
                cost_of_data = round(revenue * (1 - gross_margin_pct), 2)
                gross_margin = round(revenue - cost_of_data, 2)

                base_customers = PRODUCT_CUSTOMER_BASE[product]
                customer_count = int(base_customers * year_growth * random.uniform(0.9, 1.1))

                rows.append({
                    "fiscal_quarter": q,
                    "fiscal_year": fy,
                    "product_line": product,
                    "revenue": revenue,
                    "cost_of_data": cost_of_data,
                    "gross_margin": gross_margin,
                    "customer_count": customer_count,
                })
    return rows


def write_csv(rows: list[dict], output_path: str) -> str:
    os.makedirs(output_path, exist_ok=True)
    filepath = os.path.join(output_path, "financial_summaries.csv")

    fieldnames = list(rows[0].keys())
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

    with open(filepath, "w") as f:
        f.write(buf.getvalue())

    return filepath


def main(output_path: str | None = None):
    path = output_path or STAGING_PATHS["financials"]
    rows = generate_financials()
    filepath = write_csv(rows, path)
    print(f"Generated {len(rows)} financial summary rows -> {filepath}")
    return rows


if __name__ == "__main__":
    main()
