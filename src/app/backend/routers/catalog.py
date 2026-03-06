"""Data catalog API endpoints for the customer regulatory view.

Provides a browsable catalog of regulatory data products with
subscription-aware access controls, schema details, and sample data.
"""

import os

from fastapi import APIRouter, HTTPException, Query

from backend.cache import ttl_cache
from backend.db import execute_query

router = APIRouter()

_catalog = os.environ.get("MERIDIAN_CATALOG", "serverless_stable_k2zkdm_catalog")

SUBSCRIPTION_TIERS = {
    "sec_only": {"regulatory_actions", "company_entities", "company_risk_signals"},
    "fda_only": {"regulatory_actions", "patent_landscape", "company_entities", "company_risk_signals"},
    "full": {"regulatory_actions", "patent_landscape", "company_entities", "company_risk_signals"},
}


@router.get("/products")
@ttl_cache(seconds=120)
def list_data_products(subscription_tier: str = Query("sec_only")):
    """List available data products with subscription status."""
    subscribed_tables = SUBSCRIPTION_TIERS.get(subscription_tier, set())

    products = execute_query(f"""
        SELECT
            t.table_name,
            t.comment,
            t.last_altered,
            COUNT(c.column_name) AS column_count
        FROM {_catalog}.information_schema.tables t
        LEFT JOIN {_catalog}.information_schema.columns c
            ON t.table_catalog = c.table_catalog
            AND t.table_schema = c.table_schema
            AND t.table_name = c.table_name
        WHERE t.table_schema = 'meridian_regulatory'
            AND t.table_type IN ('TABLE', 'MATERIALIZED_VIEW')
            AND t.table_name IN ('regulatory_actions', 'patent_landscape',
                                 'company_entities', 'company_risk_signals')
        GROUP BY t.table_name, t.comment, t.last_altered
        ORDER BY t.table_name
    """)

    for product in products:
        product["is_subscribed"] = product["table_name"] in subscribed_tables
        if product["last_altered"]:
            product["freshness"] = str(product["last_altered"])

    return products


_VALID_TABLES = {"regulatory_actions", "patent_landscape", "company_entities", "company_risk_signals"}


@router.get("/products/{table_name}")
@ttl_cache(seconds=120)
def get_product_detail(table_name: str, subscription_tier: str = Query("sec_only")):
    """Get product schema, sample records, and freshness info."""
    if table_name not in _VALID_TABLES:
        raise HTTPException(status_code=404, detail="Table not found")

    subscribed = table_name in SUBSCRIPTION_TIERS.get(subscription_tier, set())

    schema = execute_query(
        f"SELECT column_name, data_type, comment "
        f"FROM {_catalog}.information_schema.columns "
        f"WHERE table_schema = 'meridian_regulatory' "
        f"AND table_name = %(table_name)s "
        f"ORDER BY ordinal_position",
        {"table_name": table_name},
    )

    sample_rows = []
    if subscribed:
        sample_rows = execute_query(
            f"SELECT * FROM {_catalog}.meridian_regulatory.{table_name} LIMIT 5"
        )

    row_count_result = execute_query(
        f"SELECT COUNT(*) AS cnt FROM {_catalog}.meridian_regulatory.{table_name}"
    )
    row_count = row_count_result[0]["cnt"] if row_count_result else 0

    return {
        "table_name": table_name,
        "is_subscribed": subscribed,
        "schema": schema,
        "sample_rows": sample_rows,
        "row_count": row_count,
    }
