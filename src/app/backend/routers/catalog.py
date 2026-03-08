"""Data catalog API endpoints for the customer regulatory view.

Provides a browsable catalog of regulatory data products with
subscription-aware access controls, schema details, sample data,
and a regulatory intelligence feed with entity-level risk signals.
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

_SOURCE_TIERS = {
    "sec_only": ["SEC"],
    "fda_only": ["SEC", "FDA"],
    "full": ["SEC", "FDA", "USPTO"],
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


@router.get("/feed")
@ttl_cache(seconds=120)
def get_regulatory_feed(
    subscription_tier: str = Query("sec_only"),
    limit: int = Query(25, le=100),
):
    """Recent regulatory actions with entity context and risk signals.

    Joins regulatory_actions with company_entities and company_risk_signals
    to produce a rich intelligence feed. Items from unsubscribed sources
    are returned with minimal detail and a locked flag for teaser display.
    """
    subscribed_sources = _SOURCE_TIERS.get(subscription_tier, ["SEC"])
    all_sources = ["SEC", "FDA", "USPTO"]

    source_list = ", ".join(f"'{s}'" for s in all_sources)
    query = f"""
        SELECT
            ra.action_id,
            ra.action_date,
            ra.action_source AS source,
            ra.action_type,
            ra.action_type AS title,
            ra.action_description AS description,
            ra.company_name,
            CAST(NULL AS STRING) AS filing_url,
            ce.entity_id,
            CAST(NULL AS STRING) AS industry,
            ra.source_reference_id AS cik_number,
            ce.primary_state AS jurisdiction,
            crs.risk_tier AS overall_risk_level,
            crs.total_actions AS risk_signal_count,
            crs.latest_action_date AS latest_signal_date
        FROM {_catalog}.meridian_regulatory.regulatory_actions ra
        LEFT JOIN {_catalog}.meridian_regulatory.company_entities ce
            ON ra.company_name = ce.company_name
        LEFT JOIN {_catalog}.meridian_regulatory.company_risk_signals crs
            ON ce.company_name = crs.company_name
        WHERE ra.action_source IN ({source_list})
        ORDER BY ra.action_date DESC
        LIMIT {int(limit)}
    """
    rows = execute_query(query)

    for row in rows:
        row["is_subscribed"] = row.get("source") in subscribed_sources
        if not row["is_subscribed"]:
            row["description"] = None
            row["filing_url"] = None
            row["cik_number"] = None

    return rows


@router.get("/feed/summary")
@ttl_cache(seconds=120)
def get_feed_summary(subscription_tier: str = Query("sec_only")):
    """Aggregate counts for the feed header: total actions, entities, risk signals."""
    subscribed_sources = _SOURCE_TIERS.get(subscription_tier, ["SEC"])
    source_list = ", ".join(f"'{s}'" for s in subscribed_sources)

    stats = execute_query(f"""
        SELECT
            COUNT(DISTINCT ra.action_id) AS total_actions,
            COUNT(DISTINCT ra.company_name) AS total_entities,
            COALESCE(SUM(crs.total_actions), 0) AS total_risk_signals
        FROM {_catalog}.meridian_regulatory.regulatory_actions ra
        LEFT JOIN {_catalog}.meridian_regulatory.company_risk_signals crs
            ON ra.company_name = crs.company_name
        WHERE ra.action_source IN ({source_list})
            AND ra.action_date >= CURRENT_DATE - INTERVAL 90 DAYS
    """)

    return stats[0] if stats else {"total_actions": 0, "total_entities": 0, "total_risk_signals": 0}
