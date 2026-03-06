"""Internal analytics API endpoints for the Sarah Chen (RevOps) view.

Provides sales pipeline, product usage, and revenue summary data
scoped to the internal business unit.
"""

import os

from fastapi import APIRouter, Query

from backend.cache import ttl_cache
from backend.db import execute_query

router = APIRouter()

_catalog = os.environ.get("MERIDIAN_CATALOG", "serverless_stable_k2zkdm_catalog")


@router.get("/sales-pipeline")
@ttl_cache(seconds=60)
def get_sales_pipeline(
    product_line: str | None = Query(None),
    region: str | None = Query(None),
):
    """Sales pipeline by stage with optional product/region filter."""
    clauses = []
    params: dict = {}

    if product_line:
        clauses.append("product_line = %(product_line)s")
        params["product_line"] = product_line
    if region:
        clauses.append("region = %(region)s")
        params["region"] = region

    where = (" AND ".join(clauses)) if clauses else "1=1"
    query = f"SELECT * FROM {_catalog}.meridian_internal.sales_pipeline WHERE {where} ORDER BY stage"
    return execute_query(query, params or None)


@router.get("/product-usage")
@ttl_cache(seconds=60)
def get_product_usage(
    account_name: str | None = Query(None),
    product: str | None = Query(None),
    limit: int = Query(100, le=1000),
):
    """Product usage metrics by account and product."""
    clauses = []
    params: dict = {}

    if account_name:
        clauses.append("account_name = %(account_name)s")
        params["account_name"] = account_name
    if product:
        clauses.append("product = %(product)s")
        params["product"] = product

    where = (" AND ".join(clauses)) if clauses else "1=1"
    query = f"SELECT * FROM {_catalog}.meridian_internal.product_usage WHERE {where} ORDER BY period DESC LIMIT {int(limit)}"
    return execute_query(query, params or None)


@router.get("/revenue")
@ttl_cache(seconds=60)
def get_revenue_summary(
    fiscal_year: int | None = Query(None),
    product_line: str | None = Query(None),
):
    """Revenue summary with YoY growth by quarter and product."""
    clauses = []
    params: dict = {}

    if fiscal_year:
        clauses.append("fiscal_year = %(fiscal_year)s")
        params["fiscal_year"] = fiscal_year
    if product_line:
        clauses.append("product_line = %(product_line)s")
        params["product_line"] = product_line

    where = (" AND ".join(clauses)) if clauses else "1=1"
    query = f"SELECT * FROM {_catalog}.meridian_internal.revenue_summary WHERE {where} ORDER BY fiscal_year, fiscal_quarter"
    return execute_query(query, params or None)


@router.get("/customer-health")
@ttl_cache(seconds=60)
def get_customer_health(
    health_tier: str | None = Query(None),
    limit: int = Query(50, le=500),
):
    """Customer health scores and tiers."""
    clauses = []
    params: dict = {}

    if health_tier:
        clauses.append("health_tier = %(health_tier)s")
        params["health_tier"] = health_tier

    where = (" AND ".join(clauses)) if clauses else "1=1"
    query = f"SELECT * FROM {_catalog}.meridian_internal.customer_health WHERE {where} ORDER BY arr DESC LIMIT {int(limit)}"
    return execute_query(query, params or None)


@router.get("/query-activity")
@ttl_cache(seconds=60)
def get_query_activity(days: int = Query(30, le=90)):
    """Daily query activity against the Meridian catalog from system tables."""
    query = (
        f"SELECT event_date, user_email, action_name, query_count "
        f"FROM {_catalog}.meridian_system.query_activity "
        f"WHERE event_date >= CURRENT_DATE - INTERVAL {int(days)} DAYS "
        f"ORDER BY event_date DESC, query_count DESC"
    )
    return execute_query(query)


@router.get("/table-access")
@ttl_cache(seconds=60)
def get_table_access(limit: int = Query(20, le=100)):
    """Most-accessed tables across Meridian schemas."""
    query = (
        f"SELECT schema_name, table_name, unique_users, access_count, last_accessed "
        f"FROM {_catalog}.meridian_system.table_access_patterns "
        f"ORDER BY access_count DESC LIMIT {int(limit)}"
    )
    return execute_query(query)


@router.get("/compute-consumption")
@ttl_cache(seconds=60)
def get_compute_consumption(days: int = Query(30, le=90)):
    """Daily compute consumption (DBUs) by SKU."""
    query = (
        f"SELECT usage_date, sku_name, usage_unit, total_dbus "
        f"FROM {_catalog}.meridian_system.compute_consumption "
        f"WHERE usage_date >= CURRENT_DATE - INTERVAL {int(days)} DAYS "
        f"ORDER BY usage_date DESC, total_dbus DESC"
    )
    return execute_query(query)
