"""Internal analytics API endpoints for the Sarah Chen (RevOps) view.

Provides sales pipeline, product usage, and revenue summary data
scoped to the internal business unit.
"""

import os

from fastapi import APIRouter, Query

from backend.db import execute_query

router = APIRouter()

_catalog = os.environ.get("MERIDIAN_CATALOG", "serverless_stable_k2zkdm_catalog")


@router.get("/sales-pipeline")
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
