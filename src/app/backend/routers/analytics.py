"""Internal analytics API endpoints for the Sarah Chen (RevOps) view.

Provides sales pipeline, product usage, and revenue summary data
scoped to the internal business unit.
"""

import os

from fastapi import APIRouter, Query

from src.app.backend.db import execute_query
from src.common.config import CATALOG

router = APIRouter()

_catalog = os.environ.get("MERIDIAN_CATALOG", CATALOG)


@router.get("/sales-pipeline")
def get_sales_pipeline(
    product_line: str | None = Query(None),
    region: str | None = Query(None),
):
    """Sales pipeline by stage with optional product/region filter."""
    query = f"SELECT * FROM {_catalog}.internal.sales_pipeline WHERE 1=1"
    if product_line:
        query += f" AND product_line = '{product_line}'"
    if region:
        query += f" AND region = '{region}'"
    query += " ORDER BY stage"
    return execute_query(query)


@router.get("/product-usage")
def get_product_usage(
    account_name: str | None = Query(None),
    product: str | None = Query(None),
    limit: int = Query(100, le=1000),
):
    """Product usage metrics by account and product."""
    query = f"SELECT * FROM {_catalog}.internal.product_usage WHERE 1=1"
    if account_name:
        query += f" AND account_name = '{account_name}'"
    if product:
        query += f" AND product = '{product}'"
    query += f" ORDER BY period DESC LIMIT {limit}"
    return execute_query(query)


@router.get("/revenue")
def get_revenue_summary(
    fiscal_year: int | None = Query(None),
    product_line: str | None = Query(None),
):
    """Revenue summary with YoY growth by quarter and product."""
    query = f"SELECT * FROM {_catalog}.internal.revenue_summary WHERE 1=1"
    if fiscal_year:
        query += f" AND fiscal_year = {fiscal_year}"
    if product_line:
        query += f" AND product_line = '{product_line}'"
    query += " ORDER BY fiscal_year, fiscal_quarter"
    return execute_query(query)


@router.get("/customer-health")
def get_customer_health(
    health_tier: str | None = Query(None),
    limit: int = Query(50, le=500),
):
    """Customer health scores and tiers."""
    query = f"SELECT * FROM {_catalog}.internal.customer_health WHERE 1=1"
    if health_tier:
        query += f" AND health_tier = '{health_tier}'"
    query += f" ORDER BY arr DESC LIMIT {limit}"
    return execute_query(query)
