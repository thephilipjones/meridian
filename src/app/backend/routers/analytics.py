"""Internal analytics API endpoints for the Sarah Chen (VP, Product Analytics) view.

Provides sales pipeline, product usage, revenue summary data, and an
AI-generated business brief powered by Foundation Model API.
"""

import logging
import os
from functools import lru_cache

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
from fastapi import APIRouter, Query

from backend.cache import ttl_cache
from backend.db import execute_query

router = APIRouter()
log = logging.getLogger(__name__)

_catalog = os.environ.get("MERIDIAN_CATALOG", "serverless_stable_k2zkdm_catalog")
_llm_endpoint = os.environ.get("MERIDIAN_LLM_ENDPOINT", "databricks-meta-llama-3-3-70b-instruct")


@lru_cache(maxsize=1)
def _get_ws_client() -> WorkspaceClient:
    host = os.environ.get("DATABRICKS_HOST")
    client_id = os.environ.get("DATABRICKS_CLIENT_ID")
    client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET")
    if not all([host, client_id, client_secret]):
        missing = [k for k in ("DATABRICKS_HOST", "DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET")
                   if not os.environ.get(k)]
        raise RuntimeError(f"Missing required env vars: {missing}")
    return WorkspaceClient(host=f"https://{host}", client_id=client_id, client_secret=client_secret)


_BRIEF_SYSTEM_PROMPT = """You are Meridian's Chief Analytics Officer. Produce a concise executive business brief with exactly 4 highlights. Each highlight must be a JSON object with these fields:
- "icon": one of "trend-up", "alert", "chart", "insight"
- "title": short headline (5-7 words max)
- "detail": 2-3 sentence explanation with specific numbers from the data
- "sentiment": one of "positive", "warning", "negative", "neutral"

Respond ONLY with a JSON array of exactly 4 highlight objects. No markdown, no extra text.

Guidelines:
1. First highlight: revenue/pipeline headline (use "trend-up" icon)
2. Second highlight: customer health changes (use "alert" icon)
3. Third highlight: product adoption signal (use "chart" icon)
4. Fourth highlight: a forward-looking insight or pattern (use "insight" icon)

Use specific numbers from the data provided. If metrics are improving, sentiment is "positive". If there are concerning signals, use "warning" or "negative"."""


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


@router.get("/business-brief")
@ttl_cache(seconds=300)
def get_business_brief():
    """AI-generated daily business summary using Foundation Model API.

    Queries all 4 internal gold tables, composes them into structured context,
    and calls the LLM to produce a 4-highlight executive brief.
    Cached for 5 minutes to avoid repeated LLM calls on tab switches.
    """
    pipeline = execute_query(
        f"SELECT stage, SUM(deal_count) AS deals, SUM(total_amount) AS amount, "
        f"SUM(total_arr) AS arr, AVG(conversion_rate) AS avg_conversion "
        f"FROM {_catalog}.meridian_internal.sales_pipeline GROUP BY stage ORDER BY stage"
    )
    revenue = execute_query(
        f"SELECT fiscal_year, fiscal_quarter, product_line, revenue, "
        f"gross_margin_pct, yoy_revenue_growth "
        f"FROM {_catalog}.meridian_internal.revenue_summary "
        f"ORDER BY fiscal_year DESC, fiscal_quarter DESC LIMIT 20"
    )
    health = execute_query(
        f"SELECT account_name, arr, health_score, health_tier, "
        f"products_subscribed, error_rate_30d "
        f"FROM {_catalog}.meridian_internal.customer_health "
        f"ORDER BY arr DESC LIMIT 20"
    )
    usage = execute_query(
        f"SELECT product, SUM(api_calls) AS total_calls, "
        f"COUNT(DISTINCT account_name) AS unique_accounts, "
        f"AVG(error_rate) AS avg_error_rate "
        f"FROM {_catalog}.meridian_internal.product_usage "
        f"GROUP BY product ORDER BY total_calls DESC"
    )

    context_parts = []

    total_pipeline = sum(r.get("amount", 0) for r in pipeline)
    total_arr = sum(r.get("arr", 0) for r in pipeline)
    total_deals = sum(r.get("deals", 0) for r in pipeline)
    stage_details = ", ".join(
        "{}: {} deals (${:,.0f})".format(r.get("stage"), r.get("deals"), r.get("amount", 0))
        for r in pipeline
    )
    context_parts.append(
        f"PIPELINE SUMMARY: {total_deals} active deals, "
        f"${total_pipeline:,.0f} total pipeline, ${total_arr:,.0f} total ARR.\n"
        f"By stage: {stage_details}"
    )

    if revenue:
        latest_q = revenue[0]
        product_details = ", ".join(
            "{}: ${:,.0f}".format(r.get("product_line"), r.get("revenue", 0))
            for r in revenue[:6]
        )
        context_parts.append(
            f"LATEST REVENUE: FY{latest_q.get('fiscal_year')} {latest_q.get('fiscal_quarter')} — "
            f"${latest_q.get('revenue', 0):,.0f} revenue, "
            f"{latest_q.get('gross_margin_pct', 0):.1f}% gross margin, "
            f"{latest_q.get('yoy_revenue_growth') or 'N/A'}% YoY growth.\n"
            f"By product: {product_details}"
        )

    at_risk = [h for h in health if h.get("health_tier") in ("At Risk", "Churning")]
    healthy = [h for h in health if h.get("health_tier") == "Healthy"]
    at_risk_details = ", ".join(
        "{} (${:,.0f} ARR, score {})".format(h.get("account_name"), h.get("arr", 0), h.get("health_score"))
        for h in at_risk[:5]
    ) or "None"
    top_acct_details = ", ".join(
        "{} (${:,.0f}, {})".format(h.get("account_name"), h.get("arr", 0), h.get("health_tier"))
        for h in health[:5]
    )
    context_parts.append(
        f"CUSTOMER HEALTH: {len(healthy)} healthy accounts, {len(at_risk)} at-risk/churning.\n"
        f"At-risk accounts: {at_risk_details}.\n"
        f"Top accounts by ARR: {top_acct_details}"
    )

    usage_details = ", ".join(
        "{}: {:,} calls from {} accounts ({:.1%} error rate)".format(
            u.get("product"), u.get("total_calls", 0), u.get("unique_accounts", 0), u.get("avg_error_rate", 0)
        )
        for u in usage
    )
    context_parts.append(f"PRODUCT USAGE: {usage_details}")

    context = "\n\n".join(context_parts)

    try:
        w = _get_ws_client()
        response = w.serving_endpoints.query(
            name=_llm_endpoint,
            messages=[
                ChatMessage(role=ChatMessageRole.SYSTEM, content=_BRIEF_SYSTEM_PROMPT),
                ChatMessage(role=ChatMessageRole.USER, content=f"Generate the executive business brief from this data:\n\n{context}"),
            ],
            max_tokens=800,
            temperature=0.2,
        )
        raw_answer = response.choices[0].message.content.strip()

        import json
        raw_answer = raw_answer.strip()
        if raw_answer.startswith("```"):
            raw_answer = raw_answer.split("\n", 1)[1].rsplit("```", 1)[0].strip()
        parsed = json.loads(raw_answer)
        highlights = parsed if isinstance(parsed, list) else ([parsed] if isinstance(parsed, dict) else [])
        highlights = [h for h in highlights if isinstance(h, dict)]

        from datetime import datetime, timezone
        return {
            "highlights": highlights[:4],
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as e:
        log.error("Business brief generation failed: %s", e)
        from datetime import datetime, timezone
        return {
            "highlights": [
                {"icon": "trend-up", "title": "Revenue Data Available", "detail": f"Total pipeline: ${total_pipeline:,.0f} across {total_deals} deals. Connect to see the full AI-generated brief.", "sentiment": "neutral"},
                {"icon": "alert", "title": f"{len(at_risk)} At-Risk Accounts", "detail": f"Accounts flagged for health score decline. Total healthy: {len(healthy)}.", "sentiment": "warning" if at_risk else "positive"},
                {"icon": "chart", "title": "Product Usage Active", "detail": f"{len(usage)} products tracked across customer base.", "sentiment": "neutral"},
                {"icon": "insight", "title": "Brief Generation Unavailable", "detail": "The AI business brief could not be generated. Data summaries above are from live gold tables.", "sentiment": "neutral"},
            ],
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "error": str(e),
        }
