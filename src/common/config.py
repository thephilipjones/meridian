"""Central configuration for the Meridian Insights demo.

Single source of truth for catalog names, schema names, table names, and
volume paths. Every pipeline, data generator, notebook, and app module
imports from here — never hardcode table references elsewhere.
"""

import os

CATALOG = os.environ.get("MERIDIAN_CATALOG", "serverless_stable_k2zkdm_catalog")

# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------
SCHEMA_REGULATORY = "meridian_regulatory"
SCHEMA_RESEARCH = "meridian_research"
SCHEMA_INTERNAL = "meridian_internal"
SCHEMA_STAGING = "meridian_staging"

# ---------------------------------------------------------------------------
# Staging volume paths (raw files land here before pipeline ingestion)
# ---------------------------------------------------------------------------
STAGING_BASE = f"/Volumes/{CATALOG}/{SCHEMA_STAGING}"

STAGING_PATHS = {
    # Regulatory (Phase 2)
    "sec_filings": f"{STAGING_BASE}/sec_filings",
    "fda_actions": f"{STAGING_BASE}/fda_actions",
    "patents": f"{STAGING_BASE}/patents",
    # Research
    "pubmed": f"{STAGING_BASE}/pubmed",
    "arxiv": f"{STAGING_BASE}/arxiv",
    "crossref": f"{STAGING_BASE}/crossref",
    # Internal (synthetic)
    "crm": f"{STAGING_BASE}/crm",
    "web_events": f"{STAGING_BASE}/web_events",
    "financials": f"{STAGING_BASE}/financials",
}

# ---------------------------------------------------------------------------
# Table names — fully qualified as {catalog}.{schema}.{table}
# ---------------------------------------------------------------------------

def _fq(schema: str, table: str) -> str:
    return f"{CATALOG}.{schema}.{table}"


class Tables:
    """Fully-qualified table names grouped by business unit and medallion layer."""

    # -- Regulatory (Phase 2) -----------------------------------------------
    RAW_SEC_FILINGS = _fq(SCHEMA_REGULATORY, "raw_sec_filings")
    RAW_FDA_ACTIONS = _fq(SCHEMA_REGULATORY, "raw_fda_actions")
    RAW_PATENTS = _fq(SCHEMA_REGULATORY, "raw_patents")

    CLEANED_SEC_FILINGS = _fq(SCHEMA_REGULATORY, "cleaned_sec_filings")
    CLEANED_FDA_ACTIONS = _fq(SCHEMA_REGULATORY, "cleaned_fda_actions")
    CLEANED_PATENTS = _fq(SCHEMA_REGULATORY, "cleaned_patents")
    QUARANTINE_REGULATORY = _fq(SCHEMA_REGULATORY, "quarantine_regulatory")

    REGULATORY_ACTIONS = _fq(SCHEMA_REGULATORY, "regulatory_actions")
    PATENT_LANDSCAPE = _fq(SCHEMA_REGULATORY, "patent_landscape")
    COMPANY_ENTITIES = _fq(SCHEMA_REGULATORY, "company_entities")
    COMPANY_RISK_SIGNALS = _fq(SCHEMA_REGULATORY, "company_risk_signals")

    # -- Research -----------------------------------------------------------
    RAW_PUBMED_ARTICLES = _fq(SCHEMA_RESEARCH, "raw_pubmed_articles")
    RAW_ARXIV_ARTICLES = _fq(SCHEMA_RESEARCH, "raw_arxiv_articles")
    RAW_CROSSREF_METADATA = _fq(SCHEMA_RESEARCH, "raw_crossref_metadata")

    CLEANED_ARTICLES = _fq(SCHEMA_RESEARCH, "cleaned_articles")
    CLEANED_AUTHORS = _fq(SCHEMA_RESEARCH, "cleaned_authors")
    CLEANED_CITATIONS = _fq(SCHEMA_RESEARCH, "cleaned_citations")
    QUARANTINE_RESEARCH = _fq(SCHEMA_RESEARCH, "quarantine_research")

    ARTICLES = _fq(SCHEMA_RESEARCH, "articles")
    AUTHORS = _fq(SCHEMA_RESEARCH, "authors")
    CITATIONS = _fq(SCHEMA_RESEARCH, "citations")
    MESH_TERMS = _fq(SCHEMA_RESEARCH, "mesh_terms")
    ARTICLE_SEARCH = _fq(SCHEMA_RESEARCH, "article_search")

    # -- Internal -----------------------------------------------------------
    RAW_CRM_DEALS = _fq(SCHEMA_INTERNAL, "raw_crm_deals")
    RAW_WEB_EVENTS = _fq(SCHEMA_INTERNAL, "raw_web_events")
    RAW_FINANCIALS = _fq(SCHEMA_INTERNAL, "raw_financials")

    CLEANED_DEALS = _fq(SCHEMA_INTERNAL, "cleaned_deals")
    CLEANED_WEB_EVENTS = _fq(SCHEMA_INTERNAL, "cleaned_web_events")
    CLEANED_FINANCIALS = _fq(SCHEMA_INTERNAL, "cleaned_financials")
    QUARANTINE_INTERNAL = _fq(SCHEMA_INTERNAL, "quarantine_internal")

    SALES_PIPELINE = _fq(SCHEMA_INTERNAL, "sales_pipeline")
    PRODUCT_USAGE = _fq(SCHEMA_INTERNAL, "product_usage")
    REVENUE_SUMMARY = _fq(SCHEMA_INTERNAL, "revenue_summary")
    CUSTOMER_HEALTH = _fq(SCHEMA_INTERNAL, "customer_health")


# ---------------------------------------------------------------------------
# Product names used across synthetic data and the app
# ---------------------------------------------------------------------------
PRODUCT_NAMES = [
    "Regulatory Feed",
    "Research Platform",
    "Patent Monitor",
    "Custom Analytics",
]

# Meridian fiscal year starts February 1
FISCAL_YEAR_START_MONTH = 2
