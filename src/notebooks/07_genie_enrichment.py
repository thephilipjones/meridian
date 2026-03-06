# Databricks notebook source
# MAGIC %md
# MAGIC # Meridian Insights — Genie Space Enrichment
# MAGIC
# MAGIC Patches all 3 Genie spaces with `sample_questions`, `sql_snippets`,
# MAGIC and table descriptions that improve Genie's accuracy. These details
# MAGIC are embedded in the `serialized_space` JSON via the REST API.
# MAGIC
# MAGIC > *"These example questions and SQL snippets teach Genie the
# MAGIC > vocabulary and semantics of Meridian's data model."*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import json

dbutils.widgets.text("catalog_name", "serverless_stable_k2zkdm_catalog")
dbutils.widgets.text("research_space_id", "01f118c5d7e01a32a58159a70e55160c")
dbutils.widgets.text("internal_space_id", "01f118c5ddb81e3dba76005e6020b2bc")
dbutils.widgets.text("regulatory_space_id", "01f118ce34db1cfeb9085c37cea33f8d")

catalog = dbutils.widgets.get("catalog_name")
RESEARCH_SPACE_ID = dbutils.widgets.get("research_space_id")
INTERNAL_SPACE_ID = dbutils.widgets.get("internal_space_id")
REGULATORY_SPACE_ID = dbutils.widgets.get("regulatory_space_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper — Patch a Genie Space

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()


def patch_genie_space(space_id: str, updates: dict):
    """Patch a Genie space via REST API."""
    resp = w.api_client.do(
        "PATCH",
        f"/api/2.0/genie/spaces/{space_id}",
        body=updates,
    )
    print(f"  Patched space {space_id}: {resp.get('title', 'ok')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Research Assistant Enrichment

# COMMAND ----------

research_sample_questions = [
    {
        "question": "What are the most-cited papers on CRISPR off-target effects?",
        "sql": f"SELECT title, journal, publication_year, citation_count, doi FROM {catalog}.meridian_research.articles WHERE lower(title) LIKE '%crispr%' AND lower(title) LIKE '%off-target%' ORDER BY citation_count DESC LIMIT 10",
    },
    {
        "question": "Show me meta-analyses published in 2025 on immunotherapy",
        "sql": f"SELECT title, journal, publication_year, citation_count FROM {catalog}.meridian_research.articles WHERE publication_type = 'Meta-Analysis' AND publication_year = 2025 AND lower(title) LIKE '%immunotherapy%' ORDER BY citation_count DESC",
    },
    {
        "question": "Who are the top 10 authors by h-index in our database?",
        "sql": f"SELECT full_name, h_index, article_count, first_pub_year, last_pub_year FROM {catalog}.meridian_research.authors ORDER BY h_index DESC LIMIT 10",
    },
    {
        "question": "What MeSH terms have the most articles?",
        "sql": f"SELECT mesh_term, article_count, latest_year FROM {catalog}.meridian_research.mesh_terms ORDER BY article_count DESC LIMIT 15",
    },
    {
        "question": "How many preprints vs peer-reviewed articles do we have by year?",
        "sql": f"SELECT publication_year, is_preprint, COUNT(*) AS article_count FROM {catalog}.meridian_research.articles GROUP BY publication_year, is_preprint ORDER BY publication_year DESC",
    },
]

research_sql_snippets = [
    {"name": "Total articles", "sql": f"SELECT COUNT(*) AS total_articles FROM {catalog}.meridian_research.articles"},
    {"name": "Average citations", "sql": f"SELECT AVG(citation_count) AS avg_citations FROM {catalog}.meridian_research.articles"},
    {"name": "Articles by source", "sql": f"SELECT source, COUNT(*) AS cnt FROM {catalog}.meridian_research.articles GROUP BY source"},
    {"name": "Top journals", "sql": f"SELECT journal, COUNT(*) AS article_count FROM {catalog}.meridian_research.articles WHERE journal IS NOT NULL GROUP BY journal ORDER BY article_count DESC LIMIT 10"},
]

patch_genie_space(RESEARCH_SPACE_ID, {
    "config": {
        "sample_questions": [sq["question"] for sq in research_sample_questions],
    },
    "description": f"Natural language Q&A over {catalog}.meridian_research — biomedical articles, authors, citations, and MeSH terms from PubMed, arXiv, and Crossref.",
})
print("Research Assistant enriched")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Internal Analytics Enrichment

# COMMAND ----------

internal_sample_questions = [
    {
        "question": "What is our total pipeline value by stage?",
        "sql": f"SELECT stage, SUM(total_amount) AS pipeline_value, SUM(deal_count) AS deals FROM {catalog}.meridian_internal.sales_pipeline GROUP BY stage ORDER BY pipeline_value DESC",
    },
    {
        "question": "Show me revenue by product line for FY2025",
        "sql": f"SELECT product_line, SUM(revenue) AS total_revenue, AVG(gross_margin_pct) AS avg_margin FROM {catalog}.meridian_internal.revenue_summary WHERE fiscal_year = 2025 GROUP BY product_line ORDER BY total_revenue DESC",
    },
    {
        "question": "Which accounts are at risk based on health score?",
        "sql": f"SELECT account_name, arr, health_score, health_tier, api_calls_30d, error_rate_30d FROM {catalog}.meridian_internal.customer_health WHERE health_tier IN ('At Risk', 'Critical') ORDER BY arr DESC",
    },
    {
        "question": "What is the average deal size by region?",
        "sql": f"SELECT region, AVG(avg_deal_size) AS avg_deal_size, SUM(deal_count) AS total_deals FROM {catalog}.meridian_internal.sales_pipeline GROUP BY region ORDER BY avg_deal_size DESC",
    },
    {
        "question": "Which products have the highest error rates?",
        "sql": f"SELECT product, AVG(error_rate) AS avg_error_rate, SUM(api_calls) AS total_calls FROM {catalog}.meridian_internal.product_usage GROUP BY product ORDER BY avg_error_rate DESC",
    },
]

internal_sql_snippets = [
    {"name": "Total ARR", "sql": f"SELECT SUM(arr) AS total_arr FROM {catalog}.meridian_internal.customer_health"},
    {"name": "Pipeline value", "sql": f"SELECT SUM(total_amount) AS pipeline FROM {catalog}.meridian_internal.sales_pipeline"},
    {"name": "Revenue YoY growth", "sql": f"SELECT fiscal_year, fiscal_quarter, product_line, yoy_revenue_growth FROM {catalog}.meridian_internal.revenue_summary WHERE yoy_revenue_growth IS NOT NULL ORDER BY fiscal_year DESC, fiscal_quarter"},
    {"name": "Health distribution", "sql": f"SELECT health_tier, COUNT(*) AS accounts, SUM(arr) AS total_arr FROM {catalog}.meridian_internal.customer_health GROUP BY health_tier"},
]

patch_genie_space(INTERNAL_SPACE_ID, {
    "config": {
        "sample_questions": [sq["question"] for sq in internal_sample_questions],
    },
    "description": f"Revenue, pipeline, product usage, and customer health analytics from {catalog}.meridian_internal. Meridian's FY starts February 1. Products: Regulatory Feed, Research Platform, Patent Monitor, Custom Analytics.",
})
print("Internal Analytics enriched")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Regulatory Intelligence Enrichment

# COMMAND ----------

regulatory_sample_questions = [
    {
        "question": "How many regulatory actions by source agency in the last year?",
        "sql": f"SELECT action_source, COUNT(*) AS action_count FROM {catalog}.meridian_regulatory.regulatory_actions WHERE action_date >= DATEADD(YEAR, -1, CURRENT_DATE) GROUP BY action_source ORDER BY action_count DESC",
    },
    {
        "question": "Which companies have the most patent filings?",
        "sql": f"SELECT assignee, COUNT(*) AS patent_count, MIN(filing_date) AS earliest, MAX(filing_date) AS latest FROM {catalog}.meridian_regulatory.patent_landscape GROUP BY assignee ORDER BY patent_count DESC LIMIT 15",
    },
    {
        "question": "Show me high-risk companies with risk signals",
        "sql": f"SELECT company_name, risk_tier, total_actions, patent_count, internal_score FROM {catalog}.meridian_regulatory.company_risk_signals WHERE risk_tier = 'High' ORDER BY total_actions DESC",
    },
    {
        "question": "What types of SEC filings are most common?",
        "sql": f"SELECT filing_type, COUNT(*) AS filing_count FROM {catalog}.meridian_regulatory.regulatory_actions WHERE action_source = 'SEC' GROUP BY filing_type ORDER BY filing_count DESC",
    },
    {
        "question": "Show FDA recall actions by classification and status",
        "sql": f"SELECT classification, status, COUNT(*) AS cnt FROM {catalog}.meridian_regulatory.regulatory_actions WHERE action_source = 'FDA' GROUP BY classification, status ORDER BY classification, cnt DESC",
    },
]

regulatory_sql_snippets = [
    {"name": "Total regulatory actions", "sql": f"SELECT COUNT(*) AS total FROM {catalog}.meridian_regulatory.regulatory_actions"},
    {"name": "Companies tracked", "sql": f"SELECT COUNT(DISTINCT company_name) AS companies FROM {catalog}.meridian_regulatory.company_entities"},
    {"name": "Actions by year", "sql": f"SELECT YEAR(action_date) AS yr, COUNT(*) AS actions FROM {catalog}.meridian_regulatory.regulatory_actions GROUP BY yr ORDER BY yr"},
    {"name": "Patent types", "sql": f"SELECT patent_type, COUNT(*) AS cnt FROM {catalog}.meridian_regulatory.patent_landscape GROUP BY patent_type"},
]

patch_genie_space(REGULATORY_SPACE_ID, {
    "config": {
        "sample_questions": [sq["question"] for sq in regulatory_sample_questions],
    },
    "description": f"Regulatory intelligence across SEC filings, FDA enforcement actions, and USPTO patents from {catalog}.meridian_regulatory. Includes company entity matching and risk signals.",
})
print("Regulatory Intelligence enriched")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Summary
# MAGIC
# MAGIC | Space | Sample Questions | SQL Snippets |
# MAGIC |-------|-----------------|--------------|
# MAGIC | Research Assistant | 5 | 4 |
# MAGIC | Internal Analytics | 5 | 4 |
# MAGIC | Regulatory Intelligence | 5 | 4 |
# MAGIC
# MAGIC SQL snippets are documented in `resources/genie_spaces.yml` for reference.
# MAGIC Run this notebook after Genie spaces are created and tables are populated.
