# Databricks notebook source
# MAGIC %md
# MAGIC # Meridian Insights — Metric Views
# MAGIC
# MAGIC Creates **Unity Catalog Metric Views** on the internal gold tables.
# MAGIC Metric Views define governed, reusable business metrics in YAML that
# MAGIC separate measure definitions from dimension groupings — ensuring
# MAGIC Genie, AI/BI Dashboards, and ad-hoc SQL all share the same KPI
# MAGIC definitions. No metric drift across teams.
# MAGIC
# MAGIC **Prerequisites:** Internal pipeline must have run (gold tables populated).
# MAGIC
# MAGIC **Requires:** DBR 17.2+

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog_name", "serverless_stable_k2zkdm_catalog")
catalog = dbutils.widgets.get("catalog_name")
spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Revenue Metrics
# MAGIC
# MAGIC Governed revenue KPIs consumed by Sarah Chen's internal dashboards
# MAGIC and the Internal Analytics Genie space. Defines Total Revenue,
# MAGIC Gross Margin %, Revenue per Customer, and YoY Growth as reusable
# MAGIC measures with fiscal-aware dimensions.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.meridian_internal.revenue_metrics
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: "Meridian revenue KPIs — single source of truth for all dashboards and Genie"
  source: {catalog}.meridian_internal.revenue_summary

  dimensions:
    - name: Fiscal Year
      expr: fiscal_year
      comment: "Meridian fiscal year (starts February 1)"
    - name: Fiscal Quarter
      expr: fiscal_quarter
      comment: "Fiscal quarter within the year"
    - name: Product Line
      expr: product_line
      comment: "Meridian product: Regulatory Feed, Research Platform, Patent Monitor, Custom Analytics"

  measures:
    - name: Total Revenue
      expr: SUM(revenue)
      comment: "Gross revenue across all products"
    - name: Cost of Data
      expr: SUM(cost_of_data)
      comment: "Data acquisition and processing costs"
    - name: Gross Margin
      expr: SUM(gross_margin)
      comment: "Revenue minus cost of data"
    - name: Gross Margin Pct
      expr: SUM(gross_margin) / NULLIF(SUM(revenue), 0)
      comment: "Gross margin as percentage of revenue"
    - name: Revenue per Customer
      expr: SUM(revenue) / NULLIF(SUM(customer_count), 0)
      comment: "Average revenue per unique customer"
    - name: Customer Count
      expr: SUM(customer_count)
      comment: "Total customers across product lines"
$$
""")

print("revenue_metrics created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Metrics
# MAGIC
# MAGIC Sales pipeline KPIs for deal flow analysis — deal volume, pipeline
# MAGIC value, average deal size, and conversion rates by stage and region.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.meridian_internal.pipeline_metrics
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: "Meridian sales pipeline KPIs — deal flow and conversion analytics"
  source: {catalog}.meridian_internal.sales_pipeline

  dimensions:
    - name: Stage
      expr: stage
      comment: "Deal stage: Prospecting, Qualification, Proposal, Negotiation, Closed Won, Closed Lost"
    - name: Product Line
      expr: product_line
    - name: Region
      expr: region
    - name: Fiscal Quarter
      expr: fiscal_quarter

  measures:
    - name: Deal Count
      expr: SUM(deal_count)
      comment: "Number of deals"
    - name: Pipeline Value
      expr: SUM(total_amount)
      comment: "Total deal value in pipeline"
    - name: Total ARR
      expr: SUM(total_arr)
      comment: "Total annual recurring revenue"
    - name: Avg Deal Size
      expr: SUM(total_amount) / NULLIF(SUM(deal_count), 0)
      comment: "Average deal value"
$$
""")

print("pipeline_metrics created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer Health Metrics
# MAGIC
# MAGIC Account-level health scoring for customer success — usage activity,
# MAGIC performance, error rates, and composite health tier distribution.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.meridian_internal.customer_health_metrics
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: "Meridian customer health KPIs — engagement and retention risk scoring"
  source: {catalog}.meridian_internal.customer_health

  dimensions:
    - name: Health Tier
      expr: health_tier
      comment: "Healthy, At Risk, or Critical"
    - name: Account Name
      expr: account_name

  measures:
    - name: Account Count
      expr: COUNT(1)
      comment: "Number of accounts"
    - name: Total ARR
      expr: SUM(arr)
      comment: "Total ARR across accounts"
    - name: Avg Health Score
      expr: AVG(health_score)
      comment: "Average health score (0-1)"
    - name: Avg Products Subscribed
      expr: AVG(products_subscribed)
      comment: "Average products per account"
    - name: Avg API Calls 30d
      expr: AVG(api_calls_30d)
      comment: "Average API calls per account in last 30 days"
    - name: Avg Error Rate
      expr: AVG(error_rate_30d)
      comment: "Average error rate across accounts"
$$
""")

print("customer_health_metrics created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Publication Metrics
# MAGIC
# MAGIC Research publication KPIs consumed by Dr. Anika Park's Research
# MAGIC Intelligence dashboard and the Research Assistant Genie space.
# MAGIC Tracks article volume, citation impact, and source distribution.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.meridian_research.publication_metrics
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: "Meridian research publication KPIs — article volume and citation impact"
  source: {catalog}.meridian_research.articles

  dimensions:
    - name: Publication Year
      expr: publication_year
      comment: "Year of publication"
    - name: Journal
      expr: journal
      comment: "Journal or venue name"
    - name: Source
      expr: source
      comment: "Data source: PubMed, arXiv, Crossref"
    - name: Is Preprint
      expr: is_preprint
      comment: "Whether the article is a preprint (true/false)"
    - name: Publication Type
      expr: publication_type
      comment: "Type of publication (research-article, review, etc.)"

  measures:
    - name: Article Count
      expr: COUNT(1)
      comment: "Total number of articles"
    - name: Avg Citation Count
      expr: AVG(citation_count)
      comment: "Average citations per article"
    - name: Total Citations
      expr: SUM(citation_count)
      comment: "Sum of all citations across articles"
$$
""")

print("publication_metrics created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Author Metrics
# MAGIC
# MAGIC Research author productivity and impact KPIs — h-index
# MAGIC distribution, publication volume, and career span analytics.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.meridian_research.author_metrics
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: "Meridian research author KPIs — productivity and impact"
  source: {catalog}.meridian_research.authors

  dimensions:
    - name: First Pub Year
      expr: first_pub_year
      comment: "Year of first publication"
    - name: Last Pub Year
      expr: last_pub_year
      comment: "Year of most recent publication"

  measures:
    - name: Author Count
      expr: COUNT(1)
      comment: "Total number of authors"
    - name: Avg H-Index
      expr: AVG(h_index)
      comment: "Average h-index across authors"
    - name: Avg Article Count
      expr: AVG(article_count)
      comment: "Average articles per author"
    - name: Max H-Index
      expr: MAX(h_index)
      comment: "Highest h-index in the dataset"
$$
""")

print("author_metrics created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Regulatory Action Metrics
# MAGIC
# MAGIC Regulatory action KPIs for James Rivera's Regulatory Landscape
# MAGIC dashboard — tracks action volume, source distribution, and
# MAGIC subscription-tier scoping for data product governance demos.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.meridian_regulatory.regulatory_action_metrics
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: "Meridian regulatory action KPIs — enforcement and filing analytics"
  source: {catalog}.meridian_regulatory.regulatory_actions

  dimensions:
    - name: Action Source
      expr: action_source
      comment: "Regulatory body: SEC, FDA, etc."
    - name: Action Type
      expr: action_type
      comment: "Type of regulatory action"
    - name: Action Year
      expr: action_year
      comment: "Year the action was taken"
    - name: Classification
      expr: classification
      comment: "Severity or risk classification"
    - name: Subscription Tier
      expr: subscription_tier
      comment: "Data access tier: basic, standard, premium"

  measures:
    - name: Action Count
      expr: COUNT(1)
      comment: "Total regulatory actions"
    - name: Distinct Companies
      expr: COUNT(DISTINCT company_name)
      comment: "Number of unique companies with actions"
$$
""")

print("regulatory_action_metrics created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Company Risk Metrics
# MAGIC
# MAGIC Company-level risk scoring KPIs — risk tier distribution,
# MAGIC filing counts, and composite internal risk scores.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.meridian_regulatory.company_risk_metrics
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: "Meridian company risk KPIs — risk tiers and filing analytics"
  source: {catalog}.meridian_regulatory.company_risk_signals

  dimensions:
    - name: Risk Tier
      expr: risk_tier
      comment: "Risk classification: High, Medium, Low"
    - name: Primary State
      expr: primary_state
      comment: "Primary US state of the company"

  measures:
    - name: Company Count
      expr: COUNT(1)
      comment: "Number of companies"
    - name: Avg Internal Score
      expr: AVG(internal_score)
      comment: "Average composite risk score"
    - name: Total Actions
      expr: SUM(total_actions)
      comment: "Sum of all regulatory actions across companies"
    - name: Total SEC Filings
      expr: SUM(sec_filings)
      comment: "Sum of SEC filings across companies"
    - name: Total FDA Actions
      expr: SUM(fda_actions)
      comment: "Sum of FDA enforcement actions across companies"
$$
""")

print("company_risk_metrics created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Metric Views

# COMMAND ----------

# Query revenue metrics using MEASURE() syntax
display(spark.sql(f"""
    SELECT
        `Fiscal Year`,
        `Product Line`,
        MEASURE(`Total Revenue`) AS total_revenue,
        MEASURE(`Gross Margin Pct`) AS margin_pct,
        MEASURE(`Revenue per Customer`) AS rev_per_customer
    FROM {catalog}.meridian_internal.revenue_metrics
    GROUP BY ALL
    ORDER BY `Fiscal Year` DESC, total_revenue DESC
"""))

# COMMAND ----------

# Query pipeline metrics
display(spark.sql(f"""
    SELECT
        `Stage`,
        MEASURE(`Deal Count`) AS deals,
        MEASURE(`Pipeline Value`) AS pipeline_value,
        MEASURE(`Avg Deal Size`) AS avg_deal
    FROM {catalog}.meridian_internal.pipeline_metrics
    GROUP BY ALL
    ORDER BY pipeline_value DESC
"""))

# COMMAND ----------

# Query customer health distribution
display(spark.sql(f"""
    SELECT
        `Health Tier`,
        MEASURE(`Account Count`) AS accounts,
        MEASURE(`Total ARR`) AS total_arr,
        MEASURE(`Avg Health Score`) AS avg_score
    FROM {catalog}.meridian_internal.customer_health_metrics
    GROUP BY ALL
    ORDER BY avg_score DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Seven metric views created across three schemas:
# MAGIC
# MAGIC | Metric View | Schema | Measures | Consumed By |
# MAGIC |---|---|---|---|
# MAGIC | `revenue_metrics` | `meridian_internal` | Total Revenue, Gross Margin %, Revenue per Customer | Internal Dashboard, Internal Genie |
# MAGIC | `pipeline_metrics` | `meridian_internal` | Deal Count, Pipeline Value, ARR, Avg Deal Size | Internal Dashboard, Internal Genie |
# MAGIC | `customer_health_metrics` | `meridian_internal` | Account Count, Total ARR, Avg Health Score | Internal Dashboard, Internal Genie |
# MAGIC | `publication_metrics` | `meridian_research` | Article Count, Avg Citations, Total Citations | Research Dashboard, Research Genie |
# MAGIC | `author_metrics` | `meridian_research` | Author Count, Avg H-Index, Avg Article Count | Research Dashboard, Research Genie |
# MAGIC | `regulatory_action_metrics` | `meridian_regulatory` | Action Count, Distinct Companies | Regulatory Dashboard, Regulatory Genie |
# MAGIC | `company_risk_metrics` | `meridian_regulatory` | Company Count, Avg Internal Score, Total Actions | Regulatory Dashboard, Regulatory Genie |
# MAGIC
# MAGIC These metric views ensure that Genie, AI/BI Dashboards, and SQL queries
# MAGIC all use the same metric definitions — no inconsistency across tools.
