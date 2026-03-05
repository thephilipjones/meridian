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
# MAGIC Three metric views created in `meridian.internal`:
# MAGIC
# MAGIC | Metric View | Measures | Consumed By |
# MAGIC |---|---|---|
# MAGIC | `revenue_metrics` | Total Revenue, Gross Margin %, Revenue per Customer | AI/BI Dashboard, Internal Genie |
# MAGIC | `pipeline_metrics` | Deal Count, Pipeline Value, ARR, Avg Deal Size | AI/BI Dashboard, Internal Genie |
# MAGIC | `customer_health_metrics` | Account Count, Total ARR, Avg Health Score | AI/BI Dashboard, Internal Genie |
# MAGIC
# MAGIC These metric views ensure that Genie, AI/BI Dashboards, and SQL queries
# MAGIC all use the same metric definitions — no inconsistency across tools.
