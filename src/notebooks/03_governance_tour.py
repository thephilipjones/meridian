# Databricks notebook source
# MAGIC %md
# MAGIC # Meridian Insights — Governance Tour
# MAGIC
# MAGIC Deep dive into Unity Catalog governance features: lineage, tags,
# MAGIC Metric Views, row-level security, and system table audit queries.
# MAGIC
# MAGIC > *"For a data provider, governance isn't optional — it's the product."*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog_name", "serverless_stable_k2zkdm_catalog")
catalog = dbutils.widgets.get("catalog_name")
spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Table Tags & Discovery
# MAGIC
# MAGIC Unity Catalog tags make data assets discoverable by business context.
# MAGIC Meridian tags every table with its medallion layer, business unit,
# MAGIC and whether it's an externally distributed data product.

# COMMAND ----------

# All gold-layer data product tables — using pipe syntax
display(spark.sql(f"""
    FROM {catalog}.information_schema.table_tags
    |> WHERE tag_name = 'quality' AND tag_value = 'gold'
    |> SELECT table_schema, table_name, tag_name, tag_value
    |> ORDER BY table_schema, table_name
"""))

# COMMAND ----------

# Cross-reference: which gold tables are external data products?
display(spark.sql(f"""
    FROM {catalog}.information_schema.table_tags
    |> WHERE tag_name IN ('quality', 'meridian.data_product', 'meridian.business_unit')
    |> SELECT table_schema, table_name, tag_name, tag_value
    |> ORDER BY table_schema, table_name, tag_name
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Lineage
# MAGIC
# MAGIC > *"Trace a gold `articles` record back through the silver cleansing
# MAGIC > layer to the original PubMed raw data. This lineage is automatic —
# MAGIC > no extra configuration needed."*
# MAGIC
# MAGIC **Navigate to Catalog Explorer → `meridian.research.articles` → Lineage**
# MAGIC to show the visual lineage graph in the UI.
# MAGIC
# MAGIC Programmatically, we can query the lineage system table:

# COMMAND ----------

display(spark.sql(f"""
    FROM system.access.table_lineage
    |> WHERE target_table_catalog = '{catalog}'
           AND target_table_schema = 'meridian_research'
           AND target_table_name = 'articles'
    |> SELECT source_table_full_name, target_table_full_name, event_time
    |> ORDER BY event_time DESC
    |> LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Metric Views — Governed KPIs
# MAGIC
# MAGIC Metric Views define business metrics as first-class Unity Catalog
# MAGIC objects. The same metric definition is consumed by Genie, AI/BI
# MAGIC Dashboards, and ad-hoc SQL — no metric drift across teams.

# COMMAND ----------

# Revenue metrics — query the governed definitions
display(spark.sql(f"""
    SELECT
        `Fiscal Year`, `Product Line`,
        MEASURE(`Total Revenue`) AS total_revenue,
        MEASURE(`Gross Margin Pct`) AS margin_pct,
        MEASURE(`Revenue per Customer`) AS rev_per_customer
    FROM {catalog}.meridian_internal.revenue_metrics
    GROUP BY ALL
    ORDER BY `Fiscal Year` DESC, total_revenue DESC
"""))

# COMMAND ----------

# Customer health metrics — same governed definition used by the dashboard
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
# MAGIC ---
# MAGIC ## 4. Liquid Clustering
# MAGIC
# MAGIC Gold tables use **liquid clustering** instead of traditional
# MAGIC partitioning. The layout adapts to actual query patterns with
# MAGIC zero maintenance — no guessing partition columns upfront.

# COMMAND ----------

# Verify liquid clustering is configured on gold tables
display(spark.sql(f"""
    DESCRIBE DETAIL {catalog}.meridian_research.articles
"""))

# COMMAND ----------

display(spark.sql(f"""
    DESCRIBE DETAIL {catalog}.meridian_internal.revenue_summary
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Row-Level Security & Column Masks
# MAGIC
# MAGIC Gold tables include a `subscription_tier` column. Row filters
# MAGIC restrict external customer profiles to their entitled rows, while
# MAGIC column masks hide internal scoring fields from external users.
# MAGIC
# MAGIC > *"James at Acme Bank subscribed to SEC data only. He sees SEC
# MAGIC > filings but not FDA recalls — and he can't see internal risk
# MAGIC > scores that Meridian uses for pricing decisions."*

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5a. Create the Row Filter Function
# MAGIC
# MAGIC The filter grants full access to members of the `meridian_internal`
# MAGIC group and restricts everyone else to their subscription tier.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE FUNCTION {catalog}.meridian_regulatory.tier_row_filter(tier STRING)
    RETURNS BOOLEAN
    RETURN
        IS_ACCOUNT_GROUP_MEMBER('meridian_internal')
        OR tier = 'sec_only'
""")
print("Row filter function created")

# COMMAND ----------

# Apply the row filter to regulatory_actions
spark.sql(f"""
    ALTER TABLE {catalog}.meridian_regulatory.regulatory_actions
    SET ROW FILTER {catalog}.meridian_regulatory.tier_row_filter
    ON (subscription_tier)
""")
print("Row filter applied to regulatory_actions")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5b. Demonstrate Row-Level Security
# MAGIC
# MAGIC As an internal user (member of `meridian_internal`), you see all rows.
# MAGIC An external user with `sec_only` tier would only see SEC actions.

# COMMAND ----------

# Full view — internal user sees everything
display(spark.sql(f"""
    FROM {catalog}.meridian_regulatory.regulatory_actions
    |> AGGREGATE COUNT(*) AS total_rows,
       COUNT_IF(action_source = 'SEC') AS sec_rows,
       COUNT_IF(action_source = 'FDA') AS fda_rows
"""))

# COMMAND ----------

# Simulated external view — only sec_only tier visible
display(spark.sql(f"""
    FROM {catalog}.meridian_regulatory.regulatory_actions
    |> WHERE subscription_tier = 'sec_only'
    |> AGGREGATE COUNT(*) AS visible_rows,
       COUNT_IF(action_source = 'SEC') AS sec_rows,
       COUNT_IF(action_source = 'FDA') AS fda_rows
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5c. Create a Column Mask
# MAGIC
# MAGIC The `internal_score` on `company_risk_signals` is a proprietary
# MAGIC metric used for pricing. External users see it as NULL.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE FUNCTION {catalog}.meridian_regulatory.mask_internal_score(score DOUBLE)
    RETURNS DOUBLE
    RETURN
        CASE WHEN IS_ACCOUNT_GROUP_MEMBER('meridian_internal')
            THEN score
            ELSE NULL
        END
""")
print("Column mask function created")

# COMMAND ----------

spark.sql(f"""
    ALTER TABLE {catalog}.meridian_regulatory.company_risk_signals
    SET COLUMN MASK {catalog}.meridian_regulatory.mask_internal_score
    ON (internal_score)
""")
print("Column mask applied to company_risk_signals.internal_score")

# COMMAND ----------

# Internal user sees the actual score; external user would see NULL
display(spark.sql(f"""
    FROM {catalog}.meridian_regulatory.company_risk_signals
    |> SELECT company_name, risk_tier, internal_score, total_actions, patent_count
    |> ORDER BY internal_score DESC
    |> LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6. System Tables — Audit & Consumption
# MAGIC
# MAGIC System tables provide meta-analytics: who queried what, when,
# MAGIC and how much compute was consumed. Enables Meridian to answer
# MAGIC questions like "Which users queried the most data products?"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6a. Audit Log — Query Activity

# COMMAND ----------

# Who has been querying Meridian data in the last 7 days?
display(spark.sql(f"""
    FROM system.access.audit
    |> WHERE request_params.catalog_name = '{catalog}'
           AND event_date >= CURRENT_DATE - INTERVAL 7 DAYS
    |> AGGREGATE COUNT(*) AS query_count
       GROUP BY user_identity.email, action_name
    |> ORDER BY query_count DESC
    |> LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6b. Audit — Table-Level Access Patterns

# COMMAND ----------

# Which tables are queried most often?
display(spark.sql(f"""
    FROM system.access.audit
    |> WHERE request_params.catalog_name = '{catalog}'
           AND event_date >= CURRENT_DATE - INTERVAL 7 DAYS
           AND action_name IN ('commandSubmit', 'sqlStatement')
    |> AGGREGATE COUNT(*) AS access_count
       GROUP BY request_params.schema_name, request_params.table_name
    |> WHERE request_params.table_name IS NOT NULL
    |> ORDER BY access_count DESC
    |> LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6c. Billing — Compute Consumption

# COMMAND ----------

# How much compute has the meridian catalog consumed?
display(spark.sql(f"""
    FROM system.billing.usage
    |> WHERE usage_date >= CURRENT_DATE - INTERVAL 30 DAYS
    |> AGGREGATE
       SUM(usage_quantity) AS total_dbus,
       COUNT(DISTINCT usage_date) AS active_days
       GROUP BY sku_name, usage_unit
    |> ORDER BY total_dbus DESC
    |> LIMIT 15
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Summary
# MAGIC
# MAGIC | Feature | What it does | Demo moment |
# MAGIC |---------|-------------|-------------|
# MAGIC | **Tags** | Business context on every asset | "Find all gold data products" |
# MAGIC | **Lineage** | Automatic source-to-product tracing | "Trace this record to the raw SEC filing" |
# MAGIC | **Metric Views** | Governed KPI definitions | "Same metric in Genie, dashboard, and SQL" |
# MAGIC | **Liquid Clustering** | Zero-maintenance layout | "No partition columns to guess" |
# MAGIC | **Row Filters** | Subscription-tier data access | "James sees SEC, not FDA" |
# MAGIC | **Column Masks** | Hide internal fields from externals | "Pricing scores stay internal" |
# MAGIC | **System Tables** | Audit trail + consumption tracking | "Who queried what, and how much did it cost?" |
