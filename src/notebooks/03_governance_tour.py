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
# MAGIC ## 5. Row-Level Security (Phase 2)
# MAGIC
# MAGIC Gold tables include a `subscription_tier` or `access_group` column.
# MAGIC Row filters restrict external customer profiles to their entitled rows.
# MAGIC
# MAGIC ```sql
# MAGIC -- Example: restrict regulatory data by subscription tier
# MAGIC ALTER TABLE meridian.regulatory.regulatory_actions
# MAGIC SET ROW FILTER meridian.internal.tier_filter ON (subscription_tier);
# MAGIC
# MAGIC -- Example: mask internal scoring fields for external profiles
# MAGIC ALTER TABLE meridian.regulatory.company_risk_signals
# MAGIC SET COLUMN MASK meridian.internal.mask_internal_score ON (internal_score);
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6. System Tables — Audit & Consumption (Phase 2)
# MAGIC
# MAGIC System tables provide meta-analytics: who queried what, when,
# MAGIC and how much compute was consumed. Enables Meridian to answer
# MAGIC questions like "Which customers queried the most data products?"

# COMMAND ----------

# Preview audit log for the meridian catalog
display(spark.sql(f"""
    FROM system.access.audit
    |> WHERE request_params.catalog_name = '{catalog}'
           AND event_date >= CURRENT_DATE - INTERVAL 7 DAYS
    |> AGGREGATE COUNT(*) AS query_count
       GROUP BY user_identity.email, action_name
    |> ORDER BY query_count DESC
    |> LIMIT 20
"""))
