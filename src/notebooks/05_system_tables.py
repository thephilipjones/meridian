# Databricks notebook source
# MAGIC %md
# MAGIC # Meridian Insights — System Tables Meta-Analytics
# MAGIC
# MAGIC Creates materialized views in `meridian_system` that aggregate audit
# MAGIC and billing system tables, filtered to the Meridian catalog. These
# MAGIC power the "Platform Analytics" tab in Sarah Chen's Internal view.
# MAGIC
# MAGIC > *"Meridian monitors who accesses what data and how much compute it costs."*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog_name", "serverless_stable_k2zkdm_catalog")
catalog = dbutils.widgets.get("catalog_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ensure Schema Exists

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.meridian_system")
spark.sql(f"USE CATALOG {catalog}")
print(f"Schema '{catalog}.meridian_system' ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Query Activity
# MAGIC
# MAGIC Aggregated query activity from `system.access.audit` filtered to the
# MAGIC Meridian catalog, grouped by user, action, and day.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE MATERIALIZED VIEW {catalog}.meridian_system.query_activity
    COMMENT 'Daily query activity against Meridian catalog assets — sourced from system.access.audit'
    AS
    FROM system.access.audit
    |> WHERE event_date >= CURRENT_DATE - INTERVAL 90 DAYS
           AND request_params.catalog_name = '{catalog}'
    |> SELECT
        event_date,
        user_identity.email AS user_email,
        action_name,
        COUNT(*) AS query_count
    |> GROUP BY event_date, user_identity.email, action_name
""")
print("Materialized view 'query_activity' created")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Table Access Patterns
# MAGIC
# MAGIC Which tables are queried most often? Helps Meridian understand
# MAGIC which data products have the highest demand.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE MATERIALIZED VIEW {catalog}.meridian_system.table_access_patterns
    COMMENT 'Table-level access frequency across Meridian schemas — most queried data products'
    AS
    FROM system.access.audit
    |> WHERE event_date >= CURRENT_DATE - INTERVAL 90 DAYS
           AND request_params.catalog_name = '{catalog}'
           AND request_params.table_name IS NOT NULL
    |> SELECT
        request_params.schema_name AS schema_name,
        request_params.table_name AS table_name,
        COUNT(DISTINCT user_identity.email) AS unique_users,
        COUNT(*) AS access_count,
        MAX(event_date) AS last_accessed
    |> GROUP BY request_params.schema_name, request_params.table_name
""")
print("Materialized view 'table_access_patterns' created")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Compute Consumption
# MAGIC
# MAGIC DBU consumption from `system.billing.usage` grouped by SKU and day.
# MAGIC Shows the cost profile of running the Meridian platform.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE MATERIALIZED VIEW {catalog}.meridian_system.compute_consumption
    COMMENT 'Daily compute consumption (DBUs) by SKU — cost profile for the Meridian platform'
    AS
    FROM system.billing.usage
    |> WHERE usage_date >= CURRENT_DATE - INTERVAL 90 DAYS
    |> SELECT
        usage_date,
        sku_name,
        usage_unit,
        SUM(usage_quantity) AS total_dbus
    |> GROUP BY usage_date, sku_name, usage_unit
""")
print("Materialized view 'compute_consumption' created")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Grant Access to App Service Principal
# MAGIC
# MAGIC The Meridian Portal app SP needs `USE SCHEMA` and `SELECT` on
# MAGIC `meridian_system` to serve the Platform Analytics tab.

# COMMAND ----------

sp_client_id = "077b548f-d7c0-4781-bf91-ba0ad3eb6d38"

spark.sql(f"GRANT USE SCHEMA ON SCHEMA {catalog}.meridian_system TO `{sp_client_id}`")
spark.sql(f"GRANT SELECT ON SCHEMA {catalog}.meridian_system TO `{sp_client_id}`")
print(f"Grants applied for SP {sp_client_id} on meridian_system")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Verify

# COMMAND ----------

display(spark.sql(f"""
    FROM {catalog}.information_schema.tables
    |> WHERE table_schema = 'meridian_system'
    |> SELECT table_name, table_type, comment
    |> ORDER BY table_name
"""))
