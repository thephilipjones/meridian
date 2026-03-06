# Databricks notebook source
# MAGIC %md
# MAGIC # Meridian Insights — System Tables Meta-Analytics
# MAGIC
# MAGIC Pre-computes snapshot tables in `meridian_system` from audit and billing
# MAGIC system tables. Snapshot tables are instant to query from the app —
# MAGIC unlike views which re-scan on every request.
# MAGIC
# MAGIC **Re-run this notebook periodically to refresh the snapshots.**

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

for v in ["query_activity", "table_access_patterns", "compute_consumption"]:
    spark.sql(f"DROP TABLE IF EXISTS {catalog}.meridian_system.{v}")
    spark.sql(f"DROP VIEW IF EXISTS {catalog}.meridian_system.{v}")

print(f"Schema '{catalog}.meridian_system' ready (stale objects dropped)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Query Activity
# MAGIC
# MAGIC Daily query counts from `system.access.audit`, filtered to actions
# MAGIC that reference the Meridian catalog.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {catalog}.meridian_system.query_activity
    COMMENT 'Daily query activity against Meridian catalog — snapshot from system.access.audit'
    AS
    SELECT
        event_date,
        user_identity.email AS user_email,
        action_name,
        COUNT(*) AS query_count
    FROM system.access.audit
    WHERE event_date >= CURRENT_DATE - INTERVAL 30 DAYS
      AND request_params['catalog_name'] = '{catalog}'
    GROUP BY event_date, user_identity.email, action_name
""")
print("Table 'query_activity' created")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Table Access Patterns
# MAGIC
# MAGIC Extracts table-level access from UC audit events that carry
# MAGIC `full_name_arg` (e.g. `getTable`, `generateTemporaryTableCredential`).

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {catalog}.meridian_system.table_access_patterns
    COMMENT 'Table-level access frequency across Meridian schemas — snapshot from system.access.audit'
    AS
    WITH parsed AS (
        SELECT
            event_date,
            user_identity.email AS user_email,
            split(request_params['full_name_arg'], '\\\\.') AS parts
        FROM system.access.audit
        WHERE event_date >= CURRENT_DATE - INTERVAL 30 DAYS
          AND request_params['full_name_arg'] LIKE '{catalog}.meridian\\_%'
    )
    SELECT
        get(parts, 1) AS schema_name,
        get(parts, 2) AS table_name,
        COUNT(DISTINCT user_email) AS unique_users,
        COUNT(*) AS access_count,
        MAX(event_date) AS last_accessed
    FROM parsed
    WHERE size(parts) >= 3
    GROUP BY get(parts, 1), get(parts, 2)
""")
print("Table 'table_access_patterns' created")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Compute Consumption
# MAGIC
# MAGIC DBU consumption from `system.billing.usage`, grouped by SKU and day.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {catalog}.meridian_system.compute_consumption
    COMMENT 'Daily compute consumption (DBUs) by SKU — snapshot from system.billing.usage'
    AS
    SELECT
        usage_date,
        sku_name,
        usage_unit,
        SUM(usage_quantity) AS total_dbus
    FROM system.billing.usage
    WHERE usage_date >= CURRENT_DATE - INTERVAL 30 DAYS
    GROUP BY usage_date, sku_name, usage_unit
""")
print("Table 'compute_consumption' created")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Grant Access to App Service Principal

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

for tbl in ["query_activity", "table_access_patterns", "compute_consumption"]:
    cnt = spark.sql(f"SELECT COUNT(*) AS n FROM {catalog}.meridian_system.{tbl}").collect()[0]["n"]
    print(f"  {tbl}: {cnt} rows")
