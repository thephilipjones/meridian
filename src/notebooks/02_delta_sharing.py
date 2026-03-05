# Databricks notebook source
# MAGIC %md
# MAGIC # Meridian Insights — Delta Sharing Setup
# MAGIC
# MAGIC Demonstrates creating Delta Sharing shares, recipients, and the
# MAGIC consumer experience. Shows how Meridian distributes regulated data
# MAGIC products to external customers with live, no-copy access.
# MAGIC
# MAGIC > *"Enterprise customers don't want a portal — they want the data
# MAGIC > in their own environment. Delta Sharing makes this possible."*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog_name", "serverless_stable_k2zkdm_catalog")
catalog = dbutils.widgets.get("catalog_name")
spark.sql(f"USE CATALOG {catalog}")

share_name = "meridian_regulatory_share"
recipient_name = "acme_bank"

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Create the Share
# MAGIC
# MAGIC A **share** is a read-only collection of tables and partitions that
# MAGIC a data provider makes available to one or more recipients. The shared
# MAGIC tables are live — recipients see the latest data without copies.

# COMMAND ----------

spark.sql(f"CREATE SHARE IF NOT EXISTS {share_name}")
print(f"Share '{share_name}' ready")

# COMMAND ----------

# Add gold regulatory tables to the share
spark.sql(f"""
    ALTER SHARE {share_name}
    ADD TABLE {catalog}.meridian_regulatory.regulatory_actions
""")

spark.sql(f"""
    ALTER SHARE {share_name}
    ADD TABLE {catalog}.meridian_regulatory.company_entities
""")

print(f"Added regulatory_actions and company_entities to share '{share_name}'")

# COMMAND ----------

# Verify share contents
display(spark.sql(f"SHOW ALL IN SHARE {share_name}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Create a Recipient
# MAGIC
# MAGIC A **recipient** is an external entity that can access shared data.
# MAGIC The activation link provides credentials for the recipient to
# MAGIC connect from their own environment.

# COMMAND ----------

spark.sql(f"CREATE RECIPIENT IF NOT EXISTS {recipient_name}")
print(f"Recipient '{recipient_name}' ready")

# COMMAND ----------

# Show recipient info including activation link
display(spark.sql(f"DESCRIBE RECIPIENT {recipient_name}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Grant Share Access to Recipient
# MAGIC
# MAGIC Connecting the share to the recipient enables access.

# COMMAND ----------

spark.sql(f"GRANT SELECT ON SHARE {share_name} TO RECIPIENT {recipient_name}")
print(f"Granted '{recipient_name}' access to share '{share_name}'")

# COMMAND ----------

# Verify grants
display(spark.sql(f"SHOW GRANTS ON SHARE {share_name}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Consumer Simulation
# MAGIC
# MAGIC In a real deployment, the recipient would use the activation link
# MAGIC and Delta Sharing connectors from their own environment. Here we
# MAGIC simulate the consumer experience by querying the shared tables
# MAGIC directly (as the provider, demonstrating what the consumer sees).
# MAGIC
# MAGIC **In a multi-workspace demo:** Use the activation link in a second
# MAGIC workspace to show true cross-environment sharing.

# COMMAND ----------

# MAGIC %md
# MAGIC ### What the consumer sees: Regulatory Actions

# COMMAND ----------

display(spark.sql(f"""
    FROM {catalog}.meridian_regulatory.regulatory_actions
    |> WHERE action_source = 'SEC'
    |> SELECT action_id, company_name, action_type, action_date, action_description
    |> ORDER BY action_date DESC
    |> LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### What the consumer sees: Company Entities

# COMMAND ----------

display(spark.sql(f"""
    FROM {catalog}.meridian_regulatory.company_entities
    |> SELECT company_name, data_sources, source_count, primary_state
    |> WHERE source_count > 1
    |> ORDER BY source_count DESC
    |> LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Consumer sample queries (code snippets for handoff)
# MAGIC
# MAGIC ```python
# MAGIC # Databricks consumer
# MAGIC df = spark.read.format("deltaSharing") \
# MAGIC     .load("meridian_regulatory_share.meridian_regulatory.regulatory_actions")
# MAGIC display(df.limit(10))
# MAGIC ```
# MAGIC
# MAGIC ```python
# MAGIC # Pandas consumer (open connector)
# MAGIC import delta_sharing
# MAGIC profile = "credentials.share"
# MAGIC table_url = f"{profile}#meridian_regulatory_share.meridian_regulatory.regulatory_actions"
# MAGIC df = delta_sharing.load_as_pandas(table_url)
# MAGIC print(df.head())
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Access Revocation
# MAGIC
# MAGIC > *"When a subscription lapses, access is cut immediately — no stale
# MAGIC > data floating around."*
# MAGIC
# MAGIC Removing a table from a share or revoking a recipient's access is
# MAGIC instantaneous. The consumer's next query will fail.

# COMMAND ----------

# Demonstrate revocation by removing company_entities from the share
spark.sql(f"""
    ALTER SHARE {share_name}
    REMOVE TABLE {catalog}.meridian_regulatory.company_entities
""")
print("Removed company_entities from share — consumer access revoked immediately")

# COMMAND ----------

# Show the share now only contains regulatory_actions
display(spark.sql(f"SHOW ALL IN SHARE {share_name}"))

# COMMAND ----------

# Re-add the table to restore access for the demo
spark.sql(f"""
    ALTER SHARE {share_name}
    ADD TABLE {catalog}.meridian_regulatory.company_entities
""")
print("Restored company_entities to share")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Summary
# MAGIC
# MAGIC | Step | SQL | Effect |
# MAGIC |------|-----|--------|
# MAGIC | Create share | `CREATE SHARE` | Empty share container |
# MAGIC | Add tables | `ALTER SHARE ... ADD TABLE` | Tables available for sharing |
# MAGIC | Create recipient | `CREATE RECIPIENT` | External consumer identity |
# MAGIC | Grant access | `GRANT SELECT ON SHARE TO RECIPIENT` | Consumer can query |
# MAGIC | Revoke access | `ALTER SHARE ... REMOVE TABLE` | Immediate cutoff |
# MAGIC
# MAGIC All managed through Unity Catalog — full audit trail, no data copies.
