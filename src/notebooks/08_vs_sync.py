# Databricks notebook source
# MAGIC %md
# MAGIC # Meridian Insights — Vector Search Index Sync
# MAGIC
# MAGIC Triggers a sync of the research articles Vector Search index.
# MAGIC Designed for scheduled execution (e.g., daily refresh job).
# MAGIC
# MAGIC Requires the endpoint and index to already exist (created by `06_vector_search.py`).

# COMMAND ----------

dbutils.widgets.text("catalog_name", "serverless_stable_k2zkdm_catalog")
catalog = dbutils.widgets.get("catalog_name")

VS_INDEX_NAME = f"{catalog}.meridian_research.articles_vs_index"

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

try:
    idx = w.vector_search_indexes.get_index(VS_INDEX_NAME)
    if idx.status and idx.status.ready:
        w.vector_search_indexes.sync_index(VS_INDEX_NAME)
        print(f"Sync triggered for '{VS_INDEX_NAME}'")
    else:
        print(f"Index '{VS_INDEX_NAME}' exists but is not ready — skipping sync")
except Exception as e:
    print(f"Index '{VS_INDEX_NAME}' not found or sync failed: {e}")
    print("Run 06_vector_search.py first to create the endpoint and index.")
