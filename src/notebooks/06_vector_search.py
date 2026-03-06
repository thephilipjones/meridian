# Databricks notebook source
# MAGIC %md
# MAGIC # Meridian Insights — Vector Search Setup
# MAGIC
# MAGIC Creates a Vector Search endpoint and delta-sync index on research
# MAGIC article abstracts for semantic search in the Research Q&A view.
# MAGIC
# MAGIC > *"Dr. Park asks a research question in natural language and gets
# MAGIC > semantically relevant papers — not just keyword matches."*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog_name", "serverless_stable_k2zkdm_catalog")
catalog = dbutils.widgets.get("catalog_name")

VS_ENDPOINT_NAME = "meridian-research-vs"
VS_INDEX_NAME = f"{catalog}.meridian_research.articles_vs_index"
SOURCE_TABLE = f"{catalog}.meridian_research.articles_vs_source"

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Create Vector Search Endpoint

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

try:
    endpoint = w.vector_search_endpoints.get_endpoint(VS_ENDPOINT_NAME)
    print(f"Endpoint '{VS_ENDPOINT_NAME}' already exists — status: {endpoint.endpoint_status.state}")
except Exception:
    print(f"Creating endpoint '{VS_ENDPOINT_NAME}'...")
    w.vector_search_endpoints.create_endpoint(
        name=VS_ENDPOINT_NAME,
        endpoint_type="STORAGE_OPTIMIZED",
    )
    print(f"Endpoint '{VS_ENDPOINT_NAME}' creation initiated (may take a few minutes)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Wait for Endpoint to be Ready

# COMMAND ----------

import time

for _ in range(30):
    ep = w.vector_search_endpoints.get_endpoint(VS_ENDPOINT_NAME)
    state = ep.endpoint_status.state.value if ep.endpoint_status else "UNKNOWN"
    if state == "ONLINE":
        print(f"Endpoint '{VS_ENDPOINT_NAME}' is ONLINE")
        break
    print(f"  Status: {state} — waiting 30s...")
    time.sleep(30)
else:
    print("WARNING: Endpoint not ONLINE after 15 minutes. Index creation may still work.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Create VS Source Table
# MAGIC
# MAGIC SDP gold tables are materialized views which don't support Change Data Feed.
# MAGIC We create a managed Delta table snapshot for VS indexing.

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SOURCE_TABLE}
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
    AS SELECT article_id, doi, title, abstract, journal,
              publication_date, publication_year, source,
              is_preprint, publication_type, citation_count
    FROM {catalog}.meridian_research.articles
""")
print(f"Source table '{SOURCE_TABLE}' ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Create Delta Sync Index with Managed Embeddings
# MAGIC
# MAGIC The index syncs from `articles_vs_source`, embedding the
# MAGIC `abstract` column using `databricks-gte-large-en`. Primary key
# MAGIC is `article_id`. Triggered sync — articles don't change often.

# COMMAND ----------

try:
    existing = w.vector_search_indexes.get_index(VS_INDEX_NAME)
    print(f"Index '{VS_INDEX_NAME}' already exists — status: {existing.status.ready}")
except Exception:
    print(f"Creating index '{VS_INDEX_NAME}'...")
    w.vector_search_indexes.create_index(
        name=VS_INDEX_NAME,
        endpoint_name=VS_ENDPOINT_NAME,
        primary_key="article_id",
        index_type="DELTA_SYNC",
        delta_sync_index_spec={
            "source_table": SOURCE_TABLE,
            "embedding_source_columns": [
                {
                    "name": "abstract",
                    "embedding_model_endpoint_name": "databricks-gte-large-en",
                }
            ],
            "pipeline_type": "TRIGGERED",
        },
    )
    print(f"Index '{VS_INDEX_NAME}' creation initiated")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Trigger Initial Sync

# COMMAND ----------

try:
    w.vector_search_indexes.sync_index(VS_INDEX_NAME)
    print(f"Sync triggered for '{VS_INDEX_NAME}'")
except Exception as e:
    print(f"Sync may already be in progress: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6. Verify — Test Query

# COMMAND ----------

import time

for _ in range(20):
    idx = w.vector_search_indexes.get_index(VS_INDEX_NAME)
    if idx.status and idx.status.ready:
        print("Index is ready!")
        break
    print("  Index syncing — waiting 30s...")
    time.sleep(30)
else:
    print("WARNING: Index not ready after 10 minutes. Test query may fail.")

# COMMAND ----------

results = w.vector_search_indexes.query_index(
    index_name=VS_INDEX_NAME,
    columns=["article_id", "title", "abstract", "journal", "publication_year", "citation_count"],
    query_text="CRISPR gene editing off-target effects",
    num_results=5,
)

print("Top 5 semantic search results:")
for row in results.result.data_array:
    score = row[-1]
    title = row[1]
    year = row[4]
    print(f"  [{score:.3f}] ({year}) {title}")
