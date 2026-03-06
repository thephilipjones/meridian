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
SOURCE_TABLE = f"{catalog}.meridian_research.articles_vs_source"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Refresh VS Source Table
# MAGIC
# MAGIC Gold tables are materialized views; the VS source table is a
# MAGIC managed Delta snapshot. Uses MERGE to add new articles while
# MAGIC preserving AI-enriched abstracts (from `10_abstract_enrichment.py`).
# MAGIC Only non-abstract columns are updated for existing rows.

# COMMAND ----------

table_exists = spark.catalog.tableExists(SOURCE_TABLE)

if not table_exists:
    spark.sql(f"""
        CREATE TABLE {SOURCE_TABLE}
        TBLPROPERTIES (delta.enableChangeDataFeed = true)
        AS SELECT article_id, doi, title, abstract, journal,
                  publication_date, publication_year, source,
                  is_preprint, publication_type, citation_count
        FROM {catalog}.meridian_research.articles
    """)
    print(f"Created '{SOURCE_TABLE}' (initial load)")
else:
    spark.sql(f"""
        MERGE INTO {SOURCE_TABLE} t
        USING (
            SELECT article_id, doi, title, abstract, journal,
                   publication_date, publication_year, source,
                   is_preprint, publication_type, citation_count
            FROM {catalog}.meridian_research.articles
        ) s
        ON t.article_id = s.article_id
        WHEN MATCHED THEN UPDATE SET
            t.doi = s.doi, t.title = s.title,
            t.journal = s.journal, t.publication_date = s.publication_date,
            t.publication_year = s.publication_year, t.source = s.source,
            t.is_preprint = s.is_preprint, t.publication_type = s.publication_type,
            t.citation_count = s.citation_count
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"Merged updates into '{SOURCE_TABLE}' (abstracts preserved)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trigger Index Sync

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
