# Databricks notebook source
# MAGIC %md
# MAGIC # Meridian Insights — Demo Walkthrough
# MAGIC
# MAGIC A guided narrative notebook for live demos. Follow the chapters below,
# MAGIC running each cell to illustrate the Meridian Insights platform story.
# MAGIC
# MAGIC **Total time: ~20 minutes** (can be shortened to ~10 by skipping Chapter 2
# MAGIC detail and Chapter 4).
# MAGIC
# MAGIC See `docs/DEMO_SCRIPT.md` for full talking points and presenter notes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

dbutils.widgets.text("catalog_name", "serverless_stable_k2zkdm_catalog")
catalog = dbutils.widgets.get("catalog_name")
spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Chapter 1: "The Data Challenge" (3 min)
# MAGIC
# MAGIC > *"Meridian Insights is a data provider — think Bloomberg, Clarivate, S&P Global.
# MAGIC > They ingest messy raw data from dozens of sources and turn it into curated,
# MAGIC > trustworthy data products that their customers pay for."*
# MAGIC
# MAGIC ### Show the raw data landing in volumes

# COMMAND ----------

# List staging volumes to show the variety of sources
display(spark.sql(f"SHOW VOLUMES IN {catalog}.meridian_staging"))

# COMMAND ----------

# Peek at raw PubMed JSON files in the staging volume
dbutils.fs.ls(f"/Volumes/{catalog}/staging/pubmed/")

# COMMAND ----------

# Peek at raw CRM CSV files
dbutils.fs.ls(f"/Volumes/{catalog}/staging/crm/")

# COMMAND ----------

# MAGIC %md
# MAGIC > *"Different formats (JSON, XML, CSV), different cadences (real-time events
# MAGIC > vs quarterly snapshots), different quality levels. Databricks handles all
# MAGIC > of them within the same pipeline framework."*

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Chapter 2: "The Pipeline" (5 min)
# MAGIC
# MAGIC > *"Meridian uses Spark Declarative Pipelines to build a medallion architecture —
# MAGIC > bronze for raw ingestion, silver for cleansing, gold for business-ready
# MAGIC > data products."*
# MAGIC
# MAGIC ### Show the bronze layer — raw ingestion

# COMMAND ----------

# Raw PubMed articles — as they arrived
display(spark.sql(f"""
    SELECT pmid, doi, title, journal, publication_date, source
    FROM {catalog}.meridian_research.raw_pubmed_articles
    LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Show the silver layer — quality gates

# COMMAND ----------

# Cleaned articles with parsed dates, deduplication, preprint flagging
display(spark.sql(f"""
    SELECT article_id, doi, title, publication_year, source, is_preprint, publication_type
    FROM {catalog}.meridian_research.cleaned_articles
    LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC > *"Notice how preprints are flagged, publication types are classified, and
# MAGIC > every record has a DOI-based article_id for deduplication."*

# COMMAND ----------

# MAGIC %md
# MAGIC ### Show the gold layer — data products

# COMMAND ----------

# Articles with citation counts — pipe syntax for a clean top-N query
display(spark.sql(f"""
    FROM {catalog}.meridian_research.articles
    |> ORDER BY citation_count DESC
    |> LIMIT 10
    |> SELECT title, journal, publication_year, publication_type, citation_count
"""))

# COMMAND ----------

# Author profiles with h-index — pipe syntax
display(spark.sql(f"""
    FROM {catalog}.meridian_research.authors
    |> ORDER BY h_index DESC
    |> LIMIT 10
    |> SELECT full_name, article_count, h_index, first_pub_year, last_pub_year
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Show lineage in Unity Catalog
# MAGIC
# MAGIC > *"Trace a gold `articles` record back through the silver cleansing layer
# MAGIC > to the original PubMed raw data. This lineage is automatic — no extra
# MAGIC > configuration needed."*
# MAGIC
# MAGIC Navigate to **Catalog Explorer → meridian.research.articles → Lineage** in the UI.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Chapter 3: "The Portal" (7 min)
# MAGIC
# MAGIC > *"Meridian's customers and internal teams interact with data through the
# MAGIC > Meridian Portal — built as a Databricks App."*
# MAGIC
# MAGIC **Open the Meridian Portal app in a separate tab.**
# MAGIC
# MAGIC ### As Sarah Chen (Internal RevOps)

# COMMAND ----------

# The data behind Sarah's dashboard — using pipe syntax (|>)
display(spark.sql(f"""
    FROM {catalog}.meridian_internal.sales_pipeline
    |> AGGREGATE SUM(deal_count) AS deals, ROUND(SUM(total_amount), 0) AS pipeline_value
       GROUP BY stage
    |> ORDER BY deals DESC
"""))

# COMMAND ----------

# Revenue via Metric Views — governed KPIs, consistent everywhere
display(spark.sql(f"""
    SELECT
        `Fiscal Year`, `Fiscal Quarter`, `Product Line`,
        MEASURE(`Total Revenue`) AS revenue,
        MEASURE(`Gross Margin Pct`) AS margin_pct
    FROM {catalog}.meridian_internal.revenue_metrics
    GROUP BY ALL
    ORDER BY `Fiscal Year` DESC, `Fiscal Quarter`
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Switch to Dr. Anika Park (Research)
# MAGIC
# MAGIC > *"What are the latest findings on CRISPR off-target effects?"*

# COMMAND ----------

display(spark.sql(f"""
    FROM {catalog}.meridian_research.article_search
    |> WHERE lower(search_text) LIKE '%crispr%off-target%'
    |> ORDER BY citation_count DESC
    |> LIMIT 10
    |> SELECT title, journal, publication_year, publication_type, citation_count,
             CASE WHEN is_preprint = 'true' THEN 'PREPRINT' ELSE 'Peer-reviewed' END AS status
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Chapter 4: "The Distribution" (3 min)
# MAGIC
# MAGIC > *"Meridian's enterprise customers want the data in their own environment.
# MAGIC > Delta Sharing makes this possible — live data, no copies."*
# MAGIC
# MAGIC **Phase 2 — See `src/notebooks/02_delta_sharing.py` for the full demo.**
# MAGIC
# MAGIC Key points to show:
# MAGIC 1. Share creation in Unity Catalog
# MAGIC 2. Recipient activation link
# MAGIC 3. Consumer querying shared tables from their environment
# MAGIC 4. Access revocation

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Chapter 5: "The Governance Story" (2 min)
# MAGIC
# MAGIC > *"For a data provider, governance isn't optional — it's the product."*

# COMMAND ----------

# Show table tags — pipe syntax
display(spark.sql(f"""
    FROM {catalog}.information_schema.table_tags
    |> WHERE schema_name = 'meridian_research'
    |> ORDER BY table_name, tag_name
    |> SELECT table_name, tag_name, tag_value
"""))

# COMMAND ----------

# Customer health via Metric View — governed health KPIs
display(spark.sql(f"""
    SELECT
        `Account Name`,
        MEASURE(`Total ARR`) AS arr,
        MEASURE(`Avg Health Score`) AS health_score
    FROM {catalog}.meridian_internal.customer_health_metrics
    GROUP BY ALL
    ORDER BY arr DESC
    LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Demo Complete
# MAGIC
# MAGIC **Key takeaways:**
# MAGIC - One platform for ingestion, transformation, governance, and distribution
# MAGIC - Medallion architecture enforces quality at every layer
# MAGIC - **Liquid Clustering** adapts table layout to query patterns — zero maintenance
# MAGIC - **Metric Views** define KPIs once in Unity Catalog — consumed consistently by Genie, dashboards, and SQL
# MAGIC - **AI/BI Dashboards** provide governed analytics built on Metric Views
# MAGIC - Genie puts natural language on top of governed data
# MAGIC - Delta Sharing distributes live data products without copies
# MAGIC - Unity Catalog provides lineage, tags, and access control
