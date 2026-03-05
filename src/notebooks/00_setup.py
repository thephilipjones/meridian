# Databricks notebook source
# MAGIC %md
# MAGIC # Meridian Insights — Environment Setup
# MAGIC
# MAGIC This notebook creates the Unity Catalog objects required for the Meridian
# MAGIC Insights demo: catalog, schemas, staging volumes, and table tags.
# MAGIC
# MAGIC **Idempotent** — safe to run multiple times. Uses `IF NOT EXISTS` throughout.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

catalog_name = spark.conf.get("meridian.catalog", "meridian")

schemas = ["regulatory", "research", "internal", "staging"]

staging_sources = [
    "sec_filings", "fda_actions", "patents",
    "pubmed", "arxiv", "crossref",
    "crm", "web_events", "financials",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Catalog

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"USE CATALOG {catalog_name}")
print(f"Catalog '{catalog_name}' ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Schemas

# COMMAND ----------

for schema in schemas:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema}")
    print(f"  Schema '{catalog_name}.{schema}' ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Staging Volumes

# COMMAND ----------

for source in staging_sources:
    volume_path = f"{catalog_name}.staging.{source}"
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {volume_path}")
    print(f"  Volume '{volume_path}' ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Tags
# MAGIC
# MAGIC Tags are applied to schemas to indicate their business unit and purpose.
# MAGIC Table-level tags (medallion layer, data product status) are set by the
# MAGIC SDP pipeline table_properties and can also be applied here after
# MAGIC pipelines have run.

# COMMAND ----------

schema_tags = {
    "regulatory": {"business_unit": "regulatory", "description": "SEC, FDA, USPTO regulatory data products"},
    "research": {"business_unit": "research", "description": "PubMed, arXiv, Crossref research articles"},
    "internal": {"business_unit": "internal", "description": "CRM, web analytics, financial summaries"},
    "staging": {"business_unit": "shared", "description": "Raw file staging volumes for pipeline ingestion"},
}

for schema, tags in schema_tags.items():
    for key, value in tags.items():
        try:
            spark.sql(f"ALTER SCHEMA {catalog_name}.{schema} SET TAGS ('{key}' = '{value}')")
        except Exception as e:
            print(f"  Warning: Could not set tag {key} on {schema}: {e}")

print("Schema tags applied")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Setup

# COMMAND ----------

print(f"\n{'='*60}")
print(f"Meridian Insights — Setup Complete")
print(f"{'='*60}")
print(f"Catalog: {catalog_name}")
print(f"Schemas: {', '.join(schemas)}")
print(f"Staging volumes: {len(staging_sources)} sources")
print(f"\nNext steps:")
print(f"  1. Run data_gen_job to generate synthetic internal data")
print(f"  2. (Optional) Run data_fetch_job to fetch PubMed articles")
print(f"  3. Run research_pipeline and internal_pipeline")
print(f"  4. Deploy the Meridian Portal app")
