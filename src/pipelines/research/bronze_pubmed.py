"""Bronze layer: ingest raw PubMed article JSON from staging volume via Auto Loader.

Streaming table with schema enforcement. Raw data lands as-is with
ingest metadata (_ingest_timestamp, _source_file).
"""

import databricks.declarative_pipelines as dp
from pyspark.sql import functions as F

from src.common.config import CATALOG, SCHEMA_STAGING


@dp.table(
    name="raw_pubmed_articles",
    comment="Raw PubMed article metadata ingested via Auto Loader from staging volume",
    table_properties={
        "quality": "bronze",
        "meridian.business_unit": "research",
        "meridian.source": "pubmed",
    },
)
def raw_pubmed_articles():
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821 — spark available in DLT runtime
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaHints", "pmid STRING, doi STRING, title STRING")
        .load(f"/Volumes/{CATALOG}/{SCHEMA_STAGING}/pubmed")
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("source", F.lit("pubmed"))
    )
