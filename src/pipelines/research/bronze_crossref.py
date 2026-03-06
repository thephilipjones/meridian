# Databricks notebook source
"""Bronze layer: ingest raw Crossref citation links from staging volume.

Reads JSON-lines files from the crossref staging volume via Auto Loader,
producing a streaming table of citation edges (citing_doi → cited_doi).
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("meridian.catalog")  # noqa: F821
SCHEMA_STAGING = "meridian_staging"


@dp.table(
    name="raw_crossref_citations",
    comment="Raw Crossref citation links ingested via Auto Loader",
    table_properties={
        "quality": "bronze",
        "meridian.business_unit": "research",
        "meridian.source": "crossref",
    },
)
def raw_crossref_citations():
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"/Volumes/{CATALOG}/{SCHEMA_STAGING}/crossref/_schema")
        .load(f"/Volumes/{CATALOG}/{SCHEMA_STAGING}/crossref/")
        .select(
            F.col("citing_doi"),
            F.col("cited_doi"),
            F.col("source"),
            F.current_timestamp().alias("_ingest_timestamp"),
            F.col("_metadata.file_path").alias("_source_file"),
        )
    )
