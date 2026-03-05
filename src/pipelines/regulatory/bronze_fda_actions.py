# Databricks notebook source
"""Bronze layer: ingest raw FDA enforcement/recall action data via Auto Loader.

Reads JSON-lines files from the FDA actions staging volume, tracking files
for incremental ingestion on re-runs.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("meridian.catalog")  # noqa: F821
SCHEMA_STAGING = "meridian_staging"


@dp.table(
    name="raw_fda_actions",
    comment="Raw FDA enforcement and recall actions ingested via Auto Loader from staging volume",
    table_properties={
        "quality": "bronze",
        "meridian.business_unit": "regulatory",
        "meridian.source": "openfda",
    },
)
def raw_fda_actions():
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"/Volumes/{CATALOG}/{SCHEMA_STAGING}/fda_actions")
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
