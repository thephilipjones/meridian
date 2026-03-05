# Databricks notebook source
"""Bronze layer: ingest raw SEC EDGAR filing metadata via Auto Loader.

Reads JSON-lines files from the SEC filings staging volume, tracking files
for incremental ingestion on re-runs.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("meridian.catalog")  # noqa: F821
SCHEMA_STAGING = "meridian_staging"


@dp.table(
    name="raw_sec_filings",
    comment="Raw SEC EDGAR filing metadata ingested via Auto Loader from staging volume",
    table_properties={
        "quality": "bronze",
        "meridian.business_unit": "regulatory",
        "meridian.source": "sec_edgar",
    },
)
def raw_sec_filings():
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"/Volumes/{CATALOG}/{SCHEMA_STAGING}/sec_filings")
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
