# Databricks notebook source
"""Bronze layer: stream synthetic web analytics events via Auto Loader.

Demonstrates the Auto Loader (cloudFiles) ingestion approach for
high-volume JSON event streams.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("meridian.catalog")  # noqa: F821
SCHEMA_STAGING = "meridian_staging"


@dp.table(
    name="raw_web_events",
    comment="Raw web analytics / API usage events ingested via Auto Loader",
    table_properties={
        "quality": "bronze",
        "meridian.business_unit": "internal",
        "meridian.source": "synthetic_web",
    },
)
def raw_web_events():
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"/Volumes/{CATALOG}/{SCHEMA_STAGING}/web_events")
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
