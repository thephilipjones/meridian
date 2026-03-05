# Databricks notebook source
"""Bronze layer: batch-load USPTO patent metadata via COPY INTO equivalent.

Uses a batch read (not Auto Loader streaming) to demonstrate the COPY INTO
pattern for periodic bulk snapshot files — contrasting with the Auto Loader
streaming approach used for SEC and FDA data.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("meridian.catalog")  # noqa: F821
SCHEMA_STAGING = "meridian_staging"


@dp.table(
    name="raw_patents",
    comment="Raw USPTO patent grant metadata loaded via batch read from staging volume",
    table_properties={
        "quality": "bronze",
        "meridian.business_unit": "regulatory",
        "meridian.source": "uspto",
    },
)
def raw_patents():
    return (
        spark.read.format("json")  # noqa: F821
        .load(f"/Volumes/{CATALOG}/{SCHEMA_STAGING}/patents")
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
