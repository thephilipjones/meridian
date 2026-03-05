# Databricks notebook source
"""Bronze layer: ingest raw Crossref DOI metadata and citation links.

Phase 2 stub — creates an empty streaming table with the correct schema.

TODO Phase 2: Implement Auto Loader ingestion from Crossref staging volume
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("meridian.catalog")  # noqa: F821
SCHEMA_STAGING = "meridian_staging"


@dp.table(
    name="raw_crossref_metadata",
    comment="[Phase 2] Raw Crossref DOI metadata and citation references",
    table_properties={
        "quality": "bronze",
        "meridian.business_unit": "research",
        "meridian.source": "crossref",
    },
)
def raw_crossref_metadata():
    return spark.createDataFrame(  # noqa: F821
        [],
        "doi STRING, title STRING, authors_raw STRING, publisher STRING, "
        "type STRING, issued_date STRING, references_json STRING, "
        "source STRING, _ingest_timestamp TIMESTAMP, _source_file STRING",
    )
