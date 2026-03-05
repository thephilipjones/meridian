# Databricks notebook source
"""Bronze layer: ingest raw PubMed article metadata.

Phase 1 stub — creates an empty table with the correct schema so
downstream silver/gold tables can resolve. Run data_fetch_job to
populate the staging volume, then swap this to Auto Loader ingestion.

TODO Phase 2: Switch to Auto Loader from /Volumes/.../meridian_staging/pubmed
"""

from pyspark import pipelines as dp


@dp.table(
    name="raw_pubmed_articles",
    comment="Raw PubMed article metadata (stub — run data_fetch_job to populate)",
    table_properties={
        "quality": "bronze",
        "meridian.business_unit": "research",
        "meridian.source": "pubmed",
    },
)
def raw_pubmed_articles():
    return spark.createDataFrame(  # noqa: F821
        [],
        "pmid STRING, doi STRING, title STRING, abstract STRING, "
        "authors_raw STRING, journal STRING, publication_date STRING, "
        "mesh_terms_raw STRING, publication_types STRING, "
        "source STRING, _ingest_timestamp TIMESTAMP, _source_file STRING",
    )
