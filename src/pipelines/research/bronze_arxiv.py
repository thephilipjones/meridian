"""Bronze layer: ingest raw arXiv preprint metadata.

Phase 2 stub — creates an empty streaming table with the correct schema
so downstream silver/gold tables can reference it without errors.

TODO Phase 2: Implement Auto Loader ingestion from arXiv staging volume
"""

import dlt
from pyspark.sql import functions as F

from src.common.config import CATALOG, SCHEMA_STAGING


@dlt.table(
    name="raw_arxiv_articles",
    comment="[Phase 2] Raw arXiv preprint metadata",
    table_properties={
        "quality": "bronze",
        "meridian.business_unit": "research",
        "meridian.source": "arxiv",
    },
)
def raw_arxiv_articles():
    return spark.createDataFrame(  # noqa: F821
        [],
        "arxiv_id STRING, doi STRING, title STRING, abstract STRING, "
        "authors_raw STRING, categories STRING, published_date STRING, "
        "updated_date STRING, source STRING, _ingest_timestamp TIMESTAMP, _source_file STRING",
    )
