"""Bronze layer: batch-load synthetic CRM deal CSVs via COPY INTO pattern.

Demonstrates the COPY INTO ingestion approach for known-schema batch files.
"""

import dlt
from pyspark.sql import functions as F

from src.common.config import CATALOG, SCHEMA_STAGING


@dlt.table(
    name="raw_crm_deals",
    comment="Raw CRM deal/opportunity records loaded from CSV staging files",
    table_properties={
        "quality": "bronze",
        "meridian.business_unit": "internal",
        "meridian.source": "synthetic_crm",
    },
)
def raw_crm_deals():
    return (
        spark.read.format("csv")  # noqa: F821
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"/Volumes/{CATALOG}/{SCHEMA_STAGING}/crm")
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )
