"""Bronze layer: batch-load synthetic financial summary CSVs via COPY INTO pattern.

Demonstrates the COPY INTO approach for periodic quarterly snapshot files.
"""

import dlt
from pyspark.sql import functions as F

from src.common.config import CATALOG, SCHEMA_STAGING


@dlt.table(
    name="raw_financials",
    comment="Raw quarterly financial summaries loaded from CSV staging files",
    table_properties={
        "quality": "bronze",
        "meridian.business_unit": "internal",
        "meridian.source": "synthetic_finance",
    },
)
def raw_financials():
    return (
        spark.read.format("csv")  # noqa: F821
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"/Volumes/{CATALOG}/{SCHEMA_STAGING}/financials")
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )
