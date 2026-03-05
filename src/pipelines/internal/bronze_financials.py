"""Bronze layer: batch-load synthetic financial summary CSVs via Auto Loader.

Uses cloudFiles (Auto Loader) to demonstrate incremental, idempotent
batch loading of periodic quarterly snapshot files — the SDP-native
equivalent of COPY INTO. Files are tracked so re-runs don't re-ingest.
"""

import databricks.declarative_pipelines as dp
from pyspark.sql import functions as F

from src.common.config import CATALOG, SCHEMA_STAGING


@dp.table(
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
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaHints",
                "fiscal_quarter STRING, fiscal_year INT, product_line STRING, "
                "revenue DOUBLE, cost_of_data DOUBLE, gross_margin DOUBLE, "
                "customer_count INT")
        .load(f"/Volumes/{CATALOG}/{SCHEMA_STAGING}/financials")
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )
