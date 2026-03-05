"""Bronze layer: batch-load synthetic CRM deal CSVs via Auto Loader.

Uses cloudFiles (Auto Loader) to demonstrate incremental, idempotent
batch loading of known-schema CSV files — the SDP-native equivalent of
COPY INTO. Files are tracked so re-runs don't re-ingest old data.
"""

import databricks.declarative_pipelines as dp
from pyspark.sql import functions as F

from src.common.config import CATALOG, SCHEMA_STAGING


@dp.table(
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
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaHints",
                "deal_id STRING, account_name STRING, account_id STRING, "
                "deal_name STRING, stage STRING, amount DOUBLE, arr DOUBLE, "
                "close_date STRING, created_date STRING, owner STRING, "
                "product_line STRING, region STRING")
        .load(f"/Volumes/{CATALOG}/{SCHEMA_STAGING}/crm")
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )
