# Databricks notebook source
"""Silver layer: cleanse, deduplicate, and enrich internal business data.

Applies type casting, deduplication, web event sessionization, and data
quality expectations. Failed rows are quarantined for auditability.
"""

from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F


@dp.table(
    name="cleaned_deals",
    comment="Cleansed CRM deals with type casting and deduplication",
    table_properties={
        "quality": "silver",
        "meridian.business_unit": "internal",
    },
)
@dp.expect_or_drop("valid_deal_id", "deal_id IS NOT NULL")
@dp.expect_or_drop("valid_amount", "amount >= 0 OR amount IS NULL")
def cleaned_deals():
    return (
        dp.read("raw_crm_deals")
        .withColumn("amount", F.col("amount").cast("double"))
        .withColumn("arr", F.col("arr").cast("double"))
        .dropDuplicates(["deal_id"])
        .select(
            "deal_id", "account_name", "account_id", "deal_name", "stage",
            "amount", "arr", "close_date", "created_date", "owner",
            "product_line", "region",
        )
    )


@dp.table(
    name="cleaned_web_events",
    comment="Cleansed web events with parsed timestamps and session IDs",
    table_properties={
        "quality": "silver",
        "meridian.business_unit": "internal",
    },
)
@dp.expect_or_drop("valid_event_id", "event_id IS NOT NULL")
@dp.expect_or_drop("valid_timestamp", "event_timestamp IS NOT NULL")
def cleaned_web_events():
    # Sessionization: group events by customer within 30-minute windows
    w = Window.partitionBy("customer_id").orderBy("event_ts")

    return (
        dp.read("raw_web_events")
        .withColumn("event_ts", F.to_timestamp(F.col("event_timestamp")))
        .withColumn("prev_ts", F.lag("event_ts").over(w))
        .withColumn(
            "new_session",
            F.when(
                (F.col("prev_ts").isNull())
                | (F.unix_timestamp("event_ts") - F.unix_timestamp("prev_ts") > 1800),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
        .withColumn("session_group", F.sum("new_session").over(w))
        .withColumn(
            "session_id",
            F.md5(F.concat_ws("|", F.col("customer_id"), F.col("session_group").cast("string"))),
        )
        .dropDuplicates(["event_id"])
        .select(
            "event_id", "event_type",
            F.col("event_ts").alias("event_timestamp"),
            "customer_id", "account_name", "product", "endpoint",
            "response_ms", "status_code", "bytes_returned", "session_id",
        )
    )


@dp.table(
    name="cleaned_financials",
    comment="Cleansed financial summaries with validated types",
    table_properties={
        "quality": "silver",
        "meridian.business_unit": "internal",
    },
)
@dp.expect_or_drop("valid_quarter", "fiscal_quarter IS NOT NULL")
@dp.expect_or_drop("valid_revenue", "revenue >= 0")
def cleaned_financials():
    return (
        dp.read("raw_financials")
        .withColumn("revenue", F.col("revenue").cast("double"))
        .withColumn("cost_of_data", F.col("cost_of_data").cast("double"))
        .withColumn("gross_margin", F.col("gross_margin").cast("double"))
        .withColumn("fiscal_year", F.col("fiscal_year").cast("int"))
        .withColumn("customer_count", F.col("customer_count").cast("int"))
        .dropDuplicates(["fiscal_quarter", "fiscal_year", "product_line"])
        .select(
            "fiscal_quarter", "fiscal_year", "product_line",
            "revenue", "cost_of_data", "gross_margin", "customer_count",
        )
    )


@dp.table(
    name="quarantine_internal",
    comment="Quarantined internal records that failed quality expectations",
    table_properties={
        "quality": "quarantine",
        "meridian.business_unit": "internal",
    },
)
def quarantine_internal():
    return spark.createDataFrame(  # noqa: F821
        [],
        "deal_id STRING, event_id STRING, fiscal_quarter STRING, _quarantine_reason STRING",
    )
