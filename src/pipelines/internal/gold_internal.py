# Databricks notebook source
"""Gold layer: business-ready internal analytics data products.

Produces governed tables consumed by the Internal Analytics Genie space
and the Meridian Portal's Sarah Chen (RevOps) view: sales_pipeline,
product_usage, revenue_summary, and customer_health.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="sales_pipeline",
    comment="Sales pipeline analytics — deal counts, amounts, and conversion rates by stage",
    cluster_by=["stage", "product_line", "fiscal_quarter"],
    table_properties={
        "quality": "gold",
        "meridian.business_unit": "internal",
        "meridian.data_product": "false",
    },
)
def sales_pipeline():
    deals = dp.read("cleaned_deals")
    total_deals = deals.count()

    return (
        deals
        .withColumn(
            "fiscal_quarter",
            F.concat(
                F.lit("Q"),
                F.ceil(F.month(F.to_date(F.col("close_date"))) / 3).cast("string"),
            ),
        )
        .groupBy("stage", "product_line", "region", "fiscal_quarter")
        .agg(
            F.count("deal_id").alias("deal_count"),
            F.sum("amount").alias("total_amount"),
            F.sum("arr").alias("total_arr"),
            F.avg("amount").alias("avg_deal_size"),
        )
        .withColumn("conversion_rate", F.col("deal_count") / F.lit(total_deals))
    )


@dp.table(
    name="product_usage",
    comment="Product usage aggregates by account and product — API calls, response times, error rates",
    cluster_by=["account_name", "product", "period"],
    table_properties={
        "quality": "gold",
        "meridian.business_unit": "internal",
        "meridian.data_product": "false",
    },
)
def product_usage():
    events = dp.read("cleaned_web_events")

    return (
        events
        .withColumn("period", F.date_format(F.col("event_timestamp"), "yyyy-MM"))
        .groupBy("account_name", "product", "period")
        .agg(
            F.count("event_id").alias("api_calls"),
            F.countDistinct("customer_id").alias("unique_users"),
            F.avg("response_ms").alias("avg_response_ms"),
            (F.sum(F.when(F.col("status_code") >= 400, 1).otherwise(0)) / F.count("*")).alias("error_rate"),
            F.sum("bytes_returned").alias("bytes_served"),
        )
    )


@dp.table(
    name="revenue_summary",
    comment="Revenue summary with YoY growth calculations by product and quarter",
    cluster_by=["fiscal_year", "fiscal_quarter", "product_line"],
    table_properties={
        "quality": "gold",
        "meridian.business_unit": "internal",
        "meridian.data_product": "false",
    },
)
def revenue_summary():
    fin = dp.read("cleaned_financials")

    fin_with_margin_pct = fin.withColumn(
        "gross_margin_pct",
        F.when(F.col("revenue") > 0, F.col("gross_margin") / F.col("revenue")).otherwise(0.0),
    ).withColumn(
        "revenue_per_customer",
        F.when(F.col("customer_count") > 0, F.col("revenue") / F.col("customer_count")).otherwise(0.0),
    )

    # Self-join for YoY growth
    prev_year = fin.select(
        F.col("fiscal_quarter"),
        (F.col("fiscal_year") + 1).alias("fiscal_year"),
        F.col("product_line"),
        F.col("revenue").alias("prev_revenue"),
    )

    return (
        fin_with_margin_pct
        .join(prev_year, on=["fiscal_quarter", "fiscal_year", "product_line"], how="left")
        .withColumn(
            "yoy_revenue_growth",
            F.when(
                F.col("prev_revenue").isNotNull() & (F.col("prev_revenue") > 0),
                (F.col("revenue") - F.col("prev_revenue")) / F.col("prev_revenue"),
            ),
        )
        .select(
            "fiscal_quarter", "fiscal_year", "product_line",
            "revenue", "cost_of_data", "gross_margin", "gross_margin_pct",
            "customer_count", "revenue_per_customer", "yoy_revenue_growth",
        )
    )


@dp.table(
    name="customer_health",
    comment="Customer health scores based on usage patterns, deal value, and product adoption",
    cluster_by=["health_tier", "account_name"],
    table_properties={
        "quality": "gold",
        "meridian.business_unit": "internal",
        "meridian.data_product": "false",
    },
)
def customer_health():
    deals = dp.read("cleaned_deals")
    usage = dp.read("product_usage")

    account_arr = (
        deals
        .filter(F.col("stage") == "Closed Won")
        .groupBy("account_name", "account_id")
        .agg(
            F.sum("arr").alias("arr"),
            F.countDistinct("product_line").alias("products_subscribed"),
        )
    )

    recent_usage = (
        usage
        .groupBy("account_name")
        .agg(
            F.sum("api_calls").alias("api_calls_30d"),
            F.avg("avg_response_ms").alias("avg_response_ms_30d"),
            F.avg("error_rate").alias("error_rate_30d"),
        )
    )

    return (
        account_arr
        .join(recent_usage, on="account_name", how="left")
        .withColumn("api_calls_30d", F.coalesce(F.col("api_calls_30d"), F.lit(0)))
        .withColumn("avg_response_ms_30d", F.coalesce(F.col("avg_response_ms_30d"), F.lit(0.0)))
        .withColumn("error_rate_30d", F.coalesce(F.col("error_rate_30d"), F.lit(0.0)))
        .withColumn(
            "health_score",
            (
                F.least(F.col("api_calls_30d") / 1000.0, F.lit(1.0)) * 0.4
                + F.when(F.col("avg_response_ms_30d") < 500, 1.0)
                .when(F.col("avg_response_ms_30d") < 2000, 0.6)
                .otherwise(0.2)
                * 0.3
                + F.when(F.col("error_rate_30d") < 0.01, 1.0)
                .when(F.col("error_rate_30d") < 0.05, 0.6)
                .otherwise(0.2)
                * 0.3
            ),
        )
        .withColumn(
            "health_tier",
            F.when(F.col("health_score") >= 0.8, "Healthy")
            .when(F.col("health_score") >= 0.5, "At Risk")
            .otherwise("Critical"),
        )
        .select(
            "account_name", "account_id", "arr", "products_subscribed",
            "api_calls_30d", "avg_response_ms_30d", "error_rate_30d",
            "health_score", "health_tier",
        )
    )
