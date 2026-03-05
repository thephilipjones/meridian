# Databricks notebook source
"""Gold layer: business-ready regulatory data products.

Produces the final governed tables consumed by the Regulatory Genie space,
the Meridian Portal's James Rivera (Acme Bank) view, and Delta Sharing:
regulatory_actions (unified SEC+FDA), patent_landscape, company_entities
(master entity), and company_risk_signals (derived).
"""

from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F


@dp.table(
    name="regulatory_actions",
    comment="Unified regulatory actions across SEC filings and FDA enforcement — primary data product",
    cluster_by=["action_year", "action_source", "company_name"],
    table_properties={
        "quality": "gold",
        "meridian.business_unit": "regulatory",
        "meridian.data_product": "true",
    },
)
def regulatory_actions():
    sec = dp.read("cleaned_sec_filings").select(
        F.col("filing_id").alias("action_id"),
        F.lit("SEC").alias("action_source"),
        F.col("company_name"),
        F.col("filing_type").alias("action_type"),
        F.col("filing_date").alias("action_date"),
        F.col("description").alias("action_description"),
        F.col("cik").alias("source_reference_id"),
        F.col("state"),
        F.lit(None).cast("string").alias("classification"),
        F.lit(None).cast("string").alias("status"),
    )

    fda = dp.read("cleaned_fda_actions").select(
        F.col("action_id"),
        F.lit("FDA").alias("action_source"),
        F.col("company_name"),
        F.col("classification").alias("action_type"),
        F.col("recall_initiation_date").alias("action_date"),
        F.col("reason_for_recall").alias("action_description"),
        F.col("action_id").alias("source_reference_id"),
        F.col("state"),
        F.col("classification"),
        F.col("status"),
    )

    unified = sec.unionByName(fda, allowMissingColumns=True)

    return (
        unified
        .withColumn("action_year", F.year(F.col("action_date")))
        .withColumn(
            "subscription_tier",
            F.when(F.col("action_source") == "SEC", "sec_only")
            .when(F.col("action_source") == "FDA", "fda_only")
            .otherwise("full"),
        )
    )


@dp.table(
    name="patent_landscape",
    comment="Patent portfolio analytics by assignee, year, and technology class",
    cluster_by=["assignee", "filing_year", "uspc_class"],
    table_properties={
        "quality": "gold",
        "meridian.business_unit": "regulatory",
        "meridian.data_product": "true",
    },
)
def patent_landscape():
    patents = dp.read("cleaned_patents")

    return (
        patents
        .withColumn("filing_year", F.year(F.col("filing_date")))
        .withColumn("grant_year", F.year(F.col("grant_date")))
        .withColumn(
            "grant_lag_days",
            F.datediff(F.col("grant_date"), F.col("filing_date")),
        )
        .select(
            "patent_number", "title", "abstract", "assignee",
            "filing_date", "grant_date", "filing_year", "grant_year",
            "patent_type", "uspc_class", "grant_lag_days",
        )
    )


@dp.table(
    name="company_entities",
    comment="Master entity table — companies resolved across SEC, FDA, and patent sources",
    cluster_by=["company_name"],
    table_properties={
        "quality": "gold",
        "meridian.business_unit": "regulatory",
        "meridian.data_product": "true",
    },
)
def company_entities():
    sec_companies = (
        dp.read("cleaned_sec_filings")
        .select(
            F.col("company_name"),
            F.lit("SEC").alias("source"),
            F.col("cik").alias("source_id"),
            F.col("state"),
        )
        .dropDuplicates(["company_name", "source"])
    )

    fda_companies = (
        dp.read("cleaned_fda_actions")
        .select(
            F.col("company_name"),
            F.lit("FDA").alias("source"),
            F.col("action_id").alias("source_id"),
            F.col("state"),
        )
        .dropDuplicates(["company_name", "source"])
    )

    patent_companies = (
        dp.read("cleaned_patents")
        .select(
            F.col("assignee").alias("company_name"),
            F.lit("USPTO").alias("source"),
            F.col("patent_number").alias("source_id"),
            F.lit(None).cast("string").alias("state"),
        )
        .dropDuplicates(["company_name", "source"])
    )

    all_companies = sec_companies.unionByName(fda_companies).unionByName(patent_companies)

    return (
        all_companies
        .groupBy("company_name")
        .agg(
            F.collect_set("source").alias("data_sources"),
            F.count("*").alias("source_count"),
            F.first("state", ignorenulls=True).alias("primary_state"),
        )
        .withColumn("entity_id", F.md5(F.lower(F.trim(F.col("company_name")))))
        .select(
            "entity_id", "company_name", "data_sources",
            "source_count", "primary_state",
        )
    )


@dp.table(
    name="company_risk_signals",
    comment="Derived risk signals from regulatory action frequency, recall severity, and filing patterns",
    cluster_by=["risk_tier", "company_name"],
    table_properties={
        "quality": "gold",
        "meridian.business_unit": "regulatory",
        "meridian.data_product": "true",
    },
)
def company_risk_signals():
    entities = dp.read("company_entities")
    actions = dp.read("regulatory_actions")
    patents = dp.read("patent_landscape")

    action_stats = (
        actions
        .groupBy("company_name")
        .agg(
            F.count("*").alias("total_actions"),
            F.sum(F.when(F.col("action_source") == "SEC", 1).otherwise(0)).alias("sec_filings"),
            F.sum(F.when(F.col("action_source") == "FDA", 1).otherwise(0)).alias("fda_actions"),
            F.sum(
                F.when(F.col("classification") == "Class I", 1).otherwise(0)
            ).alias("class_i_recalls"),
            F.max("action_date").alias("latest_action_date"),
        )
    )

    patent_stats = (
        patents
        .groupBy("assignee")
        .agg(
            F.count("*").alias("patent_count"),
            F.avg("grant_lag_days").alias("avg_grant_lag_days"),
        )
    )

    return (
        entities
        .join(action_stats, on="company_name", how="left")
        .join(patent_stats, entities.company_name == patent_stats.assignee, "left")
        .drop("assignee")
        .withColumn("total_actions", F.coalesce(F.col("total_actions"), F.lit(0)))
        .withColumn("sec_filings", F.coalesce(F.col("sec_filings"), F.lit(0)))
        .withColumn("fda_actions", F.coalesce(F.col("fda_actions"), F.lit(0)))
        .withColumn("class_i_recalls", F.coalesce(F.col("class_i_recalls"), F.lit(0)))
        .withColumn("patent_count", F.coalesce(F.col("patent_count"), F.lit(0)))
        .withColumn(
            "internal_score",
            (
                F.col("class_i_recalls") * 10
                + F.col("fda_actions") * 2
                + F.col("sec_filings") * 1
                - F.col("patent_count") * 0.5
            ),
        )
        .withColumn(
            "risk_tier",
            F.when(F.col("internal_score") >= 30, "High")
            .when(F.col("internal_score") >= 10, "Medium")
            .otherwise("Low"),
        )
        .select(
            "entity_id", "company_name", "data_sources", "primary_state",
            "total_actions", "sec_filings", "fda_actions", "class_i_recalls",
            "patent_count", "avg_grant_lag_days", "latest_action_date",
            "internal_score", "risk_tier",
        )
    )
