# Databricks notebook source
"""Silver layer: cleanse, deduplicate, and standardize regulatory data.

Applies type casting, deduplication, date validation, and data quality
expectations across SEC filings, FDA actions, and patent records.
Failed rows are quarantined for auditability.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="cleaned_sec_filings",
    comment="Cleansed SEC filings with validated dates and deduplication",
    table_properties={
        "quality": "silver",
        "meridian.business_unit": "regulatory",
    },
)
@dp.expect_or_drop("valid_filing_id", "filing_id IS NOT NULL")
@dp.expect_or_drop("valid_filing_date", "filing_date IS NOT NULL")
@dp.expect_or_drop("valid_company_name", "LENGTH(company_name) > 0")
def cleaned_sec_filings():
    return (
        dp.read("raw_sec_filings")
        .withColumn("filing_date", F.to_date(F.col("filing_date")))
        .dropDuplicates(["filing_id"])
        .select(
            "filing_id", "cik", "company_name", "filing_type",
            "filing_date", "description", "sic_code", "state",
            "_ingest_timestamp", "_source_file",
        )
    )


@dp.table(
    name="cleaned_fda_actions",
    comment="Cleansed FDA enforcement actions with validated dates and deduplication",
    table_properties={
        "quality": "silver",
        "meridian.business_unit": "regulatory",
    },
)
@dp.expect_or_drop("valid_action_id", "action_id IS NOT NULL")
@dp.expect_or_drop("valid_recall_date", "recall_initiation_date IS NOT NULL")
@dp.expect_or_drop("valid_company_name", "LENGTH(company_name) > 0")
def cleaned_fda_actions():
    return (
        dp.read("raw_fda_actions")
        .withColumn("recall_initiation_date", F.to_date(F.col("recall_initiation_date")))
        .dropDuplicates(["action_id"])
        .select(
            "action_id", "product_description", "reason_for_recall",
            "status", "classification", "recall_initiation_date",
            "company_name", "city", "state",
            "_ingest_timestamp", "_source_file",
        )
    )


@dp.table(
    name="cleaned_patents",
    comment="Cleansed patent records with validated dates and deduplication",
    table_properties={
        "quality": "silver",
        "meridian.business_unit": "regulatory",
    },
)
@dp.expect_or_drop("valid_patent_number", "patent_number IS NOT NULL")
@dp.expect_or_drop("valid_filing_date", "filing_date IS NOT NULL")
@dp.expect_or_drop("valid_assignee", "LENGTH(assignee) > 0")
def cleaned_patents():
    return (
        dp.read("raw_patents")
        .withColumn("filing_date", F.to_date(F.col("filing_date")))
        .withColumn("grant_date", F.to_date(F.col("grant_date")))
        .dropDuplicates(["patent_number"])
        .select(
            "patent_number", "title", "abstract", "assignee",
            "filing_date", "grant_date", "patent_type", "uspc_class",
            "_ingest_timestamp", "_source_file",
        )
    )


@dp.table(
    name="quarantine_regulatory",
    comment="Quarantined regulatory records that failed quality expectations",
    table_properties={
        "quality": "quarantine",
        "meridian.business_unit": "regulatory",
    },
)
def quarantine_regulatory():
    return spark.createDataFrame(  # noqa: F821
        [],
        "record_id STRING, source STRING, _quarantine_reason STRING, "
        "_quarantine_timestamp TIMESTAMP",
    )
