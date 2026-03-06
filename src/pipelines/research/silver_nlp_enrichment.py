# Databricks notebook source
"""NLP entity extraction from research article abstracts.

Uses ai_query() with the Foundation Model API to extract drug names,
gene symbols, and disease mentions from article title+abstract text.
Produces a gold-layer entity table for Genie and app queries.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

catalog = spark.conf.get("meridian.catalog")
LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"

EXTRACTION_PROMPT = """Extract biomedical entities from the following article text.
Return a JSON array of objects, each with keys: "entity_type" (one of: "drug", "gene", "disease"), "entity_value" (the entity name), and "confidence" (float 0-1).
Only include entities you are confident about. If no entities are found, return an empty array [].
Return ONLY the JSON array, no other text.

Article text:
{text}"""


@dp.table(
    name="article_entities",
    comment="Biomedical entities (drugs, genes, diseases) extracted from article abstracts via LLM",
    cluster_by=["entity_type", "article_id"],
    table_properties={
        "quality": "gold",
        "meridian.business_unit": "research",
        "meridian.data_product": "true",
    },
)
def article_entities():
    articles = dp.read("cleaned_articles").filter(
        F.col("abstract").isNotNull() & (F.length(F.col("abstract")) > 50)
    )

    text_col = F.concat_ws(". ", F.col("title"), F.col("abstract"))
    truncated = F.substring(text_col, 1, 2000)

    with_extraction = articles.withColumn(
        "llm_response",
        F.expr(f"""
            ai_query(
                '{LLM_ENDPOINT}',
                concat(
                    'Extract biomedical entities from the following article text.\\n',
                    'Return a JSON array of objects, each with keys: \"entity_type\" (one of: \"drug\", \"gene\", \"disease\"), \"entity_value\" (the entity name), and \"confidence\" (float 0-1).\\n',
                    'Only include entities you are confident about. If no entities are found, return an empty array [].\\n',
                    'Return ONLY the JSON array, no other text.\\n\\nArticle text:\\n',
                    substring(concat_ws('. ', title, abstract), 1, 2000)
                )
            )
        """),
    )

    entity_schema = "ARRAY<STRUCT<entity_type: STRING, entity_value: STRING, confidence: DOUBLE>>"

    parsed = with_extraction.withColumn(
        "entities",
        F.from_json(F.col("llm_response"), entity_schema),
    ).filter(F.col("entities").isNotNull())

    return (
        parsed
        .select("article_id", "doi", F.explode("entities").alias("entity"))
        .select(
            "article_id",
            "doi",
            F.col("entity.entity_type").alias("entity_type"),
            F.col("entity.entity_value").alias("entity_value"),
            F.col("entity.confidence").alias("confidence"),
        )
        .filter(
            F.col("entity_type").isin("drug", "gene", "disease")
            & F.col("entity_value").isNotNull()
            & (F.length(F.col("entity_value")) > 1)
        )
    )
