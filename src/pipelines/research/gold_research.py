# Databricks notebook source
"""Gold layer: business-ready research data products.

Produces the final governed tables consumed by the Research Genie space
and the Meridian Portal app: articles (with citation counts), authors
(with h-index), citations, mesh_terms, and article_search.
"""

from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F


@dp.table(
    name="articles",
    comment="Unified research articles with citation counts — primary data product",
    cluster_by=["publication_year", "source", "journal"],
    table_properties={
        "quality": "gold",
        "meridian.business_unit": "research",
        "meridian.data_product": "true",
    },
)
def articles():
    cleaned = dp.read("cleaned_articles")
    citations = dp.read("cleaned_citations")

    citation_counts = (
        citations
        .groupBy("cited_doi")
        .agg(F.count("*").alias("citation_count"))
    )

    return (
        cleaned
        .join(citation_counts, cleaned.doi == citation_counts.cited_doi, "left")
        .withColumn("citation_count", F.coalesce(F.col("citation_count"), F.lit(0)))
        .withColumn(
            "search_text",
            F.concat_ws(" ", F.col("title"), F.col("abstract"), F.col("journal")),
        )
        .select(
            "article_id", "doi", "pmid", "arxiv_id", "title", "abstract",
            "journal", "publication_date", "publication_year", "source",
            "is_preprint", "publication_type", "citation_count", "search_text",
        )
    )


@dp.table(
    name="authors",
    comment="Author profiles with publication counts and h-index",
    cluster_by=["last_name", "first_name"],
    table_properties={
        "quality": "gold",
        "meridian.business_unit": "research",
        "meridian.data_product": "true",
    },
)
def authors():
    cleaned_auth = dp.read("cleaned_authors")
    articles_df = dp.read("articles")

    author_articles = (
        cleaned_auth
        .join(articles_df.select("article_id", "publication_year", "citation_count"), on="article_id", how="left")
    )

    # h-index: largest h such that h papers have >= h citations each
    w = Window.partitionBy("full_name").orderBy(F.desc("citation_count"))
    with_rank = author_articles.withColumn("rank", F.row_number().over(w))
    h_index_df = (
        with_rank
        .filter(F.col("citation_count") >= F.col("rank"))
        .groupBy("full_name")
        .agg(F.max("rank").alias("h_index"))
    )

    author_stats = (
        author_articles
        .groupBy("full_name", "last_name", "first_name")
        .agg(
            F.countDistinct("article_id").alias("article_count"),
            F.min("publication_year").alias("first_pub_year"),
            F.max("publication_year").alias("last_pub_year"),
        )
    )

    return (
        author_stats
        .join(h_index_df, on="full_name", how="left")
        .withColumn("h_index", F.coalesce(F.col("h_index"), F.lit(0)))
        .withColumn("author_id", F.md5(F.lower(F.col("full_name"))))
        .select(
            "author_id", "full_name", "last_name", "first_name",
            "article_count", "h_index", "first_pub_year", "last_pub_year",
        )
    )


@dp.table(
    name="citations",
    comment="Citation relationships enriched with article titles and publication years",
    cluster_by=["citing_doi", "cited_doi"],
    table_properties={
        "quality": "gold",
        "meridian.business_unit": "research",
        "meridian.data_product": "true",
    },
)
def citations():
    cites = dp.read("cleaned_citations")
    articles_df = dp.read("articles").select(
        F.col("doi"), F.col("title"), F.col("publication_year"),
    )

    citing = articles_df.withColumnRenamed("title", "citing_title").withColumnRenamed("publication_year", "citing_year")
    cited = articles_df.withColumnRenamed("title", "cited_title").withColumnRenamed("publication_year", "cited_year")

    return (
        cites
        .join(citing, cites.citing_doi == citing.doi, "left")
        .drop(citing.doi)
        .join(cited, cites.cited_doi == cited.doi, "left")
        .drop(cited.doi)
        .select("citing_doi", "cited_doi", "citing_title", "cited_title", "citing_year", "cited_year")
    )


@dp.table(
    name="mesh_terms",
    comment="MeSH term frequency and recency across the article corpus",
    cluster_by=["mesh_term"],
    table_properties={
        "quality": "gold",
        "meridian.business_unit": "research",
        "meridian.data_product": "true",
    },
)
def mesh_terms():
    cleaned = dp.read("cleaned_articles")
    return (
        cleaned
        .filter(F.col("source") == "pubmed")
        .select("article_id", "publication_year", F.from_json("mesh_terms_raw", "ARRAY<STRING>").alias("terms"))
        .filter(F.col("terms").isNotNull())
        .withColumn("mesh_term", F.explode("terms"))
        .groupBy("mesh_term")
        .agg(
            F.count("article_id").alias("article_count"),
            F.max("publication_year").alias("latest_year"),
        )
    )


@dp.table(
    name="article_search",
    comment="Optimized search view for Genie — combines key fields into a single searchable table",
    cluster_by=["publication_year", "source"],
    table_properties={
        "quality": "gold",
        "meridian.business_unit": "research",
        "meridian.data_product": "true",
    },
)
def article_search():
    return (
        dp.read("articles")
        .select(
            "article_id", "doi", "title", "abstract", "journal",
            "publication_date", "publication_year", "source",
            "is_preprint", "publication_type", "citation_count", "search_text",
        )
    )
