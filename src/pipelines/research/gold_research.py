"""Gold layer: business-ready research data products.

Produces the final governed tables consumed by the Research Genie space
and the Meridian Portal app: articles (with citation counts), authors
(with h-index), citations, mesh_terms, and article_search.
"""

import dlt
from pyspark.sql import Window
from pyspark.sql import functions as F


@dlt.table(
    name="articles",
    comment="Unified research articles with citation counts — primary data product",
    table_properties={
        "quality": "gold",
        "meridian.business_unit": "research",
        "meridian.data_product": "true",
    },
)
def articles():
    cleaned = dlt.read("cleaned_articles")
    citations = dlt.read("cleaned_citations")

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


@dlt.table(
    name="authors",
    comment="Author profiles with publication counts and h-index",
    table_properties={
        "quality": "gold",
        "meridian.business_unit": "research",
        "meridian.data_product": "true",
    },
)
def authors():
    cleaned_auth = dlt.read("cleaned_authors")
    articles_df = dlt.read("articles")

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


@dlt.table(
    name="citations",
    comment="[Phase 2] Citation relationships enriched with article titles and years",
    table_properties={
        "quality": "gold",
        "meridian.business_unit": "research",
        "meridian.data_product": "true",
    },
)
def citations():
    return spark.createDataFrame(  # noqa: F821
        [],
        "citing_doi STRING, cited_doi STRING, citing_title STRING, "
        "cited_title STRING, citing_year INT, cited_year INT",
    )


@dlt.table(
    name="mesh_terms",
    comment="MeSH term frequency and recency across the article corpus",
    table_properties={
        "quality": "gold",
        "meridian.business_unit": "research",
        "meridian.data_product": "true",
    },
)
def mesh_terms():
    cleaned = dlt.read("cleaned_articles")
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


@dlt.table(
    name="article_search",
    comment="Optimized search view for Genie — combines key fields into a single searchable table",
    table_properties={
        "quality": "gold",
        "meridian.business_unit": "research",
        "meridian.data_product": "true",
    },
)
def article_search():
    return (
        dlt.read("articles")
        .select(
            "article_id", "doi", "title", "abstract", "journal",
            "publication_date", "publication_year", "source",
            "is_preprint", "publication_type", "citation_count", "search_text",
        )
    )
