"""Silver layer: cleanse, normalize, and enrich research article data.

Parses raw author strings, extracts MeSH terms, normalizes dates, and
deduplicates by DOI. Failed rows are quarantined. Expectations enforce
data quality gates.
"""

import databricks.declarative_pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType


@dp.table(
    name="cleaned_articles",
    comment="Cleansed and normalized research articles from all sources (PubMed, arXiv, Crossref)",
    table_properties={
        "quality": "silver",
        "meridian.business_unit": "research",
    },
)
@dp.expect_or_quarantine("valid_title", "length(title) > 0", "quarantine_research")
@dp.expect_or_quarantine("valid_pub_date", "publication_date IS NOT NULL", "quarantine_research")
def cleaned_articles():
    pubmed = dp.read("raw_pubmed_articles").withColumn("arxiv_id", F.lit(None).cast("string"))
    arxiv = (
        dp.read("raw_arxiv_articles")
        .withColumnRenamed("published_date", "publication_date")
        .withColumn("pmid", F.lit(None).cast("string"))
        .withColumn("journal", F.lit(None).cast("string"))
        .withColumn("mesh_terms_raw", F.lit(None).cast("string"))
        .withColumn("publication_types", F.lit(None).cast("string"))
    )

    common_cols = [
        "pmid", "arxiv_id", "doi", "title", "abstract", "authors_raw",
        "journal", "publication_date", "mesh_terms_raw", "publication_types", "source",
    ]

    unified = pubmed.select(*common_cols).unionByName(arxiv.select(*common_cols))

    return (
        unified
        .withColumn(
            "article_id",
            F.coalesce(F.col("doi"), F.concat(F.col("source"), F.lit(":"), F.coalesce(F.col("pmid"), F.col("arxiv_id")))),
        )
        .withColumn("publication_year", F.year(F.to_date(F.col("publication_date"))))
        .withColumn("is_preprint", F.when(F.col("source") == "arxiv", "true").otherwise("false"))
        .withColumn(
            "publication_type",
            F.when(F.col("publication_types").contains("Meta-Analysis"), "Meta-Analysis")
            .when(F.col("publication_types").contains("Randomized Controlled Trial"), "RCT")
            .when(F.col("publication_types").contains("Review"), "Review")
            .when(F.col("publication_types").contains("Clinical Trial"), "Clinical Trial")
            .otherwise("Journal Article"),
        )
        .dropDuplicates(["article_id"])
        .select(
            "article_id", "doi", "pmid", "arxiv_id", "title", "abstract",
            "journal", "publication_date", "publication_year", "source",
            "is_preprint", "publication_type", "authors_raw", "mesh_terms_raw",
        )
    )


@dp.table(
    name="cleaned_authors",
    comment="Normalized author records linked to articles",
    table_properties={
        "quality": "silver",
        "meridian.business_unit": "research",
    },
)
def cleaned_authors():
    articles = dp.read("cleaned_articles")
    return (
        articles
        .filter(F.col("authors_raw").isNotNull())
        .withColumn("authors_array", F.from_json(F.col("authors_raw"), ArrayType(StringType())))
        .withColumn("author_exploded", F.explode_outer(F.col("authors_array")))
        .withColumn("author_position", F.monotonically_increasing_id())
        .withColumn("last_name", F.split(F.col("author_exploded"), ", ").getItem(0))
        .withColumn("first_name", F.split(F.col("author_exploded"), ", ").getItem(1))
        .withColumn("full_name", F.col("author_exploded"))
        .withColumn(
            "author_id",
            F.md5(F.concat_ws("|", F.lower(F.col("full_name")), F.col("article_id"))),
        )
        .select(
            "author_id", "article_id", "full_name", "last_name", "first_name",
            F.lit(None).cast("string").alias("affiliation"),
            "author_position",
        )
    )


@dp.table(
    name="cleaned_citations",
    comment="[Phase 2] Citation links from Crossref — currently empty",
    table_properties={
        "quality": "silver",
        "meridian.business_unit": "research",
    },
)
def cleaned_citations():
    return spark.createDataFrame(  # noqa: F821
        [],
        "citing_doi STRING, cited_doi STRING, source STRING",
    )


@dp.table(
    name="quarantine_research",
    comment="Quarantined research records that failed quality expectations",
    table_properties={
        "quality": "quarantine",
        "meridian.business_unit": "research",
    },
)
def quarantine_research():
    return spark.createDataFrame(  # noqa: F821
        [],
        "article_id STRING, title STRING, publication_date STRING, source STRING, _quarantine_reason STRING",
    )
