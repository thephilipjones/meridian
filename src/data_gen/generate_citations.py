# Databricks notebook source
# MAGIC %pip install faker -q

# COMMAND ----------

"""Generate synthetic citation relationships for Meridian Research.

Creates citation links between DOIs extracted from the actual PubMed
article pool, producing a realistic co-citation graph where titles
and years resolve correctly in the gold citations table.
"""

import json
import os
import random

dbutils.widgets.text("catalog_name", "serverless_stable_k2zkdm_catalog")  # noqa: F821
_catalog = dbutils.widgets.get("catalog_name")  # noqa: F821

STAGING_PATH = f"/Volumes/{_catalog}/meridian_staging/crossref"

random.seed(45)

RECORDS_PER_FILE = 500


def extract_doi_pool_from_articles(articles: list[dict]) -> list[str]:
    """Extract DOIs from a list of generated articles."""
    return [a["doi"] for a in articles if a.get("doi")]


def generate_citations(doi_pool: list[str]) -> list[dict]:
    """Generate citation edges — each DOI cites 3-15 other DOIs from the pool."""
    random.seed(45)
    citations = []
    pool_size = len(doi_pool)

    for citing_doi in doi_pool:
        if random.random() < 0.4:
            continue

        num_refs = random.randint(3, min(15, pool_size - 1))
        cited_dois = random.sample([d for d in doi_pool if d != citing_doi], num_refs)

        for cited_doi in cited_dois:
            citations.append({
                "citing_doi": citing_doi,
                "cited_doi": cited_doi,
                "source": "crossref",
            })

    return citations


def write_json_files(citations: list[dict], output_path: str) -> list[str]:
    os.makedirs(output_path, exist_ok=True)
    filepaths = []

    for i in range(0, len(citations), RECORDS_PER_FILE):
        chunk = citations[i : i + RECORDS_PER_FILE]
        filename = f"citations_{i // RECORDS_PER_FILE:04d}.json"
        filepath = os.path.join(output_path, filename)
        with open(filepath, "w") as f:
            for record in chunk:
                f.write(json.dumps(record) + "\n")
        filepaths.append(filepath)

    return filepaths


def main(output_path: str | None = None, articles: list[dict] | None = None):
    path = output_path or STAGING_PATH
    if articles is None:
        from src.data_gen.generate_pubmed import generate_articles
        articles = generate_articles(5000)
    doi_pool = extract_doi_pool_from_articles(articles)
    citations = generate_citations(doi_pool)
    filepaths = write_json_files(citations, path)
    print(f"Generated {len(citations)} citations from {len(doi_pool)} DOIs in {len(filepaths)} files -> {path}")
    return citations


if __name__ == "__main__":
    main()
