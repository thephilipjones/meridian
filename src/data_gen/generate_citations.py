# Databricks notebook source
# MAGIC %pip install faker -q

# COMMAND ----------

"""Generate synthetic citation relationships for Meridian Research.

Reads DOIs from the existing PubMed staging files (or articles table)
and generates realistic citation edges between them. On Databricks,
DOIs are pulled from the articles gold table. For local testing, the
PubMed generator is called to produce a DOI pool.
"""

import json
import os
import random

dbutils.widgets.text("catalog_name", "serverless_stable_k2zkdm_catalog")  # noqa: F821
_catalog = dbutils.widgets.get("catalog_name")  # noqa: F821

STAGING_PATH = f"/Volumes/{_catalog}/meridian_staging/crossref"
PUBMED_STAGING = f"/Volumes/{_catalog}/meridian_staging/pubmed"

random.seed(45)

RECORDS_PER_FILE = 500

# COMMAND ----------


def _read_dois_from_staging(pubmed_path: str) -> list[str]:
    """Read DOIs from existing PubMed staging JSON-lines files."""
    dois = []
    if not os.path.isdir(pubmed_path):
        return dois
    for fname in sorted(os.listdir(pubmed_path)):
        if not fname.endswith(".json"):
            continue
        with open(os.path.join(pubmed_path, fname)) as f:
            for line in f:
                record = json.loads(line)
                if record.get("doi"):
                    dois.append(record["doi"])
    return dois


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

# COMMAND ----------

doi_pool = _read_dois_from_staging(PUBMED_STAGING)
print(f"Read {len(doi_pool)} DOIs from PubMed staging files")

if len(doi_pool) == 0:
    print("No staging files found — falling back to generating DOIs from PubMed generator")
    from src.data_gen.generate_pubmed import generate_articles  # noqa: E402
    articles = generate_articles(5000)
    doi_pool = [a["doi"] for a in articles if a.get("doi")]

# COMMAND ----------

citations = generate_citations(doi_pool)
filepaths = write_json_files(citations, STAGING_PATH)
print(f"Generated {len(citations)} citations from {len(doi_pool)} DOIs in {len(filepaths)} files -> {STAGING_PATH}")
