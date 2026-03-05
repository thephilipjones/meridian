# Databricks notebook source
"""Fetch PubMed article metadata via NCBI E-utilities API.

Retrieves article abstracts, authors, MeSH terms, and DOIs for a scoped
subset (last 2 years, biomedical). Writes JSON files to the PubMed staging
volume for Auto Loader ingestion.

Rate limit: 3 requests/sec without API key, 10 with (set NCBI_API_KEY env var).
"""

# COMMAND ----------

import json
import os
import time
from datetime import datetime, timedelta
from xml.etree import ElementTree as ET

import requests

dbutils.widgets.text("catalog_name", "serverless_stable_k2zkdm_catalog")
_catalog = dbutils.widgets.get("catalog_name")

STAGING_PATH = f"/Volumes/{_catalog}/meridian_staging/pubmed"

EUTILS_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
SEARCH_URL = f"{EUTILS_BASE}/esearch.fcgi"
FETCH_URL = f"{EUTILS_BASE}/efetch.fcgi"

DEFAULT_QUERY = "biomedical[MeSH Terms]"
BATCH_SIZE = 200
MAX_RESULTS = 5000
RECORDS_PER_FILE = 500

# COMMAND ----------


def _rate_delay() -> float:
    api_key = os.environ.get("NCBI_API_KEY")
    return 0.11 if api_key else 0.34


def _api_params() -> dict:
    params = {}
    api_key = os.environ.get("NCBI_API_KEY")
    if api_key:
        params["api_key"] = api_key
    return params


def search_pmids(query: str, max_results: int = MAX_RESULTS) -> list[str]:
    """Search PubMed and return a list of PMIDs."""
    min_date = (datetime.now() - timedelta(days=730)).strftime("%Y/%m/%d")
    max_date = datetime.now().strftime("%Y/%m/%d")

    pmids = []
    for retstart in range(0, max_results, BATCH_SIZE):
        params = {
            "db": "pubmed",
            "term": query,
            "retmax": min(BATCH_SIZE, max_results - retstart),
            "retstart": retstart,
            "retmode": "json",
            "datetype": "pdat",
            "mindate": min_date,
            "maxdate": max_date,
            "sort": "relevance",
            **_api_params(),
        }
        resp = requests.get(SEARCH_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        batch_ids = data.get("esearchresult", {}).get("idlist", [])
        if not batch_ids:
            break
        pmids.extend(batch_ids)
        time.sleep(_rate_delay())

    return pmids[:max_results]


def fetch_articles(pmids: list[str]) -> list[dict]:
    """Fetch full article metadata for a list of PMIDs."""
    articles = []
    for i in range(0, len(pmids), BATCH_SIZE):
        batch = pmids[i : i + BATCH_SIZE]
        params = {
            "db": "pubmed",
            "id": ",".join(batch),
            "retmode": "xml",
            **_api_params(),
        }
        resp = requests.get(FETCH_URL, params=params, timeout=60)
        resp.raise_for_status()
        articles.extend(_parse_pubmed_xml(resp.text))
        time.sleep(_rate_delay())

    return articles


def _parse_pubmed_xml(xml_text: str) -> list[dict]:
    """Parse PubMed XML response into flat article dicts."""
    root = ET.fromstring(xml_text)
    articles = []

    for article_elem in root.findall(".//PubmedArticle"):
        medline = article_elem.find("MedlineCitation")
        if medline is None:
            continue

        pmid_elem = medline.find("PMID")
        article_node = medline.find("Article")
        if pmid_elem is None or article_node is None:
            continue

        title_elem = article_node.find("ArticleTitle")
        abstract_elem = article_node.find("Abstract/AbstractText")
        journal_elem = article_node.find("Journal/Title")

        authors = []
        author_list = article_node.find("AuthorList")
        if author_list is not None:
            for author in author_list.findall("Author"):
                last = author.findtext("LastName", "")
                first = author.findtext("ForeName", "")
                if last:
                    authors.append(f"{last}, {first}".strip(", "))

        mesh_list = medline.find("MeshHeadingList")
        mesh_terms = []
        if mesh_list is not None:
            for heading in mesh_list.findall("MeshHeading/DescriptorName"):
                if heading.text:
                    mesh_terms.append(heading.text)

        pub_types = []
        for pt in article_node.findall("PublicationTypeList/PublicationType"):
            if pt.text:
                pub_types.append(pt.text)

        doi = None
        for eid in article_elem.findall(".//ArticleId"):
            if eid.get("IdType") == "doi":
                doi = eid.text

        pub_date = article_node.find("Journal/JournalIssue/PubDate")
        date_str = ""
        if pub_date is not None:
            year = pub_date.findtext("Year", "")
            month = pub_date.findtext("Month", "01")
            day = pub_date.findtext("Day", "01")
            date_str = f"{year}-{month}-{day}"

        articles.append({
            "pmid": pmid_elem.text,
            "doi": doi,
            "title": title_elem.text if title_elem is not None else None,
            "abstract": abstract_elem.text if abstract_elem is not None else None,
            "authors_raw": json.dumps(authors),
            "journal": journal_elem.text if journal_elem is not None else None,
            "publication_date": date_str,
            "mesh_terms_raw": json.dumps(mesh_terms),
            "publication_types": json.dumps(pub_types),
            "source": "pubmed",
        })

    return articles


def write_json_files(articles: list[dict], output_path: str) -> list[str]:
    """Write articles as chunked JSON-lines files."""
    os.makedirs(output_path, exist_ok=True)
    filepaths = []

    for i in range(0, len(articles), RECORDS_PER_FILE):
        chunk = articles[i : i + RECORDS_PER_FILE]
        filename = f"pubmed_{i // RECORDS_PER_FILE:04d}.json"
        filepath = os.path.join(output_path, filename)
        with open(filepath, "w") as f:
            for article in chunk:
                f.write(json.dumps(article) + "\n")
        filepaths.append(filepath)

    return filepaths

# COMMAND ----------

query = DEFAULT_QUERY
path = STAGING_PATH
print(f"Searching PubMed: '{query}' (max {MAX_RESULTS} results)...")
pmids = search_pmids(query, MAX_RESULTS)
print(f"Found {len(pmids)} PMIDs, fetching metadata...")
articles = fetch_articles(pmids)
filepaths = write_json_files(articles, path)
print(f"Wrote {len(articles)} articles across {len(filepaths)} files -> {path}")
