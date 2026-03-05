"""Research API endpoints for the Dr. Anika Park view.

Provides article search, author lookup, and citation exploration
scoped to the research business unit.
"""

import os

from fastapi import APIRouter, Query

from src.app.backend.db import execute_query
from src.common.config import CATALOG

router = APIRouter()

_catalog = os.environ.get("MERIDIAN_CATALOG", CATALOG)


@router.get("/articles")
def get_articles(
    search: str | None = Query(None, description="Full-text search across titles and abstracts"),
    publication_type: str | None = Query(None),
    year: int | None = Query(None),
    source: str | None = Query(None),
    limit: int = Query(50, le=500),
):
    """Search and filter research articles."""
    clauses = []
    params: dict = {}

    if search:
        clauses.append("lower(search_text) LIKE %(search_pattern)s")
        params["search_pattern"] = f"%{search.lower()}%"
    if publication_type:
        clauses.append("publication_type = %(publication_type)s")
        params["publication_type"] = publication_type
    if year:
        clauses.append("publication_year = %(year)s")
        params["year"] = year
    if source:
        clauses.append("source = %(source)s")
        params["source"] = source

    where = (" AND ".join(clauses)) if clauses else "1=1"
    query = (
        f"SELECT article_id, doi, title, journal, publication_date, publication_year, "
        f"source, is_preprint, publication_type, citation_count "
        f"FROM {_catalog}.meridian_research.articles WHERE {where} "
        f"ORDER BY citation_count DESC, publication_year DESC LIMIT {int(limit)}"
    )
    return execute_query(query, params or None)


@router.get("/articles/{article_id}")
def get_article_detail(article_id: str):
    """Get full article details including abstract."""
    query = f"SELECT * FROM {_catalog}.meridian_research.articles WHERE article_id = %(article_id)s"
    results = execute_query(query, {"article_id": article_id})
    return results[0] if results else {"error": "Article not found"}


@router.get("/authors")
def get_authors(
    search: str | None = Query(None),
    min_h_index: int | None = Query(None),
    limit: int = Query(50, le=500),
):
    """Search authors by name or filter by h-index."""
    clauses = []
    params: dict = {}

    if search:
        clauses.append("lower(full_name) LIKE %(search_pattern)s")
        params["search_pattern"] = f"%{search.lower()}%"
    if min_h_index:
        clauses.append("h_index >= %(min_h_index)s")
        params["min_h_index"] = min_h_index

    where = (" AND ".join(clauses)) if clauses else "1=1"
    query = f"SELECT * FROM {_catalog}.meridian_research.authors WHERE {where} ORDER BY h_index DESC, article_count DESC LIMIT {int(limit)}"
    return execute_query(query, params or None)


@router.get("/search")
def search_articles(
    q: str = Query(..., description="Natural language search query"),
    limit: int = Query(20, le=100),
):
    """Search articles using the optimized article_search table."""
    query = (
        f"SELECT article_id, doi, title, journal, publication_date, publication_year, "
        f"source, is_preprint, publication_type, citation_count "
        f"FROM {_catalog}.meridian_research.article_search "
        f"WHERE lower(search_text) LIKE %(search_pattern)s "
        f"ORDER BY citation_count DESC LIMIT {int(limit)}"
    )
    return execute_query(query, {"search_pattern": f"%{q.lower()}%"})


@router.get("/mesh-terms")
def get_mesh_terms(limit: int = Query(50, le=500)):
    """Top MeSH terms by article count."""
    query = f"SELECT * FROM {_catalog}.meridian_research.mesh_terms ORDER BY article_count DESC LIMIT {int(limit)}"
    return execute_query(query)
