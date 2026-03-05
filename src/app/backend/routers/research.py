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
    query = f"SELECT article_id, doi, title, journal, publication_date, publication_year, source, is_preprint, publication_type, citation_count FROM {_catalog}.research.articles WHERE 1=1"
    if search:
        query += f" AND lower(search_text) LIKE '%{search.lower()}%'"
    if publication_type:
        query += f" AND publication_type = '{publication_type}'"
    if year:
        query += f" AND publication_year = {year}"
    if source:
        query += f" AND source = '{source}'"
    query += f" ORDER BY citation_count DESC, publication_year DESC LIMIT {limit}"
    return execute_query(query)


@router.get("/articles/{article_id}")
def get_article_detail(article_id: str):
    """Get full article details including abstract."""
    query = f"SELECT * FROM {_catalog}.research.articles WHERE article_id = '{article_id}'"
    results = execute_query(query)
    return results[0] if results else {"error": "Article not found"}


@router.get("/authors")
def get_authors(
    search: str | None = Query(None),
    min_h_index: int | None = Query(None),
    limit: int = Query(50, le=500),
):
    """Search authors by name or filter by h-index."""
    query = f"SELECT * FROM {_catalog}.research.authors WHERE 1=1"
    if search:
        query += f" AND lower(full_name) LIKE '%{search.lower()}%'"
    if min_h_index:
        query += f" AND h_index >= {min_h_index}"
    query += f" ORDER BY h_index DESC, article_count DESC LIMIT {limit}"
    return execute_query(query)


@router.get("/search")
def search_articles(
    q: str = Query(..., description="Natural language search query"),
    limit: int = Query(20, le=100),
):
    """Search articles using the optimized article_search table."""
    query = f"""
    SELECT article_id, doi, title, journal, publication_date, publication_year,
           source, is_preprint, publication_type, citation_count
    FROM {_catalog}.research.article_search
    WHERE lower(search_text) LIKE '%{q.lower()}%'
    ORDER BY citation_count DESC
    LIMIT {limit}
    """
    return execute_query(query)


@router.get("/mesh-terms")
def get_mesh_terms(limit: int = Query(50, le=500)):
    """Top MeSH terms by article count."""
    query = f"SELECT * FROM {_catalog}.research.mesh_terms ORDER BY article_count DESC LIMIT {limit}"
    return execute_query(query)
