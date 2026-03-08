"""Research API endpoints for the Dr. Anika Park view.

Provides article search, author lookup, citation exploration,
semantic search via Vector Search, and RAG-powered Q&A using
Foundation Model API — all scoped to the research business unit.
"""

import logging
import os
from functools import lru_cache

from backend.cache import ttl_cache
from backend.db import execute_query
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
from fastapi import APIRouter, Query
from pydantic import BaseModel

router = APIRouter()

log = logging.getLogger(__name__)
_catalog = os.environ.get("MERIDIAN_CATALOG", "serverless_stable_k2zkdm_catalog")
_vs_index = f"{_catalog}.meridian_research.articles_vs_index"
_llm_endpoint = os.environ.get("MERIDIAN_LLM_ENDPOINT", "databricks-meta-llama-3-3-70b-instruct")

_SCORE_COLUMN = "score"
_MIN_SIMILARITY = 0.45


@lru_cache(maxsize=1)
def _get_ws_client() -> WorkspaceClient:
    host = os.environ.get("DATABRICKS_HOST")
    client_id = os.environ.get("DATABRICKS_CLIENT_ID")
    client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET")
    if not all([host, client_id, client_secret]):
        missing = [k for k in ("DATABRICKS_HOST", "DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET")
                   if not os.environ.get(k)]
        raise RuntimeError(f"Missing required env vars: {missing}")
    return WorkspaceClient(host=f"https://{host}", client_id=client_id, client_secret=client_secret)


@router.get("/articles")
@ttl_cache(seconds=60)
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
@ttl_cache(seconds=60)
def get_article_detail(article_id: str):
    """Get full article details including abstract."""
    query = f"SELECT * FROM {_catalog}.meridian_research.articles WHERE article_id = %(article_id)s"
    results = execute_query(query, {"article_id": article_id})
    return results[0] if results else {"error": "Article not found"}


@router.get("/authors")
@ttl_cache(seconds=60)
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
@ttl_cache(seconds=60)
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


@router.get("/semantic-search")
def semantic_search(
    q: str = Query(..., description="Natural language research question"),
    limit: int = Query(10, le=50),
):
    """Semantic search over article abstracts using Vector Search.

    Falls back to LIKE-based keyword search if the vector index is
    unavailable (e.g. not yet provisioned or endpoint is down).
    """
    try:
        w = _get_ws_client()
        results = w.vector_search_indexes.query_index(
            index_name=_vs_index,
            columns=[
                "article_id", "doi", "title", "abstract", "journal",
                "publication_date", "publication_year", "source",
                "is_preprint", "publication_type", "citation_count",
            ],
            query_text=q,
            num_results=limit * 2,
        )
        articles = _parse_vs_results(results)
        filtered = [a for a in articles if (a.get("similarity_score") or 0) >= _MIN_SIMILARITY]
        return {"mode": "semantic", "results": filtered[:limit]}
    except Exception as e:
        log.warning("Vector Search unavailable, falling back to keyword search: %s", e)
        keyword_results = search_articles(q=q, limit=limit)
        return {"mode": "keyword", "results": keyword_results}


@router.get("/citations")
@ttl_cache(seconds=60)
def get_citations(
    doi: str | None = Query(None, description="Search citing or cited DOI"),
    title: str | None = Query(None, description="Search in citing or cited title"),
    limit: int = Query(50, le=500),
):
    """Citation relationships between articles."""
    clauses = []
    params: dict = {}

    if doi:
        clauses.append("(citing_doi = %(doi)s OR cited_doi = %(doi)s)")
        params["doi"] = doi
    if title:
        clauses.append(
            "(lower(citing_title) LIKE %(title_pat)s OR lower(cited_title) LIKE %(title_pat)s)"
        )
        params["title_pat"] = f"%{title.lower()}%"

    where = (" AND ".join(clauses)) if clauses else "1=1"
    query = (
        f"SELECT citing_doi, cited_doi, citing_title, cited_title, citing_year, cited_year "
        f"FROM {_catalog}.meridian_research.citations WHERE {where} "
        f"ORDER BY citing_year DESC LIMIT {int(limit)}"
    )
    return execute_query(query, params or None)


@router.get("/overview")
@ttl_cache(seconds=120)
def get_research_overview():
    """Bibliometric summary statistics for the research corpus.

    Aggregates total articles, citations, authors, preprint ratio,
    publication trend by year, top journals, and top authors.
    """
    totals = execute_query(f"""
        SELECT
            COUNT(*) AS total_articles,
            COALESCE(SUM(citation_count), 0) AS total_citations,
            SUM(CASE WHEN LOWER(is_preprint) = 'true' THEN 1 ELSE 0 END) AS preprint_count
        FROM {_catalog}.meridian_research.articles
    """)
    stats = totals[0] if totals else {"total_articles": 0, "total_citations": 0, "preprint_count": 0}

    author_count = execute_query(f"SELECT COUNT(*) AS cnt FROM {_catalog}.meridian_research.authors")
    stats["total_authors"] = author_count[0]["cnt"] if author_count else 0

    total = stats["total_articles"] or 1
    stats["preprint_ratio"] = round(stats["preprint_count"] / total, 3)
    stats["peer_reviewed_pct"] = round((1 - stats["preprint_count"] / total) * 100, 1)

    pub_trend = execute_query(f"""
        SELECT publication_year AS year, COUNT(*) AS count
        FROM {_catalog}.meridian_research.articles
        WHERE publication_year IS NOT NULL
        GROUP BY publication_year
        ORDER BY publication_year
    """)
    stats["publication_trend"] = pub_trend

    top_journals = execute_query(f"""
        SELECT journal, COUNT(*) AS count
        FROM {_catalog}.meridian_research.articles
        WHERE journal IS NOT NULL
        GROUP BY journal
        ORDER BY count DESC
        LIMIT 5
    """)
    stats["top_journals"] = top_journals

    top_authors = execute_query(f"""
        SELECT full_name AS name, h_index, article_count AS articles
        FROM {_catalog}.meridian_research.authors
        ORDER BY h_index DESC, article_count DESC
        LIMIT 5
    """)
    stats["top_authors"] = top_authors

    return stats


@router.get("/mesh-terms")
@ttl_cache(seconds=60)
def get_mesh_terms(limit: int = Query(50, le=500)):
    """Top MeSH terms by article count."""
    query = f"SELECT * FROM {_catalog}.meridian_research.mesh_terms ORDER BY article_count DESC LIMIT {int(limit)}"
    return execute_query(query)


class AskRequest(BaseModel):
    question: str
    history: list[dict[str, str]] | None = None


def _parse_vs_results(results) -> list[dict]:
    """Parse Vector Search query results using manifest column names."""
    columns = [c.name for c in results.manifest.columns]
    articles = []
    for row in results.result.data_array:
        article = dict(zip(columns, row))
        if _SCORE_COLUMN in article:
            article["similarity_score"] = article.pop(_SCORE_COLUMN)
        articles.append(article)
    return articles


def _retrieve_articles(question: str, limit: int = 8) -> list[dict]:
    """Retrieve relevant articles from Vector Search for RAG context.

    Over-fetches 2x then filters by similarity score to discard results
    that are semantically distant from the query — avoids confusing the
    LLM with unrelated articles when the topic isn't in the corpus.
    """
    w = _get_ws_client()
    results = w.vector_search_indexes.query_index(
        index_name=_vs_index,
        columns=[
            "article_id", "doi", "title", "abstract", "journal",
            "publication_date", "publication_year", "source",
            "is_preprint", "publication_type", "citation_count",
        ],
        query_text=question,
        num_results=limit * 2,
    )
    articles = _parse_vs_results(results)
    filtered = [a for a in articles if (a.get("similarity_score") or 0) >= _MIN_SIMILARITY]
    return filtered[:limit]


_SYSTEM_PROMPT = """You are the Meridian Research Assistant, an AI that answers biomedical and scientific research questions using a curated database of peer-reviewed articles and preprints.

RULES:
- Base your answer ONLY on the provided articles. Do not fabricate information.
- RELEVANCE CHECK: First assess whether the retrieved articles actually address the user's question. Each article has a Relevance score (0-1). If articles have low relevance scores (<0.55) or their topics clearly don't match the question, state clearly: "The research database does not contain articles directly addressing this topic." Then briefly describe what related topics were found, if any.
- Cite articles using [1], [2], etc. matching the numbered source list.
- For each claim, cite the specific article(s) that support it.
- Distinguish between peer-reviewed publications and preprints (flag preprints explicitly).
- Note study type (RCT, meta-analysis, cohort, case study) and sample size when available.
- Prioritize meta-analyses and systematic reviews over individual studies.
- Include publication year to help assess recency.
- If the articles don't contain enough information, say so honestly.
- Use the phrasing "the available articles suggest..." not "research proves..."
- Keep your response concise but thorough (2-4 paragraphs)."""


def _build_context(articles: list[dict]) -> str:
    """Format retrieved articles as numbered context for the LLM."""
    parts = []
    for i, a in enumerate(articles, 1):
        preprint_flag = " [PREPRINT]" if str(a.get("is_preprint", "")).lower() == "true" else ""
        pub_type = f" | {a['publication_type']}" if a.get("publication_type") else ""
        score = a.get("similarity_score", 0)
        score_str = f" | Relevance: {score:.2f}" if score else ""
        abstract = (a.get("abstract") or "No abstract available.")[:1500]
        parts.append(
            f"[{i}] {a.get('title', 'Untitled')}{preprint_flag}\n"
            f"    Journal: {a.get('journal', 'Unknown')} ({a.get('publication_year', '?')}){pub_type}\n"
            f"    DOI: {a.get('doi', 'N/A')} | Citations: {a.get('citation_count', 0)}{score_str}\n"
            f"    Abstract: {abstract}"
        )
    return "\n\n".join(parts)


@router.post("/ask")
def ask_research_question(request: AskRequest):
    """RAG-powered Research Q&A: retrieves relevant articles via Vector Search,
    then generates a cited answer using Foundation Model API."""
    try:
        articles = _retrieve_articles(request.question)
    except Exception as e:
        log.warning("Vector Search retrieval failed: %s", e)
        return {
            "answer": "I'm unable to search the research database right now. Please try the semantic search tab instead.",
            "sources": [],
            "error": str(e),
        }

    if not articles:
        return {
            "answer": "No relevant articles were found for your question. Try rephrasing or broadening your query.",
            "sources": [],
        }

    context = _build_context(articles)
    _role_map = {"system": ChatMessageRole.SYSTEM, "user": ChatMessageRole.USER, "assistant": ChatMessageRole.ASSISTANT}
    messages = [ChatMessage(role=ChatMessageRole.SYSTEM, content=_SYSTEM_PROMPT)]

    if request.history:
        for msg in request.history[-6:]:
            messages.append(ChatMessage(
                role=_role_map.get(msg.get("role", "user"), ChatMessageRole.USER),
                content=msg.get("content", ""),
            ))

    messages.append(ChatMessage(
        role=ChatMessageRole.USER,
        content=f"Based on the following research articles, answer this question:\n\n"
                f"**Question:** {request.question}\n\n"
                f"**Articles:**\n{context}",
    ))

    try:
        w = _get_ws_client()
        response = w.serving_endpoints.query(
            name=_llm_endpoint,
            messages=messages,
            max_tokens=1024,
            temperature=0.1,
        )
        answer = response.choices[0].message.content
    except Exception as e:
        log.error("Foundation Model API call failed: %s", e)
        return {
            "answer": "I found relevant articles but couldn't generate a summary. The source articles are listed below.",
            "sources": [
                {
                    "index": i + 1,
                    "article_id": a.get("article_id"),
                    "title": a.get("title"),
                    "doi": a.get("doi"),
                    "journal": a.get("journal"),
                    "publication_year": a.get("publication_year"),
                    "is_preprint": a.get("is_preprint"),
                    "publication_type": a.get("publication_type"),
                    "citation_count": a.get("citation_count"),
                    "similarity_score": a.get("similarity_score"),
                }
                for i, a in enumerate(articles)
            ],
            "error": str(e),
        }

    sources = [
        {
            "index": i + 1,
            "article_id": a.get("article_id"),
            "title": a.get("title"),
            "doi": a.get("doi"),
            "journal": a.get("journal"),
            "publication_year": a.get("publication_year"),
            "is_preprint": a.get("is_preprint"),
            "publication_type": a.get("publication_type"),
            "citation_count": a.get("citation_count"),
            "similarity_score": a.get("similarity_score"),
        }
        for i, a in enumerate(articles)
    ]

    return {"answer": answer, "sources": sources}
