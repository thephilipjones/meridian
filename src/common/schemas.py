"""PySpark StructType definitions for every Meridian table.

Shared between data generators, SDP pipelines, and tests to ensure
schema consistency across the entire project. Phase 2 schemas are
included so generators and pipelines can reference them as stubs.
"""

from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ===================================================================
# RESEARCH — Bronze
# ===================================================================

RAW_PUBMED_ARTICLES = StructType([
    StructField("pmid", StringType(), False),
    StructField("doi", StringType(), True),
    StructField("title", StringType(), True),
    StructField("abstract", StringType(), True),
    StructField("authors_raw", StringType(), True),
    StructField("journal", StringType(), True),
    StructField("publication_date", StringType(), True),
    StructField("mesh_terms_raw", StringType(), True),
    StructField("publication_types", StringType(), True),
    StructField("source", StringType(), False),
    StructField("_ingest_timestamp", TimestampType(), False),
    StructField("_source_file", StringType(), True),
])

RAW_ARXIV_ARTICLES = StructType([
    StructField("arxiv_id", StringType(), False),
    StructField("doi", StringType(), True),
    StructField("title", StringType(), True),
    StructField("abstract", StringType(), True),
    StructField("authors_raw", StringType(), True),
    StructField("categories", StringType(), True),
    StructField("published_date", StringType(), True),
    StructField("updated_date", StringType(), True),
    StructField("source", StringType(), False),
    StructField("_ingest_timestamp", TimestampType(), False),
    StructField("_source_file", StringType(), True),
])

RAW_CROSSREF_METADATA = StructType([
    StructField("doi", StringType(), False),
    StructField("title", StringType(), True),
    StructField("authors_raw", StringType(), True),
    StructField("publisher", StringType(), True),
    StructField("type", StringType(), True),
    StructField("issued_date", StringType(), True),
    StructField("references_json", StringType(), True),
    StructField("source", StringType(), False),
    StructField("_ingest_timestamp", TimestampType(), False),
    StructField("_source_file", StringType(), True),
])

# ===================================================================
# RESEARCH — Silver
# ===================================================================

CLEANED_ARTICLES = StructType([
    StructField("article_id", StringType(), False),
    StructField("doi", StringType(), True),
    StructField("pmid", StringType(), True),
    StructField("arxiv_id", StringType(), True),
    StructField("title", StringType(), False),
    StructField("abstract", StringType(), True),
    StructField("journal", StringType(), True),
    StructField("publication_date", StringType(), True),
    StructField("publication_year", IntegerType(), True),
    StructField("source", StringType(), False),
    StructField("is_preprint", StringType(), False),
    StructField("publication_type", StringType(), True),
])

CLEANED_AUTHORS = StructType([
    StructField("author_id", StringType(), False),
    StructField("article_id", StringType(), False),
    StructField("full_name", StringType(), False),
    StructField("last_name", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("affiliation", StringType(), True),
    StructField("author_position", IntegerType(), True),
])

CLEANED_CITATIONS = StructType([
    StructField("citing_doi", StringType(), False),
    StructField("cited_doi", StringType(), False),
    StructField("source", StringType(), True),
])

# ===================================================================
# RESEARCH — Gold
# ===================================================================

ARTICLES_GOLD = StructType([
    StructField("article_id", StringType(), False),
    StructField("doi", StringType(), True),
    StructField("pmid", StringType(), True),
    StructField("arxiv_id", StringType(), True),
    StructField("title", StringType(), False),
    StructField("abstract", StringType(), True),
    StructField("journal", StringType(), True),
    StructField("publication_date", StringType(), True),
    StructField("publication_year", IntegerType(), True),
    StructField("source", StringType(), False),
    StructField("is_preprint", StringType(), False),
    StructField("publication_type", StringType(), True),
    StructField("citation_count", IntegerType(), True),
    StructField("search_text", StringType(), True),
])

AUTHORS_GOLD = StructType([
    StructField("author_id", StringType(), False),
    StructField("full_name", StringType(), False),
    StructField("last_name", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("article_count", IntegerType(), True),
    StructField("h_index", IntegerType(), True),
    StructField("first_pub_year", IntegerType(), True),
    StructField("last_pub_year", IntegerType(), True),
])

CITATIONS_GOLD = StructType([
    StructField("citing_doi", StringType(), False),
    StructField("cited_doi", StringType(), False),
    StructField("citing_title", StringType(), True),
    StructField("cited_title", StringType(), True),
    StructField("citing_year", IntegerType(), True),
    StructField("cited_year", IntegerType(), True),
])

MESH_TERMS_GOLD = StructType([
    StructField("mesh_term", StringType(), False),
    StructField("article_count", IntegerType(), True),
    StructField("latest_year", IntegerType(), True),
])

# ===================================================================
# INTERNAL — Bronze
# ===================================================================

RAW_CRM_DEALS = StructType([
    StructField("deal_id", StringType(), False),
    StructField("account_name", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("deal_name", StringType(), True),
    StructField("stage", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("arr", DoubleType(), True),
    StructField("close_date", StringType(), True),
    StructField("created_date", StringType(), True),
    StructField("owner", StringType(), True),
    StructField("product_line", StringType(), True),
    StructField("region", StringType(), True),
    StructField("_ingest_timestamp", TimestampType(), True),
    StructField("_source_file", StringType(), True),
])

RAW_WEB_EVENTS = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_type", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("account_name", StringType(), True),
    StructField("product", StringType(), True),
    StructField("endpoint", StringType(), True),
    StructField("response_ms", IntegerType(), True),
    StructField("status_code", IntegerType(), True),
    StructField("bytes_returned", LongType(), True),
    StructField("_ingest_timestamp", TimestampType(), True),
    StructField("_source_file", StringType(), True),
])

RAW_FINANCIALS = StructType([
    StructField("fiscal_quarter", StringType(), False),
    StructField("fiscal_year", IntegerType(), False),
    StructField("product_line", StringType(), True),
    StructField("revenue", DoubleType(), True),
    StructField("cost_of_data", DoubleType(), True),
    StructField("gross_margin", DoubleType(), True),
    StructField("customer_count", IntegerType(), True),
    StructField("_ingest_timestamp", TimestampType(), True),
    StructField("_source_file", StringType(), True),
])

# ===================================================================
# INTERNAL — Silver
# ===================================================================

CLEANED_DEALS = StructType([
    StructField("deal_id", StringType(), False),
    StructField("account_name", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("deal_name", StringType(), True),
    StructField("stage", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("arr", DoubleType(), True),
    StructField("close_date", StringType(), True),
    StructField("created_date", StringType(), True),
    StructField("owner", StringType(), True),
    StructField("product_line", StringType(), True),
    StructField("region", StringType(), True),
])

CLEANED_WEB_EVENTS = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_type", StringType(), True),
    StructField("event_timestamp", TimestampType(), True),
    StructField("customer_id", StringType(), True),
    StructField("account_name", StringType(), True),
    StructField("product", StringType(), True),
    StructField("endpoint", StringType(), True),
    StructField("response_ms", IntegerType(), True),
    StructField("status_code", IntegerType(), True),
    StructField("bytes_returned", LongType(), True),
    StructField("session_id", StringType(), True),
])

CLEANED_FINANCIALS = StructType([
    StructField("fiscal_quarter", StringType(), False),
    StructField("fiscal_year", IntegerType(), False),
    StructField("product_line", StringType(), True),
    StructField("revenue", DoubleType(), True),
    StructField("cost_of_data", DoubleType(), True),
    StructField("gross_margin", DoubleType(), True),
    StructField("customer_count", IntegerType(), True),
])

# ===================================================================
# INTERNAL — Gold
# ===================================================================

SALES_PIPELINE = StructType([
    StructField("stage", StringType(), False),
    StructField("deal_count", IntegerType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("total_arr", DoubleType(), True),
    StructField("avg_deal_size", DoubleType(), True),
    StructField("conversion_rate", DoubleType(), True),
    StructField("product_line", StringType(), True),
    StructField("region", StringType(), True),
    StructField("fiscal_quarter", StringType(), True),
])

PRODUCT_USAGE = StructType([
    StructField("account_name", StringType(), False),
    StructField("product", StringType(), False),
    StructField("period", StringType(), True),
    StructField("api_calls", LongType(), True),
    StructField("unique_users", IntegerType(), True),
    StructField("avg_response_ms", DoubleType(), True),
    StructField("error_rate", DoubleType(), True),
    StructField("bytes_served", LongType(), True),
])

REVENUE_SUMMARY = StructType([
    StructField("fiscal_quarter", StringType(), False),
    StructField("fiscal_year", IntegerType(), False),
    StructField("product_line", StringType(), True),
    StructField("revenue", DoubleType(), True),
    StructField("cost_of_data", DoubleType(), True),
    StructField("gross_margin", DoubleType(), True),
    StructField("gross_margin_pct", DoubleType(), True),
    StructField("customer_count", IntegerType(), True),
    StructField("revenue_per_customer", DoubleType(), True),
    StructField("yoy_revenue_growth", DoubleType(), True),
])

CUSTOMER_HEALTH = StructType([
    StructField("account_name", StringType(), False),
    StructField("account_id", StringType(), True),
    StructField("arr", DoubleType(), True),
    StructField("products_subscribed", IntegerType(), True),
    StructField("api_calls_30d", LongType(), True),
    StructField("avg_response_ms_30d", DoubleType(), True),
    StructField("error_rate_30d", DoubleType(), True),
    StructField("health_score", DoubleType(), True),
    StructField("health_tier", StringType(), True),
])

# ===================================================================
# REGULATORY — Bronze (Phase 2 stubs)
# ===================================================================

RAW_SEC_FILINGS = StructType([
    StructField("accession_number", StringType(), False),
    StructField("cik", StringType(), True),
    StructField("company_name", StringType(), True),
    StructField("form_type", StringType(), True),
    StructField("filing_date", StringType(), True),
    StructField("document_url", StringType(), True),
    StructField("content", StringType(), True),
    StructField("source", StringType(), False),
    StructField("_ingest_timestamp", TimestampType(), False),
    StructField("_source_file", StringType(), True),
])

RAW_FDA_ACTIONS = StructType([
    StructField("event_id", StringType(), False),
    StructField("product_description", StringType(), True),
    StructField("reason_for_recall", StringType(), True),
    StructField("classification", StringType(), True),
    StructField("status", StringType(), True),
    StructField("report_date", StringType(), True),
    StructField("recalling_firm", StringType(), True),
    StructField("source", StringType(), False),
    StructField("_ingest_timestamp", TimestampType(), False),
    StructField("_source_file", StringType(), True),
])

RAW_PATENTS = StructType([
    StructField("patent_number", StringType(), False),
    StructField("title", StringType(), True),
    StructField("abstract", StringType(), True),
    StructField("assignee", StringType(), True),
    StructField("filing_date", StringType(), True),
    StructField("grant_date", StringType(), True),
    StructField("uspc_class", StringType(), True),
    StructField("source", StringType(), False),
    StructField("_ingest_timestamp", TimestampType(), False),
    StructField("_source_file", StringType(), True),
])
