# Meridian Insights Demo Platform

A self-contained Databricks demo showing how a **data product company** uses the platform end-to-end: ingesting raw data, curating it through a medallion architecture, distributing governed data products, and providing natural-language analytics.

Packaged as a **Databricks Asset Bundle (DAB)** — deploy to any Unity Catalog-enabled workspace.

## Platform Capabilities

| Capability | Implementation |
|---|---|
| **SDP Pipelines** | 3 medallion pipelines (Research, Internal, Regulatory) — 15 notebooks, bronze/silver/gold |
| **Liquid Clustering** | All gold tables use `cluster_by` for zero-maintenance adaptive layout |
| **Metric Views** | Revenue and customer health KPIs as governed UC objects |
| **AI/BI Dashboards** | Internal analytics dashboard built on Metric Views |
| **Genie Spaces** | 3 spaces (Research, Internal, Regulatory) with sample questions and SQL snippets |
| **Vector Search** | Semantic search over article abstracts via `databricks-gte-large-en` embeddings |
| **Foundation Model API** | RAG-powered AI Research Assistant (Vector Search retrieval + LLM generation with cited sources) |
| **Databricks App** | FastAPI + React portal with profile switcher and 4 views |
| **Delta Sharing** | Share regulatory data products with external recipients (no-copy access) |
| **Governance** | Row filters, column masks, table tags, lineage, system tables meta-analytics |
| **SQL Pipe Syntax** | Modern `\|>` syntax throughout demo notebooks |

## Quick Start

```bash
# 1. Authenticate with the Databricks workspace
databricks auth login --profile <your-profile>

# 2. Install Python dependencies
uv venv --python 3.11 && source .venv/bin/activate
uv pip install -e ".[dev]"

# 3. Deploy the bundle
databricks bundle deploy -t dev --profile <your-profile>

# 4. Run initial setup (creates catalog, schemas, volumes)
databricks bundle run setup_job -t dev

# 5. Generate synthetic data (includes citation graph)
databricks bundle run data_gen_job -t dev

# 6. Fetch real data (optional — demo works with synthetic only)
databricks bundle run data_fetch_job -t dev

# 7. Run pipelines and create metric views
databricks bundle run run_pipelines_job -t dev

# 8. Set up system tables and Vector Search
#    Run notebooks 05_system_tables.py and 06_vector_search.py

# 9. Enrich Genie spaces with sample questions
#    Run notebook 07_genie_enrichment.py

# 10. Deploy the Meridian Portal app
databricks bundle run deploy_app -t dev
```

## Architecture

Three business units, one platform:

| Business Unit | Data Sources | Demo Persona | Key Features |
|---|---|---|---|
| **Meridian Regulatory** | SEC EDGAR, FDA openFDA, USPTO | James Rivera (Acme Bank) | Data catalog, scoped Genie, Delta Sharing |
| **Meridian Research** | PubMed, arXiv, Crossref | Dr. Anika Park (NIH) | Semantic search, citation explorer, Genie |
| **Meridian Internal** | Synthetic CRM, web analytics, financials | Sarah Chen (RevOps) | Sales dashboard, platform analytics, Genie |

## Project Structure

```
src/
  pipelines/       # SDP pipeline files (bronze/silver/gold per BU, liquid clustering)
  data_gen/        # Synthetic data generators (Faker-based, including citations)
  data_fetch/      # Scripts to fetch real public data (PubMed, EDGAR, etc.)
  dashboards/      # AI/BI Dashboard definitions (Lakeview JSON)
  notebooks/       # Setup, demo, governance, system tables, vector search, Genie enrichment
  app/             # Databricks App (FastAPI backend + React frontend)
    backend/       #   FastAPI routers: analytics, research, catalog, sharing, genie
    frontend/      #   React components: InternalView, ResearchView, CitationExplorer, etc.
resources/         # DAB resource definitions (pipelines, jobs, dashboards, genie, shares)
tests/             # Unit tests for data gen and pipeline expectations (41 tests)
docs/              # PRD, demo script, deployment guide
```

## Schemas

| Schema | Purpose | Tables |
|---|---|---|
| `meridian_regulatory` | SEC, FDA, USPTO data products | 11 (bronze → gold) |
| `meridian_research` | PubMed, arXiv, Crossref articles | 12 (bronze → gold + search index) |
| `meridian_internal` | CRM, web analytics, financials | 10 (bronze → gold + metric views) |
| `meridian_staging` | Raw file staging volumes | 9 volumes |
| `meridian_system` | System tables meta-analytics | 3 materialized views |

## Testing

```bash
# Run all tests (requires venv with dev dependencies)
.venv/bin/python -m pytest tests/ -v
```

Tests cover all 7 data generators (CRM, web events, financials, SEC filings, FDA actions, patents, citations) and validate pipeline expectations.

## Documentation

- [docs/PRD.md](docs/PRD.md) — Full product requirements
- [docs/DEMO_SCRIPT.md](docs/DEMO_SCRIPT.md) — 20-minute presenter's guide
- [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) — Step-by-step deployment instructions