# Meridian Insights Demo Platform

A self-contained Databricks demo showing how a **data product company** uses the platform end-to-end: ingesting raw data, curating it through a medallion architecture, distributing governed data products, and providing natural-language analytics.

Packaged as a **Databricks Asset Bundle (DAB)** — deploy to any Unity Catalog-enabled workspace.

## Quick Start

```bash
# 1. Configure Databricks CLI for your target workspace
databricks configure --profile meridian

# 2. Install Python dependencies
uv venv --python 3.11 && source .venv/bin/activate
uv pip install -e ".[dev]"

# 3. Deploy the bundle
databricks bundle deploy -t dev

# 4. Run initial setup (creates catalog, schemas, volumes)
databricks bundle run setup_job -t dev

# 5. Generate synthetic data
databricks bundle run data_gen_job -t dev

# 6. Fetch real data (optional — demo works with synthetic only)
databricks bundle run data_fetch_job -t dev

# 7. Run pipelines
databricks bundle run research_pipeline -t dev
databricks bundle run internal_pipeline -t dev

# 8. Deploy the Meridian Portal app
databricks bundle run deploy_app -t dev
```

## Architecture

Three business units, one platform:

| Business Unit | Data Sources | Demo Persona |
|---|---|---|
| **Meridian Regulatory** | SEC EDGAR, FDA openFDA, USPTO | James Rivera (Acme Bank) |
| **Meridian Research** | PubMed, arXiv, Crossref | Dr. Anika Park (NIH) |
| **Meridian Internal** | Synthetic CRM, web analytics, financials | Sarah Chen (RevOps) |

## Project Structure

```
src/
  common/          # Shared config, schemas — single source of truth
  pipelines/       # SDP pipeline files (bronze/silver/gold per BU)
  data_fetch/      # Scripts to fetch real public data (PubMed, EDGAR, etc.)
  data_gen/        # Synthetic data generators (Faker-based)
  app/             # Databricks App (FastAPI backend + React frontend)
  notebooks/       # Setup, demo walkthrough, governance tour
resources/         # DAB resource definitions (pipelines, jobs, genie)
tests/             # Unit tests for data gen and pipeline expectations
docs/              # PRD, demo script, deployment guide
```

See [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) for detailed deployment instructions and [docs/DEMO_SCRIPT.md](docs/DEMO_SCRIPT.md) for the presenter's guide.
