# Meridian Insights — Deployment Guide

## Prerequisites

- [ ] Databricks workspace with Unity Catalog enabled
- [ ] Databricks CLI installed and configured (`databricks configure`)
- [ ] A SQL warehouse (Serverless preferred) — note the warehouse ID
- [ ] Python 3.11+ with pip
- [ ] Node.js 18+ with npm (for frontend build)

## Step 1: Clone and Configure

```bash
git clone <repo-url> && cd meridian-insights-demo

# Install Python dependencies
uv venv --python 3.11 && source .venv/bin/activate
uv pip install -e ".[dev]"
```

## Step 2: Set Bundle Variables

Edit `databricks.yml` or set environment variables:

```bash
# Required: your SQL warehouse ID
export DATABRICKS_WAREHOUSE_ID="your-warehouse-id-here"

# Optional: custom catalog name (default: meridian)
export MERIDIAN_CATALOG="meridian"
```

Or add to the target in `databricks.yml`:

```yaml
targets:
  dev:
    variables:
      warehouse_id: "your-warehouse-id-here"
      catalog_name: "meridian"
```

## Step 3: Build Frontend

```bash
cd src/app/frontend
npm install
npm run build
cd ../../..
```

This creates `src/app/frontend/dist/` which the FastAPI app serves as static files.

## Step 4: Deploy the Bundle

```bash
databricks bundle deploy -t dev
```

## Step 5: Run Setup Job

Creates the catalog, schemas, and staging volumes:

```bash
databricks bundle run setup_job -t dev
```

**Verify:** Check that `meridian` catalog exists with `regulatory`, `research`, `internal`, and `staging` schemas.

## Step 6: Generate Synthetic Data

```bash
databricks bundle run data_gen_job -t dev
```

This populates staging volumes with:
- CRM deals (CSV)
- Web analytics events (JSON)
- Financial summaries (CSV)

## Step 7: Fetch Real Data (Optional)

```bash
databricks bundle run data_fetch_job -t dev
```

Fetches PubMed article metadata. The demo works with synthetic data only if you skip this step.

## Step 8: Run Pipelines

```bash
databricks bundle run research_pipeline -t dev
databricks bundle run internal_pipeline -t dev
```

Wait for pipelines to complete. Verify gold tables are populated:

```sql
SELECT COUNT(*) FROM meridian.research.articles;
SELECT COUNT(*) FROM meridian.internal.sales_pipeline;
```

## Step 9: Set Up Vector Search

Creates the VS endpoint, source table, and delta-sync index for semantic search:

```bash
# Run 06_vector_search.py via the workspace or as a notebook task
# This creates endpoint 'meridian-research-vs' and index 'articles_vs_index'
# with managed embeddings on article abstracts (takes ~5 min to sync)
```

**Why a separate source table?** SDP gold tables are materialized views that don't support Change Data Feed. The notebook creates `articles_vs_source` (a managed Delta table with CDF enabled) that the VS index syncs from.

## Step 10: Deploy the App

Use the automated deploy script:

```bash
./scripts/deploy.sh                    # standard deploy
./scripts/deploy.sh --setup-genie     # also create/update Genie spaces
```

The script handles all steps: frontend build, bundle deploy, `frontend/dist` upload, app deploy, and SP permission grants. With `--setup-genie`, it also runs `genie_setup_job` which creates all 3 Genie spaces via REST API and applies enrichment (sample questions, SQL snippets).

**Manual deploy (if needed):**

```bash
databricks bundle run deploy_app -t dev
```

## Step 11: Set Up Genie Spaces (if not using --setup-genie)

Run the Genie setup job to create all 3 spaces programmatically:

```bash
databricks bundle run genie_setup_job -t dev
```

This runs `07_create_genie_spaces.py` (creates/updates spaces via REST API, binds resources to the app) followed by `08_genie_enrichment.py` (applies sample questions and SQL snippets).

After running the setup job, copy the output `app.yaml` snippet (printed by `07_create_genie_spaces.py`), paste it into `src/app/app.yaml`, and redeploy the app:

```bash
databricks apps deploy meridian-portal --source-code-path "..." --profile k2zkdm
```

## Verification Checklist

- [ ] `databricks bundle deploy -t dev` completes without errors
- [ ] Catalog `meridian` exists with 4 schemas
- [ ] Staging volumes have data files
- [ ] Pipelines complete without errors
- [ ] Gold tables are populated (run COUNT queries)
- [ ] App is accessible and profile switcher works
- [ ] Genie responds to natural language questions
- [ ] Demo can be walked through in under 20 minutes

## Environment Targets

| Target | Purpose | Command |
|---|---|---|
| `dev` | Your workspace, iterative development | `databricks bundle deploy -t dev` |
| `demo` | Clean demo workspace, pre-loaded | `databricks bundle deploy -t demo` |
| `customer` | Ephemeral per-customer workspace | `databricks bundle deploy -t customer` |

## Troubleshooting

**Pipeline fails on missing volume:** Ensure `setup_job` ran first and created staging volumes.

**No data in gold tables:** Check that `data_gen_job` ran and staging volumes have files. Then re-run pipelines.

**App won't start:** Verify `frontend/dist` exists (run `npm run build`). Check that `frontend/dist` is NOT in `.gitignore`.

**Genie not responding:** Confirm the Genie space ID is set in `profiles.py` and the SQL warehouse is running.
