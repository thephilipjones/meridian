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

## Step 9: Set Up Genie Spaces

Genie spaces may need manual creation. See `resources/genie_spaces.yml` for the configuration:

1. Go to **SQL > Genie Spaces** in the Databricks workspace
2. Create "Meridian Research Assistant" with:
   - Tables: `meridian.research.articles`, `meridian.research.authors`, `meridian.research.citations`, `meridian.research.mesh_terms`
   - Custom instructions: copy from `resources/genie_spaces.yml`
3. Note the Genie space ID
4. Update `src/app/backend/profiles.py` with the `genie_space_id` for each profile

## Step 10: Deploy the App

```bash
databricks bundle run deploy_app -t dev
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
