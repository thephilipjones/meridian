# Meridian Insights — Project Rules

## Project Context
Databricks Asset Bundle (DAB) demo platform. Deploys to FEVM workspace via CLI profile `k2zkdm`, target `dev`. Catalog: `serverless_stable_k2zkdm_catalog`.

## DAB Bundle Sync
- `databricks bundle deploy` follows `.gitignore` for sync — gitignored files won't reach the workspace
- `.databricksignore` overrides `.gitignore` for sync when present (CLI uses it *instead of* `.gitignore`)
- **`frontend/dist/` is NOT synced by bundle deploy** even though `.databricksignore` allows it — the bundle sync still skips it. After every `databricks bundle deploy`, manually run:
  ```
  databricks workspace import-dir src/app/frontend/dist \
    "/Workspace/Users/p.jones@databricks.com/meridian/files/src/app/frontend/dist" \
    --profile k2zkdm --overwrite
  ```
- Always run `cd src/app/frontend && npm run build` before deploy

## DAB Genie Spaces
- DAB does not support `genie_spaces` as a resource type — shows `unknown field` warnings
- `resources/genie_spaces.yml` is documentation only; spaces must be created/updated via REST API
- Create: `databricks api post /api/2.0/genie/spaces --profile k2zkdm`
- Update: `databricks api patch /api/2.0/genie/spaces/{space_id} --profile k2zkdm`
- The `serialized_space` field is a JSON string with `version: 2`, `data_sources.tables[]` (each with `identifier`), `instructions`, `config.sample_questions`
- Tables in `serialized_space` must be sorted alphabetically by `identifier`; IDs must be 32-char lowercase hex

## App Deployment (three-step)
1. `databricks bundle deploy -t dev` — syncs source to workspace
2. `databricks workspace import-dir ...` — uploads `frontend/dist/` (see above)
3. `databricks apps deploy meridian-portal --source-code-path "/Workspace/Users/p.jones@databricks.com/meridian/files/src/app" --profile k2zkdm` — takes snapshot and starts app
- Bundle deploy alone does NOT trigger app redeploy — step 3 is required
- The app container mounts source at `/app/python/source_code/` — `Path(__file__)` resolves there, not the workspace path

## App Runtime Environment
- The App runtime auto-injects: `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`, `DATABRICKS_APP_PORT`
- It does NOT inject `DATABRICKS_TOKEN` or `DATABRICKS_SERVER_HOSTNAME` — don't rely on these
- `valueFrom` in `app.yaml` does NOT work for `genie_space` resources — runtime returns "Error resolving resource". Use hardcoded `value:` for Genie space IDs.
- For SQL warehouse: set `DATABRICKS_HTTP_PATH` as a plain `value:` — `valueFrom` for warehouse properties does not inject correctly
- For auth in `db.py`: use `WorkspaceClient(host=..., client_id=..., client_secret=...)` to get an OAuth token, then pass to `dbsql.connect(access_token=...)`

## App Resources & Permissions
- Declare Genie spaces as resources in `app.yaml` with `genie_space` type and `permission: CAN_RUN`
- Bind resources via `databricks apps update meridian-portal --json '{"resources": [...]}'`
- The SP also needs explicit grants: `GRANT USE CATALOG`, `GRANT USE SCHEMA, SELECT ON SCHEMA` for each schema
- Genie space permissions: `PATCH /api/2.0/permissions/genie/{space_id}` with `service_principal_name` = SP's application/client ID (UUID), not display name
- SDP gold tables are `MATERIALIZED_VIEW` type, not `TABLE` — use `table_type IN ('TABLE', 'MATERIALIZED_VIEW')` in information_schema queries

## Vector Search
- SDP gold tables are materialized views — VS requires Delta tables with CDF. Use a managed snapshot table (`articles_vs_source`) as the index source.
- Use `STORAGE_OPTIMIZED` endpoint type (not `STANDARD`) — `STANDARD` gets stuck on "pending endpoint provisioning" in serverless workspaces
- VS sync notebook (`09_vs_sync.py`) must refresh `articles_vs_source` before triggering index sync
- Bind resources via `databricks apps update` using `space_id` field (not `id`) for genie_space resources

## Genie Integration (Best Practices)
- Use SDK typed methods: `w.genie.start_conversation_and_wait()` and `w.genie.create_message_and_wait()` — handles polling and retries
- Query results via `w.genie.get_message_attachment_query_result(attachment_id=att.attachment_id)`
- `GenieAttachment` has `attachment_id` not `id`
- Support multi-turn conversations by passing `conversation_id` to `create_message_and_wait()`
- Throughput limit: 5 queries/min/workspace during Public Preview

## SDP Pipeline Notebooks
- First line must be `# Databricks notebook source`
- Use `from pyspark import pipelines as dp` (not `dlt` or `delta.tables`)
- Read catalog via `spark.conf.get("meridian.catalog")` — set in `pipelines.yml` `configuration` block
- Expectations: `@dp.expect_or_drop("name", "constraint")` — no quarantine table support
- Source file tracking: `F.col("_metadata.file_path")` not `F.input_file_name()`
- Gold tables: always set `cluster_by=[...]` for liquid clustering

## Job/Data-Gen Notebooks
- Read catalog via `dbutils.widgets.text("catalog_name", "serverless_stable_k2zkdm_catalog")` then `dbutils.widgets.get("catalog_name")`
- Data gen notebooks: `# MAGIC %pip install faker -q`, inline all constants, no `src.common.config` imports
- Write files with `open()` + `os.makedirs()` to `/Volumes/{catalog}/meridian_staging/{source}/`

## Databricks CLI Gotchas
- `databricks sql` command does not exist — use `databricks api post /api/2.0/sql/statements` with `warehouse_id` and `statement`
- SQL statements API needs `"wait_timeout": "30s"` for inline results
- Only one statement per API call — no semicolon-separated multi-statement
- Pre-commit hook secret scanner will false-positive on minified JS — never commit `frontend/dist/`

## Schema Naming
- All schemas use `meridian_` prefix: `meridian_staging`, `meridian_research`, `meridian_internal`, `meridian_regulatory`
- This differs from the PRD's `meridian.research` notation — the PRD uses catalog.schema, but schemas are flat names within the catalog
