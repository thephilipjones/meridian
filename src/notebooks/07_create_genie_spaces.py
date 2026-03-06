# Databricks notebook source
# MAGIC %md
# MAGIC # Meridian Insights — Create / Update Genie Spaces
# MAGIC
# MAGIC Creates (or updates) all 3 Genie spaces for the Meridian Portal:
# MAGIC **Research Assistant**, **Internal Analytics**, **Regulatory Intelligence**.
# MAGIC
# MAGIC This notebook is idempotent — if a space with the same title already exists,
# MAGIC it will be updated instead of duplicated.
# MAGIC
# MAGIC After running, the notebook prints:
# MAGIC 1. The space IDs for `app.yaml` env vars and resource bindings
# MAGIC 2. CLI commands to bind resources to the app service principal
# MAGIC 3. Updated `app.yaml` snippet ready to paste
# MAGIC
# MAGIC > **Run this once per workspace, before `07_genie_enrichment.py`.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog_name", "serverless_stable_k2zkdm_catalog")
dbutils.widgets.text("warehouse_id", "e8eadc734c07e7f5")
dbutils.widgets.text("app_name", "meridian-portal")

catalog = dbutils.widgets.get("catalog_name")
warehouse_id = dbutils.widgets.get("warehouse_id")
app_name = dbutils.widgets.get("app_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Space Definitions

# COMMAND ----------

SPACE_DEFINITIONS = {
    "research": {
        "title": "Meridian Research Assistant",
        "description": (
            f"Natural language Q&A over {catalog}.meridian_research — "
            "biomedical articles, authors, citations, and MeSH terms "
            "from PubMed, arXiv, and Crossref."
        ),
        "table_identifiers": [
            f"{catalog}.meridian_research.articles",
            f"{catalog}.meridian_research.authors",
            f"{catalog}.meridian_research.citations",
            f"{catalog}.meridian_research.mesh_terms",
        ],
        "instructions": (
            "You are the Meridian Research Assistant, helping researchers explore "
            "biomedical literature across PubMed, arXiv, and Crossref sources.\n\n"
            "RULES:\n"
            "- Always cite paper DOI and first author when referencing findings.\n"
            "- When summarizing findings, note the study type (RCT, meta-analysis, "
            "cohort, case study) and sample size when available.\n"
            "- Distinguish between peer-reviewed publications and preprints (arXiv). "
            'Flag preprints explicitly with "[PREPRINT]" in your response.\n'
            "- When asked broad questions, prioritize meta-analyses and systematic "
            "reviews over individual studies.\n"
            "- Include publication year to help the user assess recency.\n"
            '- Do not speculate beyond what the data contains — say "the available '
            'articles suggest..." not "research proves..."\n'
            "- When multiple relevant articles exist, summarize the consensus and "
            "note any conflicting findings."
        ),
        "sample_questions": [
            "What are the most-cited papers on CRISPR off-target effects?",
            "Show me meta-analyses published in 2025 on immunotherapy",
            "Who are the top 10 authors by h-index in our database?",
            "What MeSH terms have the most articles?",
            "How many preprints vs peer-reviewed articles do we have by year?",
        ],
        "env_var": "RESEARCH_GENIE_SPACE_ID",
        "resource_name": "genie-research",
    },
    "internal": {
        "title": "Meridian Internal Analytics",
        "description": (
            f"Revenue, pipeline, product usage, and customer health analytics "
            f"from {catalog}.meridian_internal. Meridian's FY starts February 1. "
            "Products: Regulatory Feed, Research Platform, Patent Monitor, Custom Analytics."
        ),
        "table_identifiers": [
            f"{catalog}.meridian_internal.customer_health",
            f"{catalog}.meridian_internal.product_usage",
            f"{catalog}.meridian_internal.revenue_summary",
            f"{catalog}.meridian_internal.sales_pipeline",
        ],
        "instructions": (
            "You are the Meridian Internal Analytics assistant for the RevOps team.\n\n"
            "RULES:\n"
            "- Use fiscal quarters (Meridian's FY starts February 1).\n"
            "- Always show YoY comparison when discussing revenue or pipeline metrics.\n"
            '- When asked about "top customers," rank by ARR unless otherwise specified.\n'
            '- Product names: "Regulatory Feed," "Research Platform," "Patent Monitor," '
            '"Custom Analytics."\n'
            "- When showing pipeline data, include stage conversion rates."
        ),
        "sample_questions": [
            "What is our total pipeline value by stage?",
            "Show me revenue by product line for FY2025",
            "Which accounts are at risk based on health score?",
            "What is the average deal size by region?",
            "Which products have the highest error rates?",
        ],
        "env_var": "INTERNAL_GENIE_SPACE_ID",
        "resource_name": "genie-internal",
    },
    "regulatory": {
        "title": "Meridian Regulatory Intelligence",
        "description": (
            f"Regulatory intelligence across SEC filings, FDA enforcement actions, "
            f"and USPTO patents from {catalog}.meridian_regulatory. "
            "Includes company entity matching and risk signals."
        ),
        "table_identifiers": [
            f"{catalog}.meridian_regulatory.company_entities",
            f"{catalog}.meridian_regulatory.company_risk_signals",
            f"{catalog}.meridian_regulatory.patent_landscape",
            f"{catalog}.meridian_regulatory.regulatory_actions",
        ],
        "instructions": (
            "You are the Meridian Regulatory Intelligence assistant.\n\n"
            "RULES:\n"
            "- Always cite the specific filing ID, source agency, and date in responses.\n"
            "- When asked about trends, automatically compare to the prior year period.\n"
            "- When referencing companies, include their CIK (SEC) or application "
            "number (USPTO) for traceability.\n"
            "- If a question is ambiguous between SEC and FDA data, ask for clarification.\n"
            "- Format monetary values in USD with appropriate scale (thousands, "
            "millions, billions)."
        ),
        "sample_questions": [
            "How many regulatory actions by source agency in the last year?",
            "Which companies have the most patent filings?",
            "Show me high-risk companies with risk signals",
            "What types of SEC filings are most common?",
            "Show FDA recall actions by classification and status",
        ],
        "env_var": "REGULATORY_GENIE_SPACE_ID",
        "resource_name": "genie-regulatory",
    },
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers — Find, Create, Update Spaces

# COMMAND ----------

import json

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()


def list_existing_spaces():
    """List all Genie spaces accessible to the current user."""
    try:
        resp = w.api_client.do("GET", "/api/2.0/genie/spaces")
        return resp.get("spaces", [])
    except Exception as e:
        print(f"  Could not list spaces: {e}")
        return []


def find_space_by_title(title, existing_spaces):
    """Find a space by exact title match."""
    for space in existing_spaces:
        if space.get("title") == title:
            return space.get("space_id")
    return None


def create_space(definition):
    """Create a new Genie space."""
    body = {
        "title": definition["title"],
        "description": definition["description"],
        "warehouse_id": warehouse_id,
        "table_identifiers": definition["table_identifiers"],
    }
    resp = w.api_client.do("POST", "/api/2.0/genie/spaces", body=body)
    space_id = resp.get("space_id")
    print(f"  Created: {definition['title']} -> {space_id}")
    return space_id


def update_space(space_id, definition):
    """Update an existing Genie space."""
    body = {
        "title": definition["title"],
        "description": definition["description"],
        "warehouse_id": warehouse_id,
        "table_identifiers": definition["table_identifiers"],
    }
    w.api_client.do("PATCH", f"/api/2.0/genie/spaces/{space_id}", body=body)
    print(f"  Updated: {definition['title']} -> {space_id}")
    return space_id


def apply_instructions_and_samples(space_id, definition):
    """Patch instructions and sample questions onto an existing space."""
    updates = {
        "config": {
            "sample_questions": definition["sample_questions"],
        },
    }
    if definition.get("instructions"):
        updates["instructions"] = definition["instructions"]
    w.api_client.do("PATCH", f"/api/2.0/genie/spaces/{space_id}", body=updates)
    print(f"  Applied instructions + {len(definition['sample_questions'])} sample questions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create or Update All 3 Spaces

# COMMAND ----------

print("Listing existing Genie spaces...")
existing = list_existing_spaces()
print(f"  Found {len(existing)} existing spaces\n")

space_ids = {}

for key, definition in SPACE_DEFINITIONS.items():
    print(f"--- {definition['title']} ---")
    existing_id = find_space_by_title(definition["title"], existing)

    if existing_id:
        print(f"  Found existing space: {existing_id}")
        update_space(existing_id, definition)
        space_ids[key] = existing_id
    else:
        space_ids[key] = create_space(definition)

    apply_instructions_and_samples(space_ids[key], definition)
    print()

print("All spaces ready:")
for key, sid in space_ids.items():
    print(f"  {key}: {sid}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant App SP Access to Genie Spaces
# MAGIC
# MAGIC The app service principal needs `CAN_RUN` on each Genie space.

# COMMAND ----------

from databricks.sdk.service.iam import ObjectPermissions

for key, definition in SPACE_DEFINITIONS.items():
    sid = space_ids[key]
    try:
        w.api_client.do(
            "PATCH",
            f"/api/2.0/permissions/genie/{sid}",
            body={
                "access_control_list": [
                    {
                        "service_principal_name": app_name,
                        "all_permissions": [{"permission_level": "CAN_RUN"}],
                    }
                ]
            },
        )
        print(f"  Granted CAN_RUN on {definition['title']} to {app_name}")
    except Exception as e:
        print(f"  WARNING: Could not grant permissions on {definition['title']}: {e}")
        print(f"  Manual: databricks api patch /api/2.0/permissions/genie/{sid} ...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bind Resources to App

# COMMAND ----------

resources = [
    {
        "name": "sql_warehouse",
        "sql_warehouse": {"id": warehouse_id, "permission": "CAN_USE"},
    },
]

for key, definition in SPACE_DEFINITIONS.items():
    resources.append({
        "name": definition["resource_name"],
        "genie_space": {"id": space_ids[key], "permission": "CAN_RUN"},
    })

try:
    w.api_client.do(
        "PATCH",
        f"/api/2.0/apps/{app_name}",
        body={"resources": resources},
    )
    print(f"Bound {len(resources)} resources to {app_name}")
except Exception as e:
    print(f"WARNING: Could not bind resources: {e}")
    print("You may need to run this manually after the app is deployed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Output — `app.yaml` Snippet
# MAGIC
# MAGIC Copy this into `src/app/app.yaml` to update the hardcoded IDs:

# COMMAND ----------

research_id = space_ids["research"]
internal_id = space_ids["internal"]
regulatory_id = space_ids["regulatory"]

yaml_snippet = f"""env:
  - name: DATABRICKS_HTTP_PATH
    value: "/sql/1.0/warehouses/{warehouse_id}"
  - name: MERIDIAN_CATALOG
    value: "{catalog}"
  - name: MERIDIAN_LLM_ENDPOINT
    value: "databricks-meta-llama-3-3-70b-instruct"
  - name: RESEARCH_GENIE_SPACE_ID
    value: "{research_id}"
  - name: REGULATORY_GENIE_SPACE_ID
    value: "{regulatory_id}"
  - name: INTERNAL_GENIE_SPACE_ID
    value: "{internal_id}"

resources:
  - name: sql_warehouse
    sql_warehouse:
      warehouse_id: "{warehouse_id}"
      permission: CAN_USE
  - name: genie-research
    genie_space:
      space_id: "{research_id}"
      permission: CAN_RUN
  - name: genie-regulatory
    genie_space:
      space_id: "{regulatory_id}"
      permission: CAN_RUN
  - name: genie-internal
    genie_space:
      space_id: "{internal_id}"
      permission: CAN_RUN"""

print(yaml_snippet)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Output — Enrichment Notebook IDs
# MAGIC
# MAGIC If you need to update `07_genie_enrichment.py` hardcoded IDs:

# COMMAND ----------

print(f'RESEARCH_SPACE_ID = "{research_id}"')
print(f'INTERNAL_SPACE_ID = "{internal_id}"')
print(f'REGULATORY_SPACE_ID = "{regulatory_id}"')
