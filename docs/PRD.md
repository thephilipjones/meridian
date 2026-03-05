# Meridian Insights Demo Platform — Product Requirements Document

> **Status:** Draft v1
> **Author:** Philip Jones, Solution Architect
> **Created:** 2026-03-05
> **Target Customers:** Bloomberg Industry Group (BIG), Clarivate (Academia & Government)
> **Fictional Brand:** Meridian Insights — a regulatory intelligence and research analytics data provider

---

## 1. Executive Summary

Meridian Insights is a reusable, self-contained Databricks demo that shows how a **data product company** can use the Databricks platform end-to-end: ingesting raw source data, curating it through a medallion architecture, distributing governed data products to external customers, and providing natural-language analytics to both internal and external users.

The demo is packaged as a **Databricks Asset Bundle (DAB)** deployable to ephemeral workspaces with minimal manual setup.

### Customer Mapping

| Meridian Business Unit | Bloomberg Industry Group | Clarivate |
|---|---|---|
| **Meridian Regulatory** — curated regulatory/legal data products for external subscribers | Core external data product business (legal, regulatory, government affairs) | Patent/IP data products |
| **Meridian Research** — academic intelligence platform with natural-language Q&A over scholarly literature | Less central, but "research tools" resonates | Academia & Government (primary spender) |
| **Meridian Internal** — sales reporting, financial reporting, web/product usage analytics | Internal BI/reporting organization | Internal consumption analytics |

When demoing to BIG: lead with Regulatory + Internal.
When demoing to Clarivate: lead with Research + Regulatory.
The platform is identical — only the demo narrative changes.

---

## 2. Goals

1. **Engage imagination** — Show the customer how the Databricks platform maps to their actual business, not just technical capabilities in isolation.
2. **Breadth of platform** — Touch SDP, Unity Catalog, Databricks Apps, Genie, Delta Sharing, and (optionally) Clean Rooms in a single coherent story.
3. **Reusable baseline** — A demo that can be further customized per customer engagement without rebuilding from scratch.
4. **Self-running and click-through** — Works as a recorded walkthrough or a live interactive demo.
5. **Deployable** — DAB-packaged, deployable to a fresh workspace with `databricks bundle deploy`.

---

## 3. Architecture Overview

```
                         DATA SOURCES
          ┌──────────────────┬──────────────────┐
          |                  |                  |
     SEC EDGAR /        PubMed /           Synthetic
     FDA openFDA /      arXiv /            CRM, Web,
     USPTO              Crossref           Finance
          |                  |                  |
          v                  v                  v
    ┌─────────────────────────────────────────────────┐
    |        SPARK DECLARATIVE PIPELINES               |
    |                                                  |
    |  BRONZE ──────────> SILVER ──────────> GOLD      |
    |  (raw ingest)    (cleanse/enrich)   (products)   |
    |                                                  |
    |  Ingestion patterns per source:                  |
    |   - Auto Loader (cloud files, streaming)         |
    |   - COPY INTO (batch file loads)                 |
    |   - Read stream from API-staged files            |
    |                                                  |
    |  Gold tables use LIQUID CLUSTERING              |
    |  Expectations at silver for quality gates        |
    └───────────────────────┬─────────────────────────┘
                            |
              ┌─────────────┼─────────────────┐
              v             v                 v
     meridian.regulatory  meridian.research  meridian.internal
     ────────────────────────────────────────────────────
       Unity Catalog: governed, tagged, lineage-tracked
              |             |                 |
              v             v                 v
    ┌─────────────────────────────────────────────────┐
    |           DATABRICKS APP — Meridian Portal       |
    |                                                  |
    |  Profile Switcher (no login/logout):             |
    |   [Sarah Chen, RevOps]  Internal analytics       |
    |   [James Rivera, Acme Bank]  Customer portal     |
    |   [Dr. Anika Park, NIH]  Research Q&A            |
    |                                                  |
    |  Each profile sees different:                    |
    |   - Navigation tabs                              |
    |   - Genie space (scoped data + instructions)     |
    |   - Data catalog / dashboard content             |
    └──────────────────────┬──────────────────────────┘
                           |
              ┌────────────┼──────────────────┐
              v            v                v
        Delta Sharing   Genie (x3)     AI/BI Dashboards
        (live feeds     (per BU,       (built on Metric
         to customers)  Metric View-    Views — governed
                        aware)          KPIs, no drift)
```

---

## 4. Data Sources

### 4.1 Regulatory Data (Real Public Feeds)

| Source | Content | Format | Ingestion Pattern | Notes |
|---|---|---|---|---|
| **SEC EDGAR** | Company filings (10-K, 10-Q, 8-K) | JSON (full-text submissions API) | **Auto Loader** — files staged to cloud storage via scheduled fetch | Free, no API key required. Rate-limited to 10 req/sec. |
| **FDA openFDA** | Drug enforcement actions, adverse events, recalls | JSON (REST API) | **API fetch -> staged files -> Auto Loader** | Free, API key optional. Rich structured data. |
| **USPTO PatentsView** | Patent grants and applications | JSON (bulk download or API) | **COPY INTO** — periodic bulk load of snapshot files | Demonstrates batch loading pattern alongside streaming. |

### 4.2 Research Data (Real Public Feeds)

| Source | Content | Format | Ingestion Pattern | Notes |
|---|---|---|---|---|
| **PubMed / MEDLINE** | 36M+ biomedical article abstracts, MeSH terms, authors, affiliations | XML (E-utilities API or annual baseline files) | **Auto Loader** — XML files staged incrementally | Free. Annual baseline + daily updates available. Richest dataset for the research demo. |
| **arXiv** | Preprint metadata (CS, physics, math, bio) | XML (OAI-PMH) or JSON (API) | **Auto Loader** — harvested metadata files | Free. Good for showing preprint vs peer-reviewed distinction. |
| **Crossref** | DOI metadata, citation links, publisher info | JSON (REST API) | **Read stream from API-staged JSON** — small incremental files | Free for metadata. Useful for citation graph enrichment. |

### 4.3 Internal Data (Synthetic)

| Dataset | Content | Generation | Ingestion Pattern |
|---|---|---|---|
| **CRM / Sales Pipeline** | Opportunities, accounts, stages, close dates, ARR | Faker + realistic distributions | **COPY INTO** — demonstrates batch pattern with CSVs |
| **Product Usage / Web Analytics** | Customer API calls, query counts, data product access logs | Faker + time-series patterns (seasonality, spikes) | **Auto Loader** — streaming JSON event files |
| **Financial Summaries** | Revenue by product line, cost of data acquisition, margins | Faker + consistent with CRM data | **COPY INTO** — quarterly snapshot CSVs |

### 4.4 Ingestion Pattern Variability

The demo intentionally uses **three distinct ingestion approaches** within SDP to show breadth:

1. **Auto Loader (cloudFiles)** — Used for regulatory filings, PubMed articles, and web analytics events. Demonstrates incremental, schema-evolution-capable streaming ingestion. This is the primary recommended pattern.
2. **COPY INTO** — Used for USPTO bulk snapshots, CRM CSVs, and financial summaries. Demonstrates batch loading of known-schema files. Shows when this simpler pattern is appropriate.
3. **API-staged incremental files** — Used for Crossref and openFDA. A lightweight Python script fetches from REST APIs and writes JSON files to a staging volume; Auto Loader picks them up. Shows the pattern for API sources that don't have native Databricks connectors.

This variability lets the presenter say: "Different sources have different characteristics — Databricks handles all of them within the same pipeline framework."

---

## 5. Medallion Architecture (per Business Unit)

### 5.1 Regulatory Pipeline (`meridian.regulatory`)

| Layer | Tables | Key Transformations |
|---|---|---|
| **Bronze** | `raw_sec_filings`, `raw_fda_actions`, `raw_patents` | Raw ingestion, metadata capture (source, ingest timestamp, file path). No business logic. |
| **Silver** | `cleaned_sec_filings`, `cleaned_fda_actions`, `cleaned_patents`, `quarantine_regulatory` | Schema normalization, deduplication by filing ID, date parsing, null handling. **Expectations**: filing_id NOT NULL, filing_date is valid date, company_name length > 0. Failed rows quarantined. |
| **Gold** | `regulatory_actions` (unified view across SEC/FDA), `patent_landscape`, `company_entities` (master entity table), `company_risk_signals` (derived) | Cross-source entity resolution (company name matching), trend aggregations, risk signal derivation. These are the "data products." |

### 5.2 Research Pipeline (`meridian.research`)

| Layer | Tables | Key Transformations |
|---|---|---|
| **Bronze** | `raw_pubmed_articles`, `raw_arxiv_articles`, `raw_crossref_metadata` | Raw XML/JSON ingested as-is with metadata. |
| **Silver** | `cleaned_articles`, `cleaned_authors`, `cleaned_citations`, `quarantine_research` | XML parsing, author name normalization, MeSH term extraction, DOI deduplication. **Expectations**: doi NOT NULL, title length > 0, publication_date is valid. |
| **Gold** | `articles` (unified), `authors`, `citations`, `mesh_terms`, `article_search` (optimized for Genie) | Citation graph construction, h-index calculation per author, topic clustering via MeSH, full-text search column for Genie. |

### 5.3 Internal Pipeline (`meridian.internal`)

| Layer | Tables | Key Transformations |
|---|---|---|
| **Bronze** | `raw_crm_deals`, `raw_web_events`, `raw_financials` | Raw CSVs and JSON events. |
| **Silver** | `cleaned_deals`, `cleaned_web_events`, `cleaned_financials` | Type casting, deduplication, sessionization of web events. **Expectations**: deal_id unique, event_timestamp NOT NULL, revenue >= 0. |
| **Gold** | `sales_pipeline`, `product_usage`, `revenue_summary`, `customer_health` | Pipeline stage analytics, product adoption scoring, cohort analysis, usage-based health scores. |

---

## 6. Genie Spaces

Three Genie spaces, each with tailored custom instructions and scoped tables.

### 6.1 Regulatory Intelligence

- **Tables:** `meridian.regulatory.regulatory_actions`, `meridian.regulatory.patent_landscape`, `meridian.regulatory.company_entities`, `meridian.regulatory.company_risk_signals`
- **Custom Instructions:**
  - Always cite the specific filing ID, source agency, and date in responses.
  - When asked about trends, automatically compare to the prior year period.
  - When referencing companies, include their CIK (SEC) or application number (USPTO) for traceability.
  - If a question is ambiguous between SEC and FDA data, ask for clarification.
  - Format monetary values in USD with appropriate scale (thousands, millions, billions).

### 6.2 Research Assistant

- **Tables:** `meridian.research.articles`, `meridian.research.authors`, `meridian.research.citations`, `meridian.research.mesh_terms`
- **Custom Instructions:**
  - Always cite paper DOI and first author when referencing findings.
  - When summarizing findings, note the study type (RCT, meta-analysis, cohort, case study) and sample size when available.
  - Distinguish between peer-reviewed publications and preprints (arXiv). Flag preprints explicitly.
  - When asked broad questions, prioritize meta-analyses and systematic reviews over individual studies.
  - Include publication year to help the user assess recency.
  - Do not speculate beyond what the data contains — say "the available articles suggest..." not "research proves..."

### 6.3 Internal Analytics

- **Tables:** `meridian.internal.sales_pipeline`, `meridian.internal.product_usage`, `meridian.internal.revenue_summary`, `meridian.internal.customer_health`
- **Custom Instructions:**
  - Use fiscal quarters (Meridian's FY starts February 1).
  - Always show YoY comparison when discussing revenue or pipeline metrics.
  - When asked about "top customers," rank by ARR unless otherwise specified.
  - Product names: "Regulatory Feed," "Research Platform," "Patent Monitor," "Custom Analytics."
  - When showing pipeline data, include stage conversion rates.

---

## 7. Databricks App — Meridian Portal

### 7.1 Overview

A full-stack Databricks App (FastAPI + React) serving as the unified portal for Meridian Insights. It demonstrates how a data provider could build a customer-facing and internal analytics product on Databricks.

### 7.2 User Profiles (Demo Switcher)

Instead of authentication, the app uses a **profile selector dropdown** in the top navigation bar. Switching profiles changes the visible navigation, data scope, and embedded Genie space.

| Profile | Persona | Role | Sees |
|---|---|---|---|
| **Sarah Chen** | RevOps Analyst | Internal | Sales dashboards, product usage analytics, revenue Genie, all customers' data |
| **James Rivera** | Data Engineering Lead at Acme Bank | External Customer (Regulatory) | Regulatory data catalog (subscribed products only), regulatory Genie, Delta Sharing connection info, sample queries |
| **Dr. Anika Park** | Research Director at NIH | External Customer (Research) | Research Q&A interface, paper browser, citation explorer, research Genie |

### 7.3 App Views

#### Internal View (Sarah Chen)

- **Sales Dashboard** — Pipeline by stage, bookings trend, top accounts by ARR. Embedded AI/BI dashboard or custom charts.
- **Product Usage** — Which customers are querying which data products, how often, freshness SLA compliance. Heatmap of usage by product x customer.
- **Genie** — Internal analytics Genie space embedded via iframe or SDK.

#### Customer View — Regulatory (James Rivera)

- **Data Catalog** — Browse available data products with descriptions, schemas, sample records, freshness indicators. Products James hasn't subscribed to are visible but grayed out with "Contact Sales" prompt.
- **Genie** — Regulatory Genie scoped to James's subscription tier (e.g., only SEC data, not FDA).
- **Connect Your Environment** — Instructions and credentials for Delta Sharing. Pre-built code snippets for Databricks, Snowflake, Pandas, and Power BI.

#### Customer View — Research (Dr. Anika Park)

- **Research Q&A** — A clean interface wrapping the Research Genie. Dr. Park types a research question; the response includes sourced papers with DOIs, guided interpretation, and links to full text where available.
- **Paper Browser** — Searchable/filterable table of articles by topic, date range, journal, author. Click to expand abstract and citation details.
- **Citation Explorer** — Visual or tabular view of citation relationships for a given paper or author.

### 7.4 Technical Requirements

- **Framework:** FastAPI (backend) + React (frontend), deployed as a Databricks App
- **State management:** Profile selection stored in app state (React context), drives API calls and component rendering
- **Data access:** Backend queries Unity Catalog tables via Databricks SQL connector, respecting profile-based scoping
- **Genie integration:** Embedded Genie spaces, switching based on active profile
- **Styling:** Clean, professional. Meridian brand palette (blues/grays). Responsive layout.

---

## 8. Delta Sharing

### 8.1 Demo Scenario

Meridian distributes its gold-layer data products to external customers via Delta Sharing. The demo shows:

1. **Share creation** — A Unity Catalog share containing `meridian.regulatory.regulatory_actions` and `meridian.regulatory.company_entities`, with row-level filtering by subscription tier.
2. **Recipient setup** — A simulated external recipient ("Acme Bank") with an activation link.
3. **Consumer experience** — A separate notebook (or second workspace) where the recipient queries shared tables using open Delta Sharing connectors. Data is live — no copies.
4. **Access revocation** — Removing a table from a share or deactivating a recipient, showing subscription lifecycle management.

### 8.2 Implementation Notes

- Delta Sharing setup can be scripted in the DAB via Terraform provider or CLI commands in a post-deploy script.
- For a self-contained demo on a single workspace, use a second catalog as the "recipient environment."
- Clean Rooms are a Phase 3 expansion and require a separate workspace; the DAB will include documentation for manual setup.

---

## 9. Unity Catalog & Governance

### 9.1 Catalog Structure

```
meridian (catalog)
  |-- regulatory (schema)
  |     |-- raw_sec_filings (bronze)
  |     |-- raw_fda_actions (bronze)
  |     |-- raw_patents (bronze)
  |     |-- cleaned_sec_filings (silver)
  |     |-- cleaned_fda_actions (silver)
  |     |-- cleaned_patents (silver)
  |     |-- quarantine_regulatory (silver)
  |     |-- regulatory_actions (gold)
  |     |-- patent_landscape (gold)
  |     |-- company_entities (gold)
  |     |-- company_risk_signals (gold)
  |
  |-- research (schema)
  |     |-- raw_pubmed_articles (bronze)
  |     |-- raw_arxiv_articles (bronze)
  |     |-- raw_crossref_metadata (bronze)
  |     |-- cleaned_articles (silver)
  |     |-- cleaned_authors (silver)
  |     |-- cleaned_citations (silver)
  |     |-- quarantine_research (silver)
  |     |-- articles (gold)
  |     |-- authors (gold)
  |     |-- citations (gold)
  |     |-- mesh_terms (gold)
  |     |-- article_search (gold)
  |
  |-- internal (schema)
  |     |-- raw_crm_deals (bronze)
  |     |-- raw_web_events (bronze)
  |     |-- raw_financials (bronze)
  |     |-- cleaned_deals (silver)
  |     |-- cleaned_web_events (silver)
  |     |-- cleaned_financials (silver)
  |     |-- sales_pipeline (gold)
  |     |-- product_usage (gold)
  |     |-- revenue_summary (gold)
  |     |-- customer_health (gold)
```

### 9.2 Governance Features Demonstrated

- **Tags:** Tables tagged with `pii:false`, `data_product:true/false`, `medallion_layer:bronze/silver/gold`, `business_unit:regulatory/research/internal`.
- **Lineage:** End-to-end lineage from bronze raw tables through silver to gold, visible in Catalog Explorer. A key demo moment: tracing a `regulatory_actions` record back to the original SEC filing.
- **Row-level security:** Gold tables include a `subscription_tier` or `access_group` column. Dynamic views or row filters restrict external customer profiles to their entitled rows.
- **Column masking:** Optionally mask sensitive columns (e.g., internal scoring fields) for external profiles.

---

## 10. Databricks Asset Bundle (DAB) Structure

```
meridian-insights-demo/
  |-- databricks.yml                    # Bundle configuration
  |-- bundle.auto.tfvars.json           # Per-environment variable overrides
  |
  |-- resources/
  |     |-- pipelines.yml               # SDP pipeline definitions (3 pipelines)
  |     |-- jobs.yml                     # Orchestration jobs (data fetch, pipeline trigger)
  |     |-- dashboards.yml             # AI/BI Dashboard definitions
  |     |-- genie_spaces.yml            # Genie space definitions (if API-manageable)
  |     |-- shares.yml                  # Delta Sharing share + recipient definitions
  |
  |-- src/
  |     |-- pipelines/                     # Gold tables use liquid clustering
  |     |     |-- regulatory/
  |     |     |     |-- bronze_sec_filings.py
  |     |     |     |-- bronze_fda_actions.py
  |     |     |     |-- bronze_patents.py
  |     |     |     |-- silver_regulatory.py
  |     |     |     |-- gold_regulatory.py
  |     |     |
  |     |     |-- research/
  |     |     |     |-- bronze_pubmed.py
  |     |     |     |-- bronze_arxiv.py
  |     |     |     |-- bronze_crossref.py
  |     |     |     |-- silver_research.py
  |     |     |     |-- gold_research.py
  |     |     |
  |     |     |-- internal/
  |     |           |-- bronze_crm.py
  |     |           |-- bronze_web_events.py
  |     |           |-- bronze_financials.py
  |     |           |-- silver_internal.py
  |     |           |-- gold_internal.py
  |     |
  |     |-- data_fetch/
  |     |     |-- fetch_edgar.py         # Scheduled script: SEC EDGAR -> staging volume
  |     |     |-- fetch_openfda.py       # Scheduled script: openFDA -> staging volume
  |     |     |-- fetch_pubmed.py        # Scheduled script: PubMed -> staging volume
  |     |     |-- fetch_arxiv.py         # Scheduled script: arXiv -> staging volume
  |     |     |-- fetch_crossref.py      # Scheduled script: Crossref -> staging volume
  |     |
  |     |-- data_gen/
  |     |     |-- generate_crm.py        # Synthetic CRM/sales data
  |     |     |-- generate_web_events.py # Synthetic web analytics
  |     |     |-- generate_financials.py # Synthetic financial summaries
  |     |
  |     |-- app/
  |     |     |-- backend/
  |     |     |     |-- main.py           # FastAPI app
  |     |     |     |-- profiles.py       # User profile definitions and scoping logic
  |     |     |     |-- routers/
  |     |     |     |     |-- catalog.py   # Data catalog endpoints
  |     |     |     |     |-- analytics.py # Internal analytics endpoints
  |     |     |     |     |-- research.py  # Research Q&A endpoints
  |     |     |     |     |-- sharing.py   # Delta Sharing info endpoints
  |     |     |     |-- db.py             # Databricks SQL connector
  |     |     |
  |     |     |-- frontend/
  |     |     |     |-- src/
  |     |     |     |     |-- App.tsx
  |     |     |     |     |-- components/
  |     |     |     |     |     |-- ProfileSwitcher.tsx
  |     |     |     |     |     |-- InternalView.tsx
  |     |     |     |     |     |-- CustomerRegulatoryView.tsx
  |     |     |     |     |     |-- ResearchView.tsx
  |     |     |     |     |     |-- GenieEmbed.tsx
  |     |     |     |     |     |-- DataCatalog.tsx
  |     |     |     |     |     |-- PaperBrowser.tsx
  |     |     |     |     |     |-- CitationExplorer.tsx
  |     |     |     |     |-- contexts/
  |     |     |     |           |-- ProfileContext.tsx
  |     |     |     |-- package.json
  |     |     |
  |     |     |-- app.yml               # Databricks App manifest
  |     |
  |     |-- dashboards/
  |     |     |-- internal_analytics.lvdash.json  # AI/BI Dashboard (built on Metric Views)
  |     |
  |     |-- notebooks/
  |           |-- 00_setup.py            # Catalog/schema creation, volume setup
  |           |-- 01_demo_walkthrough.py # Guided narrative notebook (pipe syntax)
  |           |-- 02_delta_sharing.py    # Delta Sharing setup and consumer simulation
  |           |-- 03_governance_tour.py  # Lineage, tags, Metric Views, RLS queries
  |           |-- 04_metric_views.py     # Create governed Metric Views on gold KPIs
  |
  |-- tests/
  |     |-- test_data_gen.py
  |     |-- test_pipeline_expectations.py
  |
  |-- docs/
  |     |-- PRD.md                       # This document
  |     |-- DEMO_SCRIPT.md              # Presenter's guide with talking points
  |     |-- DEPLOYMENT.md               # Step-by-step deployment instructions
  |
  |-- .gitignore
  |-- README.md
```

---

## 11. Deployment & Environment Requirements

### 11.1 Prerequisites

- Databricks workspace with Unity Catalog enabled
- A catalog named `meridian` (or configurable via bundle variables)
- A SQL warehouse (Serverless preferred) for Genie and app queries
- Cloud storage volume for staging raw files (created by setup notebook)
- Databricks CLI configured with a profile for the target workspace

### 11.2 Deployment Steps

```bash
# Clone and deploy
git clone <repo-url> && cd meridian-insights-demo
databricks bundle deploy -t dev

# Run initial setup (creates catalog, schemas, volumes)
databricks bundle run setup_job -t dev

# Generate synthetic data
databricks bundle run data_gen_job -t dev

# Fetch real data (optional — can demo with synthetic only)
databricks bundle run data_fetch_job -t dev

# Run pipelines
databricks bundle run regulatory_pipeline -t dev
databricks bundle run research_pipeline -t dev
databricks bundle run internal_pipeline -t dev

# Deploy the app
databricks bundle run deploy_app -t dev
```

### 11.3 Environment Targets

| Target | Purpose | Notes |
|---|---|---|
| `dev` | Builder's own workspace | Full access, iterative development |
| `demo` | Clean demo workspace | Pre-loaded data, polished state |
| `customer` | Ephemeral per-customer workspace | Deploy, demo, tear down |

---

## 12. Demo Flow — Presenter's Narrative

### Chapter 1: "The Data Challenge" (3 min)

> "Meridian Insights is a data provider — think Bloomberg, Clarivate, S&P Global. They ingest messy raw data from dozens of sources and turn it into curated, trustworthy data products that their customers pay for. Let's see how they do it on Databricks."

- Show raw data landing in volumes: SEC JSON files, PubMed XML, messy CSVs.
- Highlight the variety: different formats, different cadences, different quality levels.

### Chapter 2: "The Pipeline" (5 min)

> "Meridian uses Spark Declarative Pipelines to build a medallion architecture — bronze for raw ingestion, silver for cleansing and enrichment, gold for business-ready data products."

- Walk through the SDP pipeline graph in the UI.
- Show Auto Loader picking up new files incrementally.
- Show a COPY INTO batch load for contrast.
- Trigger an expectation failure: a malformed record hits a quality gate, gets quarantined. "Data providers live and die by quality — this is how Meridian catches problems before customers see them."
- Show lineage in Unity Catalog: trace a gold `regulatory_actions` record back to the original SEC filing.

### Chapter 3: "The Portal" (7 min)

> "Meridian's customers and internal teams interact with data through the Meridian Portal — built as a Databricks App."

- **As Sarah (Internal):** Show sales dashboard, product usage heatmap. Ask Genie: "What was Q4 revenue by product line compared to last year?" Show the answer with YoY comparison.
- **Switch to James (Customer):** Show the data catalog — subscribed products are accessible, others are grayed out. Ask Genie: "Show me all FDA enforcement actions related to cardiovascular drugs in 2025." Show scoped results.
- **Switch to Dr. Park (Researcher):** Ask: "What are the latest findings on CRISPR off-target effects?" Show sourced papers with DOIs, study types flagged, preprints distinguished. "This is what it looks like when you put Genie in front of 36 million research articles with expert instructions guiding the interpretation."

### Chapter 4: "The Distribution" (3 min)

> "Meridian's enterprise customers don't want to use a portal — they want the data in their own environment. Delta Sharing makes this possible."

- Show the share in Unity Catalog containing gold tables.
- Open the "consumer notebook" — simulate Acme Bank querying the shared data from their own environment. Live, no copies.
- Revoke access to one table. "When a subscription lapses, access is cut immediately — no stale data floating around."

### Chapter 5: "The Governance Story" (2 min)

> "For a data provider, governance isn't optional — it's the product. Unity Catalog gives Meridian auditability, access control, and lineage across the entire platform."

- Show table tags (`data_product:true`, `medallion_layer:gold`).
- Show row-level security: James sees only SEC data, not FDA.
- Show system tables: who queried what, when.

**Total: ~20 minutes. Can be shortened to 10 by skipping Chapter 2 detail and Chapter 4.**

---

## 13. Phased Delivery

### Phase 1 — Core Demo (MVP)

Deliverables:
- DAB scaffold with `databricks.yml` and environment targets
- **Research pipeline** end-to-end (PubMed data, bronze/silver/gold, all three ingestion patterns demonstrated)
- **Internal pipeline** with synthetic data (CRM, web events, financials)
- **Liquid Clustering** on all gold tables (`cluster_by` replacing traditional partitioning — zero-maintenance adaptive layout)
- **Metric Views** for internal gold KPIs (revenue, pipeline, health metrics defined as governed UC objects in YAML — consumed natively by Genie and AI/BI Dashboards)
- **AI/BI Dashboards** for the Internal Analytics view (revenue trend, pipeline funnel, customer health — built on Metric Views)
- Databricks App with profile switcher and two views (Research, Internal)
- One Genie space (Research Assistant)
- **SQL pipe syntax** (`|>`) used in demo walkthrough and governance tour notebooks to showcase modern Databricks SQL
- **DAB best practices:** `run_as` and `permissions` blocks on all jobs and pipelines
- Setup and demo walkthrough notebooks
- `DEMO_SCRIPT.md` with talking points

Why Research first: PubMed has the easiest API, richest free data, and the "ask a research question, get sourced papers" moment is the strongest demo beat.

### Phase 2 — Full Platform

Deliverables:
- **Regulatory pipeline** (EDGAR, openFDA, USPTO)
- Regulatory Genie space
- Internal Analytics Genie space
- Customer Regulatory view in the App (catalog, scoped Genie, Delta Sharing info)
- Delta Sharing setup (share, recipient, consumer notebook)
- Governance tour notebook (lineage, tags, RLS with **row filters and column masks** via SQL syntax)
- **Zerobus Ingest** for web analytics events (direct gRPC ingestion into Delta tables — replaces staging volume pattern for real-time sources)
- **System Tables** meta-analytics schema (materialized views over `system.access.audit` and `system.billing.usage` filtered to the `meridian` catalog)
- **Vector Search** index on research article abstracts for RAG-powered Research Q&A

### Phase 3 — Advanced Scenarios

Deliverables:
- Clean Rooms demo (requires second workspace, documented manual setup)
- **Agent Bricks** Knowledge Assistant for Research Q&A (RAG over Vector Search index + Foundation Model API)
- NLP enrichment via Model Serving (entity extraction in the silver layer)
- Scheduled orchestration via Databricks Workflows (periodic data fetch + pipeline refresh)
- **Lakeflow Connect** evaluation for managed connectors (replace custom fetch scripts where connectors exist)

---

## 14. Open Questions

1. **Genie embedding** — Confirm current method for embedding Genie in a Databricks App (iframe? SDK? API?). This affects the app architecture.
2. **DAB support for Genie** — Can Genie spaces be defined declaratively in the bundle, or do they require manual/API setup as a post-deploy step?
3. **PubMed data volume** — Full baseline is ~36M records. For demo purposes, should we limit to a specific date range or topic subset (e.g., last 2 years, biomedical only)?
4. **Delta Sharing consumer** — Single-workspace simulation (second catalog) vs. multi-workspace demo? Former is simpler to deploy; latter is more realistic.
5. **App hosting** — Confirm the target workspace supports Databricks Apps (GA, not gated).

---

## 15. Success Criteria

- [ ] `databricks bundle deploy -t demo` completes without manual intervention (other than secrets/auth)
- [ ] Pipelines run end-to-end and produce queryable gold tables
- [ ] App launches, profile switcher works, all three views render correctly
- [ ] Genie answers natural-language questions with sourced, instruction-guided responses
- [ ] Demo can be delivered in under 20 minutes with clear talking points
- [ ] A new SA can clone the repo, deploy, and deliver the demo with only `DEMO_SCRIPT.md` and `DEPLOYMENT.md`
