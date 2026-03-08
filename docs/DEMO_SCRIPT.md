# Meridian Insights — Demo Script

> **Total time:** ~20 minutes (can be shortened to ~10 by skipping Chapter 2 detail and Chapter 4)
>
> **Prerequisites:** Bundle deployed (`databricks bundle deploy -t dev`), all jobs run (`setup_job`, `data_gen_job`, `run_pipelines_job`), app running, Genie spaces created.
>
> **Audience mapping:**
> - Bloomberg Industry Group (BIG): Lead with Internal + Research. Emphasize data product quality and monetization.
> - Clarivate: Lead with Research + Internal. Emphasize scholarly data at scale and NLP-powered Q&A.

---

## Pre-Demo Checklist

- [ ] Workspace open in browser
- [ ] Meridian Portal app open in a separate tab: `https://meridian-portal-7474650913311373.aws.databricksapps.com`
- [ ] Demo walkthrough notebook open: `src/notebooks/01_demo_walkthrough.py`
- [ ] Pipeline graph visible (navigate to **Pipelines** in sidebar)
- [ ] Genie spaces accessible (Research Assistant, Internal Analytics)

---

## Chapter 1: "The Data Challenge" (3 min)

### Narrative

> *"Meridian Insights is a data provider — think Bloomberg, Clarivate, S&P Global. They ingest messy raw data from dozens of sources and turn it into curated, trustworthy data products that their customers pay for. Let's see how they do it on Databricks."*

### What to Show

1. **Run cell: "Show staging volumes"** — `SHOW VOLUMES IN {catalog}.meridian_staging`
   - Talk about the variety: PubMed JSON, CRM CSVs, financial snapshots, web event streams
   - *"Each of these volumes is a landing zone for a different data source — different formats, different cadences, different quality levels."*

2. **Run cell: "Peek at raw PubMed JSON"** — `dbutils.fs.ls(...pubmed/)`
   - Show the raw JSON files from the data generator
   - *"These are biomedical research articles from PubMed — 5,000 articles across 10 files, each one a JSON-lines document with titles, abstracts, authors, and MeSH terms."*

3. **Run cell: "Peek at raw CRM CSV"** — `dbutils.fs.ls(...crm/)`
   - *"And here's the internal CRM data — 2,000 synthetic deals. Different format, different schema, same platform."*

### Key Talking Points
- Databricks handles JSON, CSV, XML — all within the same framework
- Volumes provide governed cloud storage within Unity Catalog
- No external object stores to manage separately

---

## Chapter 2: "The Pipeline" (5 min)

### Narrative

> *"Meridian uses Spark Declarative Pipelines to build a medallion architecture — bronze for raw ingestion, silver for cleansing and enrichment, gold for business-ready data products."*

### What to Show

1. **Navigate to the Pipeline UI** — Open the Research Pipeline from the Pipelines sidebar
   - Show the DAG: `bronze_pubmed → silver (cleaned_articles, cleaned_authors) → gold (articles, authors, mesh_terms, article_search)`
   - *"This is the full pipeline graph. Notice how it flows from raw ingestion through quality gates to finished data products — all defined declaratively in Python."*

2. **Run cell: "Raw PubMed articles"** — Shows bronze data
   - *"At the bronze layer, we see raw records exactly as they arrived. Auto Loader tracked which files have been processed, so re-runs don't re-ingest."*

3. **Run cell: "Cleaned articles"** — Shows silver data with parsed dates, deduplication
   - *"The silver layer is where quality happens. Notice the preprint flag, publication type classification, and DOI-based deduplication. Two expectations enforce this: every article must have a title and a valid publication date. Records that fail get dropped."*
   - **Show the expectation metrics** in the pipeline UI if available

4. **Run cell: "Articles with citation counts"** — Gold layer, pipe syntax
   - *"The gold layer produces governed data products. Notice the pipe syntax — this is Databricks SQL's modern query style."*
   - Point out `|> ORDER BY citation_count DESC |> LIMIT 10`

5. **Run cell: "Author profiles with h-index"** — Gold layer
   - *"We've computed h-index for every author in the corpus — a standard academic metric, calculated at scale."*

6. **Show Lineage** — Navigate to Catalog Explorer → `meridian_research.articles` → Lineage tab
   - *"Trace any gold record back through silver to the original raw data. This lineage is automatic — no extra configuration. For a data provider, this is how you prove provenance to customers."*

### Key Talking Points
- Spark Declarative Pipelines: define tables declaratively, Databricks handles orchestration
- Auto Loader: incremental, exactly-once file ingestion
- Expectations: data quality gates (`expect_or_drop`) prevent bad data from reaching customers
- Liquid Clustering: `cluster_by` on gold tables adapts layout to query patterns — zero maintenance
- Lineage: automatic column-level tracking across the full pipeline

---

## Chapter 3: "The Portal" (9 min)

### Narrative

> *"Meridian's customers and internal teams interact with data through the Meridian Portal — built as a Databricks App with FastAPI on the backend and React on the frontend. Each persona lands on an intelligence-first view — not raw data, but actionable insight."*

### What to Show

**Open the Meridian Portal app in the browser.**

#### As Sarah Chen (VP, Product Analytics) — 3 min

> Thread: Sarah opens Meridian the way a Bloomberg Terminal user hits `TOP<GO>` — with an AI-generated summary of what matters today.

1. Click **Sarah Chen** in the profile switcher
2. The **Sales Dashboard** tab opens with the **AI Business Brief** at the top
   - *"Sarah's morning starts here — an AI-generated executive summary powered by Foundation Model API. The LLM synthesizes across four separate governed gold tables in a single brief: revenue trajectory, customer risk changes, product adoption anomalies, platform health."*
   - Point out the 2×2 highlight cards with sentiment-colored borders (green/amber/red/blue)
   - *"This is Foundation Model API used for business intelligence summarization — not just RAG, but a novel pattern that composes structured data into narrative insight."*
   - Click the **Refresh** button to show the brief regenerates
   - *"The brief is cached for 5 minutes — fast on tab switches, but always fresh."*

3. Scroll down to the **Sales Dashboard** charts
   - *"Below the brief, Sarah has the full picture — pipeline by stage, revenue by product, customer health. This is powered by gold tables and Metric Views."*

4. **Run notebook cell: "Revenue via Metric Views"**
   - *"Metric Views are governed KPI definitions stored in Unity Catalog. Total Revenue, Gross Margin Pct — defined once, consumed everywhere: Genie, dashboards, SQL, and the AI Business Brief."*

5. **Open the Internal Analytics Genie space** (or Genie tab in the portal)
   - Ask: *"What was Q4 revenue by product line compared to last year?"*
   - *"Genie generates SQL against the governed gold tables and Metric Views. The custom instructions we defined ensure it uses fiscal quarters — Meridian's FY starts February — and always shows YoY comparison."*

#### As James Rivera (VP, Regulatory Affairs, Acme Bank) — 3 min

> Thread: James monitors regulatory activity the way a Bloomberg Government subscriber does — temporal, entity-centric, actionable.

1. Click **James Rivera** in the profile switcher
2. The **Regulatory Feed** tab opens — the new default landing
   - *"James doesn't browse table schemas — he monitors regulatory activity. This is a live intelligence feed: recent SEC filings, entity risk profiles, and regulatory actions relevant to his coverage universe."*
   - Point out the summary bar: total actions, entities, risk signals over the last 90 days
   - *"This is gold table data products rendered as products — not schemas, but intelligence."*

3. Click **View entity profile** on one of the feed items
   - *"James can drill into any entity to see its full risk profile — industry, jurisdiction, CIK, risk signal count, and filing links."*
   - Point out the cross-table join: regulatory_actions + company_entities + company_risk_signals

4. Scroll to a **locked item** (FDA or USPTO source)
   - *"Notice the locked items — unsubscribed data sources appear as teasers with an upgrade CTA. This mirrors Bloomberg Terminal's module-locked experience. The subscription tier gates access at the API level."*

5. Switch to the **Genie** tab
   - Ask: *"Which companies had the most SEC filings this quarter?"*
   - *"From the feed to Genie for investigation — the flow feels natural. James goes from monitoring to analysis seamlessly."*

6. Switch to **Data Catalog** tab
   - *"For the technically curious, the full schema documentation is still available — but it's no longer the first thing James sees."*

#### As Dr. Anika Park (Research Director, NIH — Oncology Informatics) — 3 min

> Thread: Anika sees the shape of the literature before she asks a question — the Web of Science "Analyze Results" experience.

1. Click **Dr. Anika Park** in the profile switcher
2. The **Research Overview** tab opens — the new default landing
   - *"Anika doesn't start with a blank search box — she starts with her landscape. 5,000+ articles, citation counts, author profiles, and trending MeSH topics — all at a glance."*
   - Point out the KPI ribbon: total articles, total citations, total authors, peer-reviewed percentage
   - *"This is bibliometric intelligence — the same kind of corpus-level analytics that defines Clarivate's Web of Science."*

3. Point out the **Publication Trend** chart
   - *"The publication trend shows output over time — Anika can see the field's trajectory before asking a single question."*

4. Point out **Top Journals** and **Top Authors by h-index**
   - *"Top journals and authors with h-index — standard academic metrics, computed at scale from the governed research tables."*

5. Scroll to the **MeSH Topic Grid**
   - *"MeSH terms rendered as proportional bars — Anika can see which research topics dominate the corpus. This uses the /api/research/mesh-terms endpoint we built in Phase 2 but never surfaced until now."*
   - *"Notice 'Neoplasms' and 'Immunotherapy' are prominent — of course they are, this is an oncology informatics researcher's view."*

6. Switch to **Research Q&A** tab
   - *"The overview creates context for the AI Research Assistant. Anika saw oncology trending — now she asks a specific question."*
   - Click: *"What is the role of checkpoint inhibitors in treating non-small cell lung cancer?"*
   - Point out the cited sources, preprint flags, and relevance scores

7. **Open the Research Assistant Genie space** or use the Genie tab
   - Ask: *"Who are the top 5 authors by h-index in immunotherapy?"*
   - *"Genie crosses tables seamlessly — articles, authors, citations — to answer composite questions."*

#### Key Talking Points
- Databricks Apps: full-stack (FastAPI + React) deployed and managed by the platform
- **Intelligence-first UX**: each persona lands on actionable insight, not raw data
- **Foundation Model API**: used for business summarization (Sarah's brief) and RAG Q&A (Anika's research assistant)
- **Cross-table composition**: gold tables joined into entity-level intelligence (James's feed)
- **Subscription-gated value**: locked teasers for unsubscribed sources (James's locked items)
- Profile switcher: same app, different data scopes — demonstrates multi-tenant UX
- Metric Views: governed KPIs consumed by Genie, dashboards, and the AI brief consistently
- MeSH terms endpoint: built in Phase 2, now surfaced in the corpus overview

---

## Chapter 4: "The Distribution" (3 min)

> **Phase 2 — describe the concept, no live demo yet.**

### Narrative

> *"Meridian's enterprise customers don't want to use a portal — they want the data in their own environment. Delta Sharing makes this possible."*

### Talking Points

- Delta Sharing: open protocol for secure, live data sharing without copying
- Share gold tables directly to customers via Unity Catalog
- Recipient activation: customer gets a credential file, queries from their own Spark, Pandas, or BI tool
- Access revocation: when a subscription lapses, access is cut immediately
- *"No stale data floating around, no ETL pipelines to maintain, no storage costs for copies."*

---

## Chapter 5: "The Governance Story" (2 min)

### Narrative

> *"For a data provider, governance isn't optional — it's the product. Unity Catalog gives Meridian auditability, access control, and lineage across the entire platform."*

### What to Show

1. **Run notebook cell: "Table tags"** — pipe syntax over `information_schema.table_tags`
   - *"Every table is tagged with its quality tier, business unit, and whether it's a data product. These tags drive access policies and catalog discovery."*

2. **Run notebook cell: "Customer health via Metric View"**
   - *"Metric Views ensure the same KPI definitions appear everywhere — dashboards, Genie, ad-hoc SQL. No more 'which revenue number is right?' debates."*

### Key Talking Points
- Unity Catalog: single pane for access control, lineage, tagging, and sharing
- Table properties: `quality: gold`, `data_product: true` — metadata-driven governance
- System tables (Phase 2): audit logs showing who queried what and when
- Row-level security (Phase 2): different customers see different data slices

---

## Demo Complete — Summary Slide

### Key Takeaways

1. **One platform** for ingestion, transformation, governance, and distribution
2. **Medallion architecture** enforces quality at every layer — bronze, silver, gold
3. **Liquid Clustering** adapts table layout to query patterns — zero maintenance
4. **Metric Views** define KPIs once in Unity Catalog — consumed consistently by Genie, dashboards, and SQL
5. **AI/BI Dashboards** provide governed analytics built on Metric Views
6. **Genie** puts natural language on top of governed data with domain-expert instructions
7. **Databricks Apps** deliver custom UX without leaving the platform
8. **Delta Sharing** distributes live data products without copies (Phase 2)
9. **Unity Catalog** provides lineage, tags, and access control across everything

---

## Appendix: Genie Questions to Ask

### Research Assistant
- "What are the most cited articles on CRISPR gene editing?"
- "Show me recent meta-analyses on immunotherapy for lung cancer"
- "Who are the top authors by h-index in machine learning for drug discovery?"
- "What MeSH terms are trending in the last year?"
- "Find preprints about single-cell RNA sequencing"
- "Compare publication volume for breast cancer vs lung cancer research by year"

### Internal Analytics
- "What is our total pipeline value by stage?"
- "Show revenue by product line for the last 4 quarters with YoY growth"
- "Which customers have the lowest health scores?"
- "What is our Closed Won conversion rate by region?"
- "Compare API usage across products this quarter vs last quarter"
- "What is the average deal size for Research Platform vs Regulatory Feed?"

---

## Appendix: Quick Reference

| Asset | Location |
|---|---|
| Demo walkthrough notebook | `src/notebooks/01_demo_walkthrough.py` |
| Research Pipeline | Pipelines UI → "Meridian Research Pipeline" |
| Internal Pipeline | Pipelines UI → "Meridian Internal Pipeline" |
| AI/BI Dashboard | Dashboards → "Meridian Internal Analytics" |
| Meridian Portal App | `https://meridian-portal-7474650913311373.aws.databricksapps.com` |
| Research Genie Space | ID: `01f118c5d7e01a32a58159a70e55160c` |
| Internal Genie Space | ID: `01f118c5ddb81e3dba76005e6020b2bc` |
| Catalog | `serverless_stable_k2zkdm_catalog` |
| Schemas | `meridian_staging`, `meridian_research`, `meridian_internal` |
