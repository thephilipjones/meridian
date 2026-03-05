# Meridian Insights — Presenter's Demo Script

> **Total Time:** ~20 minutes (can be shortened to ~10 by condensing Chapters 2 and 4)

---

## Before the Demo

1. Confirm pipelines have run and gold tables are populated
2. Open three browser tabs:
   - Databricks workspace (for pipeline graph, lineage, catalog explorer)
   - Meridian Portal app
   - Demo walkthrough notebook (`01_demo_walkthrough.py`)
3. Set the notebook's `meridian.catalog` config if not using default

---

## Chapter 1: "The Data Challenge" (3 min)

**Talking point:**
> "Meridian Insights is a data provider — think Bloomberg, Clarivate, S&P Global. They ingest messy raw data from dozens of sources and turn it into curated, trustworthy data products that their customers pay for. Let's see how they do it on Databricks."

**Show:**
- Staging volumes in Catalog Explorer — point out the variety (JSON, XML, CSV)
- Raw files in the PubMed volume vs CRM CSV vs web event JSON
- Emphasize: different formats, different cadences, different quality levels

**Key message:** "Real data companies deal with messy heterogeneous sources. Databricks handles all of them."

---

## Chapter 2: "The Pipeline" (5 min)

**Talking point:**
> "Meridian uses Spark Declarative Pipelines to build a medallion architecture — bronze for raw ingestion, silver for cleansing and enrichment, gold for business-ready data products."

**Show:**
1. **Pipeline graph in UI** — walk through bronze → silver → gold flow
2. **Auto Loader** — show PubMed bronze table with `_source_file` and `_ingest_timestamp`
3. **COPY INTO contrast** — show CRM bronze loaded from CSVs (different pattern, same framework)
4. **Quality gates** — show the expectations on the silver layer. Optional: trigger a failure by uploading a malformed file
5. **Lineage** — navigate to `meridian.research.articles` in Catalog Explorer, click Lineage tab. Trace back through silver to the raw PubMed source

**Key message:** "Data providers live and die by quality. SDP gives you declarative quality gates, automatic lineage, and multiple ingestion patterns in one framework."

**Fallback query (if UI is slow):**
```sql
SELECT title, journal, publication_year, publication_type, citation_count
FROM meridian.research.articles
ORDER BY citation_count DESC LIMIT 10
```

---

## Chapter 3: "The Portal" (7 min)

**Talking point:**
> "Meridian's customers and internal teams interact with data through the Meridian Portal — built as a Databricks App."

### As Sarah Chen (Internal — RevOps)

**Show:**
- Sales dashboard with pipeline by stage, revenue trend
- Product usage heatmap
- Customer health table with red/yellow/green tiers

**Genie question:**
> "What was Q4 revenue by product line compared to last year?"

**Fallback query:**
```sql
SELECT product_line, revenue, yoy_revenue_growth
FROM meridian.internal.revenue_summary
WHERE fiscal_quarter = 'Q4'
ORDER BY fiscal_year DESC
```

### Switch to Dr. Anika Park (Research)

**Show:**
- Research Q&A interface
- Paper Browser with filters

**Genie question:**
> "What are the latest findings on CRISPR off-target effects?"

**Point out:**
- Preprint vs peer-reviewed badges
- Study type classification (RCT, meta-analysis, etc.)
- DOI links for traceability

**Key message:** "This is what it looks like when you put Genie in front of 36 million research articles with expert instructions guiding the interpretation."

### Switch to James Rivera (Regulatory — Phase 2)

**If Phase 2 is ready**, show:
- Data catalog with subscribed vs unsubscribed products
- Scoped regulatory Genie
- Delta Sharing connection info

**If Phase 2 is not ready**, say:
> "James would see a data product catalog showing exactly what his subscription includes, with connection instructions for pulling live data into his own environment via Delta Sharing."

---

## Chapter 4: "The Distribution" (3 min)

**Talking point:**
> "Meridian's enterprise customers don't want to use a portal — they want the data in their own environment. Delta Sharing makes this possible."

**Show (if Phase 2 ready):**
1. Share in Unity Catalog containing gold tables
2. Consumer notebook querying shared data from a separate catalog
3. Access revocation — remove a table, show immediate effect

**Key message:** "When a subscription lapses, access is cut immediately — no stale data floating around."

---

## Chapter 5: "The Governance Story" (2 min)

**Talking point:**
> "For a data provider, governance isn't optional — it's the product. Unity Catalog gives Meridian auditability, access control, and lineage across the entire platform."

**Show:**
- Table tags: `data_product:true`, `quality:gold`, `business_unit:research`
- Row-level security concept (James sees only SEC data, not FDA)
- System tables: who queried what, when

**Key message:** "Every table, every query, every access decision is tracked. This is what enterprise-grade governance looks like."

---

## Closing (1 min)

> "Everything you've seen — the pipelines, the app, the Genie spaces, the sharing, the governance — is running on a single Databricks workspace, deployed from a single Asset Bundle. Clone the repo, run `databricks bundle deploy`, and you have the full platform in your own workspace."

---

## Adapting for Specific Customers

| Customer | Lead with | Emphasize | De-emphasize |
|---|---|---|---|
| **Bloomberg Industry Group** | Regulatory + Internal | Data product curation, governance, customer distribution | Research (mention briefly) |
| **Clarivate** | Research + Regulatory | Academic Q&A, citation analytics, research Genie | Internal analytics (mention briefly) |
| **Generic data company** | All three equally | Platform breadth, deployability | None |
