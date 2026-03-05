# Regulatory Pipeline — Phase 2

This directory will contain the SDP pipeline files for the Meridian Regulatory business unit:

- `bronze_sec_filings.py` — SEC EDGAR filings via Auto Loader
- `bronze_fda_actions.py` — FDA openFDA enforcement actions via API-staged files
- `bronze_patents.py` — USPTO PatentsView via COPY INTO
- `silver_regulatory.py` — Cleansing, entity normalization, quality expectations
- `gold_regulatory.py` — `regulatory_actions`, `patent_landscape`, `company_entities`, `company_risk_signals`

Schemas are already defined in `src/common/schemas.py`. Data fetch scripts are stubbed in `src/data_fetch/`.
