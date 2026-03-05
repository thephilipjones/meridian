"""Fetch SEC EDGAR company filings (10-K, 10-Q, 8-K) via the full-text submissions API.

Phase 2 — not yet implemented. Will stage JSON files for Auto Loader ingestion.
Rate limit: 10 requests/sec with User-Agent header.

TODO Phase 2: Implement EDGAR full-text submissions fetch
"""

from src.common.config import STAGING_PATHS


def main(output_path: str | None = None):
    path = output_path or STAGING_PATHS["sec_filings"]
    raise NotImplementedError("Phase 2: SEC EDGAR fetch not yet implemented")


if __name__ == "__main__":
    main()
