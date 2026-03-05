"""Fetch Crossref DOI metadata and citation links via REST API.

Phase 2 — not yet implemented. Will stage incremental JSON files for Auto Loader.

TODO Phase 2: Implement Crossref metadata and citation graph fetch
"""

from src.common.config import STAGING_PATHS


def main(output_path: str | None = None):
    path = output_path or STAGING_PATHS["crossref"]
    raise NotImplementedError("Phase 2: Crossref fetch not yet implemented")


if __name__ == "__main__":
    main()
