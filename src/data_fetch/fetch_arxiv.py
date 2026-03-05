"""Fetch arXiv preprint metadata via OAI-PMH or REST API.

Phase 2 — not yet implemented. Will stage XML/JSON files for Auto Loader ingestion.

TODO Phase 2: Implement arXiv metadata harvest
"""

from src.common.config import STAGING_PATHS


def main(output_path: str | None = None):
    path = output_path or STAGING_PATHS["arxiv"]
    raise NotImplementedError("Phase 2: arXiv fetch not yet implemented")


if __name__ == "__main__":
    main()
