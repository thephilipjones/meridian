"""Fetch FDA openFDA drug enforcement actions, adverse events, and recalls.

Phase 2 — not yet implemented. Will stage JSON files for Auto Loader ingestion.
Free API, key optional.

TODO Phase 2: Implement openFDA REST API fetch
"""

from src.common.config import STAGING_PATHS


def main(output_path: str | None = None):
    path = output_path or STAGING_PATHS["fda_actions"]
    raise NotImplementedError("Phase 2: openFDA fetch not yet implemented")


if __name__ == "__main__":
    main()
