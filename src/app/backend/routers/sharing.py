"""Delta Sharing info endpoints for the customer view.

Phase 2 stub — will provide connection instructions and
code snippets for accessing shared data.

TODO Phase 2: Implement Delta Sharing connection info and code snippets
"""

from fastapi import APIRouter

router = APIRouter()


@router.get("/connection-info")
def get_connection_info():
    """Return Delta Sharing connection details for the current customer."""
    return {"message": "Phase 2: Delta Sharing connection info not yet implemented"}


@router.get("/code-snippets")
def get_code_snippets():
    """Return pre-built code snippets for various platforms."""
    return {"message": "Phase 2: Code snippets not yet implemented"}
