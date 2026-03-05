"""Data catalog API endpoints for the customer regulatory view.

Phase 2 stub — will provide browsable data product catalog with
subscription-aware access controls.

TODO Phase 2: Implement data catalog endpoints with subscription tier filtering
"""

from fastapi import APIRouter

router = APIRouter()


@router.get("/products")
def list_data_products():
    """List available data products with subscription status."""
    return {"message": "Phase 2: Data catalog not yet implemented"}


@router.get("/products/{product_id}")
def get_product_detail(product_id: str):
    """Get product schema, sample records, and freshness info."""
    return {"message": f"Phase 2: Product detail for {product_id} not yet implemented"}
