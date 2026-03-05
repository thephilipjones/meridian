"""FastAPI application for the Meridian Portal.

Serves the React frontend as static files and provides API endpoints
for analytics, research, and profile management.
"""

import os
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from src.app.backend.profiles import get_all_profiles
from src.app.backend.routers import analytics, catalog, research, sharing

app = FastAPI(
    title="Meridian Portal",
    description="Meridian Insights — unified portal for regulatory intelligence, research analytics, and internal operations",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(analytics.router, prefix="/api/analytics", tags=["Internal Analytics"])
app.include_router(research.router, prefix="/api/research", tags=["Research"])
app.include_router(catalog.router, prefix="/api/catalog", tags=["Data Catalog"])
app.include_router(sharing.router, prefix="/api/sharing", tags=["Delta Sharing"])


@app.get("/api/profiles")
def list_profiles():
    """Return all available demo profiles for the frontend switcher."""
    return get_all_profiles()


@app.get("/api/health")
def health_check():
    return {"status": "ok", "app": "meridian-portal"}


FRONTEND_DIR = Path(__file__).parent.parent / "frontend" / "dist"
if FRONTEND_DIR.exists():
    app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")
