"""FastAPI application for the Meridian Portal.

Serves the React frontend as static files and provides API endpoints
for analytics, research, and profile management.
"""

from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from backend.profiles import get_all_profiles
from backend.routers import analytics, catalog, genie, research, sharing

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
app.include_router(genie.router, prefix="/api/genie", tags=["Genie"])


@app.get("/api/profiles")
def list_profiles():
    """Return all available demo profiles for the frontend switcher."""
    return get_all_profiles()


@app.get("/api/health")
def health_check():
    return {"status": "ok", "app": "meridian-portal"}



def _find_frontend_dist() -> Path | None:
    candidates = [
        Path(__file__).parent.parent / "frontend" / "dist",
        Path.cwd() / "frontend" / "dist",
    ]
    for p in candidates:
        if p.exists() and (p / "index.html").exists():
            return p
    return None


_dist = _find_frontend_dist()
if _dist:
    app.mount("/assets", StaticFiles(directory=str(_dist / "assets")), name="static-assets")

    @app.get("/{full_path:path}")
    async def serve_frontend(full_path: str):
        """Serve static files from dist, or fall back to index.html for SPA routing."""
        file_path = _dist / full_path
        if full_path and file_path.is_file():
            return FileResponse(str(file_path))
        return FileResponse(str(_dist / "index.html"))
