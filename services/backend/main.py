"""Run from the repository root: python -m uvicorn services.backend.main:app."""

from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .api import router
from .repository import DemoIncidentRepository, IncidentRepository

FRONTEND_DIR = Path(__file__).resolve().parents[1] / "frontend"


def create_app(repository: IncidentRepository | None = None) -> FastAPI:
    app = FastAPI(
        title="ETL/ELT Reliability Intelligence — Application API",
        version="0.1.0",
        description="Read-only iteration 1. Demo data; no live platform connection.",
    )
    app.state.repository = repository if repository is not None else DemoIncidentRepository()
    app.include_router(router)

    @app.get("/health", tags=["system"])
    def health() -> dict[str, str]:
        return {"status": "ok", "data_mode": app.state.repository.data_mode}

    @app.get("/", include_in_schema=False)
    def dashboard() -> FileResponse:
        return FileResponse(FRONTEND_DIR / "index.html")

    app.mount("/assets", StaticFiles(directory=FRONTEND_DIR), name="assets")
    return app


app = create_app()
