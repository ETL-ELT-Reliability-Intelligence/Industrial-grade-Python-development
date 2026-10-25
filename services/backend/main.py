"""Run with: python -m uvicorn services.backend.main:app."""

import os
from collections.abc import Callable
from contextlib import AbstractContextManager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles

from .api import Repository, router
from .postgres import open_repository
from .repository import DatasetNotFound, ReadRepository, StorageUnavailable

FRONTEND_DIR = Path(__file__).resolve().parents[1] / "frontend"
RepositoryFactory = Callable[[], AbstractContextManager[ReadRepository]]


def create_app(*, repository_factory: RepositoryFactory | None = None) -> FastAPI:
    app = FastAPI(
        title="ETL/ELT Reliability Intelligence — Dataset API",
        version="0.2.0",
        description="Read-only access to datasets and stored quality evidence. No quality evaluation.",
    )
    dsn = os.environ.get("DATABASE_URL")
    app.state.repository_factory = repository_factory or (lambda: open_repository(dsn))
    app.include_router(router)

    @app.exception_handler(StorageUnavailable)
    async def unavailable(request: Request, exc: StorageUnavailable) -> JSONResponse:
        return JSONResponse(
            status_code=503, content={"detail": "State store unavailable"}
        )

    @app.exception_handler(DatasetNotFound)
    async def not_found(request: Request, exc: DatasetNotFound) -> JSONResponse:
        return JSONResponse(status_code=404, content={"detail": "Dataset not found"})

    @app.get("/health", tags=["system"])
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/ready", tags=["system"])
    def ready(repository: Repository) -> dict[str, str]:
        repository.ready()
        return {"status": "ok"}

    @app.get("/", include_in_schema=False)
    def dashboard() -> Response:
        if not (FRONTEND_DIR / "index.html").exists():
            return JSONResponse({"detail": "Dataset API; see /docs for endpoints"})
        return FileResponse(FRONTEND_DIR / "index.html")

    if FRONTEND_DIR.is_dir():
        app.mount("/assets", StaticFiles(directory=FRONTEND_DIR), name="assets")
    return app


app = create_app()
