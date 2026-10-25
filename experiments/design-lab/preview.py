"""Explicit local design preview; real frontend/API, fixed in-memory data.

Run from the repository root: python experiments/design-lab/preview.py
Never loaded by the production backend. No automatic database fallback.
"""

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from fastapi import FastAPI  # noqa: E402
from fastapi.responses import FileResponse, HTMLResponse  # noqa: E402
from fastapi.staticfiles import StaticFiles  # noqa: E402

from contracts.quality import (  # noqa: E402
    NullRateConstraint, NullRateObservation, QualityContext,
    RowCountConstraint, RowCountObservation,
)
from contracts.state import Batch  # noqa: E402
from data_quality.checks.null_rate import check_null_rate  # noqa: E402
from data_quality.checks.row_count import check_row_count  # noqa: E402
from data_quality.policies import evaluate_quality  # noqa: E402
from services.backend.main import FRONTEND_DIR, create_app  # noqa: E402
from services.backend.models import Dataset, QualityScope, ScopeQuality, StoredCheck  # noqa: E402

NOW = datetime(2026, 10, 25, 7, 30, tzinfo=timezone.utc)
LAB = Path(__file__).resolve().parent


class PreviewRepository:
    """The read repository protocol with an explicitly fictional snapshot."""

    def __init__(self):
        self.datasets = [
            Dataset(dataset_id=name, created_at=NOW - timedelta(minutes=30))
            for name in ("sample-app-empty", "sample-app-orders")
        ]
        self.batches = [
            Batch(dataset_id="sample-app-orders", batch_id="sample-batch-2",
                  records=0, received_at=NOW - timedelta(minutes=1), schema_version=1),
            Batch(dataset_id="sample-app-orders", batch_id="sample-batch-1",
                  records=120, received_at=NOW - timedelta(minutes=2), schema_version=1),
        ]
        context = QualityContext(dataset_id="sample-app-orders", scope_id="sample-window-1")
        results = [
            check_row_count(RowCountObservation(row_count=0), RowCountConstraint(min_count=100),
                            check_id="sample-volume", context=context),
            check_null_rate(NullRateObservation(null_rate=None), NullRateConstraint(max_null_rate=0.05),
                            check_id="sample-nulls", context=context.model_copy(update={"column": "email"})),
        ]
        self.quality = ScopeQuality(
            dataset_id=context.dataset_id, scope_id=context.scope_id,
            checks=[StoredCheck(result=result, recorded_at=NOW) for result in results],
            decision=evaluate_quality(results), decided_at=NOW,
        )

    @contextmanager
    def factory(self):
        yield self

    def ready(self):
        pass

    def list_datasets(self, limit, offset):
        return self.datasets[offset:offset + limit]

    def get_dataset(self, dataset_id):
        return next((item for item in self.datasets if item.dataset_id == dataset_id), None)

    def list_batches(self, dataset_id, limit, offset):
        return [item for item in self.batches if item.dataset_id == dataset_id][offset:offset + limit]

    def list_quality_scopes(self, dataset_id, limit, offset):
        items = [QualityScope(scope_id=self.quality.scope_id, last_saved_at=NOW)] if dataset_id == self.quality.dataset_id else []
        return items[offset:offset + limit]

    def get_quality(self, dataset_id, scope_id):
        if (dataset_id, scope_id) == (self.quality.dataset_id, self.quality.scope_id):
            return self.quality
        return ScopeQuality(dataset_id=dataset_id, scope_id=scope_id, checks=[], decision=None, decided_at=None)


def create_preview() -> FastAPI:
    preview = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

    @preview.get("/", include_in_schema=False)
    def gallery():
        return FileResponse(LAB / "index.html")

    @preview.get("/app", include_in_schema=False)
    def application():
        html = (FRONTEND_DIR / "index.html").read_text(encoding="utf-8")
        return HTMLResponse(html.replace("<body>", '<body data-preview="true">'))

    @preview.get("/gallery.css", include_in_schema=False)
    def gallery_style():
        return FileResponse(LAB / "gallery.css")

    preview.mount("/screenshots", StaticFiles(directory=LAB / "screenshots"), name="screenshots")
    preview.mount("/", create_app(repository_factory=PreviewRepository().factory))
    return preview


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(create_preview(), host="127.0.0.1", port=8010)
