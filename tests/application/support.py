"""Explicit test double; never imported by the running application."""

from contextlib import contextmanager
from datetime import datetime, timezone

from contracts.quality import (
    NullRateConstraint, NullRateObservation, QualityContext, RowCountConstraint, RowCountObservation,
)
from contracts.state import Batch
from data_quality.checks.null_rate import check_null_rate
from data_quality.checks.row_count import check_row_count
from data_quality.policies import evaluate_quality
from services.backend.models import Dataset, QualityScope, ScopeQuality, StoredCheck

NOW = datetime(2026, 10, 12, 10, tzinfo=timezone.utc)


def evidence(dataset_id="orders", scope_id="window-1"):
    context = QualityContext(dataset_id=dataset_id, scope_id=scope_id)
    return [
        check_row_count(RowCountObservation(row_count=0), RowCountConstraint(min_count=100), check_id="volume", context=context),
        check_null_rate(NullRateObservation(null_rate=None), NullRateConstraint(max_null_rate=0.1), check_id="nulls",
                        context=QualityContext(dataset_id=dataset_id, scope_id=scope_id, column="email")),
    ]


class MemoryRepository:
    def __init__(self):
        self.datasets = [Dataset(dataset_id=name, created_at=NOW) for name in ("empty", "orders")]
        self.batches = [Batch(dataset_id="orders", batch_id="batch-1", records=0, received_at=NOW, schema_version=1)]
        results = evidence()
        self.qualities = {("orders", "window-1"): ScopeQuality(
            dataset_id="orders", scope_id="window-1",
            checks=[StoredCheck(result=item, recorded_at=NOW) for item in results],
            decision=evaluate_quality(results), decided_at=NOW,
        )}

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
        items = [QualityScope(scope_id=scope, last_saved_at=NOW) for dataset, scope in self.qualities if dataset == dataset_id]
        return items[offset:offset + limit]

    def get_quality(self, dataset_id, scope_id):
        return self.qualities.get((dataset_id, scope_id), ScopeQuality(
            dataset_id=dataset_id, scope_id=scope_id, checks=[], decision=None, decided_at=None,
        ))
