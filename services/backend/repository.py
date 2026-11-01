"""Application-owned read interface, separate from upstream write repositories."""

from typing import Protocol

from contracts.state import Batch

from .models import (
    Dataset,
    EvaluationDetail,
    EvaluationSummary,
    QualityScope,
    ScopeQuality,
)


class StorageUnavailable(Exception):
    """Storage is not configured, reachable, or ready for these queries."""


class DatasetNotFound(Exception):
    """The requested dataset is absent from the catalog."""


class EvaluationNotFound(Exception):
    """The requested evaluation does not belong to the dataset."""


class EvaluationRepository(Protocol):
    def list_evaluations(
        self, dataset_id: str, scope_id: str, limit: int, offset: int
    ) -> list[EvaluationSummary]: ...

    def get_evaluation(
        self, dataset_id: str, evaluation_id: str
    ) -> EvaluationDetail | None: ...


class DatasetReadRepository(Protocol):
    def list_datasets(self, limit: int, offset: int) -> list[Dataset]: ...

    def get_dataset(self, dataset_id: str) -> Dataset | None: ...

    def list_batches(self, dataset_id: str, limit: int, offset: int) -> list[Batch]: ...

    def list_quality_scopes(
        self, dataset_id: str, limit: int, offset: int
    ) -> list[QualityScope]: ...

    def get_quality(self, dataset_id: str, scope_id: str) -> ScopeQuality: ...


class ReadRepository(DatasetReadRepository, EvaluationRepository, Protocol):
    def ready(self) -> None: ...
