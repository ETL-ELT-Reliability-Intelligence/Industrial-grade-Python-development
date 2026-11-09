"""Application-owned read interface, separate from upstream write repositories."""

from typing import Protocol

from contracts.state import Batch, PipelineRun

from .models import (
    Dataset,
    EvaluationDetail,
    EvaluationSummary,
    Pipeline,
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
    """Read evaluation summaries and details within a dataset."""

    def list_evaluations(
        self, dataset_id: str, scope_id: str, limit: int, offset: int
    ) -> list[EvaluationSummary]:
        """Return scope evaluations by descending creation sequence, with pagination."""
        ...

    def get_evaluation(
        self, dataset_id: str, evaluation_id: str
    ) -> EvaluationDetail | None:
        """Return evaluation details, or None if absent or owned by another dataset."""
        ...


class DatasetReadRepository(Protocol):
    """Read dataset metadata and stored quality evidence without evaluating rules."""

    def list_datasets(self, limit: int, offset: int) -> list[Dataset]:
        """Return datasets ordered by identifier with the requested limit and offset."""
        ...

    def get_dataset(self, dataset_id: str) -> Dataset | None:
        """Return dataset metadata, or None if absent."""
        ...

    def list_batches(self, dataset_id: str, limit: int, offset: int) -> list[Batch]:
        """Return batches by descending receipt time and batch ID, with pagination."""
        ...

    def list_quality_scopes(
        self, dataset_id: str, limit: int, offset: int
    ) -> list[QualityScope]:
        """Return scopes by latest save time descending, breaking ties by scope ID."""
        ...

    def get_quality(self, dataset_id: str, scope_id: str) -> ScopeQuality:
        """Return stored checks and an optional decision; absent evidence stays empty."""
        ...


class PipelineReadRepository(Protocol):
    def list_pipelines(self, limit: int, offset: int) -> list[Pipeline]: ...

    def list_pipeline_runs(
        self, pipeline_id: str, limit: int, offset: int
    ) -> list[PipelineRun]: ...


class ReadRepository(
    DatasetReadRepository, EvaluationRepository, PipelineReadRepository, Protocol
):
    def ready(self) -> None: ...
