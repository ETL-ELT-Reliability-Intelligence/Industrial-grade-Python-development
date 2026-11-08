"""Read use cases: existence checks and bounded list envelopes, no quality policy."""

from typing import TypeVar

from contracts.state import Batch

from .models import (
    Dataset,
    EvaluationDetail,
    EvaluationSummary,
    Page,
    QualityScope,
    ScopeQuality,
)
from .repository import (
    DatasetNotFound,
    DatasetReadRepository,
    EvaluationNotFound,
    EvaluationRepository,
)

T = TypeVar("T")


def page(items: list[T], limit: int, offset: int) -> Page[T]:
    """Build a page from an offset-selected slice containing up to limit + 1 items."""
    return Page(
        items=items[:limit], limit=limit, offset=offset, has_more=len(items) > limit
    )


class DatasetQueries:
    """Read dataset evidence, checking dataset existence before dependent queries."""

    def __init__(self, repository: DatasetReadRepository) -> None:
        self.repository = repository

    def datasets(self, limit: int, offset: int) -> Page[Dataset]:
        return page(self.repository.list_datasets(limit + 1, offset), limit, offset)

    def dataset(self, dataset_id: str) -> Dataset:
        """Return dataset metadata or raise DatasetNotFound."""
        item = self.repository.get_dataset(dataset_id)
        if item is None:
            raise DatasetNotFound
        return item

    def batches(self, dataset_id: str, limit: int, offset: int) -> Page[Batch]:
        self.dataset(dataset_id)
        return page(
            self.repository.list_batches(dataset_id, limit + 1, offset), limit, offset
        )

    def scopes(self, dataset_id: str, limit: int, offset: int) -> Page[QualityScope]:
        self.dataset(dataset_id)
        return page(
            self.repository.list_quality_scopes(dataset_id, limit + 1, offset),
            limit,
            offset,
        )

    def quality(self, dataset_id: str, scope_id: str) -> ScopeQuality:
        self.dataset(dataset_id)
        return self.repository.get_quality(dataset_id, scope_id)


class EvaluationQueries:
    """Read evaluation history and details after verifying dataset existence."""

    def __init__(
        self, repository: EvaluationRepository, datasets: DatasetQueries
    ) -> None:
        self.repository = repository
        self.datasets = datasets

    def history(
        self, dataset_id: str, scope_id: str, limit: int, offset: int
    ) -> Page[EvaluationSummary]:
        self.datasets.dataset(dataset_id)
        return page(
            self.repository.list_evaluations(dataset_id, scope_id, limit + 1, offset),
            limit,
            offset,
        )

    def detail(self, dataset_id: str, evaluation_id: str) -> EvaluationDetail:
        """Return dataset-owned evaluation details or raise the matching not-found error."""
        self.datasets.dataset(dataset_id)
        evaluation = self.repository.get_evaluation(dataset_id, evaluation_id)
        if evaluation is None:
            raise EvaluationNotFound
        return evaluation
