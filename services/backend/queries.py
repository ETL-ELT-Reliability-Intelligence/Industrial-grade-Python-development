"""Read use cases: existence checks and bounded list envelopes, no quality policy."""

from typing import TypeVar

from contracts.state import Batch

from .models import Dataset, Page, QualityScope, ScopeQuality
from .repository import DatasetNotFound, ReadRepository

T = TypeVar("T")


def page(items: list[T], limit: int, offset: int) -> Page[T]:
    return Page(items=items[:limit], limit=limit, offset=offset, has_more=len(items) > limit)


class DatasetQueries:
    def __init__(self, repository: ReadRepository) -> None:
        self.repository = repository

    def datasets(self, limit: int, offset: int) -> Page[Dataset]:
        return page(self.repository.list_datasets(limit + 1, offset), limit, offset)

    def dataset(self, dataset_id: str) -> Dataset:
        item = self.repository.get_dataset(dataset_id)
        if item is None:
            raise DatasetNotFound
        return item

    def batches(self, dataset_id: str, limit: int, offset: int) -> Page[Batch]:
        self.dataset(dataset_id)
        return page(self.repository.list_batches(dataset_id, limit + 1, offset), limit, offset)

    def scopes(self, dataset_id: str, limit: int, offset: int) -> Page[QualityScope]:
        self.dataset(dataset_id)
        return page(self.repository.list_quality_scopes(dataset_id, limit + 1, offset), limit, offset)

    def quality(self, dataset_id: str, scope_id: str) -> ScopeQuality:
        self.dataset(dataset_id)
        return self.repository.get_quality(dataset_id, scope_id)
