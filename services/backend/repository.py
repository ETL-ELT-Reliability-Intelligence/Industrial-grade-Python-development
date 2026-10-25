"""Application-owned read interface, separate from upstream write repositories."""

from typing import Protocol

from contracts.state import Batch

from .models import Dataset, QualityScope, ScopeQuality


class StorageUnavailable(Exception):
    """Storage is not configured, reachable, or ready for these queries."""


class DatasetNotFound(Exception):
    """The requested dataset is absent from the catalog."""


class ReadRepository(Protocol):
    def ready(self) -> None: ...

    def list_datasets(self, limit: int, offset: int) -> list[Dataset]: ...

    def get_dataset(self, dataset_id: str) -> Dataset | None: ...

    def list_batches(self, dataset_id: str, limit: int, offset: int) -> list[Batch]: ...

    def list_quality_scopes(
        self, dataset_id: str, limit: int, offset: int
    ) -> list[QualityScope]: ...

    def get_quality(self, dataset_id: str, scope_id: str) -> ScopeQuality: ...
