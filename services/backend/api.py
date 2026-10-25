"""Typed HTTP boundary for dataset and quality queries."""

from collections.abc import Iterator
from typing import Annotated

from fastapi import APIRouter, Depends, Query, Request

from contracts.state import Batch

from .models import Dataset, Page, QualityScope, ScopeQuality
from .queries import DatasetQueries
from .repository import ReadRepository

router = APIRouter(prefix="/api/v1", tags=["datasets"])


def get_repository(request: Request) -> Iterator[ReadRepository]:
    with request.app.state.repository_factory() as repository:
        yield repository


Repository = Annotated[ReadRepository, Depends(get_repository)]


def get_queries(repository: Repository) -> DatasetQueries:
    return DatasetQueries(repository)


Queries = Annotated[DatasetQueries, Depends(get_queries)]
Limit = Annotated[int, Query(ge=1, le=200)]
Offset = Annotated[int, Query(ge=0)]


@router.get("/datasets", response_model=Page[Dataset])
def datasets(queries: Queries, limit: Limit = 50, offset: Offset = 0) -> Page[Dataset]:
    return queries.datasets(limit, offset)


@router.get("/datasets/{dataset_id}", response_model=Dataset)
def dataset(dataset_id: str, queries: Queries) -> Dataset:
    return queries.dataset(dataset_id)


@router.get("/datasets/{dataset_id}/batches", response_model=Page[Batch])
def batches(
    dataset_id: str, queries: Queries, limit: Limit = 50, offset: Offset = 0
) -> Page[Batch]:
    return queries.batches(dataset_id, limit, offset)


@router.get("/datasets/{dataset_id}/quality-scopes", response_model=Page[QualityScope])
def scopes(
    dataset_id: str, queries: Queries, limit: Limit = 50, offset: Offset = 0
) -> Page[QualityScope]:
    return queries.scopes(dataset_id, limit, offset)


@router.get("/datasets/{dataset_id}/quality", response_model=ScopeQuality)
def quality(
    dataset_id: str,
    queries: Queries,
    scope_id: Annotated[str, Query(min_length=1, pattern=r"\S")],
) -> ScopeQuality:
    return queries.quality(dataset_id, scope_id)
