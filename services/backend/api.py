"""Typed HTTP boundary for dataset and quality queries."""

from collections.abc import Iterator
from typing import Annotated

from fastapi import APIRouter, Depends, Query, Request

from contracts.state import Batch, PipelineRun

from .models import (
    Dataset,
    EvaluationDetail,
    EvaluationSummary,
    Page,
    Pipeline,
    QualityScope,
    ScopeQuality,
)
from .queries import DatasetQueries, EvaluationQueries, PipelineQueries
from .repository import ReadRepository

router = APIRouter(prefix="/api/v1", tags=["datasets"])
pipeline_router = APIRouter(prefix="/api/v1", tags=["pipelines"])


def get_repository(request: Request) -> Iterator[ReadRepository]:
    with request.app.state.repository_factory() as repository:
        yield repository


Repository = Annotated[ReadRepository, Depends(get_repository)]


def get_queries(repository: Repository) -> DatasetQueries:
    return DatasetQueries(repository)


Queries = Annotated[DatasetQueries, Depends(get_queries)]
Limit = Annotated[int, Query(ge=1, le=200)]
Offset = Annotated[int, Query(ge=0)]
ScopeId = Annotated[str, Query(min_length=1, pattern=r"\S")]


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
    scope_id: ScopeId,
) -> ScopeQuality:
    return queries.quality(dataset_id, scope_id)


def get_evaluation_queries(
    repository: Repository, datasets: Queries
) -> EvaluationQueries:
    return EvaluationQueries(repository, datasets)


Evaluations = Annotated[EvaluationQueries, Depends(get_evaluation_queries)]


@router.get(
    "/datasets/{dataset_id}/evaluations", response_model=Page[EvaluationSummary]
)
def evaluations(
    dataset_id: str,
    scope_id: ScopeId,
    queries: Evaluations,
    limit: Limit = 50,
    offset: Offset = 0,
) -> Page[EvaluationSummary]:
    return queries.history(dataset_id, scope_id, limit, offset)


@router.get(
    "/datasets/{dataset_id}/evaluations/{evaluation_id}",
    response_model=EvaluationDetail,
)
def evaluation(
    dataset_id: str, evaluation_id: str, queries: Evaluations
) -> EvaluationDetail:
    return queries.detail(dataset_id, evaluation_id)


def get_pipeline_queries(repository: Repository) -> PipelineQueries:
    return PipelineQueries(repository)


Pipelines = Annotated[PipelineQueries, Depends(get_pipeline_queries)]
PipelineId = Annotated[str, Query(min_length=1, pattern=r"\S")]


@pipeline_router.get("/pipelines", response_model=Page[Pipeline])
def pipelines(
    queries: Pipelines, limit: Limit = 50, offset: Offset = 0
) -> Page[Pipeline]:
    return queries.pipelines(limit, offset)


@pipeline_router.get("/pipeline-runs", response_model=Page[PipelineRun])
def pipeline_runs(
    pipeline_id: PipelineId,
    queries: Pipelines,
    limit: Limit = 50,
    offset: Offset = 0,
) -> Page[PipelineRun]:
    return queries.runs(pipeline_id, limit, offset)
