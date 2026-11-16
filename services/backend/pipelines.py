"""Bounded application reads over stored pipeline runs; no dataset association."""

from typing import Any

import psycopg

from contracts.state import PipelineRun, PipelineRunStatus

from .models import Pipeline


class PostgresPipelineReader:
    def __init__(self, connection: psycopg.Connection[dict[str, Any]]) -> None:
        self.connection = connection

    def list_pipelines(self, limit: int, offset: int) -> list[Pipeline]:
        rows = self.connection.execute(
            "SELECT DISTINCT pipeline_id FROM pipeline_runs "
            "ORDER BY pipeline_id ASC LIMIT %s OFFSET %s",
            (limit, offset),
        ).fetchall()
        return [Pipeline(**row) for row in rows]

    def list_pipeline_runs(
        self, pipeline_id: str, limit: int, offset: int
    ) -> list[PipelineRun]:
        rows = self.connection.execute(
            "SELECT pipeline_id, run_id, status, started_at, finished_at, error "
            "FROM pipeline_runs WHERE pipeline_id = %s "
            "ORDER BY started_at DESC, run_id DESC LIMIT %s OFFSET %s",
            (pipeline_id, limit, offset),
        ).fetchall()
        return [
            PipelineRun(**{**row, "status": PipelineRunStatus(row["status"])})
            for row in rows
        ]
