"""Read evaluation history without loading execution payloads for list pages."""

from typing import Any

import psycopg

from storage.postgres import PostgresQualityRepository

from .models import EvaluationDetail, EvaluationSummary


class PostgresEvaluationReader:
    def __init__(self, connection: psycopg.Connection[dict[str, Any]]) -> None:
        self.connection = connection

    def list_evaluations(
        self, dataset_id: str, scope_id: str, limit: int, offset: int
    ) -> list[EvaluationSummary]:
        rows = self.connection.execute(
            "SELECT evaluation_id, dataset_id, scope_id, created_at, updated_at, "
            "snapshot->>'state' AS state, snapshot->>'evaluated_at' AS evaluated_at, "
            "snapshot->>'config_hash' AS config_hash, "
            "snapshot->'decision'->>'status' AS decision_status "
            "FROM quality_evaluations WHERE dataset_id = %s AND scope_id = %s "
            "ORDER BY sequence DESC LIMIT %s OFFSET %s",
            (dataset_id, scope_id, limit, offset),
        ).fetchall()
        return [EvaluationSummary(**row) for row in rows]

    def get_evaluation(
        self, dataset_id: str, evaluation_id: str
    ) -> EvaluationDetail | None:
        record = self.connection.execute(
            "SELECT dataset_id, scope_id, created_at, updated_at FROM quality_evaluations "
            "WHERE dataset_id = %s AND evaluation_id = %s",
            (dataset_id, evaluation_id),
        ).fetchone()
        if record is None:
            return None
        # Shared storage overlays individually saved executions on the frozen plan.
        evaluation = PostgresQualityRepository(self.connection).get_evaluation(
            evaluation_id
        )
        return EvaluationDetail(**record, evaluation=evaluation)
