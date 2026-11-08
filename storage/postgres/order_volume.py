"""Persistent model versions and idempotent regression observations."""

from psycopg.types.json import Jsonb
from services.ml.volume import VERSION, HOUR
from storage.postgres.repositories import PostgresMetricRepository


class PostgresOrderVolumeRepository:
    def __init__(self, connection):
        self.connection = connection

    def save_model(self, model):
        metadata = model["metadata"]
        self.connection.execute(
            "INSERT INTO order_volume_models "
            "(model_version, dataset_id, source_id, cutoff, metadata, artifact) "
            "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
            (model["model_version"], metadata["dataset_id"], metadata["source_id"],
             metadata["cutoff"], Jsonb(metadata), model["artifact"]),
        )

    def find_model(self, event):
        return self.connection.execute(
            "SELECT model_version, metadata, artifact FROM order_volume_models "
            "WHERE dataset_id = %s AND source_id = %s AND cutoff <= %s "
            "AND metadata->>'implementation' = %s "
            "ORDER BY cutoff DESC, model_version DESC LIMIT 1",
            (event.dataset_id, event.source_id, event.period_start, VERSION),
        ).fetchone()

    def load_history(self, start):
        return PostgresMetricRepository(self.connection).list_profiles_between(
            "mock_orders", source_id="mock", start=start - 168 * HOUR,
            end=start, as_of=start, limit=500,
        )

    def get_prediction(self, evaluation_id):
        row = self.connection.execute(
            "SELECT result FROM order_volume_predictions WHERE evaluation_id = %s", (evaluation_id,),
        ).fetchone()
        return row["result"] if row else None

    def save_prediction(self, evaluation_id, result):
        self.connection.execute(
            "INSERT INTO order_volume_predictions (evaluation_id, result) VALUES (%s, %s) "
            "ON CONFLICT DO NOTHING", (evaluation_id, Jsonb(result)),
        )
