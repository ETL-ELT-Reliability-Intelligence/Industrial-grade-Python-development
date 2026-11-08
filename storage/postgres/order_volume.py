"""Persistent model versions and idempotent regression observations."""

from psycopg.types.json import Jsonb
import json
from contracts.profiles import BatchProfile
from services.ml.volume import VERSION, HOUR


class PostgresOrderVolumeRepository:
    """Store versioned models and the first prediction recorded per evaluation."""

    def __init__(self, connection):
        self.connection = connection

    def save_model(self, model):
        """Insert a model version, leaving an existing version unchanged."""
        metadata = model["metadata"]
        self.connection.execute(
            "INSERT INTO order_volume_models "
            "(model_version, dataset_id, source_id, cutoff, metadata, artifact) "
            "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
            (model["model_version"], metadata["dataset_id"], metadata["source_id"],
             metadata["cutoff"], Jsonb(metadata), model["artifact"]),
        )

    def find_model(self, event):
        """Return the latest compatible model with cutoff no later than the target hour."""
        return self.connection.execute(
            "SELECT model_version, metadata, artifact FROM order_volume_models "
            "WHERE dataset_id = %s AND source_id = %s AND cutoff <= %s "
            "AND metadata->>'implementation' = %s "
            "ORDER BY cutoff DESC, model_version DESC LIMIT 1",
            (event.dataset_id, event.source_id, event.period_start, VERSION),
        ).fetchone()

    def load_history(self, start):
        """Read the prior 168 mock-source hours received no later than start."""
        rows = self.connection.execute(
            "SELECT profile FROM dataset_profiles WHERE dataset_id = 'mock_orders' "
            "AND profile->'event'->>'source_id' = 'mock' "
            "AND (profile->'event'->>'period_start')::timestamptz >= %s "
            "AND (profile->'event'->>'period_start')::timestamptz < %s "
            "AND (profile->'event'->>'received_at')::timestamptz <= %s",
            (start - 168 * HOUR, start, start),
        ).fetchall()
        return [BatchProfile.model_validate_json(json.dumps(row["profile"])) for row in rows]

    def get_prediction(self, evaluation_id):
        """Return the saved evaluation prediction, or None if absent."""
        row = self.connection.execute(
            "SELECT result FROM order_volume_predictions WHERE evaluation_id = %s", (evaluation_id,),
        ).fetchone()
        return row["result"] if row else None

    def save_prediction(self, evaluation_id, result):
        """Save the first prediction for an evaluation; later writes do not replace it."""
        self.connection.execute(
            "INSERT INTO order_volume_predictions (evaluation_id, result) VALUES (%s, %s) "
            "ON CONFLICT DO NOTHING", (evaluation_id, Jsonb(result)),
        )
