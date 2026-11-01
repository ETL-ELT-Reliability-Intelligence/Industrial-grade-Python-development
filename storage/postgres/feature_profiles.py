"""Durable successful snapshots, independent attempts and evaluation associations."""

import json

from psycopg.types.json import Jsonb

from contracts.features import FeatureProfile
from storage.feature_profiles import InputConflict


class FeatureRepositoryMixin:
    def register_feature_input(self, dataset_id, batch_id, sha256):
        self._ensure_dataset(dataset_id)
        self._connection.execute(
            "INSERT INTO feature_inputs VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
            (dataset_id, batch_id, sha256),
        )
        row = self._connection.execute(
            "SELECT raw_sha256 FROM feature_inputs WHERE dataset_id = %s AND batch_id = %s",
            (dataset_id, batch_id),
        ).fetchone()
        if row["raw_sha256"] != sha256:
            raise InputConflict("batch identity already refers to different raw bytes")

    def get_feature_profile(
        self, dataset_id, batch_id, sha256, definition_hash, implementation
    ):
        row = self._connection.execute(
            "SELECT profile FROM feature_profiles WHERE dataset_id = %s AND batch_id = %s "
            "AND raw_sha256 = %s AND definition_hash = %s AND implementation_version = %s",
            (dataset_id, batch_id, sha256, definition_hash, implementation),
        ).fetchone()
        return (
            None
            if row is None
            else FeatureProfile.model_validate_json(json.dumps(row["profile"]))
        )

    def save_feature_profile(self, profile):
        with self._connection.transaction():
            self.register_feature_input(
                profile.dataset_id, profile.batch_id, profile.raw_sha256
            )
            self._connection.execute(
                "INSERT INTO feature_profiles VALUES (%s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT DO NOTHING",
                (
                    profile.profile_id,
                    profile.dataset_id,
                    profile.batch_id,
                    profile.raw_sha256,
                    profile.definition_hash,
                    profile.implementation_version,
                    Jsonb(profile.model_dump(mode="json")),
                ),
            )
            stored = self.get_feature_profile(
                profile.dataset_id,
                profile.batch_id,
                profile.raw_sha256,
                profile.definition_hash,
                profile.implementation_version,
            )
            if stored is None:
                raise ValueError("profile ID collision")
            return stored

    def save_profiling_attempt(self, attempt):
        self._connection.execute(
            "INSERT INTO profiling_attempts VALUES (%s, %s, %s) "
            "ON CONFLICT (attempt_id) DO UPDATE SET attempt = EXCLUDED.attempt",
            (
                attempt.attempt_id,
                attempt.evaluation_id,
                Jsonb(attempt.model_dump(mode="json")),
            ),
        )

    def link_feature_evaluation(self, evaluation_id, profile_id, eligible):
        self._connection.execute(
            "INSERT INTO feature_profile_evaluations VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
            (evaluation_id, profile_id, eligible),
        )
