"""Optional profiling lifecycle; a calculation failure never changes quality."""

import json
from pathlib import Path
from uuid import uuid4

from contracts.features import FeatureDefinition, ProfilingAttempt
from data_quality.history import eligible
from storage.feature_profiles import InputConflict
from .feature_calculation import build_feature_profile, implementation_version
from .raw import read_raw


def load_feature_definitions(path=None):
    path = path or Path(__file__).resolve().parents[2] / "config/features.json"
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return {
        name: FeatureDefinition.model_validate_json(json.dumps(value))
        for name, value in data.items()
    }


class FeatureAccumulator:
    def __init__(self, repository, clock, definitions=None, implementation=None):
        self.repository = repository
        self.clock = clock
        self.definitions = (
            load_feature_definitions() if definitions is None else definitions
        )
        self.implementation = implementation or implementation_version()

    def collect(self, evaluation, records=None):
        event = evaluation.event
        definition = self.definitions.get(event.dataset_id, FeatureDefinition())
        attempt = ProfilingAttempt(
            attempt_id=str(uuid4()),
            evaluation_id=evaluation.evaluation_id,
            definition_hash=definition.fingerprint,
            implementation_version=self.implementation,
            started_at=self.clock(),
        )
        self.repository.save_profiling_attempt(attempt)
        # Storage failures propagate so delivery can be retried, without revising
        # the already persisted quality decision. Only calculation/input failures
        # become profiling statuses.
        try:
            if records is None:
                if event.raw_uri is None:
                    raise ValueError("missing_raw_uri")
                records = read_raw(event.raw_uri)
            sha256 = records.sha256
        except Exception as error:
            checksum = getattr(error, "sha256", None)
            if checksum is not None:
                try:
                    if (
                        evaluation.raw_sha256 is not None
                        and evaluation.raw_sha256 != checksum
                    ):
                        raise InputConflict(
                            "raw bytes differ from the completed quality evaluation"
                        )
                    self.repository.register_feature_input(
                        event.dataset_id, event.batch_id, checksum
                    )
                except InputConflict as conflict:
                    return self._failed(attempt, conflict, "CONFLICT")
            return self._failed(attempt, error)
        try:
            if evaluation.raw_sha256 is not None and evaluation.raw_sha256 != sha256:
                raise InputConflict(
                    "raw bytes differ from the completed quality evaluation"
                )
            self.repository.register_feature_input(
                event.dataset_id, event.batch_id, sha256
            )
        except InputConflict as error:
            return self._failed(attempt, error, "CONFLICT")
        profile = self.repository.get_feature_profile(
            event.dataset_id,
            event.batch_id,
            sha256,
            definition.fingerprint,
            self.implementation,
        )
        if profile is None:
            try:
                profile = build_feature_profile(
                    event, records, definition, sha256, self.implementation
                )
            except Exception as error:
                return self._failed(attempt, error)
            profile = self.repository.save_feature_profile(profile)
        self.repository.link_feature_evaluation(
            evaluation.evaluation_id, profile.profile_id, eligible(evaluation)
        )
        attempt = attempt.model_copy(
            update={
                "status": "COMPLETED",
                "finished_at": self.clock(),
                "profile_id": profile.profile_id,
            }
        )
        self.repository.save_profiling_attempt(attempt)
        return attempt

    def _failed(self, attempt, error, status="ERROR"):
        attempt = attempt.model_copy(
            update={
                "status": status,
                "finished_at": self.clock(),
                "error": f"{type(error).__name__}: {error}",
            }
        )
        self.repository.save_profiling_attempt(attempt)
        return attempt
