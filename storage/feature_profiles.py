"""Persistence boundary for optional batch feature accumulation."""

from typing import Protocol

from contracts.features import FeatureProfile, ProfilingAttempt


class InputConflict(ValueError):
    """A previously observed batch identity now refers to different raw bytes."""


class FeatureRepository(Protocol):
    def register_feature_input(
        self, dataset_id: str, batch_id: str, sha256: str
    ) -> None: ...

    def get_feature_profile(
        self,
        dataset_id: str,
        batch_id: str,
        sha256: str,
        definition_hash: str,
        implementation: str,
    ) -> FeatureProfile | None: ...

    def save_feature_profile(self, profile: FeatureProfile) -> FeatureProfile: ...

    def save_profiling_attempt(self, attempt: ProfilingAttempt) -> None: ...

    def link_feature_evaluation(
        self, evaluation_id: str, profile_id: str, eligible: bool
    ) -> None: ...
