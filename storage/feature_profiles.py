"""Persistence boundary for optional batch feature accumulation."""

from typing import Protocol

from contracts.features import FeatureProfile, ProfilingAttempt


class InputConflict(ValueError):
    """A previously observed batch identity now refers to different raw bytes."""


class FeatureRepository(Protocol):
    """Persist feature inputs, immutable snapshots and their evaluation associations."""

    def register_feature_input(
        self, dataset_id: str, batch_id: str, sha256: str
    ) -> None:
        """Register a batch checksum or raise InputConflict if its bytes changed."""
        ...

    def get_feature_profile(
        self,
        dataset_id: str,
        batch_id: str,
        sha256: str,
        definition_hash: str,
        implementation: str,
    ) -> FeatureProfile | None:
        """Return the snapshot matching all calculation identity fields, or None."""
        ...

    def save_feature_profile(self, profile: FeatureProfile) -> FeatureProfile:
        """Store a snapshot without replacing an existing identity; return the stored one."""
        ...

    def save_profiling_attempt(self, attempt: ProfilingAttempt) -> None:
        """Insert or update the state of the same profiling attempt."""
        ...

    def link_feature_evaluation(
        self, evaluation_id: str, profile_id: str, eligible: bool
    ) -> None:
        """Record profile eligibility for an evaluation without replacing an existing link."""
        ...
