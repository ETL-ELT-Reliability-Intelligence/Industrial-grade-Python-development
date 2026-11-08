"""Versioned batch summaries used to build reproducible historical baselines."""

from pydantic import Field
from contracts.base import Contract
from contracts.events import DatasetIngestedEvent


class BatchProfile(Contract):
    event: DatasetIngestedEvent
    stats_version: str = "v2"
    config_hash: str
    row_count: int = Field(ge=0)
    columns: dict[str, dict] = Field(default_factory=dict)
    checks: dict[str, dict] = Field(default_factory=dict)
    eligible: bool = False
    order_volume: dict | None = None
