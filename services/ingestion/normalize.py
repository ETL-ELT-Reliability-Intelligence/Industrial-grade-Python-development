"""Basic validation and metadata normalization. No anomaly detection here."""

import hashlib
import json

from .sources import Record, SourceBatch


class ValidationError(ValueError):
    """A source batch has an empty dataset identifier or a non-object record."""

    pass


def validate(batch: SourceBatch) -> None:
    """Validate dataset identity and record shape, raising ValidationError on failure."""
    if not batch.dataset_id.strip():
        raise ValidationError("dataset_id must not be empty")
    for position, record in enumerate(batch.records):
        if not isinstance(record, dict):
            raise ValidationError(f"record {position} is not an object")


def column_names(records: list[Record]) -> list[str]:
    """Union of keys across records, sorted so the schema is order-independent."""
    return sorted({key for record in records for key in record})


def schema_fingerprint(columns: list[str]) -> str:
    """Hash the supplied column sequence; callers sort names for stable identity."""
    return hashlib.sha256(json.dumps(columns).encode()).hexdigest()[:16]
