"""Pure batch features. No thresholds, quality rules or historical baselines."""

from collections import Counter
from decimal import Context, Decimal, InvalidOperation, ROUND_HALF_EVEN, localcontext
import hashlib
import json
import math
from pathlib import Path

from contracts.features import ColumnFeatures, FeatureDefinition, FeatureProfile


def implementation_version() -> str:
    """Hash the full implementation and contract sources, normalizing newlines.

    Comments and docstrings also affect this version because source text is
    hashed without stripping non-executable content.
    """
    import contracts.features as contract

    digest = hashlib.sha256()
    for path in (Path(__file__), Path(contract.__file__)):
        # A checkout's newline convention is not an implementation change.
        digest.update(path.read_text(encoding="utf-8").replace("\r\n", "\n").encode())
    return "python-decimal-v1:" + digest.hexdigest()


def canonical(value):
    """Encode values as compact JSON with stable object key order."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def numeric(value):
    """Parse a finite Decimal from a string or number, rejecting booleans."""
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise ValueError("not numeric")
    result = Decimal(str(value))
    if not result.is_finite() or not math.isfinite(float(result)):
        raise ValueError("not a finite representable number")
    return result


def calculate_column(records, column, definition):
    """Calculate a column summary using the declared numeric or category settings.

    Missing keys and nulls share the null count. Numeric parse errors are
    counted separately; categorical frequencies use non-null rows as their
    denominator. Undefined ratios and empty numeric summaries remain None.
    """
    present = [r[column.name] for r in records if r.get(column.name) is not None]
    count = len(records)
    distinct = len({canonical(v) for v in present})
    values = dict(
        name=column.name,
        missing_column=not any(column.name in r for r in records),
        null_count=count - len(present),
        non_null_count=len(present),
        distinct_count=distinct,
        null_rate=(count - len(present)) / count if count else None,
        distinct_ratio=distinct / len(present) if present else None,
    )
    if column.kind == "numeric":
        numbers = []
        for value in present:
            try:
                numbers.append(numeric(value))
            except (ValueError, InvalidOperation, OverflowError):
                pass
        numbers.sort()
        values.update(
            valid_count=len(numbers), parse_error_count=len(present) - len(numbers)
        )
        if numbers:
            # Fixed precision and sorted summation make results independent of row order.
            with localcontext(Context(prec=50, rounding=ROUND_HALF_EVEN)):
                mean = sum(numbers, Decimal(0)) / len(numbers)
                variance = sum(((v - mean) ** 2 for v in numbers), Decimal(0)) / len(
                    numbers
                )
                quantiles = []
                for q in definition.quantiles:
                    position = Decimal(str(q)) * (len(numbers) - 1)
                    lower = int(position)
                    fraction = position - lower
                    upper = min(lower + 1, len(numbers) - 1)
                    quantiles.append(
                        float(
                            numbers[lower] * (1 - fraction) + numbers[upper] * fraction
                        )
                    )
                values.update(
                    minimum=float(numbers[0]),
                    maximum=float(numbers[-1]),
                    mean=float(mean),
                    stddev=float(variance.sqrt()),
                    quantiles=tuple(quantiles),
                )
    if column.kind == "categorical":
        counts = Counter(canonical(v) for v in present)
        selected = tuple(counts[canonical(v)] for v in column.categories)
        other = len(present) - sum(selected)
        values.update(
            category_counts=selected,
            other_count=other,
            category_frequencies=tuple(n / len(present) for n in selected)
            if present
            else None,
            other_frequency=other / len(present) if present else None,
        )
    return ColumnFeatures(**values)


def build_feature_profile(
    event, records, definition: FeatureDefinition, raw_sha256, implementation
):
    """Build a snapshot keyed by batch, raw checksum and calculation versions."""
    identity = canonical(
        [
            event.dataset_id,
            event.batch_id,
            raw_sha256,
            definition.fingerprint,
            implementation,
        ]
    )
    return FeatureProfile(
        profile_id=hashlib.sha256(identity.encode()).hexdigest(),
        dataset_id=event.dataset_id,
        batch_id=event.batch_id,
        raw_sha256=raw_sha256,
        source_id=event.source_id,
        received_at=event.received_at,
        schema_version=event.schema_version,
        granularity=event.granularity,
        period_start=event.period_start,
        period_end=event.period_end,
        complete=event.complete,
        raw_uri=event.raw_uri,
        definition=definition,
        definition_hash=definition.fingerprint,
        implementation_version=implementation,
        row_count=len(records),
        columns=tuple(
            calculate_column(records, c, definition) for c in definition.columns
        ),
    )
