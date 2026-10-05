"""Validation helpers shared by contract modules.

Re-exports the helpers of contracts.quality so that new contracts validate
inputs exactly like the existing ones.
"""

from math import isfinite

from contracts.quality import _count, _rate, _text, _timestamp

__all__ = [
    "_count", "_number", "_optional_text", "_rate", "_text", "_timestamp", "_version",
]


def _number(value: int | float, name: str) -> None:
    """Raise ValueError unless value is a finite number, excluding bool."""
    if type(value) not in (int, float) or not isfinite(value):
        raise ValueError(f"{name} must be a finite number")


def _version(value: int, name: str) -> None:
    """Raise ValueError unless value is a positive integer, excluding bool."""
    if type(value) is not int or value < 1:
        raise ValueError(f"{name} must be a positive integer")


def _optional_text(value: str | None, name: str) -> None:
    """Raise ValueError unless value is None or a non-blank string."""
    if value is not None:
        _text(value, name)
