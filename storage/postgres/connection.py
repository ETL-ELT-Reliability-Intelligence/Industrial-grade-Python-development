"""Connection factory shared by all PostgreSQL repositories."""

from typing import Any

import psycopg
from psycopg.rows import dict_row


def connect(dsn: str) -> psycopg.Connection[dict[str, Any]]:
    """Open an autocommit connection returning rows as dicts, in UTC.

    Statements outside an explicit transaction commit individually. Repository
    operations that require grouped writes open their own transactions.
    The caller owns and closes the connection.
    """
    return psycopg.connect(
        dsn, autocommit=True, row_factory=dict_row, options="-c timezone=UTC",
    )
