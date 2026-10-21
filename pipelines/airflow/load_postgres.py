"""Load a raw batch from the raw store into a PostgreSQL table (the L of ELT).

Airflow-independent so it can be tested without Airflow. The raw layer keeps every column as text and
adds `_batch_id` / `_loaded_at`; typing and cleaning belong to the dbt models on top. Loading is
idempotent per batch: rows of the same `_batch_id` are replaced, so a task retry never duplicates data.
"""

from typing import Any

from psycopg import Connection, sql

from services.ingestion.storage import RawStore

RAW_SCHEMA = "raw"


def load_batch(
    connection: Connection[Any],
    table: str,
    batch_id: str,
    records: list[dict[str, Any]],
    schema: str = RAW_SCHEMA,
) -> int:
    """Replace the rows of `batch_id` in `schema.table`; return the number of rows loaded."""
    columns = list(dict.fromkeys(key for record in records for key in record))
    target = sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(table))

    with connection.transaction():
        connection.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
        connection.execute(
            sql.SQL(
                "CREATE TABLE IF NOT EXISTS {} (_batch_id text NOT NULL, _loaded_at timestamptz NOT NULL DEFAULT now())"
            ).format(target)
        )
        for column in columns:  # schema evolution: new source columns are added, nothing is dropped
            connection.execute(
                sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} text").format(target, sql.Identifier(column))
            )
        connection.execute(sql.SQL("DELETE FROM {} WHERE _batch_id = %s").format(target), (batch_id,))

        if records:
            copy = sql.SQL("COPY {} ({}) FROM STDIN").format(
                target, sql.SQL(", ").join(map(sql.Identifier, ["_batch_id", *columns]))
            )
            with connection.cursor() as cursor, cursor.copy(copy) as writer:
                for record in records:
                    writer.write_row([batch_id, *(_as_text(record.get(column)) for column in columns)])
    return len(records)


def load_dataset_batch(
    connection: Connection[Any], store: RawStore, dataset_id: str, batch_id: str, schema: str = RAW_SCHEMA
) -> int:
    """Read a stored batch back from the raw store and load it into `<schema>.<dataset_id>`."""
    return load_batch(connection, dataset_id, batch_id, store.load_raw(dataset_id, batch_id), schema=schema)


def _as_text(value: Any) -> str | None:
    return None if value is None else str(value)
