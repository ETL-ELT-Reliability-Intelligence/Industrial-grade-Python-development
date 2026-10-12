"""Read adapter over the existing schema; no schema changes or data writes."""

from contextlib import contextmanager
import logging
from collections.abc import Iterator

import psycopg
from psycopg.conninfo import make_conninfo

from contracts.state import Batch
from storage.postgres import PostgresQualityRepository, connect
from storage.serialization import check_result_from_row

from .models import Dataset, QualityScope, ScopeQuality, StoredCheck
from .repository import StorageUnavailable

logger = logging.getLogger(__name__)


class PostgresReadRepository:
    def __init__(self, connection: psycopg.Connection) -> None:
        self.connection = connection

    def ready(self) -> None:
        # Verify columns and read permissions as well as connectivity. No data scan.
        for query in (
            "SELECT dataset_id, created_at FROM datasets LIMIT 0",
            "SELECT dataset_id, batch_id, records, received_at, schema_version, object_key FROM batches LIMIT 0",
            "SELECT check_id, dataset_id, scope_id, column_name, kind, status, reason_code, "
            "message, observation, constraint_spec, details, recorded_at FROM quality_check_results LIMIT 0",
            "SELECT dataset_id, scope_id, status, reasons, decided_at FROM quality_decisions LIMIT 0",
        ):
            self.connection.execute(query)

    def list_datasets(self, limit: int, offset: int) -> list[Dataset]:
        rows = self.connection.execute(
            "SELECT dataset_id, created_at FROM datasets ORDER BY dataset_id LIMIT %s OFFSET %s",
            (limit, offset),
        ).fetchall()
        return [Dataset(**row) for row in rows]

    def get_dataset(self, dataset_id: str) -> Dataset | None:
        row = self.connection.execute(
            "SELECT dataset_id, created_at FROM datasets WHERE dataset_id = %s", (dataset_id,),
        ).fetchone()
        return Dataset(**row) if row else None

    def list_batches(self, dataset_id: str, limit: int, offset: int) -> list[Batch]:
        rows = self.connection.execute(
            "SELECT dataset_id, batch_id, records, received_at, schema_version, object_key "
            "FROM batches WHERE dataset_id = %s ORDER BY received_at DESC, batch_id DESC LIMIT %s OFFSET %s",
            (dataset_id, limit, offset),
        ).fetchall()
        return [Batch(**row) for row in rows]

    def list_quality_scopes(self, dataset_id: str, limit: int, offset: int) -> list[QualityScope]:
        rows = self.connection.execute(
            "SELECT scope_id, max(saved_at) AS last_saved_at FROM ("
            "SELECT scope_id, recorded_at AS saved_at FROM quality_check_results WHERE dataset_id = %s "
            "UNION ALL SELECT scope_id, decided_at FROM quality_decisions WHERE dataset_id = %s"
            ") AS observations GROUP BY scope_id ORDER BY last_saved_at DESC, scope_id "
            "LIMIT %s OFFSET %s", (dataset_id, dataset_id, limit, offset),
        ).fetchall()
        return [QualityScope(**row) for row in rows]

    def get_quality(self, dataset_id: str, scope_id: str) -> ScopeQuality:
        rows = self.connection.execute(
            "SELECT * FROM quality_check_results WHERE dataset_id = %s AND scope_id = %s "
            "ORDER BY check_id, COALESCE(column_name, '')", (dataset_id, scope_id),
        ).fetchall()
        decision = PostgresQualityRepository(self.connection).get_quality_decision(dataset_id, scope_id)
        timestamp = self.connection.execute(
            "SELECT decided_at FROM quality_decisions WHERE dataset_id = %s AND scope_id = %s",
            (dataset_id, scope_id),
        ).fetchone()
        return ScopeQuality(
            dataset_id=dataset_id, scope_id=scope_id,
            checks=[StoredCheck(result=check_result_from_row(row), recorded_at=row["recorded_at"]) for row in rows],
            decision=decision, decided_at=timestamp["decided_at"] if timestamp else None,
        )


@contextmanager
def open_repository(dsn: str | None) -> Iterator[PostgresReadRepository]:
    if not dsn:
        raise StorageUnavailable
    try:
        # Shared factory retains UTC/dict-row behavior. Never log the DSN or driver message.
        with connect(make_conninfo(dsn, connect_timeout=5)) as connection:
            with connection.transaction():
                connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                connection.execute("SET LOCAL statement_timeout = '5s'")
                connection.execute("SET LOCAL lock_timeout = '2s'")
                yield PostgresReadRepository(connection)
    except (
        psycopg.OperationalError, psycopg.InterfaceError, psycopg.ProgrammingError,
        psycopg.errors.QueryCanceled, psycopg.errors.InsufficientPrivilege,
    ) as exc:
        # Programming errors other than missing schema/configuration remain server bugs.
        if isinstance(exc, psycopg.ProgrammingError) and not isinstance(
            exc, (psycopg.errors.UndefinedTable, psycopg.errors.UndefinedColumn,
                  psycopg.errors.InvalidSchemaName, psycopg.errors.InsufficientPrivilege)
        ):
            raise
        logger.warning("State store unavailable (%s)", type(exc).__name__)
        raise StorageUnavailable from exc
