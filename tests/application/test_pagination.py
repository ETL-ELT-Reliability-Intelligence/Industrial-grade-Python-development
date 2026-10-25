"""Keep pagination bounded at both the use-case and SQL boundaries."""

from unittest.mock import MagicMock

import pytest

from services.backend.postgres import PostgresReadRepository
from services.backend.queries import DatasetQueries
from tests.application.support import MemoryRepository


@pytest.mark.parametrize(
    ("query_name", "method_name", "args"),
    [
        ("datasets", "list_datasets", ()),
        ("batches", "list_batches", ("orders",)),
        ("scopes", "list_quality_scopes", ("orders",)),
    ],
)
@pytest.mark.parametrize("count", [0, 2, 3])
def test_queries_request_one_extra_row(query_name, method_name, args, count):
    repository = MemoryRepository()
    item = getattr(repository, method_name)(*args, 1, 0)[0]
    reader = MagicMock(return_value=[item] * count)
    setattr(repository, method_name, reader)

    result = getattr(DatasetQueries(repository), query_name)(*args, 2, 7)

    reader.assert_called_once_with(*args, 3, 7)
    assert len(result.items) == min(count, 2)
    assert result.has_more is (count > 2)
    assert (result.limit, result.offset) == (2, 7)


@pytest.mark.parametrize(
    ("method_name", "args", "parameters"),
    [
        ("list_datasets", (), (3, 7)),
        ("list_batches", ("orders",), ("orders", 3, 7)),
        ("list_quality_scopes", ("orders",), ("orders", "orders", 3, 7)),
    ],
)
def test_sql_applies_bound_limit_and_offset(method_name, args, parameters):
    connection = MagicMock()
    connection.execute.return_value.fetchall.return_value = []

    assert getattr(PostgresReadRepository(connection), method_name)(*args, 3, 7) == []

    connection.execute.assert_called_once()
    statement, bound = connection.execute.call_args.args
    assert "LIMIT %s OFFSET %s" in statement
    assert bound == parameters
