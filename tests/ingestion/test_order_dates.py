from datetime import datetime, timedelta, timezone
import logging

import pytest

from services.ingestion.sources import MockApiSource
from services.ingestion.service import IngestionService
from services.ingestion.storage import LocalRawStore
from services.ingestion.publisher import InMemoryPublisher

START = datetime(2026, 11, 8, 2, tzinfo=timezone.utc)
END = START + timedelta(hours=1)


def test_mock_dates_are_reproducible_and_inside_period():
    first = MockApiSource(period_start=START, period_end=END).fetch()
    second = MockApiSource(period_start=START, period_end=END).fetch()
    assert first == second
    assert all(START <= datetime.fromisoformat(r["created_at"]) < END for r in first.records)
    assert all(datetime.fromisoformat(r["created_at"]).utcoffset() == timedelta(0) for r in first.records)


@pytest.mark.parametrize("start,end", [(END, START), (START, None), (START.replace(tzinfo=None), END)])
def test_mock_rejects_invalid_period(start, end):
    with pytest.raises(ValueError):
        MockApiSource(period_start=start, period_end=end)


def test_empty_load_and_retry_are_logged(tmp_path, caplog):
    service = IngestionService(LocalRawStore(tmp_path), InMemoryPublisher(), clock=lambda: END)
    source = MockApiSource(base_rows=0, period_start=START, period_end=END)
    with caplog.at_level(logging.INFO):
        service.ingest(source, run_id="empty")
        service.ingest(source, run_id="empty")
    assert caplog.text.count("records=0") == 2
    assert "dataset=mock_orders" in caplog.text
    assert "period_start=2026-11-08 02:00:00+00:00" in caplog.text
