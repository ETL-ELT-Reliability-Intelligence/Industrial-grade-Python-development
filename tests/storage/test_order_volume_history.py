"""PostgreSQL filtering for list_profiles_between used by order-volume history."""

from datetime import datetime, timedelta, timezone

from contracts.events import DatasetIngestedEvent
from contracts.profiles import BatchProfile
from tests.storage.support import DatabaseTestCase

NOW = datetime(2026, 10, 5, 10, tzinfo=timezone.utc)
HOUR = timedelta(hours=1)


class OrderVolumeHistoryTests(DatabaseTestCase):
    """Period and receipt filters of the shared profile query."""

    def setUp(self):
        super().setUp()
        from storage.postgres import PostgresMetricRepository

        self.repository = PostgresMetricRepository(self.connection)

    def _make_profile(self, batch_id, *, period_start, received_at=None,
                      source_id="mock"):
        if received_at is None:
            received_at = period_start + HOUR
        event = DatasetIngestedEvent(
            dataset_id="mock_orders",
            batch_id=batch_id,
            records=10,
            received_at=received_at,
            schema_version=1,
            source_id=source_id,
            granularity="hour",
            period_start=period_start,
            period_end=period_start + HOUR,
        )
        return BatchProfile(
            event=event,
            config_hash="test",
            row_count=10,
            checks={},
        )

    def _load(self, start, end, as_of, limit=100):
        return self.repository.list_profiles_between(
            "mock_orders",
            source_id="mock",
            start=start,
            end=end,
            as_of=as_of,
            limit=limit,
        )

    def test_filters_by_source_id(self):
        """Profiles produced by another source are excluded."""
        self.repository.save_profile(self._make_profile("mock-1", period_start=NOW))
        self.repository.save_profile(
            self._make_profile("csv-1", period_start=NOW, source_id="csv")
        )
        result = self._load(NOW - HOUR, NOW + HOUR, NOW + HOUR)
        self.assertEqual([p.event.batch_id for p in result], ["mock-1"])

    def test_filters_by_period_range(self):
        """Periods outside [start, end) are excluded."""
        for index, period_start in enumerate((NOW - 3 * HOUR, NOW, NOW + 3 * HOUR)):
            self.repository.save_profile(
                self._make_profile(f"b-{index}", period_start=period_start)
            )
        result = self._load(NOW - HOUR, NOW + HOUR, NOW + timedelta(days=2))
        self.assertEqual([p.event.batch_id for p in result], ["b-1"])

    def test_excludes_profiles_received_after_as_of(self):
        """A profile received after the cutoff is not point-in-time available."""
        self.repository.save_profile(
            self._make_profile("early", period_start=NOW, received_at=NOW + HOUR)
        )
        self.repository.save_profile(
            self._make_profile("late", period_start=NOW, received_at=NOW + 5 * HOUR)
        )
        result = self._load(NOW - HOUR, NOW + HOUR, NOW + 2 * HOUR)
        self.assertEqual([p.event.batch_id for p in result], ["early"])

    def test_limit_restricts_results(self):
        """The result is truncated to the requested limit."""
        for index in range(5):
            self.repository.save_profile(
                self._make_profile(f"b-{index}", period_start=NOW + index * HOUR)
            )
        result = self._load(NOW - HOUR, NOW + 10 * HOUR, NOW + 10 * HOUR, limit=3)
        self.assertEqual(len(result), 3)
        self.assertEqual([p.event.batch_id for p in result], ["b-0", "b-1", "b-2"])
        with self.assertRaises(ValueError):
            self._load(NOW - HOUR, NOW + 10 * HOUR, NOW + 10 * HOUR, limit=0)

    def test_orders_chronologically(self):
        """Profiles are ordered by period_start, then by received_at."""
        self.repository.save_profile(
            self._make_profile("b-late", period_start=NOW, received_at=NOW + 5 * HOUR)
        )
        self.repository.save_profile(
            self._make_profile("b-first", period_start=NOW - 2 * HOUR,
                               received_at=NOW - HOUR)
        )
        self.repository.save_profile(
            self._make_profile("b-revision", period_start=NOW, received_at=NOW + HOUR)
        )
        result = self._load(NOW - 3 * HOUR, NOW + HOUR, NOW + 10 * HOUR)
        self.assertEqual(
            [p.event.batch_id for p in result],
            ["b-first", "b-revision", "b-late"],
        )
