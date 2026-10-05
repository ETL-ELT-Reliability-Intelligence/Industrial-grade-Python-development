"""Repository behaviour on a real, freshly migrated PostgreSQL database."""

from dataclasses import replace
from datetime import datetime, timedelta, timezone

from contracts.metrics import MetricKind, MetricPoint
from contracts.quality import (
    QualityContext, RowCountConstraint, RowCountObservation, SchemaConstraint,
    SchemaField, SchemaObservation,
)
from contracts.state import Anomaly, Batch, PipelineRun, PipelineRunStatus, UserAction
from data_quality.checks import check_row_count, check_schema
from data_quality.policies import evaluate_quality
from tests.storage.support import DatabaseTestCase, psycopg

NOW = datetime(2026, 10, 5, 10, tzinfo=timezone.utc)
HOUR = timedelta(hours=1)


class DatasetRepositoryTests(DatabaseTestCase):
    """Datasets, schema versions and batches."""

    def setUp(self):
        super().setUp()
        from storage.postgres import PostgresDatasetRepository
        self.repository = PostgresDatasetRepository(self.connection)

    def test_batch_upsert_is_idempotent_and_keeps_object_key(self):
        """Redelivery updates counts but never erases a stored object key."""
        batch = Batch("orders", "b-1", 100, NOW, 1, "raw/orders/b-1/data.parquet")
        self.repository.upsert_batch(batch)
        self.repository.upsert_batch(replace(batch, object_key=None, records=120))
        self.assertEqual(self.repository.get_batch("orders", "b-1"), replace(batch, records=120))
        self.assertIsNone(self.repository.get_batch("orders", "missing"))

    def test_batches_are_listed_newest_first(self):
        """Order batches by receive time and honour the limit."""
        for index in range(3):
            self.repository.upsert_batch(Batch("orders", f"b-{index}", 1, NOW + index * HOUR, 1))
        self.assertEqual([b.batch_id for b in self.repository.list_batches("orders")],
                         ["b-2", "b-1", "b-0"])
        self.assertEqual(len(self.repository.list_batches("orders", limit=2)), 2)
        with self.assertRaises(ValueError):
            self.repository.list_batches("orders", limit=0)

    def test_schema_versions_are_immutable(self):
        """Accept a repeated identical schema and reject a conflicting one."""
        fields = (SchemaField("id", "integer"), SchemaField("name", "string"))
        self.repository.save_schema("orders", 1, fields, NOW)
        self.repository.save_schema("orders", 1, tuple(reversed(fields)), NOW)
        self.assertEqual(self.repository.get_schema("orders", 1), fields)
        with self.assertRaises(ValueError):
            self.repository.save_schema("orders", 1, (SchemaField("id", "string"),), NOW)
        self.assertIsNone(self.repository.get_schema("orders", 2))
        with self.assertRaises(ValueError):
            self.repository.save_schema("orders", 2, fields, NOW.replace(tzinfo=None))


class PipelineRunRepositoryTests(DatabaseTestCase):
    """History of pipeline runs."""

    def setUp(self):
        super().setUp()
        from storage.postgres import PostgresPipelineRunRepository
        self.repository = PostgresPipelineRunRepository(self.connection)

    def test_finished_run_is_not_overwritten_by_late_events(self):
        """Keep the terminal state when an older running event is redelivered."""
        running = PipelineRun("orders_daily", "run-1", PipelineRunStatus.RUNNING, NOW)
        failed = PipelineRun("orders_daily", "run-1", PipelineRunStatus.FAILED, NOW,
                             NOW + HOUR, "boom")
        self.repository.upsert_pipeline_run(running)
        self.repository.upsert_pipeline_run(failed)
        self.repository.upsert_pipeline_run(running)
        self.repository.upsert_pipeline_run(replace(failed, status=PipelineRunStatus.SUCCESS, error=None))
        self.assertEqual(self.repository.get_pipeline_run("orders_daily", "run-1"), failed)

    def test_runs_are_listed_newest_first(self):
        """Order runs by start time."""
        for index in range(3):
            self.repository.upsert_pipeline_run(
                PipelineRun("orders_daily", f"run-{index}", PipelineRunStatus.RUNNING, NOW + index * HOUR))
        self.assertEqual([r.run_id for r in self.repository.list_pipeline_runs("orders_daily")],
                         ["run-2", "run-1", "run-0"])


class MetricRepositoryTests(DatabaseTestCase):
    """Dataset statistics."""

    def setUp(self):
        super().setUp()
        from storage.postgres import PostgresMetricRepository
        self.repository = PostgresMetricRepository(self.connection)

    def test_same_identity_is_updated_not_duplicated(self):
        """Recalculation of one scope and version replaces the value."""
        point = MetricPoint("orders", "b-1", MetricKind.ROW_COUNT, NOW, "v1", 4500000)
        self.repository.save_metric(point)
        self.repository.save_metric(replace(point, value=1800000))
        self.assertEqual(self.repository.list_metric_history("orders", MetricKind.ROW_COUNT),
                         [replace(point, value=1800000)])
        self.repository.save_metric(replace(point, stats_version="v2"))
        self.assertEqual(len(self.repository.list_metric_history("orders", MetricKind.ROW_COUNT)), 2)

    def test_history_is_newest_first_and_counts_stay_integers(self):
        """Return counts as int and order points by observation time."""
        for index in range(3):
            self.repository.save_metric(
                MetricPoint("orders", f"b-{index}", MetricKind.ROW_COUNT, NOW + index * HOUR, "v1", index))
        history = self.repository.list_metric_history("orders", MetricKind.ROW_COUNT, limit=2)
        self.assertEqual([p.value for p in history], [2, 1])
        self.assertTrue(all(type(p.value) is int for p in history))

    def test_columns_are_separate_series(self):
        """Keep null-rate history of different columns apart and allow None values."""
        for column in ("id", "comment"):
            self.repository.save_metric(
                MetricPoint("orders", "b-1", MetricKind.NULL_RATE, NOW, "v1", 0.1, column))
        self.repository.save_metric(
            MetricPoint("orders", "b-2", MetricKind.NULL_RATE, NOW + HOUR, "v1", None, "id"))
        history = self.repository.list_metric_history("orders", MetricKind.NULL_RATE, "id")
        self.assertEqual([(p.scope_id, p.value) for p in history], [("b-2", None), ("b-1", 0.1)])
        self.assertEqual(self.repository.list_metric_history("orders", MetricKind.NULL_RATE), [])


class QualityRepositoryTests(DatabaseTestCase):
    """Check results and decisions."""

    def setUp(self):
        super().setUp()
        from storage.postgres import PostgresQualityRepository
        self.repository = PostgresQualityRepository(self.connection)
        context = QualityContext("orders", "b-1")
        self.failed = check_row_count(
            RowCountObservation(1800000), RowCountConstraint(min_count=3150000),
            check_id="orders.row_count", context=context)
        self.schema = check_schema(
            SchemaObservation((SchemaField("id", "string"),)),
            SchemaConstraint((SchemaField("id", "integer"), SchemaField("a", "string"))),
            check_id="orders.schema", context=context)

    def test_results_round_trip_and_latest_result_wins(self):
        """Restore stored results and replace a result of the same check."""
        self.repository.save_check_result(self.schema)
        self.repository.save_check_result(self.failed)
        self.assertEqual(self.repository.list_check_results("orders", "b-1"),
                         [self.failed, self.schema])
        passed = check_row_count(
            RowCountObservation(4500000), RowCountConstraint(min_count=3150000),
            check_id="orders.row_count", context=self.failed.context)
        self.repository.save_check_result(passed)
        self.assertEqual(self.repository.list_check_results("orders", "b-1"), [passed, self.schema])
        self.assertEqual(self.repository.list_check_results("orders", "b-2"), [])

    def test_result_without_context_is_rejected(self):
        """Refuse results that cannot be attributed to a dataset and scope."""
        with self.assertRaises(ValueError):
            self.repository.save_check_result(replace(self.failed, context=None))

    def test_decision_round_trip_and_replacement(self):
        """Store the decision of a scope and replace it on re-evaluation."""
        decision = evaluate_quality([self.failed, self.schema])
        self.repository.save_quality_decision("orders", "b-1", decision, NOW)
        self.assertEqual(self.repository.get_quality_decision("orders", "b-1"), decision)
        warn = evaluate_quality(())
        self.repository.save_quality_decision("orders", "b-1", warn, NOW + HOUR)
        self.assertEqual(self.repository.get_quality_decision("orders", "b-1"), warn)
        self.assertIsNone(self.repository.get_quality_decision("orders", "b-2"))


class AnomalyRepositoryTests(DatabaseTestCase):
    """Detected anomalies."""

    def setUp(self):
        super().setUp()
        from storage.postgres import PostgresAnomalyRepository
        self.repository = PostgresAnomalyRepository(self.connection)

    def test_anomaly_is_immutable_once_stored(self):
        """Ignore a redelivered anomaly with the same identifier."""
        anomaly = Anomaly("a-1", "orders", "row_count", 4500000, 1800000, 0.97, "rule", NOW,
                          batch_id="b-1")
        self.repository.save_anomaly(anomaly)
        self.repository.save_anomaly(replace(anomaly, score=0.1))
        self.assertEqual(self.repository.get_anomaly("a-1"), anomaly)
        self.assertIsNone(self.repository.get_anomaly("missing"))

    def test_listing_filters_by_dataset_and_orders_newest_first(self):
        """Return anomalies of one dataset or of all datasets."""
        for index, dataset in enumerate(("orders", "orders", "payments")):
            self.repository.save_anomaly(
                Anomaly(f"a-{index}", dataset, "row_count", 10, 5, 0.9, "rule", NOW + index * HOUR))
        self.assertEqual([a.anomaly_id for a in self.repository.list_anomalies("orders")],
                         ["a-1", "a-0"])
        self.assertEqual(len(self.repository.list_anomalies()), 3)


class UserActionRepositoryTests(DatabaseTestCase):
    """Audit trail."""

    def test_actions_are_stored_once_and_listed_newest_first(self):
        """Ignore duplicates and keep details intact."""
        from storage.postgres import PostgresUserActionRepository
        repository = PostgresUserActionRepository(self.connection)
        first = UserAction("u-1", "alice", "acknowledge", "incident", "inc-042", NOW,
                           {"comment": "looking"})
        second = UserAction("u-2", "bob", "resolve", "incident", "inc-042", NOW + HOUR)
        repository.record_action(first)
        repository.record_action(replace(first, actor="mallory"))
        repository.record_action(second)
        self.assertEqual(repository.list_actions("incident", "inc-042"), [second, first])
        self.assertEqual(repository.list_actions("incident", "other"), [])


class SchemaTests(DatabaseTestCase):
    """Constraints of tables that have no repository yet."""

    def test_expected_tables_exist(self):
        """Create every table of the first schema version."""
        rows = self.connection.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        ).fetchall()
        names = {row["table_name"] for row in rows}
        for table in ("datasets", "dataset_schemas", "batches", "pipeline_runs", "dataset_metrics",
                      "quality_check_results", "quality_decisions", "anomalies", "incidents",
                      "incident_affected_datasets", "incident_observations",
                      "incident_root_causes", "incident_timeline", "incident_impact",
                      "lineage_nodes", "lineage_edges", "ml_model_metadata", "user_actions"):
            self.assertIn(table, names)

    def test_incident_constraints(self):
        """Reject invalid status, severity and ranking score."""
        insert = ("INSERT INTO incidents (incident_id, title, summary, status, severity, detected_at) "
                  "VALUES ('inc-1', 't', 's', %s, %s, now())")
        for status, severity in (("bad", "critical"), ("open", "bad")):
            with self.assertRaises(psycopg.errors.CheckViolation):
                self.connection.execute(insert, (status, severity))
        self.connection.execute(insert, ("open", "critical"))
        self.connection.execute("INSERT INTO datasets (dataset_id) VALUES ('orders')")
        with self.assertRaises(psycopg.errors.CheckViolation):
            self.connection.execute(
                "INSERT INTO incident_root_causes (incident_id, ordinal, dataset_id, description, score) "
                "VALUES ('inc-1', 0, 'orders', 'd', 1.5)")
