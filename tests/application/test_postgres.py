"""Real PostgreSQL tests. TEST_DATABASE_URL MUST name an expendable test database."""

from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

import psycopg
from fastapi.testclient import TestClient

from contracts.evaluation import Attempt, ExecutionStatus
from contracts.quality import DecisionReason, QualityDecision, QualityStatus
from contracts.state import Batch
from services.backend.ingested_sample import DATASET_ID, prepare_sample
from services.backend.main import create_app
from services.backend.postgres import open_repository
from services.backend.seed import EMPTY_DATASET, SAMPLE_DATASET, seed
from storage.postgres import PostgresDatasetRepository, PostgresQualityRepository
from tests.application.evaluation_support import evaluation_record
from tests.application.support import NOW, evidence
from tests.storage.support import TEST_DATABASE_URL, DatabaseTestCase


class ApplicationPostgresTests(DatabaseTestCase):
    def test_ingested_sample_quality_is_read_through_api(self) -> None:
        with TemporaryDirectory() as directory:
            self.addCleanup(
                self.connection.execute,
                'DROP TABLE IF EXISTS raw."ingested-app-orders"',
            )
            event = prepare_sample(TEST_DATABASE_URL, Path(directory), "api-test")
            decision = self.quality.get_quality_decision(DATASET_ID, event.batch_id)
            checks = self.quality.list_check_results(DATASET_ID, event.batch_id)
            assert decision is not None
            assert checks
            assert (
                self.connection.execute(
                    'SELECT count(*) AS count FROM raw."ingested-app-orders" WHERE _batch_id = %s',
                    (event.batch_id,),
                ).fetchone()["count"]
                == 2
            )

            response = self.client.get(
                f"/api/v1/datasets/{DATASET_ID}/quality",
                params={"scope_id": event.batch_id},
            )
            assert response.status_code == 200
            body = response.json()
            assert body["decision"] == decision.model_dump(mode="json")
            # The first batch has neither historical bounds nor a schema baseline.
            assert body["decision"]["status"] == "WARN"
            assert {reason["code"] for reason in body["decision"]["reasons"]} >= {
                "insufficient_history",
                "missing_schema",
            }
            assert body["decided_at"]
            assert {
                item["result"]["check_id"]: item["result"] for item in body["checks"]
            } == {check.check_id: check.model_dump(mode="json") for check in checks}
            assert all(item["recorded_at"] for item in body["checks"])
            batches = self.client.get(f"/api/v1/datasets/{DATASET_ID}/batches").json()[
                "items"
            ]
            assert len(batches) == 1
            assert batches[0]["records"] == 2
            assert batches[0]["object_key"] == event.raw_uri
            assert batches[0]["batch_id"] == event.batch_id

            # Retry the committed run without duplicating loaded rows or batches.
            assert (
                prepare_sample(TEST_DATABASE_URL, Path(directory), "api-test") == event
            )
            assert (
                self.connection.execute(
                    'SELECT count(*) AS count FROM raw."ingested-app-orders"'
                ).fetchone()["count"]
                == 2
            )
            assert (
                len(
                    self.client.get(f"/api/v1/datasets/{DATASET_ID}/batches").json()[
                        "items"
                    ]
                )
                == 1
            )

    def setUp(self):
        super().setUp()
        self.datasets = PostgresDatasetRepository(self.connection)
        self.quality = PostgresQualityRepository(self.connection)
        self.client = TestClient(
            create_app(repository_factory=lambda: open_repository(TEST_DATABASE_URL))
        )
        self.client.__enter__()
        self.addCleanup(self.client.__exit__, None, None, None)

    def test_catalog_and_batch_pagination_have_stable_order(self):
        for name in ("orders", "empty", "z"):
            self.datasets.ensure_dataset(name)
        for batch_id in ("b", "c", "a"):
            self.datasets.upsert_batch(
                Batch(
                    dataset_id="orders",
                    batch_id=batch_id,
                    records=0,
                    schema_version=1,
                    received_at=NOW,
                )
            )
        first = self.client.get("/api/v1/datasets?limit=1").json()
        assert first["items"][0]["dataset_id"] == "empty"
        assert first["has_more"] is True
        assert self.client.get("/api/v1/datasets/empty/batches").json()["items"] == []
        pages = [
            self.client.get(
                f"/api/v1/datasets/orders/batches?limit=1&offset={i}"
            ).json()
            for i in range(3)
        ]
        assert [p["items"][0]["batch_id"] for p in pages] == ["c", "b", "a"]
        assert [p["has_more"] for p in pages] == [True, True, False]
        assert pages[0]["items"][0]["records"] == 0

    def test_scope_union_includes_checks_only_and_decision_only(self):
        results = evidence()
        for result in results:
            self.quality.save_check_result(result)
        # Same timestamps explicitly test the stable scope-ID tiebreaker.
        self.connection.execute(
            "UPDATE quality_check_results SET recorded_at = %s", (NOW,)
        )
        decision = QualityDecision(
            status=QualityStatus.WARN,
            reasons=(DecisionReason(code="external", message="Saved upstream"),),
        )
        self.quality.save_quality_decision("orders", "decision-only", decision, NOW)
        self.quality.save_quality_decision(
            "orders", "newer", decision, NOW + timedelta(hours=1)
        )
        scopes = self.client.get(
            "/api/v1/datasets/orders/quality-scopes?limit=2"
        ).json()
        assert [item["scope_id"] for item in scopes["items"]] == [
            "newer",
            "decision-only",
        ]
        assert scopes["has_more"] is True
        tail = self.client.get(
            "/api/v1/datasets/orders/quality-scopes?limit=2&offset=2"
        ).json()
        assert [item["scope_id"] for item in tail["items"]] == ["window-1"]
        checks_only = self.client.get(
            "/api/v1/datasets/orders/quality?scope_id=window-1"
        ).json()
        assert len(checks_only["checks"]) == 2
        assert checks_only["decision"] is None
        assert checks_only["decided_at"] is None
        assert checks_only["checks"][0]["result"]["observation"]["null_rate"] is None
        assert checks_only["checks"][1]["result"]["observation"]["row_count"] == 0
        decision_only = self.client.get(
            "/api/v1/datasets/orders/quality?scope_id=decision-only"
        ).json()
        assert decision_only["checks"] == []
        assert decision_only["decision"]["status"] == "WARN"

    def test_updates_visible_on_next_request_and_sql_parameters_are_safe(self):
        dataset_id = "x' OR '1'='1"
        scope_id = "window? & ' / #"
        self.datasets.ensure_dataset(dataset_id)
        result = evidence(dataset_id, scope_id)[0]
        self.quality.save_check_result(result)
        from urllib.parse import quote

        path = f"/api/v1/datasets/{quote(dataset_id, safe='')}/quality"
        before = self.client.get(path, params={"scope_id": scope_id}).json()
        assert before["decision"] is None
        self.quality.save_quality_decision(
            dataset_id,
            scope_id,
            QualityDecision(
                status=QualityStatus.WARN,
                reasons=(DecisionReason(code="review", message="Review needed"),),
            ),
            NOW,
        )
        after = self.client.get(path, params={"scope_id": scope_id}).json()
        assert after["decision"]["status"] == "WARN"
        assert after["scope_id"] == scope_id
        assert self.client.get("/ready").status_code == 200

    def test_snapshot_consistency_and_connection_lifecycle(self):
        for result in evidence():
            self.quality.save_check_result(result)
        with open_repository(TEST_DATABASE_URL) as repository:
            connection = repository.connection
            assert repository.get_dataset("orders") is not None  # establishes snapshot
            self.quality.save_quality_decision(
                "orders",
                "window-1",
                QualityDecision(
                    status=QualityStatus.BLOCK,
                    reasons=(DecisionReason(code="new", message="New decision"),),
                ),
                NOW,
            )
            assert repository.get_quality("orders", "window-1").decision is None
        assert connection.closed
        assert (
            self.client.get("/api/v1/datasets/orders/quality?scope_id=window-1").json()[
                "decision"
            ]["status"]
            == "BLOCK"
        )
        with self.assertRaises(psycopg.errors.ReadOnlySqlTransaction):
            with open_repository(TEST_DATABASE_URL) as repository:
                rejected_connection = repository.connection
                rejected_connection.execute(
                    "INSERT INTO datasets (dataset_id) VALUES ('forbidden')"
                )
        assert rejected_connection.closed
        assert self.client.get("/api/v1/datasets/forbidden").status_code == 404

    def test_unreachable_server_returns_503_without_connection_details(self):
        with TestClient(
            create_app(
                repository_factory=lambda: open_repository(
                    "postgresql://secret:secret@127.0.0.1:1/missing",
                )
            )
        ) as client:
            response = client.get("/ready")
            assert response.status_code == 503
            assert response.json() == {"detail": "State store unavailable"}

    def test_missing_schema_returns_503_and_recovers(self):
        self.connection.execute(
            "ALTER TABLE quality_decisions RENAME TO temporarily_missing_decisions"
        )
        try:
            assert self.client.get("/ready").status_code == 503
        finally:
            self.connection.execute(
                "ALTER TABLE temporarily_missing_decisions RENAME TO quality_decisions"
            )
        assert self.client.get("/ready").status_code == 200

    def test_seed_is_explicit_and_never_overwrites(self):
        seed(TEST_DATABASE_URL)
        path = f"/api/v1/datasets/{SAMPLE_DATASET}/quality?scope_id=sample-window-1"
        before = self.client.get(path).json()
        assert before["decision"]["status"] == "BLOCK"
        assert (
            self.client.get(f"/api/v1/datasets/{EMPTY_DATASET}/batches").json()["items"]
            == []
        )
        with self.assertRaises(ValueError):
            seed(TEST_DATABASE_URL)
        assert self.client.get(path).json() == before


class EvaluationPostgresTests(DatabaseTestCase):
    def setUp(self):
        super().setUp()
        self.quality = PostgresQualityRepository(self.connection)
        self.client = TestClient(
            create_app(repository_factory=lambda: open_repository(TEST_DATABASE_URL))
        )
        self.client.__enter__()
        self.addCleanup(self.client.__exit__, None, None, None)

    def test_scope_visibility_and_sequence_order_after_reevaluation(self):
        for evaluation_id in ("first", "second", "third"):
            self.quality.save_evaluation(evaluation_record(evaluation_id).evaluation)
        # Application ordering is insertion sequence, not mutable wall-clock timestamps.
        self.connection.execute(
            "UPDATE quality_evaluations SET created_at = %s, updated_at = %s",
            (NOW, NOW),
        )
        scopes = self.client.get("/api/v1/datasets/orders/quality-scopes").json()[
            "items"
        ]
        assert [item["scope_id"] for item in scopes] == ["window-1"]
        ids = []
        for offset in range(3):
            page = self.client.get(
                f"/api/v1/datasets/orders/evaluations?scope_id=window-1&limit=1&offset={offset}"
            ).json()
            ids.append(page["items"][0]["evaluation_id"])
            assert page["has_more"] is (offset < 2)
            assert page["items"][0]["decision_status"] is None
        assert ids == ["third", "second", "first"]
        assert (
            self.client.get("/api/v1/datasets/orders/quality?scope_id=window-1").json()[
                "decision"
            ]
            is None
        )

    def test_execution_overlay_and_consistent_detail(self):
        record = evaluation_record()
        self.quality.save_evaluation(record.evaluation)
        execution = record.evaluation.checks[0].model_copy(
            update={
                "state": ExecutionStatus.ERROR,
                "code": "source_timeout",
                "message": "Read timed out",
                "attempts": (
                    Attempt(
                        number=1,
                        started_at=NOW,
                        finished_at=NOW + timedelta(seconds=1),
                        code="source_timeout",
                    ),
                ),
            }
        )
        self.quality.save_execution(record.evaluation.evaluation_id, execution)
        response = self.client.get("/api/v1/datasets/orders/evaluations/evaluation-1")
        assert response.status_code == 200
        detail = response.json()
        assert detail["evaluation"]["state"] == "PENDING"
        assert detail["evaluation"]["checks"][0] == execution.model_dump(mode="json")
        assert detail["evaluation"]["decision"] is None
        with open_repository(TEST_DATABASE_URL) as repository:
            assert (
                repository.connection.execute("SHOW transaction_read_only").fetchone()[
                    "transaction_read_only"
                ]
                == "on"
            )
            assert (
                repository.connection.execute("SHOW transaction_isolation").fetchone()[
                    "transaction_isolation"
                ]
                == "repeatable read"
            )
            assert repository.get_dataset("orders") is not None
            self.quality.save_execution(
                record.evaluation.evaluation_id,
                execution.model_copy(update={"message": "Later update"}),
            )
            assert (
                repository.get_evaluation("orders", "evaluation-1")
                .evaluation.checks[0]
                .message
                == "Read timed out"
            )
        assert (
            self.client.get("/api/v1/datasets/orders/evaluations/evaluation-1").json()[
                "evaluation"
            ]["checks"][0]["message"]
            == "Later update"
        )

    def test_pending_scope_sql_pagination_and_foreign_evaluation(self):
        for scope in ("c", "a", "b"):
            self.quality.save_evaluation(
                evaluation_record(f"evaluation-{scope}", scope_id=scope).evaluation
            )
        self.connection.execute(
            "UPDATE quality_evaluations SET created_at = %s, updated_at = %s",
            (NOW, NOW),
        )
        first = self.client.get("/api/v1/datasets/orders/quality-scopes?limit=2").json()
        assert [item["scope_id"] for item in first["items"]] == ["a", "b"]
        assert first["has_more"] is True
        second = self.client.get(
            "/api/v1/datasets/orders/quality-scopes?limit=2&offset=2"
        ).json()
        assert [item["scope_id"] for item in second["items"]] == ["c"]
        assert second["has_more"] is False
        PostgresDatasetRepository(self.connection).ensure_dataset("another-dataset")
        assert (
            self.client.get(
                "/api/v1/datasets/another-dataset/evaluations/evaluation-a"
            ).status_code
            == 404
        )

    def test_readiness_and_detail_require_execution_table(self):
        self.quality.save_evaluation(evaluation_record().evaluation)
        self.connection.execute(
            "ALTER TABLE quality_executions RENAME TO temporarily_missing_executions"
        )
        try:
            assert self.client.get("/ready").status_code == 503
            assert (
                self.client.get(
                    "/api/v1/datasets/orders/evaluations/evaluation-1"
                ).status_code
                == 503
            )
        finally:
            self.connection.execute(
                "ALTER TABLE temporarily_missing_executions RENAME TO quality_executions"
            )
        assert self.client.get("/ready").status_code == 200

    def test_completed_block_and_legacy_scope_union(self):
        from tests.application.support import evidence

        record = evaluation_record(completed=True)
        self.quality.save_evaluation(record.evaluation)
        self.quality.project_evaluation(record.evaluation)
        for result in evidence("orders", "legacy-scope"):
            self.quality.save_check_result(result)
        summaries = self.client.get(
            "/api/v1/datasets/orders/evaluations?scope_id=window-1"
        ).json()["items"]
        assert summaries[0]["state"] == "COMPLETED"
        assert summaries[0]["decision_status"] == "BLOCK"
        scopes = self.client.get("/api/v1/datasets/orders/quality-scopes").json()[
            "items"
        ]
        assert {item["scope_id"] for item in scopes} == {"window-1", "legacy-scope"}
        assert (
            self.client.get(
                "/api/v1/datasets/orders/evaluations?scope_id=legacy-scope"
            ).json()["items"]
            == []
        )
        assert self.client.get("/ready").status_code == 200
