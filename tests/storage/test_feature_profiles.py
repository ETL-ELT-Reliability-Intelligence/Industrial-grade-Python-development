"""Real PostgreSQL constraints, immutable snapshots and separate quality links."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Barrier

from storage.feature_profiles import InputConflict
from storage.postgres import (
    PostgresDatasetRepository,
    PostgresMetricRepository,
    PostgresPipelineRunRepository,
    PostgresQualityRepository,
    connect,
)
from services.stream_processor.handlers import StreamEventHandler
from tests.storage.support import DatabaseTestCase, TEST_DATABASE_URL
from tests.stream_processor.test_evaluation import event_for, suite
from tests.stream_processor.test_handlers import NOW


class FeatureStorageTests(DatabaseTestCase):
    def handler(self):
        return StreamEventHandler(
            PostgresDatasetRepository(self.connection),
            PostgresPipelineRunRepository(self.connection),
            PostgresMetricRepository(self.connection),
            PostgresQualityRepository(self.connection),
            suites={"orders": suite()},
            clock=lambda: NOW,
            sleep=lambda _: None,
        )

    def test_unique_success_and_separate_eligibility_survive_reevaluation(self):
        with TemporaryDirectory() as directory:
            event = event_for(Path(directory))
            handler = self.handler()
            first = handler.handle_dataset_ingested(event)
            handler._suites = {"orders": suite(100)}
            second = handler.handle_dataset_ingested(event, request_id="new-policy")
            rows = self.connection.execute("SELECT * FROM feature_profiles").fetchall()
            self.assertEqual(len(rows), 1)
            links = self.connection.execute(
                "SELECT * FROM feature_profile_evaluations"
            ).fetchall()
            self.assertEqual(
                {r["evaluation_id"]: r["eligible"] for r in links},
                {first.evaluation_id: True, second.evaluation_id: False},
            )
            repository = PostgresMetricRepository(self.connection)
            row = rows[0]
            profile = repository.get_feature_profile(
                "orders",
                "b1",
                row["raw_sha256"],
                row["definition_hash"],
                row["implementation_version"],
            )
            self.assertEqual(repository.save_feature_profile(profile), profile)
            # The successful identity is constrained even if a caller supplies a different ID.
            self.assertEqual(
                repository.save_feature_profile(
                    profile.model_copy(update={"profile_id": "another-id"})
                ),
                profile,
            )
            with self.assertRaisesRegex(Exception, "immutable"):
                self.connection.execute(
                    "UPDATE feature_profiles SET profile = '{}'::jsonb"
                )
            with self.assertRaises(InputConflict):
                repository.register_feature_input("orders", "b1", "changed")

    def test_concurrent_writers_share_one_successful_snapshot(self):
        with TemporaryDirectory() as directory:
            handler = self.handler()
            handler.handle_dataset_ingested(event_for(Path(directory)))
            row = self.connection.execute("SELECT * FROM feature_profiles").fetchone()
            repository = PostgresMetricRepository(self.connection)
            old = repository.get_feature_profile(
                "orders",
                "b1",
                row["raw_sha256"],
                row["definition_hash"],
                row["implementation_version"],
            )
            barrier = Barrier(2)

            def save(index):
                with connect(TEST_DATABASE_URL) as connection:
                    repo = PostgresMetricRepository(connection)
                    candidate = old.model_copy(
                        update={
                            "profile_id": f"new-{index}",
                            "implementation_version": "new-implementation",
                        }
                    )
                    barrier.wait(timeout=10)
                    return repo.save_feature_profile(candidate).profile_id

            with ThreadPoolExecutor(max_workers=2) as workers:
                ids = list(workers.map(save, (1, 2)))
            self.assertEqual(ids[0], ids[1])
            self.assertEqual(
                self.connection.execute(
                    "SELECT count(*) AS count FROM feature_profiles"
                ).fetchone()["count"],
                2,
            )
