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
    def test_training_to_stream_prediction_and_redelivery(self):
        from data_quality.config import load_suites
        from services.ml.train_order_volume import train_order_volume
        from services.stream_processor.order_volume import OrderVolumeMonitor
        from services.stream_processor.publisher import InMemoryPublisher
        from storage.postgres import PostgresAnomalyRepository
        from storage.postgres.order_volume import PostgresOrderVolumeRepository
        from tests.ml.test_volume import profiles, CUTOFF, event
        from tests.stream_processor.test_handlers import write_batch

        metrics = PostgresMetricRepository(self.connection)
        for profile in profiles():
            metrics.save_profile(profile)
        trained = train_order_volume(self.connection, CUTOFF)
        self.assertEqual(trained["status"], "TRAINED")
        volume = PostgresOrderVolumeRepository(self.connection)
        anomalies = PostgresAnomalyRepository(self.connection)
        publisher = InMemoryPublisher()
        handler = StreamEventHandler(
            PostgresDatasetRepository(self.connection), PostgresPipelineRunRepository(self.connection),
            metrics, PostgresQualityRepository(self.connection), publisher=publisher,
            suites=load_suites(Path("config/quality")),
            order_volume_monitor=OrderVolumeMonitor(volume, metrics, anomalies, publisher),
        )
        with TemporaryDirectory() as directory:
            records = [{"created_at": CUTOFF.isoformat(), "amount": 10.0, "payment_method": "card"}] * 1000
            loaded = event(CUTOFF, batch_id="live", records=1000,
                           raw_uri=write_batch(Path(directory), "live", records))
            evaluation = handler.handle_dataset_ingested(loaded)
            result = volume.get_prediction(evaluation.evaluation_id)
            self.assertEqual(result["status"], "WARN")
            self.assertEqual(result["expected"], 100.0)
            self.assertEqual(result["actual"], 1000)
            self.assertEqual(result["model_version"], trained["model_version"])
            handler.handle_dataset_ingested(loaded)
            count = self.connection.execute("SELECT count(*) AS n FROM anomalies").fetchone()["n"]
            self.assertEqual(count, 1)

    def test_order_volume_model_selection_and_immutable_prediction(self):
        from datetime import timedelta
        from storage.postgres.order_volume import PostgresOrderVolumeRepository

        repo = PostgresOrderVolumeRepository(self.connection)
        PostgresDatasetRepository(self.connection).ensure_dataset("mock_orders")
        from services.ml.volume import VERSION
        metadata = {"dataset_id": "mock_orders", "source_id": "mock", "cutoff": NOW.isoformat(),
                    "implementation": VERSION}
        model = {"model_version": "v1", "metadata": metadata, "artifact": b"test-model"}
        repo.save_model(model)
        repo.save_model({**model, "artifact": b"changed"})
        with TemporaryDirectory() as directory:
            event = event_for(Path(directory))
            lookup = event.model_copy(update={"dataset_id": "mock_orders", "source_id": "mock",
                                             "period_start": NOW - timedelta(hours=1)})
            self.assertIsNone(repo.find_model(lookup))
            lookup = lookup.model_copy(update={"period_start": NOW})
            self.assertEqual(bytes(repo.find_model(lookup)["artifact"]), b"test-model")
            repo.save_model({**model, "model_version": "legacy", "metadata": {
                **metadata, "implementation": "order-volume-v1", "cutoff": (NOW + timedelta(hours=1)).isoformat(),
            }})
            lookup = lookup.model_copy(update={"period_start": NOW + timedelta(hours=2)})
            self.assertEqual(repo.find_model(lookup)["model_version"], "v1")
            evaluation = self.handler().handle_dataset_ingested(event)
            repo.save_prediction(evaluation.evaluation_id, {"status": "SKIPPED"})
            repo.save_prediction(evaluation.evaluation_id, {"status": "PASS"})
            self.assertEqual(repo.get_prediction(evaluation.evaluation_id), {"status": "SKIPPED"})

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
