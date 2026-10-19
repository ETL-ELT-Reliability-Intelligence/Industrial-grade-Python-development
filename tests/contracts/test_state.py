"""Validation of records persisted in the state store."""

from datetime import datetime, timedelta, timezone
import unittest

from contracts.state import Anomaly, Batch, PipelineRun, PipelineRunStatus, UserAction

NOW = datetime(2026, 10, 5, tzinfo=timezone.utc)
LATER = NOW + timedelta(minutes=1)


class RecordTests(unittest.TestCase):
    """Verify consistency rules of stored records."""

    def test_batch(self):
        """Accept valid batches and reject bad counts, versions and keys."""
        Batch(dataset_id="orders", batch_id="b-1", records=0, received_at=NOW, schema_version=1)
        Batch(dataset_id="orders", batch_id="b-1", records=5, received_at=NOW, schema_version=1, object_key="raw/orders/b-1")
        for args in (("orders", "b-1", -1, NOW, 1), ("orders", "b-1", 1, NOW, 0),
                     ("orders", "", 1, NOW, 1), ("orders", "b", 1, NOW.replace(tzinfo=None), 1),
                     ("orders", "b", 1, NOW, 1, " ")):
            with self.subTest(args=args):
                with self.assertRaises(ValueError):
                    Batch(**dict(zip(('dataset_id', 'batch_id', 'records', 'received_at', 'schema_version', 'object_key'), args)))

    def test_pipeline_run_status_consistency(self):
        """Tie finish time and error to the run status."""
        PipelineRun(pipeline_id="p", run_id="r", status=PipelineRunStatus.RUNNING, started_at=NOW)
        PipelineRun(pipeline_id="p", run_id="r", status=PipelineRunStatus.SUCCESS, started_at=NOW, finished_at=LATER)
        PipelineRun(pipeline_id="p", run_id="r", status=PipelineRunStatus.FAILED, started_at=NOW, finished_at=LATER, error="boom")
        for args in (
            ("p", "r", PipelineRunStatus.RUNNING, NOW, LATER),
            ("p", "r", PipelineRunStatus.SUCCESS, NOW),
            ("p", "r", PipelineRunStatus.SUCCESS, LATER, NOW),
            ("p", "r", PipelineRunStatus.SUCCESS, NOW, LATER, "boom"),
            ("p", "r", "success", NOW, LATER),
        ):
            with self.subTest(args=args):
                with self.assertRaises(ValueError):
                    PipelineRun(**dict(zip(('pipeline_id', 'run_id', 'status', 'started_at', 'finished_at', 'error'), args)))

    def test_anomaly(self):
        """Require a ranking score in [0, 1] and finite evidence."""
        Anomaly(anomaly_id="a", dataset_id="orders", metric="row_count", expected=10, actual=5, score=0.9, detector="rule", detected_at=NOW)
        for changes in ({"score": 1.1}, {"expected": float("nan")}, {"anomaly_id": ""},
                        {"batch_id": " "}):
            with self.subTest(changes=changes):
                arguments = dict(anomaly_id="a", dataset_id="orders", metric="row_count",
                                 expected=10, actual=5, score=0.9, detector="rule",
                                 detected_at=NOW)
                arguments.update(changes)
                with self.assertRaises(ValueError):
                    Anomaly(**arguments)

    def test_user_action(self):
        """Require identifiers, an aware time and a dict of details."""
        UserAction(action_id="u1", actor="alice", action_type="acknowledge", target_type="incident", target_id="inc-042", performed_at=NOW)
        for args in (("u1", "", "ack", "incident", "inc", NOW),
                     ("u1", "alice", "ack", "incident", "inc", NOW.replace(tzinfo=None)),
                     ("u1", "alice", "ack", "incident", "inc", NOW, [])):
            with self.subTest(args=args):
                with self.assertRaises(ValueError):
                    UserAction(**dict(zip(('action_id', 'actor', 'action_type', 'target_type', 'target_id', 'performed_at', 'details'), args)))


if __name__ == "__main__":
    unittest.main()
