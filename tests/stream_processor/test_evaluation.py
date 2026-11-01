from datetime import timedelta
import json

import pytest

from contracts.evaluation import ExecutionStatus
from contracts.events import DatasetIngestedEvent
from contracts.quality import QualityStatus
from contracts.quality_config import CheckSuite
from data_quality.execution import finalize
from services.stream_processor.evaluation import transient
from tests.stream_processor.test_handlers import NOW, make_handler, write_batch


def suite(minimum=1):
    return CheckSuite.model_validate_json(
        json.dumps(
            {
                "dataset_id": "orders",
                "retry": {"max_attempts": 3, "delays_seconds": [0, 0]},
                "checks": [
                    {
                        "key": "rows",
                        "kind": "row_count",
                        "params": {"mode": "fixed", "min_count": minimum},
                    },
                    {
                        "key": "id",
                        "kind": "null_rate",
                        "column": "id",
                        "params": {"max_null_rate": 0},
                    },
                ],
            }
        )
    )


def event_for(tmp_path, rows=None):
    rows = [{"id": 1}] if rows is None else rows
    return DatasetIngestedEvent(
        dataset_id="orders",
        batch_id="b1",
        records=len(rows),
        received_at=NOW,
        schema_version=1,
        raw_uri=write_batch(tmp_path, "b1", rows),
    )


def test_replay_uses_original_time_configuration_and_results(tmp_path, monkeypatch):
    handler, _, _, _, quality, _ = make_handler(suites={"orders": suite()})
    event = event_for(tmp_path)
    first = handler.handle_dataset_ingested(event)
    handler._suites = {"orders": suite(100)}
    monkeypatch.setattr(
        "services.stream_processor.handlers.read_raw",
        lambda _: pytest.fail("completed evaluation must not reread raw"),
    )
    repeated = handler.handle_dataset_ingested(event)
    assert repeated == first
    assert len(quality.evaluations) == 1


def test_explicit_reevaluation_keeps_history_and_old_replay_does_not_replace_latest(
    tmp_path,
):
    handler, _, _, _, quality, _ = make_handler(suites={"orders": suite()})
    event = event_for(tmp_path)
    first = handler.handle_dataset_ingested(event)
    handler._suites = {"orders": suite(100)}
    second = handler.handle_dataset_ingested(event, request_id="new-policy")
    assert first.decision.status is QualityStatus.PASS
    assert second.decision.status is QualityStatus.BLOCK
    assert first.evaluation_id != second.evaluation_id
    assert quality.get_evaluation(first.evaluation_id) == first
    assert handler.handle_dataset_ingested(event, request_id="new-policy") == second
    handler.handle_dataset_ingested(event)
    assert quality.get_quality_decision("orders", "b1").status is QualityStatus.BLOCK


def test_temporary_read_error_retries_only_within_budget(tmp_path, monkeypatch):
    handler, _, _, _, _, _ = make_handler(suites={"orders": suite()})
    calls = []

    def read(_):
        calls.append(1)
        if len(calls) < 3:
            raise TimeoutError("storage timeout")
        return [{"id": 1}]

    monkeypatch.setattr("services.stream_processor.handlers.read_raw", read)
    result = handler.handle_dataset_ingested(event_for(tmp_path))
    assert result.decision.status is QualityStatus.PASS
    assert len(result.preparation_attempts) == 3
    assert [a.code for a in result.preparation_attempts] == [
        "temporary_error",
        "temporary_error",
        None,
    ]


@pytest.mark.parametrize(
    "error, attempts", [(TimeoutError("offline"), 3), (ValueError("bad input"), 1)]
)
def test_exhausted_or_permanent_preparation_failure_is_visible(
    tmp_path, monkeypatch, error, attempts
):
    handler, _, _, _, _, _ = make_handler(suites={"orders": suite()})

    def read(_):
        raise error

    monkeypatch.setattr("services.stream_processor.handlers.read_raw", read)
    event = event_for(tmp_path)
    result = handler.handle_dataset_ingested(event)
    assert len(result.preparation_attempts) == attempts
    assert result.decision.status is QualityStatus.BLOCK
    assert all(
        c.state is ExecutionStatus.ERROR and c.result is None for c in result.checks
    )
    assert handler.handle_dataset_ingested(event) == result


def test_resume_keeps_completed_checks_and_counts_interrupted_attempt(
    tmp_path, monkeypatch
):
    handler, _, _, _, quality, _ = make_handler(suites={"orders": suite()})
    event = event_for(tmp_path)
    from services.stream_processor import evaluation as module

    real = module.execute_check
    calls = []

    def interrupt(check, event):
        calls.append(check.check_id)
        if check.check_id == "orders.id":
            raise KeyboardInterrupt()
        return real(check, event)

    monkeypatch.setattr(module, "execute_check", interrupt)
    with pytest.raises(KeyboardInterrupt):
        handler.handle_dataset_ingested(event)
    pending = next(iter(quality.evaluations.values()))
    assert pending.decision is None
    assert pending.checks[0].state is ExecutionStatus.COMPLETED
    assert pending.checks[1].state is ExecutionStatus.RUNNING
    monkeypatch.setattr(module, "execute_check", real)
    monkeypatch.setattr(
        "services.stream_processor.handlers.read_raw",
        lambda _: pytest.fail("prepared inputs must be reused"),
    )
    result = handler.handle_dataset_ingested(event)
    assert len(result.checks[0].attempts) == 1
    assert len(result.checks[1].attempts) == 2
    assert result.decision.status is QualityStatus.PASS


def test_missing_expected_column_produces_unknown_instead_of_disappearing(tmp_path):
    handler, _, _, _, _, _ = make_handler(suites={"orders": suite()})
    result = handler.handle_dataset_ingested(event_for(tmp_path, [{"another": 1}]))
    assert result.decision.status is QualityStatus.WARN
    assert result.checks[1].result.status.value == "UNKNOWN"


def test_finalizer_rejects_foreign_duplicate_and_pending_results(tmp_path):
    handler, _, _, _, _, _ = make_handler(suites={"orders": suite()})
    result = handler.handle_dataset_ingested(event_for(tmp_path))
    assert finalize(result.suite, "b1", result.checks[:1]).status is QualityStatus.BLOCK
    with pytest.raises(ValueError, match="duplicate"):
        finalize(result.suite, "b1", result.checks + result.checks[:1])
    with pytest.raises(ValueError, match="scope"):
        finalize(result.suite, "another-batch", result.checks)
    pending = result.checks[1].model_copy(
        update={"state": ExecutionStatus.PENDING, "result": None}
    )
    with pytest.raises(ValueError, match="pending"):
        finalize(result.suite, "b1", (result.checks[0], pending))


def test_same_batch_with_different_metadata_is_rejected(tmp_path):
    handler, _, _, _, _, _ = make_handler(suites={"orders": suite()})
    event = event_for(tmp_path)
    handler.handle_dataset_ingested(event)
    with pytest.raises(ValueError, match="metadata"):
        handler.handle_dataset_ingested(
            event.model_copy(update={"received_at": NOW + timedelta(hours=1)})
        )


def test_error_classifier_does_not_retry_bad_input_or_missing_file():
    assert transient(ConnectionError())
    assert not transient(ValueError())
    assert not transient(FileNotFoundError())


def test_unconfigured_table_is_evaluated_using_default_profile(tmp_path):
    from data_quality.config import load_suites
    from tests.data_quality.test_configuration import config_tree, enable_fallback

    config_root = tmp_path / "config"
    config_root.mkdir()
    config_tree(config_root)
    enable_fallback(config_root)
    (config_root / "datasets/orders.yaml").unlink()
    handler, _, _, _, quality, _ = make_handler(suites=load_suites(config_root))
    event = event_for(tmp_path).model_copy(update={"dataset_id": "new_table"})
    result = handler.handle_dataset_ingested(event)
    assert result.decision.status is QualityStatus.PASS
    assert result.suite.dataset_id == "new_table"
    assert [c.check_id for c in result.checks] == ["new_table.age"]
    assert quality.get_quality_decision("new_table", "b1").status is QualityStatus.PASS


def test_fail_is_not_retried_but_notification_delivery_is(tmp_path):
    handler, _, _, _, _, publisher = make_handler(suites={"orders": suite(100)})
    delivered = []

    def publish(event):
        delivered.append(event)
        if len(delivered) < 3:
            raise ConnectionError("broker unavailable")

    publisher.publish = publish
    result = handler.handle_dataset_ingested(event_for(tmp_path))
    assert result.decision.status is QualityStatus.BLOCK
    assert all(len(c.attempts) == 1 for c in result.checks)
    assert len(result.delivery_attempts) == 3
    assert len({e.event_id for e in delivered}) == 1
    handler.handle_dataset_ingested(result.event)
    assert len(delivered) == 3


def test_interrupted_last_attempt_does_not_reset_budget(tmp_path, monkeypatch):
    configured = suite()
    configured = configured.model_copy(
        update={
            "retry": configured.retry.model_copy(
                update={"max_attempts": 1, "delays_seconds": ()}
            )
        }
    )
    handler, _, _, _, quality, _ = make_handler(suites={"orders": configured})

    def interrupted(_):
        raise KeyboardInterrupt()

    monkeypatch.setattr("services.stream_processor.handlers.read_raw", interrupted)
    event = event_for(tmp_path)
    with pytest.raises(KeyboardInterrupt):
        handler.handle_dataset_ingested(event)
    assert next(iter(quality.evaluations.values())).decision is None
    result = handler.handle_dataset_ingested(event)
    assert result.decision.status is QualityStatus.BLOCK
    assert len(result.preparation_attempts) == 1
    assert all(c.code == "interrupted" for c in result.checks)
