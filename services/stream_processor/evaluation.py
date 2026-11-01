"""Durable evaluation lifecycle, bounded retries and explicit reevaluation."""

from datetime import datetime, timezone
import errno
import json
import time
from uuid import NAMESPACE_URL, uuid5

from contracts.evaluation import Attempt, CheckExecution, Evaluation, ExecutionStatus
from data_quality.execution import execute_check, finalize


def transient(error: Exception) -> bool:
    """Retry temporary transport failures, never malformed input/configuration."""
    if isinstance(error, (TimeoutError, ConnectionError)):
        return True
    if isinstance(error, OSError) and error.errno in (
        errno.EAGAIN,
        errno.ECONNRESET,
        errno.ETIMEDOUT,
        errno.ECONNREFUSED,
    ):
        return True
    # Optional SDKs are not imported just to classify an exception.
    if type(error).__module__.startswith("botocore"):
        if type(error).__name__ in {
            "EndpointConnectionError",
            "ConnectTimeoutError",
            "ReadTimeoutError",
            "ConnectionClosedError",
        }:
            return True
        response = getattr(error, "response", {})
        if response.get("Error", {}).get("Code") in {
            "SlowDown",
            "RequestTimeout",
            "ServiceUnavailable",
            "InternalError",
        }:
            return True
    if type(error).__module__.startswith("psycopg"):
        return getattr(error, "sqlstate", None) in {"40001", "40P01", "55P03", "57014"}
    cause = error.__cause__
    return isinstance(cause, Exception) and transient(cause)


class EvaluationRunner:
    def __init__(
        self, repository, clock=lambda: datetime.now(timezone.utc), sleep=time.sleep
    ):
        self.repository = repository
        self.clock = clock
        self.sleep = sleep

    def run(self, event, suite, prepare, *, request_id: str | None = None):
        """The same explicit request_id resumes; a new one requests a new evaluation."""
        if request_id is not None and (
            not request_id.strip() or request_id == "initial"
        ):
            raise ValueError(
                "request_id must be non-empty and different from 'initial'"
            )
        identity = json.dumps(["quality", event.dataset_id, event.batch_id, request_id])
        evaluation_id = str(uuid5(NAMESPACE_URL, identity))
        with self.repository.evaluation_lock(event.dataset_id, event.batch_id):
            evaluation = self.repository.get_evaluation(evaluation_id)
            if evaluation is None:
                if suite is None:
                    raise ValueError(f"no quality configuration for {event.dataset_id}")
                evaluation = Evaluation(
                    evaluation_id=evaluation_id,
                    event=event,
                    suite=suite,
                    config_hash=suite.fingerprint,
                    evaluated_at=max(self.clock(), event.received_at),
                    checks=tuple(
                        CheckExecution(check_id=f"{suite.dataset_id}.{c.key}", spec=c)
                        for c in suite.checks
                    ),
                )
                self.repository.save_evaluation(evaluation)
            elif (
                evaluation.event.dataset_id,
                evaluation.event.batch_id,
                evaluation.event.raw_uri,
                evaluation.event.received_at,
                evaluation.event.schema_version,
                evaluation.event.records,
            ) != (
                event.dataset_id,
                event.batch_id,
                event.raw_uri,
                event.received_at,
                event.schema_version,
                event.records,
            ):
                raise ValueError(
                    "the same batch identity was delivered with different metadata"
                )
            if evaluation.decision is not None:
                return evaluation
            if not evaluation.prepared:
                evaluation = self._prepare(evaluation, prepare)
            checks = []
            for check in evaluation.checks:
                if check.state not in (
                    ExecutionStatus.COMPLETED,
                    ExecutionStatus.SKIPPED,
                    ExecutionStatus.ERROR,
                ):
                    check = self._execute(evaluation, check)
                checks.append(check)
            evaluation = evaluation.model_copy(update={"checks": tuple(checks)})
            decision = finalize(evaluation.suite, event.batch_id, evaluation.checks)
            evaluation = evaluation.model_copy(
                update={"state": ExecutionStatus.COMPLETED, "decision": decision}
            )
            self.repository.save_evaluation(evaluation)
            return evaluation

    def deliver(self, evaluation, operation):
        """Project and publish with a durable, bounded attempt budget."""
        if evaluation.published:
            return evaluation
        attempts = evaluation.delivery_attempts
        settings = evaluation.suite.retry
        if attempts and attempts[-1].code == "delivery_error":
            raise RuntimeError("Permanent delivery error; inspect the saved evaluation")
        while len(attempts) < settings.max_attempts:
            if attempts:
                self.sleep(settings.delays_seconds[len(attempts) - 1])
            attempts += (Attempt(number=len(attempts) + 1, started_at=self.clock()),)
            evaluation = evaluation.model_copy(update={"delivery_attempts": attempts})
            self.repository.save_evaluation(evaluation)
            try:
                operation(evaluation)
            except Exception as error:
                retryable = transient(error)
                attempts = attempts[:-1] + (
                    attempts[-1].model_copy(
                        update={
                            "finished_at": self.clock(),
                            "code": "temporary_error"
                            if retryable
                            else "delivery_error",
                            "message": str(error),
                        }
                    ),
                )
                evaluation = evaluation.model_copy(
                    update={"delivery_attempts": attempts}
                )
                self.repository.save_evaluation(evaluation)
                if retryable and len(attempts) < settings.max_attempts:
                    continue
                raise
            attempts = attempts[:-1] + (
                attempts[-1].model_copy(update={"finished_at": self.clock()}),
            )
            evaluation = evaluation.model_copy(
                update={"delivery_attempts": attempts, "published": True}
            )
            self.repository.save_evaluation(evaluation)
            return evaluation
        raise RuntimeError(
            "Delivery attempt budget exhausted; inspect the saved evaluation"
        )

    def _prepare(self, evaluation, prepare):
        settings = evaluation.suite.retry
        attempts = evaluation.preparation_attempts
        while len(attempts) < settings.max_attempts:
            if attempts:
                self.sleep(settings.delays_seconds[len(attempts) - 1])
            attempts += (Attempt(number=len(attempts) + 1, started_at=self.clock()),)
            evaluation = evaluation.model_copy(
                update={
                    "state": ExecutionStatus.RUNNING,
                    "preparation_attempts": attempts,
                }
            )
            self.repository.save_evaluation(evaluation)
            try:
                checks = prepare(evaluation)
            except Exception as error:
                retryable = transient(error)
                code = "temporary_error" if retryable else "preparation_error"
                attempts = attempts[:-1] + (
                    attempts[-1].model_copy(
                        update={
                            "finished_at": self.clock(),
                            "code": code,
                            "message": str(error),
                        }
                    ),
                )
                evaluation = evaluation.model_copy(
                    update={"preparation_attempts": attempts}
                )
                self.repository.save_evaluation(evaluation)
                if retryable and len(attempts) < settings.max_attempts:
                    continue
                break
            else:
                attempts = attempts[:-1] + (
                    attempts[-1].model_copy(update={"finished_at": self.clock()}),
                )
                evaluation = evaluation.model_copy(
                    update={
                        "checks": checks,
                        "prepared": True,
                        "preparation_attempts": attempts,
                    }
                )
                self.repository.save_evaluation(evaluation)
                return evaluation
        last = attempts[-1]
        checks = tuple(
            c.model_copy(
                update={
                    "state": ExecutionStatus.ERROR,
                    "code": last.code or "interrupted",
                    "message": last.message
                    or "Preparation interrupted; attempt budget exhausted.",
                }
            )
            for c in evaluation.checks
        )
        evaluation = evaluation.model_copy(update={"checks": checks})
        self.repository.save_evaluation(evaluation)
        return evaluation

    def _execute(self, evaluation, check):
        settings = evaluation.suite.retry
        attempts = check.attempts
        while len(attempts) < settings.max_attempts:
            if attempts:
                self.sleep(settings.delays_seconds[len(attempts) - 1])
            attempts += (Attempt(number=len(attempts) + 1, started_at=self.clock()),)
            check = check.model_copy(
                update={"state": ExecutionStatus.RUNNING, "attempts": attempts}
            )
            self.repository.save_execution(evaluation.evaluation_id, check)
            try:
                result = execute_check(check, evaluation.event)
            except Exception as error:
                retryable = transient(error)
                attempts = attempts[:-1] + (
                    attempts[-1].model_copy(
                        update={
                            "finished_at": self.clock(),
                            "code": "temporary_error"
                            if retryable
                            else "execution_error",
                            "message": str(error),
                        }
                    ),
                )
                check = check.model_copy(update={"attempts": attempts})
                self.repository.save_execution(evaluation.evaluation_id, check)
                if retryable and len(attempts) < settings.max_attempts:
                    continue
                break
            else:
                attempts = attempts[:-1] + (
                    attempts[-1].model_copy(update={"finished_at": self.clock()}),
                )
                check = check.model_copy(
                    update={
                        "state": ExecutionStatus.COMPLETED,
                        "attempts": attempts,
                        "result": result,
                    }
                )
                self.repository.save_execution(evaluation.evaluation_id, check)
                return check
        last = attempts[-1]
        check = check.model_copy(
            update={
                "state": ExecutionStatus.ERROR,
                "code": last.code or "interrupted",
                "message": last.message
                or "Check interrupted; attempt budget exhausted.",
            }
        )
        self.repository.save_execution(evaluation.evaluation_id, check)
        return check
