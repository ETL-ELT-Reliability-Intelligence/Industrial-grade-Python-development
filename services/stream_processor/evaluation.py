"""Durable evaluation lifecycle, bounded retries and explicit reevaluation."""

from datetime import datetime, timezone
from dataclasses import dataclass
import errno
import json
import time
from uuid import NAMESPACE_URL, uuid5

from contracts.evaluation import Attempt, CheckExecution, Evaluation, ExecutionStatus
from data_quality.execution import execute_check, finalize


@dataclass(frozen=True)
class PreparedChecks:
    """Prepared executions and the optional checksum of the raw bytes used."""

    checks: tuple[CheckExecution, ...]
    raw_sha256: str | None


def transient(error: Exception) -> bool:
    """Classify retryable transport and database failures, including chained causes."""
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
    """Persist evaluation progress and resume work within bounded retry budgets."""

    def __init__(
        self, repository, clock=lambda: datetime.now(timezone.utc), sleep=time.sleep
    ):
        self.repository = repository
        self.clock = clock
        self.sleep = sleep

    def run(self, event, suite, prepare, *, request_id: str | None = None):
        """Create or resume a batch evaluation under its repository lock.

        Args:
            event: Batch metadata; repeated identities must retain the same metadata.
            suite: Configuration for a new evaluation; resumed work uses its saved plan.
            prepare: Callback returning PreparedChecks from the persisted evaluation.
            request_id: None selects the initial evaluation. Reusing an explicit ID
                resumes that request; a new ID creates a separate evaluation.

        Returns:
            A completed evaluation with a saved quality decision. An existing
            decision is returned without rerunning checks. Delivery is separate.

        Raises:
            ValueError: The request ID is empty or reserved, a new evaluation lacks
                configuration, or repeated batch metadata conflicts.
        """
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
            elif evaluation.event.model_dump(exclude={"event_id"}) != event.model_dump(
                exclude={"event_id"}
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
        """Run projection and publication with a persisted, bounded attempt budget.

        The operation must tolerate replay after partial delivery. Published
        evaluations are returned unchanged. Temporary failures are retried;
        permanent failures and exhausted budgets propagate or raise RuntimeError.
        """
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
                prepared = prepare(evaluation)
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
                        "checks": prepared.checks,
                        "raw_sha256": prepared.raw_sha256,
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
