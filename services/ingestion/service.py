"""Ingest source batches into raw storage and publish their lifecycle events."""

from datetime import datetime, timezone
from typing import Callable
import logging

from contracts.events import (
    DatasetIngestedEvent,
    Event,
    SchemaChangedEvent,
    parse_event,
)

from .normalize import column_names, schema_fingerprint, validate
from .publisher import EventPublisher
from .sources import Source
from .storage import RawStore

logger = logging.getLogger(__name__)


class IngestionService:
    """Coordinate fetching, validation, raw storage and at-least-once publication.

    Quality and anomaly decisions belong to downstream consumers. Run records
    are committed before publication; consumers must deduplicate events.
    """

    def __init__(
        self,
        store: RawStore,
        publisher: EventPublisher,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ):
        self._store = store
        self._publisher = publisher
        self._clock = clock

    def ingest(
        self,
        source: Source,
        run_id: str | None = None,
        *,
        period_start=None,
        period_end=None,
    ) -> DatasetIngestedEvent:
        """Store a source batch and publish ingestion and schema-change events.

        A crash before the run record is saved can leave orphan raw data. A crash
        during publication can deliver only part of the event list; retrying with
        the same run_id republishes all saved events. Batch numbers are not
        reserved atomically: a FileExistsError triggers one allocation retry.
        Source, validation, storage and publication errors propagate.

        Args:
            source: Source to fetch when no committed run exists.
            run_id: Optional retry identity within the dataset. A committed run
                republishes saved events without fetching or saving another batch.
            period_start: Optional period override that sets hourly granularity.
            period_end: Optional end override; event validation checks boundaries.

        Returns:
            The ingested event, including when replaying a committed run.
        """
        if run_id is not None:
            committed = self._store.load_run(source.dataset_id, run_id)
            if committed is not None:
                events = [parse_event(payload) for payload in committed]
                self._publish_all(events)
                ingested = next(e for e in events if isinstance(e, DatasetIngestedEvent))
                self._log_batch(ingested)
                return ingested

        batch = source.fetch()
        validate(batch)

        received_at = self._clock()
        day = received_at.strftime("%Y-%m-%d")
        try:
            batch_id = f"{day}-{self._store.next_batch_number(batch.dataset_id, day):03d}"
            raw_uri = self._store.save_raw(batch.dataset_id, batch_id, batch.records)
        except FileExistsError:  # lost a race for the batch number; the next listing sees the winner
            batch_id = f"{day}-{self._store.next_batch_number(batch.dataset_id, day):03d}"
            raw_uri = self._store.save_raw(batch.dataset_id, batch_id, batch.records)
        version, previous = self._store.schema_version(
            batch.dataset_id, schema_fingerprint(column_names(batch.records))
        )

        events: list[Event] = []
        if previous is not None:
            events.append(
                SchemaChangedEvent(
                    dataset_id=batch.dataset_id,
                    batch_id=batch_id,
                    previous_version=previous,
                    new_version=version,
                    detected_at=received_at,
                )
            )
        ingested = DatasetIngestedEvent(
            dataset_id=batch.dataset_id,
            batch_id=batch_id,
            records=len(batch.records),
            received_at=received_at,
            schema_version=version,
            raw_uri=raw_uri,
            source_id=batch.source_id,
            granularity="hour" if period_start is not None else batch.granularity,
            period_start=period_start
            if period_start is not None
            else batch.period_start,
            period_end=period_end if period_end is not None else batch.period_end,
            complete=batch.complete,
        )
        events.append(ingested)

        if run_id is not None:
            self._store.save_run(
                batch.dataset_id, run_id, [e.model_dump_json() for e in events]
            )
        self._publish_all(events)
        self._log_batch(ingested)
        return ingested

    @staticmethod
    def _log_batch(event: DatasetIngestedEvent) -> None:
        logger.info(
            "Ingested dataset=%s batch_id=%s period_start=%s period_end=%s records=%d",
            event.dataset_id, event.batch_id, event.period_start, event.period_end,
            event.records,
        )

    def _publish_all(self, events: list[Event]) -> None:
        for event in events:
            self._publisher.publish(event)
