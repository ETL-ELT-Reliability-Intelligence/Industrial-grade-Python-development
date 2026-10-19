from datetime import datetime, timezone
from typing import Callable

from contracts.events import DatasetIngestedEvent, Event, SchemaChangedEvent, parse_event

from .normalize import column_names, schema_fingerprint, validate
from .publisher import EventPublisher
from .sources import Source
from .storage import RawStore


class IngestionService:
    """Fetch, validate, store raw data, publish events.

    Never judges whether a dataset is anomalous - that is Reliability Intelligence.

    Delivery is at-least-once. With a `run_id`, ingestion is idempotent: the events of a run are
    committed to the store before publishing, so retrying the same run (e.g. an Airflow task retry)
    republishes the same events instead of creating a second batch. Consumers dedupe by `dedup_key`.
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

    def ingest(self, source: Source, run_id: str | None = None) -> DatasetIngestedEvent:
        if run_id is not None:
            committed = self._store.load_run(source.dataset_id, run_id)
            if committed is not None:
                events = [parse_event(payload) for payload in committed]
                self._publish_all(events)
                return next(e for e in events if isinstance(e, DatasetIngestedEvent))

        batch = source.fetch()
        validate(batch)

        received_at = self._clock()
        day = received_at.strftime("%Y-%m-%d")
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
        )
        events.append(ingested)

        if run_id is not None:
            self._store.save_run(batch.dataset_id, run_id, [e.model_dump_json() for e in events])
        self._publish_all(events)
        return ingested

    def _publish_all(self, events: list[Event]) -> None:
        for event in events:
            self._publisher.publish(event)
