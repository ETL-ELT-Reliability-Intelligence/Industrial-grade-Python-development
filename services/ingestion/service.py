from datetime import datetime, timezone
from typing import Callable

from contracts.events import DatasetIngestedEvent, SchemaChangedEvent

from .normalize import column_names, schema_fingerprint, validate
from .publisher import EventPublisher
from .sources import Source
from .storage import RawStore


class IngestionService:
    """Fetch, validate, store raw data, publish events.

    Never judges whether a dataset is anomalous - that is Reliability Intelligence.
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

    def ingest(self, source: Source) -> DatasetIngestedEvent:
        batch = source.fetch()
        validate(batch)

        received_at = self._clock()
        day = received_at.strftime("%Y-%m-%d")
        batch_id = f"{day}-{self._store.next_batch_number(batch.dataset_id, day):03d}"

        raw_uri = self._store.save_raw(batch.dataset_id, batch_id, batch.records)
        version, previous = self._store.schema_version(
            batch.dataset_id, schema_fingerprint(column_names(batch.records))
        )

        if previous is not None:
            self._publisher.publish(
                SchemaChangedEvent(
                    dataset_id=batch.dataset_id,
                    previous_version=previous,
                    new_version=version,
                    detected_at=received_at,
                )
            )
        event = DatasetIngestedEvent(
            dataset_id=batch.dataset_id,
            batch_id=batch_id,
            records=len(batch.records),
            received_at=received_at,
            schema_version=version,
            raw_uri=raw_uri,
        )
        self._publisher.publish(event)
        return event
