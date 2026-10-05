"""Wire format and validation of Kafka event contracts."""

from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone
import json
import unittest

from contracts.events import (
    AnomalyDetected, CONTRACT_VERSION, DataQualityFailed, DatasetIngested, EventType,
    PipelineFailed, PipelineFinished, PipelineStarted, SchemaChanged,
    decode_event, encode_event, event_from_dict, event_to_dict,
)

NOW = datetime(2026, 10, 5, 10, 1, tzinfo=timezone.utc)
LATER = NOW + timedelta(minutes=5)


def sample_events():
    """Return one valid event of every supported type."""
    return (
        DatasetIngested("e1", "orders", "2026-10-05-001", 182340, NOW, 3),
        PipelineStarted("e2", "orders_daily", "run-1", NOW),
        PipelineFinished("e3", "orders_daily", "run-1", NOW, LATER),
        PipelineFailed("e4", "orders_daily", "run-1", NOW, LATER, "timeout"),
        PipelineFailed("e5", "orders_daily", "run-2", NOW, LATER, None),
        SchemaChanged("e6", "orders", "2026-10-05-001", 2, 3, NOW),
        DataQualityFailed("e7", "orders", "2026-10-05-001", "orders.row_count",
                          "below_minimum", NOW),
        AnomalyDetected("e8", "anomaly-1", "orders", "2026-10-05-001", "row_count",
                        4500000, 1800000, 0.97, "rule.row_count_ratio", None, NOW),
    )


class EventRoundTripTests(unittest.TestCase):
    """Verify that events survive serialization unchanged."""

    def test_every_event_round_trips_through_dict_and_bytes(self):
        """Encode and decode all event types without losing information."""
        for event in sample_events():
            with self.subTest(event=event):
                self.assertEqual(event_from_dict(event_to_dict(event)), event)
                self.assertEqual(decode_event(encode_event(event)), event)
                self.assertEqual(decode_event(encode_event(event).decode("utf-8")), event)

    def test_wire_format_names_type_and_version(self):
        """Expose the stable type name and contract version on the wire."""
        payload = json.loads(encode_event(sample_events()[0]))
        self.assertEqual(payload["event_type"], "dataset.ingested")
        self.assertEqual(payload["contract_version"], CONTRACT_VERSION)
        self.assertEqual(payload["received_at"], "2026-10-05T10:01:00+00:00")

    def test_utc_suffix_z_is_accepted(self):
        """Accept the ISO 8601 form used in the architecture example."""
        payload = event_to_dict(sample_events()[0])
        payload["received_at"] = "2026-10-05T10:01:00Z"
        self.assertEqual(event_from_dict(payload).received_at, NOW)

    def test_events_are_immutable(self):
        """Prevent reassignment of event fields."""
        with self.assertRaises(FrozenInstanceError):
            sample_events()[0].records = 1


class EventValidationTests(unittest.TestCase):
    """Verify rejection of invalid event values."""

    def test_invalid_values_are_rejected(self):
        """Reject blank identifiers, bad counts, naive times and bad ordering."""
        naive = NOW.replace(tzinfo=None)
        factories = (
            lambda: DatasetIngested("", "orders", "b", 1, NOW, 1),
            lambda: DatasetIngested("e", "orders", "b", -1, NOW, 1),
            lambda: DatasetIngested("e", "orders", "b", True, NOW, 1),
            lambda: DatasetIngested("e", "orders", "b", 1, naive, 1),
            lambda: DatasetIngested("e", "orders", "b", 1, NOW, 0),
            lambda: PipelineStarted("e", "p", " ", NOW),
            lambda: PipelineFinished("e", "p", "r", LATER, NOW),
            lambda: PipelineFailed("e", "p", "r", LATER, NOW, None),
            lambda: PipelineFailed("e", "p", "r", NOW, LATER, " "),
            lambda: SchemaChanged("e", "orders", "b", 3, 3, NOW),
            lambda: SchemaChanged("e", "orders", "b", 0, 1, NOW),
            lambda: DataQualityFailed("e", "orders", "b", "", "code", NOW),
            lambda: AnomalyDetected("e", "a", "orders", None, "m", 1, 2, 1.5, "d", None, NOW),
            lambda: AnomalyDetected("e", "a", "orders", None, "m", float("nan"), 2, 0.5, "d", None, NOW),
            lambda: AnomalyDetected("e", "a", "orders", None, "m", True, 2, 0.5, "d", None, NOW),
            lambda: AnomalyDetected("e", "a", "orders", " ", "m", 1, 2, 0.5, "d", None, NOW),
        )
        for factory in factories:
            with self.subTest(factory=factory):
                with self.assertRaises(ValueError):
                    factory()

    def test_unsupported_event_object_is_rejected(self):
        """Refuse to encode objects that are not event contracts."""
        with self.assertRaises(ValueError):
            event_to_dict({"event_type": "dataset.ingested"})


class EventDecodingTests(unittest.TestCase):
    """Verify rejection of malformed messages."""

    def setUp(self):
        """Create a valid payload to damage in each test."""
        self.payload = event_to_dict(sample_events()[0])

    def test_malformed_messages_are_rejected(self):
        """Reject non-JSON, non-objects, bad versions, types and fields."""
        for data in (b"not json", b"[]", b"\xff\xfe", "null"):
            with self.subTest(data=data):
                with self.assertRaises(ValueError):
                    decode_event(data)

    def test_header_problems_are_rejected(self):
        """Reject wrong contract versions and unknown or unsupported types."""
        for changes in (
            {"contract_version": 2}, {"contract_version": None},
            {"event_type": "nope"}, {"event_type": None},
            {"event_type": "incident.created"},
        ):
            with self.subTest(changes=changes):
                with self.assertRaises(ValueError):
                    event_from_dict({**self.payload, **changes})

    def test_missing_and_unexpected_fields_are_rejected(self):
        """Require exactly the fields of the event contract."""
        missing = {k: v for k, v in self.payload.items() if k != "batch_id"}
        with self.assertRaisesRegex(ValueError, "missing"):
            event_from_dict(missing)
        with self.assertRaisesRegex(ValueError, "unexpected"):
            event_from_dict({**self.payload, "extra": 1})

    def test_bad_timestamps_are_rejected(self):
        """Reject non-ISO, non-string and timezone-less timestamps."""
        for value in ("yesterday", 1760000000, "2026-10-05T10:01:00"):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    event_from_dict({**self.payload, "received_at": value})

    def test_all_declared_types_are_either_supported_or_documented(self):
        """Keep the registry aligned with the event names of the architecture."""
        supported = {event.event_type for event in sample_events()}
        self.assertEqual(
            {t.value for t in EventType} - {t.value for t in supported},
            {"incident.created", "incident.updated"},
        )


if __name__ == "__main__":
    unittest.main()
