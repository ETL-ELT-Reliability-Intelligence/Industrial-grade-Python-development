"""Explicitly reevaluate a recorded batch using current configuration."""

import argparse
import os
from pathlib import Path

from data_quality.config import load_suites
from services.stream_processor.handlers import StreamEventHandler
from services.stream_processor.publisher import KafkaPublisher
from services.stream_processor.feature_accumulation import load_feature_definitions
from storage.postgres import (
    connect,
    PostgresDatasetRepository,
    PostgresMetricRepository,
    PostgresPipelineRunRepository,
    PostgresQualityRepository,
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "evaluation_id", help="Existing evaluation whose batch should be reevaluated"
    )
    parser.add_argument(
        "--request-id",
        required=True,
        help="New ID creates an evaluation; reuse resumes it",
    )
    parser.add_argument(
        "--quality-config",
        type=Path,
        default=Path(__file__).resolve().parents[2] / "config/quality",
    )
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("--feature-config", type=Path)
    parser.add_argument(
        "--kafka", help="Publish failure notifications; omit for DB-only evaluation"
    )
    args = parser.parse_args()
    if (
        not args.database_url
        or not args.request_id.strip()
        or args.request_id == "initial"
    ):
        parser.error(
            "DATABASE_URL and a non-empty request-id other than 'initial' are required"
        )
    suites = load_suites(args.quality_config)
    with connect(args.database_url) as connection:
        quality = PostgresQualityRepository(connection)
        original = quality.get_evaluation(args.evaluation_id)
        if original is None:
            parser.error("evaluation not found")
        handler = StreamEventHandler(
            PostgresDatasetRepository(connection),
            PostgresPipelineRunRepository(connection),
            PostgresMetricRepository(connection),
            quality,
            suites=suites,
            feature_definitions=load_feature_definitions(args.feature_config),
            publisher=KafkaPublisher(args.kafka) if args.kafka else None,
        )
        evaluation = handler.handle_dataset_ingested(
            original.event, request_id=args.request_id
        )
        print(f"{evaluation.evaluation_id}: {evaluation.decision.status.value}")


if __name__ == "__main__":
    main()
