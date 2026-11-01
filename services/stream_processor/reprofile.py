"""Retry only feature accumulation for an existing quality evaluation."""

import argparse
from datetime import datetime, timezone
import os
from pathlib import Path

from storage.postgres import (
    PostgresMetricRepository,
    PostgresQualityRepository,
    connect,
)
from .feature_accumulation import FeatureAccumulator, load_feature_definitions


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("evaluation_id")
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("--feature-config", type=Path)
    args = parser.parse_args(argv)
    if not args.database_url:
        parser.error("DATABASE_URL or --database-url is required")
    definitions = load_feature_definitions(args.feature_config)
    with connect(args.database_url) as connection:
        quality = PostgresQualityRepository(connection)
        evaluation = quality.get_evaluation(args.evaluation_id)
        if evaluation is None or evaluation.decision is None:
            parser.error("a completed quality evaluation is required")
        with quality.evaluation_lock(
            evaluation.event.dataset_id, evaluation.event.batch_id
        ):
            result = FeatureAccumulator(
                PostgresMetricRepository(connection),
                lambda: datetime.now(timezone.utc),
                definitions,
            ).collect(evaluation)
        print(f"{result.attempt_id}: {result.status}")
        return 0 if result.status == "COMPLETED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
