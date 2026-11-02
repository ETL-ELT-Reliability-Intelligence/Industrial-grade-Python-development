"""Every dataset ingestion can emit must have a quality suite and a feature entry, or the consumer rejects its batches."""

import json
from pathlib import Path

import pytest

from services.ingestion.sources import MockApiSource, OlistSource

CONFIG = Path(__file__).resolve().parents[2] / "config"
DATASET_IDS = [MockApiSource.dataset_id, *OlistSource.FILES]


@pytest.mark.parametrize("dataset_id", DATASET_IDS)
def test_dataset_has_quality_suite(dataset_id):
    assert (CONFIG / "quality" / "datasets" / f"{dataset_id}.yaml").is_file()


@pytest.mark.parametrize("dataset_id", DATASET_IDS)
def test_dataset_has_feature_entry(dataset_id):
    assert dataset_id in json.loads((CONFIG / "features.json").read_text(encoding="utf-8"))
