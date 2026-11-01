from pathlib import Path

import pytest

from data_quality.config import load_suites


def config_tree(tmp_path):
    (tmp_path / "profiles").mkdir()
    (tmp_path / "datasets").mkdir()
    (tmp_path / "defaults.yaml").write_text(
        "policy:\n  on_fail: BLOCK\n  on_unknown: WARN\n", encoding="utf-8"
    )
    (tmp_path / "profiles/base.yaml").write_text(
        "checks:\n  age:\n    kind: freshness\n    params:\n      max_age_seconds: 60\n",
        encoding="utf-8",
    )
    (tmp_path / "datasets/orders.yaml").write_text(
        "dataset_id: orders\nprofile: base\noverrides:\n  age:\n    params:\n      max_age_seconds: 120\n",
        encoding="utf-8",
    )
    return tmp_path


def test_profiles_do_not_share_mutable_overrides(tmp_path):
    root = config_tree(tmp_path)
    (root / "datasets/customers.yaml").write_text(
        "dataset_id: customers\nprofile: base\n", encoding="utf-8"
    )
    suites = load_suites(root)
    assert suites["orders"].checks[0].params["max_age_seconds"] == 120
    assert suites["customers"].checks[0].params["max_age_seconds"] == 60
    assert suites["orders"].fingerprint != suites["customers"].fingerprint


@pytest.mark.parametrize(
    "content",
    [
        "dataset_id: orders\nprofile: base\noverrides:\n  typo: {}\n",
        "dataset_id: orders\nprofile: base\nchecks:\n  age:\n    kind: freshness\n",
        "dataset_id: orders\nprofile: ../base\n",
        "dataset_id: orders\ndataset_id: duplicate\nprofile: base\n",
        "dataset_id: orders\nprofile: base\noverrides:\n  age:\n    params:\n      unknown: 1\n",
        "dataset_id: orders\nprofile: base\noverrides:\n  age:\n    on_fail: PASS\n",
        "dataset_id: orders\nchecks:\n  bad:\n    kind: nonexistent\n",
        "dataset_id: orders\nchecks:\n  bad:\n    kind: null_rate\n",
        "dataset_id: orders\nchecks:\n  bad:\n    kind: row_count\n    params:\n      history_window: 2\n      min_history: 3\n",
    ],
)
def test_invalid_configuration_is_rejected_at_startup(tmp_path, content):
    root = config_tree(tmp_path)
    (root / "datasets/orders.yaml").write_text(content, encoding="utf-8")
    with pytest.raises(ValueError):
        load_suites(root)


def test_shipped_profiles_load():
    suites = load_suites(Path("config/quality"))
    assert {"orders", "customers", "order_items", "order_payments"} <= suites.keys()
    assert all(suite.checks for suite in suites.values())


def enable_fallback(root):
    path = root / "defaults.yaml"
    path.write_text(
        "default_profile: base\n" + path.read_text(encoding="utf-8"), encoding="utf-8"
    )


@pytest.mark.parametrize("directory_exists", [True, False])
def test_default_profile_works_without_dataset_files(tmp_path, directory_exists):
    root = config_tree(tmp_path)
    enable_fallback(root)
    (root / "datasets/orders.yaml").unlink()
    if not directory_exists:
        (root / "datasets").rmdir()
    suites = load_suites(root)
    orders = suites.get("orders")
    customers = suites.get("customers")
    assert orders.dataset_id == "orders"
    assert customers.dataset_id == "customers"
    assert orders.checks[0].params["max_age_seconds"] == 60
    assert orders.fingerprint != customers.fingerprint
    orders.checks[0].params["max_age_seconds"] = 999
    assert suites.get("orders").checks[0].params["max_age_seconds"] == 60


def test_dataset_override_takes_priority_over_fallback(tmp_path):
    root = config_tree(tmp_path)
    enable_fallback(root)
    path = root / "datasets/orders.yaml"
    path.write_text(
        path.read_text(encoding="utf-8").replace("profile: base\n", ""),
        encoding="utf-8",
    )
    suites = load_suites(root)
    assert suites["orders"].checks[0].params["max_age_seconds"] == 120
    assert suites["new_table"].checks[0].params["max_age_seconds"] == 60


def test_bad_dataset_configuration_does_not_fall_back_silently(tmp_path):
    root = config_tree(tmp_path)
    enable_fallback(root)
    (root / "datasets/orders.yaml").write_text(
        "dataset_id: orders\noverrides:\n  typo: {}\n", encoding="utf-8"
    )
    with pytest.raises(ValueError, match="unknown check"):
        load_suites(root)


def test_default_profile_is_validated_even_when_all_tables_have_configuration(tmp_path):
    root = config_tree(tmp_path)
    (root / "profiles/broken.yaml").write_text(
        "checks:\n  bad:\n    kind: missing\n", encoding="utf-8"
    )
    (root / "defaults.yaml").write_text("default_profile: broken\n", encoding="utf-8")
    with pytest.raises(ValueError):
        load_suites(root)


def test_missing_dataset_still_rejected_without_default_profile(tmp_path):
    suites = load_suites(config_tree(tmp_path))
    assert suites.get("unknown") is None
