"""Validated configuration: defaults -> one profile -> dataset overrides."""

from collections.abc import Mapping, Iterator
from copy import deepcopy
import json
from pathlib import Path
import yaml
from contracts.quality_config import CheckSuite, Reactions, RetrySettings


class _UniqueLoader(yaml.SafeLoader):
    pass


def _mapping(loader, node):
    result = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node)
        if not isinstance(key, str) or key in result:
            raise ValueError(f"invalid or duplicate YAML key: {key}")
        result[key] = loader.construct_object(value_node)
    return result


_UniqueLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _mapping)


def _read(path: Path) -> dict:
    value = yaml.load(path.read_text(encoding="utf-8"), Loader=_UniqueLoader)
    if not isinstance(value, dict):
        raise ValueError(f"expected a mapping in {path}")
    return value


def _merge(base: dict, override: dict) -> dict:
    if not isinstance(base, dict) or not isinstance(override, dict):
        raise ValueError("configuration sections must be mappings")
    result = deepcopy(base)
    for key, value in override.items():
        result[key] = (
            _merge(result[key], value)
            if isinstance(value, dict) and isinstance(result.get(key), dict)
            else deepcopy(value)
        )
    return result


def _keys(value: dict, allowed: set[str]) -> None:
    if value.keys() - allowed:
        raise ValueError(
            f"unknown configuration fields: {sorted(value.keys() - allowed)}"
        )


class SuiteCatalog(Mapping[str, CheckSuite]):
    """Explicit datasets plus a validated fallback; lookups never mutate the template."""

    def __init__(self, suites: dict[str, CheckSuite], fallback: CheckSuite | None):
        self._suites = suites
        self._fallback = fallback

    def __getitem__(self, dataset_id: str) -> CheckSuite:
        if dataset_id in self._suites:
            return self._suites[dataset_id]
        if self._fallback is None:
            raise KeyError(dataset_id)
        payload = self._fallback.model_dump(mode="json")
        payload["dataset_id"] = dataset_id
        return CheckSuite.model_validate_json(json.dumps(payload))

    def __iter__(self) -> Iterator[str]:
        return iter(self._suites)

    def __len__(self) -> int:
        return len(self._suites)


def _build_suite(root: Path, defaults: dict, data: dict) -> CheckSuite:
    _keys(data, {"dataset_id", "version", "profile", "checks", "overrides", "retry"})
    profile = data.get("profile", defaults.get("default_profile"))
    checks = {}
    if profile is not None:
        if (
            not isinstance(profile, str)
            or not profile.replace("_", "").replace("-", "").isalnum()
        ):
            raise ValueError("profile must be a simple file name")
        shared = _read(root / "profiles" / f"{profile}.yaml")
        _keys(shared, {"checks"})
        checks = deepcopy(shared.get("checks", {}))
    additions = data.get("checks", {})
    overrides = data.get("overrides", {})
    if not all(isinstance(v, dict) for v in (checks, additions, overrides)):
        raise ValueError("checks and overrides must be mappings")
    if checks.keys() & additions.keys():
        raise ValueError("use overrides to change inherited checks")
    checks.update(additions)
    if overrides.keys() - checks.keys():
        raise ValueError("override refers to an unknown check")
    for key, value in overrides.items():
        checks[key] = _merge(checks[key], value)
    specs = []
    for key, value in checks.items():
        if not isinstance(value, dict) or "key" in value:
            raise ValueError("check key must be declared as the mapping key")
        specs.append({**_merge(defaults.get("policy", {}), value), "key": key})
    suite = CheckSuite.model_validate_json(
        json.dumps(
            {
                "dataset_id": data.get("dataset_id"),
                "version": data.get("version", 1),
                "retry": _merge(defaults.get("retry", {}), data.get("retry", {})),
                "checks": specs,
            }
        )
    )
    return suite


def load_suites(root: Path) -> SuiteCatalog:
    """Validate explicit datasets and the optional default profile at startup."""
    defaults = _read(root / "defaults.yaml")
    _keys(defaults, {"retry", "policy", "default_profile"})
    RetrySettings.model_validate_json(json.dumps(defaults.get("retry", {})))
    Reactions.model_validate_json(json.dumps(defaults.get("policy", {})))
    fallback = None
    if "default_profile" in defaults:
        if defaults["default_profile"] is None:
            raise ValueError("default_profile must name a profile")
        fallback = _build_suite(root, defaults, {"dataset_id": "default-template"})
    suites = {}
    for path in sorted((root / "datasets").glob("*.yaml")):
        suite = _build_suite(root, defaults, _read(path))
        if suite.dataset_id in suites:
            raise ValueError(f"duplicate dataset: {suite.dataset_id}")
        suites[suite.dataset_id] = suite
    if not suites and fallback is None:
        raise ValueError("no datasets configured and no default_profile")
    return SuiteCatalog(suites, fallback)
