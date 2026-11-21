from __future__ import annotations

from pathlib import Path
from typing import Any

from .canonical import atomic_write_json, read_json, sha256_value


_STAGES = ("normalize", "extract", "arbitrate")
_REASONS = {
    ("normalize", "input"): "clause_text_or_identity_changed",
    ("normalize", "config"): "normalization_config_changed",
    ("extract", "seed"): "seed_annotation_changed",
    ("extract", "dictionary"): "element_dictionary_changed",
    ("extract", "config"): "extraction_weight_or_precision_changed",
    ("extract", "model"): "estimated_element_weights_changed",
    ("arbitrate", "policy"): "arbitration_priority_or_policy_changed",
    ("arbitrate", "effective_dates"): "effective_date_changed",
}


def build_dependency_components(
    config: dict[str, Any],
    input_rows: list[dict[str, Any]],
    seed_rows: list[dict[str, Any]],
    dictionary_entries: list[dict[str, Any]],
    model: dict[str, Any] | None = None,
) -> dict[str, dict[str, str]]:
    normalize_inputs = [
        {
            "source_index": index,
            "contract_id": row.get("contract_id"),
            "clause_text": row.get("clause_text"),
            "clause_no": row.get("clause_no"),
            "template_id": row.get("template_id"),
        }
        for index, row in enumerate(input_rows)
    ]
    effective_dates = sorted(
        {
            (str(row.get("contract_id", "")), row.get("effective_date"))
            for row in input_rows
        },
        key=lambda item: (item[0], "" if item[1] is None else str(item[1])),
    )
    extract_config = dict(config["extraction"])
    extract_config.pop("regex_weight_overrides", None)
    extract_config.pop("dictionary_weight_overrides", None)
    weights = {
        "regex": config["extraction"].get("regex_weight_overrides", {}),
        "dictionary": config["extraction"].get("dictionary_weight_overrides", {}),
    }
    components = {
        "normalize": {
            "input": sha256_value(normalize_inputs),
            "config": sha256_value(config["normalization"]),
        },
        "extract": {
            "seed": sha256_value(seed_rows),
            "dictionary": sha256_value(dictionary_entries),
            "config": sha256_value({**extract_config, "normalization": config["normalization"]}),
            "model": sha256_value(model or weights),
        },
        "arbitrate": {
            "policy": sha256_value(config["arbitration"]),
            "effective_dates": sha256_value(effective_dates),
        },
    }
    return components


def dependency_fingerprints(components: dict[str, dict[str, str]]) -> dict[str, str]:
    return {stage: sha256_value(components[stage]) for stage in _STAGES}


def _invalidations(
    previous: dict[str, Any] | None,
    components: dict[str, dict[str, str]],
    dependencies: dict[str, str],
) -> list[dict[str, Any]]:
    if not previous:
        return [
            {
                "stage": "normalize",
                "reason": "initial_build",
                "fingerprint": dependencies["normalize"],
            }
        ]
    previous_components = previous.get("dependency_components", {})
    previous_dependencies = previous.get("dependency_fingerprints", {})
    result: list[dict[str, Any]] = []
    for stage in _STAGES:
        if previous_dependencies.get(stage) == dependencies[stage]:
            continue
        reason = "stage_input_changed"
        for component, value in components[stage].items():
            if previous_components.get(stage, {}).get(component) != value:
                reason = _REASONS.get((stage, component), f"{component}_changed")
                break
        result.append({"stage": stage, "reason": reason, "fingerprint": dependencies[stage]})
        break
    return result


def stage_digests(
    dependencies: dict[str, str],
    outputs: dict[str, Any],
) -> dict[str, str]:
    normalize = sha256_value({"dependency": dependencies["normalize"], "output": outputs["normalize"]})
    extract = sha256_value(
        {
            "dependency": dependencies["extract"],
            "normalize": normalize,
            "output": outputs["extract"],
        }
    )
    arbitrate = sha256_value(
        {
            "dependency": dependencies["arbitrate"],
            "extract": extract,
            "output": outputs["arbitrate"],
        }
    )
    return {"normalize": normalize, "extract": extract, "arbitrate": arbitrate}


def load_previous_state(path: str | Path) -> dict[str, Any] | None:
    target = Path(path)
    if not target.exists():
        return None
    return read_json(target)


def build_version_state(
    previous: dict[str, Any] | None,
    components: dict[str, dict[str, str]],
    outputs: dict[str, Any],
    cache_stats: dict[str, dict[str, int]],
    run_summary: dict[str, Any],
) -> dict[str, Any]:
    dependencies = dependency_fingerprints(components)
    digests = stage_digests(dependencies, outputs)
    return {
        "schema_version": 1,
        "version_fingerprint": digests["arbitrate"],
        "stage_digests": digests,
        "dependency_fingerprints": dependencies,
        "dependency_components": components,
        "invalidations": _invalidations(previous, components, dependencies),
        "cache": cache_stats,
        "run": run_summary,
    }


def write_version_state(path: str | Path, state: dict[str, Any]) -> None:
    atomic_write_json(path, state)


class ContentCache:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def _path(self, stage: str, key: str) -> Path:
        if stage not in _STAGES:
            raise ValueError(f"unknown cache stage: {stage}")
        return self.root / stage / f"{key}.json"

    def get(self, stage: str, key: str) -> Any | None:
        path = self._path(stage, key)
        if not path.exists():
            return None
        row = read_json(path)
        if row.get("key") != key:
            return None
        return row.get("payload")

    def put(self, stage: str, key: str, payload: Any) -> None:
        atomic_write_json(self._path(stage, key), {"key": key, "payload": payload})

