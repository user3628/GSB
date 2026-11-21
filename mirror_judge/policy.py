from __future__ import annotations

import math
from typing import Any

from .errors import BusinessError
from .models import sha256_json


POLICY_FIELDS = {
    "policy_version",
    "seed",
    "sampling_rate",
    "service_weights",
    "include_path_templates",
    "exclude_path_templates",
    "dedup_window_ms",
    "replay_multiplier",
    "max_batch_samples",
    "max_batch_concurrency",
    "batch_max_bytes",
    "batch_max_duration_us",
    "replay_timeout_ms",
}

PATCHABLE_FIELDS = POLICY_FIELDS - {"policy_version"}

SELECTION_RULES = {
    "seed",
    "sampling_rate",
    "service_weights",
    "path_include",
    "path_exclude",
    "dedup_window_ms",
}


def _invalid(message: str, details: dict[str, Any] | None = None) -> BusinessError:
    return BusinessError("E_SNAPSHOT_INVALID", message, details)


def _number(policy: dict[str, Any], key: str, *, minimum: float, maximum: float | None = None) -> float:
    value = policy.get(key)
    if type(value) not in {int, float} or isinstance(value, bool):
        raise _invalid(f"Policy field {key!r} must be a number")
    number = float(value)
    if not math.isfinite(number) or number < minimum or (maximum is not None and number > maximum):
        raise _invalid(f"Policy field {key!r} is outside its allowed range")
    return number


def _integer(policy: dict[str, Any], key: str, *, minimum: int, maximum: int | None = None) -> int:
    value = policy.get(key)
    if type(value) is not int or value < minimum or (maximum is not None and value > maximum):
        raise _invalid(f"Policy field {key!r} must be an integer >= {minimum}")
    return value


def _templates(policy: dict[str, Any], key: str) -> list[str]:
    value = policy.get(key)
    if not isinstance(value, list) or any(not isinstance(item, str) or not item for item in value):
        raise _invalid(f"Policy field {key!r} must be an array of non-empty strings")
    normalized: list[str] = []
    for item in value:
        item = item.strip()
        if not item.startswith("/"):
            raise _invalid(f"Policy field {key!r} contains a template without a leading '/'", {"template": item})
        normalized.append(item)
    return normalized


def validate_policy(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise _invalid("Policy must be a JSON object")
    missing = sorted(POLICY_FIELDS - set(raw))
    extra = sorted(set(raw) - POLICY_FIELDS)
    if missing or extra:
        raise _invalid("Policy has missing or unknown fields", {"missing": missing, "unknown": extra})
    version = raw.get("policy_version")
    if not isinstance(version, str) or not version.strip():
        raise _invalid("policy_version must be a non-empty string")
    weights_raw = raw.get("service_weights")
    if not isinstance(weights_raw, dict):
        raise _invalid("service_weights must be an object")
    weights: dict[str, float] = {}
    for service, value in weights_raw.items():
        if not isinstance(service, str) or not service.strip():
            raise _invalid("service_weights keys must be non-empty strings")
        if type(value) not in {int, float} or isinstance(value, bool):
            raise _invalid(f"service weight for {service!r} must be numeric")
        number = float(value)
        if not math.isfinite(number) or number < 0 or number > 100:
            raise _invalid(f"service weight for {service!r} must be between 0 and 100")
        weights[service] = number
    seed = _integer(raw, "seed", minimum=0)
    sampling_rate = _number(raw, "sampling_rate", minimum=0.0, maximum=1.0)
    policy = {
        "policy_version": version.strip(),
        "seed": seed,
        "sampling_rate": sampling_rate,
        "service_weights": weights,
        "include_path_templates": _templates(raw, "include_path_templates"),
        "exclude_path_templates": _templates(raw, "exclude_path_templates"),
        "dedup_window_ms": _integer(raw, "dedup_window_ms", minimum=0),
        "replay_multiplier": _integer(raw, "replay_multiplier", minimum=1, maximum=100),
        "max_batch_samples": _integer(raw, "max_batch_samples", minimum=1, maximum=100000),
        "max_batch_concurrency": _integer(raw, "max_batch_concurrency", minimum=1, maximum=1024),
        "batch_max_bytes": _integer(raw, "batch_max_bytes", minimum=1),
        "batch_max_duration_us": _integer(raw, "batch_max_duration_us", minimum=1),
        "replay_timeout_ms": _integer(raw, "replay_timeout_ms", minimum=1, maximum=600000),
    }
    return policy


def validate_patch(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict) or not raw:
        raise _invalid("Policy patch must be a non-empty JSON object")
    unknown = sorted(set(raw) - PATCHABLE_FIELDS)
    if unknown:
        raise _invalid("Policy patch contains unsupported rule fields", {"unknown": unknown})
    return raw


def apply_policy_patch(base_policy: dict[str, Any], patch: dict[str, Any], new_version: str) -> dict[str, Any]:
    candidate = {
        key: (dict(value) if isinstance(value, dict) else list(value) if isinstance(value, list) else value)
        for key, value in base_policy.items()
    }
    for key, value in patch.items():
        if key == "service_weights":
            if not isinstance(value, dict):
                raise _invalid("service_weights patch must be an object")
            merged = dict(candidate["service_weights"])
            merged.update(value)
            candidate["service_weights"] = merged
        else:
            candidate[key] = value
    candidate["policy_version"] = new_version
    return validate_policy(candidate)


def policy_hash(policy: dict[str, Any]) -> str:
    return sha256_json(policy)


def policy_rule_keys(policy: dict[str, Any]) -> list[str]:
    keys = [key for key in SELECTION_RULES if key not in {"service_weights", "path_include", "path_exclude"}]
    keys.extend(f"service_weights.{service}" for service in sorted(policy["service_weights"]))
    keys.extend(["path_include", "path_exclude", "replay_multiplier", "max_batch_samples", "max_batch_concurrency", "batch_max_bytes", "batch_max_duration_us", "replay_timeout_ms"])
    return keys


def diff_policy_rules(old: dict[str, Any] | None, new: dict[str, Any]) -> list[str]:
    if old is None:
        return policy_rule_keys(new)
    changed: list[str] = []
    for key in ("seed", "sampling_rate", "dedup_window_ms", "replay_multiplier", "max_batch_samples", "max_batch_concurrency", "batch_max_bytes", "batch_max_duration_us", "replay_timeout_ms"):
        if old.get(key) != new.get(key):
            changed.append(key)
    if old.get("include_path_templates") != new.get("include_path_templates"):
        changed.append("path_include")
    if old.get("exclude_path_templates") != new.get("exclude_path_templates"):
        changed.append("path_exclude")
    old_weights = old.get("service_weights", {})
    new_weights = new.get("service_weights", {})
    for service in sorted(set(old_weights) | set(new_weights)):
        if old_weights.get(service) != new_weights.get(service):
            changed.append(f"service_weights.{service}")
    return changed


def artifact_rule_dependencies(policy: dict[str, Any]) -> dict[str, list[str]]:
    selection = [
        "seed",
        "sampling_rate",
        "dedup_window_ms",
        "path_include",
        "path_exclude",
        *[f"service_weights.{service}" for service in sorted(policy["service_weights"])],
    ]
    batches = [
        *selection,
        "replay_multiplier",
        "max_batch_samples",
        "max_batch_concurrency",
        "batch_max_bytes",
        "batch_max_duration_us",
    ]
    return {"selection": selection, "batches": batches, "bias": batches}


def affected_artifact_types(changed_rules: list[str], policy: dict[str, Any]) -> list[str]:
    deps = artifact_rule_dependencies(policy)
    changed = set(changed_rules)
    return [kind for kind, keys in deps.items() if changed.intersection(keys)]
