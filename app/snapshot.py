from __future__ import annotations

import hashlib
import hmac
import json
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from app.errors import snapshot_invalid


class _DuplicateKeyError(ValueError):
    pass


def _reject_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON number is not allowed: {value}")


def _pairs_without_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateKeyError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def load_json_decimal(text: str) -> Any:
    try:
        return json.loads(
            text,
            parse_float=Decimal,
            parse_int=int,
            parse_constant=_reject_constant,
            object_pairs_hook=_pairs_without_duplicates,
        )
    except (json.JSONDecodeError, InvalidOperation, _DuplicateKeyError, ValueError) as exc:
        raise snapshot_invalid("snapshot is not valid deterministic JSON", reason=str(exc)) from exc


def canonical_decimal(value: Decimal) -> str:
    if not value.is_finite():
        raise ValueError("non-finite decimal")
    rendered = format(value, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    if rendered in ("", "-0"):
        return "0"
    return rendered


def canonical_json(value: Any) -> str:
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, Decimal):
        return canonical_decimal(value)
    if isinstance(value, float):
        raise TypeError("canonical JSON forbids binary float values")
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, list):
        return "[" + ",".join(canonical_json(item) for item in value) + "]"
    if isinstance(value, dict):
        if not all(isinstance(key, str) for key in value):
            raise TypeError("canonical JSON object keys must be strings")
        items = sorted(value.items(), key=lambda item: item[0])
        return "{" + ",".join(
            f"{json.dumps(key, ensure_ascii=False)}:{canonical_json(item)}" for key, item in items
        ) + "}"
    raise TypeError(f"unsupported canonical JSON type: {type(value).__name__}")


def pretty_json(value: Any, level: int = 0) -> str:
    """Pretty JSON with Decimal values emitted as exact JSON numbers."""
    indent = "  " * level
    child_indent = "  " * (level + 1)
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, Decimal):
        return canonical_decimal(value)
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, list):
        if not value:
            return "[]"
        body = ",\n".join(f"{child_indent}{pretty_json(item, level + 1)}" for item in value)
        return f"[\n{body}\n{indent}]"
    if isinstance(value, dict):
        if not value:
            return "{}"
        items = sorted(value.items(), key=lambda item: item[0])
        body = ",\n".join(
            f"{child_indent}{json.dumps(key, ensure_ascii=False)}: {pretty_json(item, level + 1)}"
            for key, item in items
        )
        return f"{{\n{body}\n{indent}}}"
    raise TypeError(f"unsupported JSON type: {type(value).__name__}")


def compute_checksum(payload: dict[str, Any]) -> str:
    unsealed = {key: value for key, value in payload.items() if key != "checksum"}
    digest = hashlib.sha256(canonical_json(unsealed).encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


def attach_checksum(payload: dict[str, Any]) -> dict[str, Any]:
    sealed = {key: value for key, value in payload.items() if key != "checksum"}
    sealed["checksum"] = compute_checksum(sealed)
    return sealed


@dataclass(frozen=True)
class Policy:
    relative_tolerance: Decimal
    min_weighted_consistency: Decimal
    default_min_headroom_ratio: Decimal


@dataclass(frozen=True)
class ComponentConfig:
    component_id: str
    quota: Decimal
    ratio: Decimal
    ratio_tolerance: Decimal
    threshold: Decimal
    usage: Decimal
    min_headroom_ratio: Decimal
    limit: int
    weight: Decimal


@dataclass(frozen=True)
class Snapshot:
    schema_version: str
    snapshot_version: int
    generated_at: str
    policy: Policy
    components: dict[str, ComponentConfig]
    checksum: str
    payload: dict[str, Any]

    @property
    def ordered_components(self) -> tuple[ComponentConfig, ...]:
        return tuple(self.components[key] for key in sorted(self.components))


def _require_dict(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise snapshot_invalid(f"{field} must be an object", field=field)
    return value


def _require_string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise snapshot_invalid(f"{field} must be a non-empty string", field=field)
    return value


def _require_decimal(value: Any, field: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (int, Decimal)):
        raise snapshot_invalid(f"{field} must be a JSON number", field=field)
    result = value if isinstance(value, Decimal) else Decimal(value)
    if not result.is_finite():
        raise snapshot_invalid(f"{field} must be finite", field=field)
    return result


def _require_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise snapshot_invalid(f"{field} must be a JSON integer", field=field)
    return value


def _require_range(value: Decimal, field: str, lower: Decimal, upper: Decimal, *, lower_open: bool = False) -> None:
    lower_ok = value > lower if lower_open else value >= lower
    if not lower_ok or value > upper:
        interval = "(" if lower_open else "["
        raise snapshot_invalid(
            f"{field} is outside the allowed range {interval}{lower}, {upper}]",
            field=field,
            value=canonical_decimal(value),
        )


def parse_snapshot(data: Any, *, verify_checksum: bool = True) -> Snapshot:
    root = _require_dict(data, "snapshot")
    schema_version = _require_string(root.get("schema_version"), "schema_version")
    if schema_version != "1.0":
        raise snapshot_invalid("unsupported schema_version", schema_version=schema_version)

    snapshot_version = _require_int(root.get("snapshot_version"), "snapshot_version")
    if snapshot_version <= 0:
        raise snapshot_invalid("snapshot_version must be positive", snapshot_version=snapshot_version)

    generated_at = _require_string(root.get("generated_at"), "generated_at")
    try:
        parsed_time = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise snapshot_invalid("generated_at must be an ISO-8601 timestamp", generated_at=generated_at) from exc
    if parsed_time.tzinfo is None:
        raise snapshot_invalid("generated_at must include a timezone", generated_at=generated_at)

    checksum = _require_string(root.get("checksum"), "checksum")
    if not checksum.startswith("sha256:") or len(checksum) != 71:
        raise snapshot_invalid("checksum must use sha256:<64 lowercase hex>", checksum=checksum)
    try:
        int(checksum.removeprefix("sha256:"), 16)
    except ValueError as exc:
        raise snapshot_invalid("checksum contains non-hexadecimal characters", checksum=checksum) from exc

    if verify_checksum:
        expected = compute_checksum(root)
        if not hmac.compare_digest(expected, checksum):
            raise snapshot_invalid(
                "snapshot checksum does not match canonical payload",
                expected_checksum=expected,
                received_checksum=checksum,
            )

    policy_root = _require_dict(root.get("policy"), "policy")
    relative_tolerance = _require_decimal(policy_root.get("relative_tolerance"), "policy.relative_tolerance")
    min_weighted = _require_decimal(policy_root.get("min_weighted_consistency"), "policy.min_weighted_consistency")
    default_headroom = _require_decimal(
        policy_root.get("default_min_headroom_ratio"), "policy.default_min_headroom_ratio"
    )
    _require_range(relative_tolerance, "policy.relative_tolerance", Decimal(0), Decimal(1), lower_open=True)
    _require_range(min_weighted, "policy.min_weighted_consistency", Decimal(0), Decimal(1))
    _require_range(default_headroom, "policy.default_min_headroom_ratio", Decimal(0), Decimal(1))

    components_root = _require_dict(root.get("components"), "components")
    if not components_root:
        raise snapshot_invalid("components must not be empty", field="components")

    components: dict[str, ComponentConfig] = {}
    for component_id in sorted(components_root):
        if not component_id or component_id.strip() != component_id:
            raise snapshot_invalid("component ids must be non-empty without surrounding whitespace", component_id=component_id)
        raw = _require_dict(components_root[component_id], f"components.{component_id}")
        prefix = f"components.{component_id}"
        quota = _require_decimal(raw.get("quota"), f"{prefix}.quota")
        ratio = _require_decimal(raw.get("ratio"), f"{prefix}.ratio")
        ratio_tolerance = _require_decimal(raw.get("ratio_tolerance"), f"{prefix}.ratio_tolerance")
        threshold = _require_decimal(raw.get("threshold"), f"{prefix}.threshold")
        usage = _require_decimal(raw.get("usage"), f"{prefix}.usage")
        min_headroom = raw.get("min_headroom_ratio")
        min_headroom_ratio = (
            default_headroom
            if min_headroom is None
            else _require_decimal(min_headroom, f"{prefix}.min_headroom_ratio")
        )
        limit = _require_int(raw.get("limit"), f"{prefix}.limit")
        weight = _require_decimal(raw.get("weight"), f"{prefix}.weight")

        _require_range(quota, f"{prefix}.quota", Decimal(0), Decimal(10**18), lower_open=True)
        _require_range(ratio, f"{prefix}.ratio", Decimal(0), Decimal(1))
        _require_range(ratio_tolerance, f"{prefix}.ratio_tolerance", Decimal(0), Decimal(1), lower_open=True)
        _require_range(threshold, f"{prefix}.threshold", Decimal(0), Decimal(10**18), lower_open=True)
        _require_range(usage, f"{prefix}.usage", Decimal(0), Decimal(10**18))
        _require_range(min_headroom_ratio, f"{prefix}.min_headroom_ratio", Decimal(0), Decimal(1))
        if limit <= 0:
            raise snapshot_invalid(f"{prefix}.limit must be a positive integer", field=f"{prefix}.limit")
        _require_range(weight, f"{prefix}.weight", Decimal(0), Decimal(10**12))

        components[component_id] = ComponentConfig(
            component_id=component_id,
            quota=quota,
            ratio=ratio,
            ratio_tolerance=ratio_tolerance,
            threshold=threshold,
            usage=usage,
            min_headroom_ratio=min_headroom_ratio,
            limit=limit,
            weight=weight,
        )

    if sum((component.quota for component in components.values()), Decimal(0)) <= 0:
        raise snapshot_invalid("sum of component quotas must be positive")
    if sum((component.weight for component in components.values()), Decimal(0)) <= 0:
        raise snapshot_invalid("sum of component weights must be positive")

    return Snapshot(
        schema_version=schema_version,
        snapshot_version=snapshot_version,
        generated_at=generated_at,
        policy=Policy(relative_tolerance, min_weighted, default_headroom),
        components=components,
        checksum=checksum,
        payload=root,
    )


def parse_snapshot_text(text: str, *, verify_checksum: bool = True) -> Snapshot:
    return parse_snapshot(load_json_decimal(text), verify_checksum=verify_checksum)


def load_snapshot_file(path: str, *, verify_checksum: bool = True) -> Snapshot:
    with open(path, "r", encoding="utf-8") as handle:
        return parse_snapshot_text(handle.read(), verify_checksum=verify_checksum)


def canonical_payload_text(snapshot: Snapshot) -> str:
    return canonical_json({key: value for key, value in snapshot.payload.items() if key != "checksum"})
