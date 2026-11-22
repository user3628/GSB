from __future__ import annotations

import hmac
from datetime import datetime
from typing import Any

from .errors import BusinessError
from .utils import parse_datetime, snapshot_checksum


MAX_BYTES = 2**63 - 1
CANDIDATE_RULE = (
    "heat>=threshold moves one tier hotter; heat<threshold moves one tier colder; "
    "candidates sorted by heat DESC then shard_id ASC; target capacity is reserved "
    "against current usage plus already accepted incoming bytes"
)


def _invalid(message: str, **context: Any) -> BusinessError:
    return BusinessError(
        "E_SNAPSHOT_INVALID",
        message,
        context=context,
        status_code=400,
    )


def _require_int(value: Any, field: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise _invalid(f"{field} must be an integer", field=field, value=value)
    if value < minimum or value > maximum:
        raise _invalid(
            f"{field} is out of range",
            field=field,
            value=value,
            minimum=minimum,
            maximum=maximum,
        )
    return value


def normalize_snapshot(payload: dict[str, Any], *, verify_checksum: bool = True) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise _invalid("snapshot must be a JSON object")

    version = _require_int(payload.get("version"), "version", 1, 2**63 - 1)
    generated_at = payload.get("generated_at")
    if not isinstance(generated_at, str):
        raise _invalid("generated_at must be an ISO-8601 string", field="generated_at")
    try:
        parse_datetime(generated_at)
    except ValueError as exc:
        raise _invalid("generated_at is not a valid timezone-aware timestamp", detail=str(exc)) from exc

    layers_value = payload.get("layers")
    if not isinstance(layers_value, list) or not layers_value:
        raise _invalid("layers must be a non-empty array", field="layers")

    layers: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, layer in enumerate(layers_value):
        if not isinstance(layer, dict):
            raise _invalid("each layer must be an object", index=index)
        layer_id = layer.get("id")
        if not isinstance(layer_id, str) or not layer_id:
            raise _invalid("layer id must be a non-empty string", index=index)
        if layer_id in seen:
            raise _invalid("layer ids must be unique", layer_id=layer_id)
        seen.add(layer_id)
        capacity = _require_int(
            layer.get("capacity_bytes"),
            f"layers[{index}].capacity_bytes",
            0,
            MAX_BYTES,
        )
        layers.append({"id": layer_id, "capacity_bytes": capacity})

    heat_threshold = _require_int(payload.get("heat_threshold"), "heat_threshold", 0, 255)
    batch_limit = _require_int(payload.get("batch_limit"), "batch_limit", 1, 100_000)
    cooldown_seconds = _require_int(
        payload.get("cooldown_seconds"),
        "cooldown_seconds",
        0,
        31_536_000,
    )
    checksum = payload.get("checksum")
    if not isinstance(checksum, str) or len(checksum) != 64:
        raise _invalid("checksum must be a 64-character hexadecimal SHA-256 digest")
    try:
        int(checksum, 16)
    except ValueError as exc:
        raise _invalid("checksum must be hexadecimal") from exc

    normalized = {
        "version": version,
        "generated_at": generated_at,
        "layers": layers,
        "heat_threshold": heat_threshold,
        "batch_limit": batch_limit,
        "cooldown_seconds": cooldown_seconds,
        "checksum": checksum.lower(),
    }
    expected = snapshot_checksum(normalized)
    if verify_checksum and not hmac.compare_digest(expected, normalized["checksum"]):
        raise _invalid(
            "snapshot checksum does not match the supplied policy",
            expected=expected,
            actual=normalized["checksum"],
        )
    return normalized


def target_layer_id(
    *,
    heat: int,
    current_layer_id: str,
    layers: list[dict[str, Any]],
    heat_threshold: int,
) -> str | None:
    rank_by_id = {layer["id"]: rank for rank, layer in enumerate(layers)}
    current_rank = rank_by_id[current_layer_id]
    if heat >= heat_threshold:
        target_rank = current_rank - 1
    else:
        target_rank = current_rank + 1
    if target_rank < 0 or target_rank >= len(layers):
        return None
    return layers[target_rank]["id"]


def cooldown_elapsed(cooldown_until: str, *, now: datetime) -> bool:
    try:
        return parse_datetime(cooldown_until) <= now
    except ValueError as exc:
        raise BusinessError(
            "E_PERSISTED_STATE_INVALID",
            "persisted shard cooldown timestamp is invalid",
            context={"cooldown_until": cooldown_until, "detail": str(exc)},
            status_code=500,
        ) from exc
