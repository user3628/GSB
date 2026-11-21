"""Shape manifest discovery and validation."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .errors import E_CONFIG, E_INPUT, E_NOT_FOUND, BusinessError
from .util import read_json


def load_manifest(path: Path) -> dict[str, Any]:
    try:
        value = read_json(path)
    except FileNotFoundError as exc:
        raise BusinessError(E_NOT_FOUND, "shape manifest not found", path=str(path)) from exc
    except Exception as exc:
        raise BusinessError(E_CONFIG, f"invalid shape manifest: {exc}", path=str(path)) from exc
    if not isinstance(value, dict):
        raise BusinessError(E_CONFIG, "shape manifest must be a JSON object")
    entries = value.get("shards")
    if not isinstance(entries, list):
        raise BusinessError(E_CONFIG, "shape manifest must contain a shards array")
    return value


def find_manifest_entry(
    manifest: dict[str, Any],
    *,
    source_relative_path: str,
    shard_id: str | None = None,
) -> dict[str, Any] | None:
    for entry in manifest.get("shards", []):
        if not isinstance(entry, dict):
            continue
        if shard_id is not None and entry.get("id") == shard_id:
            return entry
        if entry.get("path") == source_relative_path:
            return entry
    return None


def manifest_shape(entry: dict[str, Any] | None, key: str) -> tuple[int, ...] | None:
    if not entry or entry.get(key) is None:
        return None
    raw = entry[key]
    if not isinstance(raw, list) or len(raw) not in (2, 4):
        raise BusinessError(E_CONFIG, f"manifest {key} must have 2 or 4 dimensions")
    result: list[int] = []
    for item in raw:
        if isinstance(item, bool) or not isinstance(item, int) or item <= 0:
            raise BusinessError(E_CONFIG, f"manifest {key} contains an invalid dimension")
        result.append(item)
    return tuple(result)


def validate_source_against_manifest(
    source_shape: tuple[int, ...], entry: dict[str, Any] | None
) -> None:
    declared = manifest_shape(entry, "shape")
    if declared is not None and tuple(declared) != tuple(source_shape):
        raise BusinessError(
            E_INPUT,
            "source shape does not match the shape manifest",
            declared_shape=list(declared),
            actual_shape=list(source_shape),
        )

