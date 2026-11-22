"""Persistent, deterministic diagnostics for business errors."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .canonical import atomic_write_json, digest
from .config import Settings
from .errors import WorkbenchError


def record_error(
    settings: Settings,
    error: WorkbenchError,
    *,
    operation: str,
    sequence_id: str | None = None,
    frame_index: int | None = None,
) -> str:
    payload: dict[str, Any] = {
        "schema_version": 1,
        "operation": operation,
        "code": error.code,
        "message": error.message,
        "sequence_id": sequence_id,
        "frame_index": frame_index,
        "context": error.context,
    }
    fingerprint = digest(payload)
    payload["diagnostic_fingerprint"] = fingerprint
    directory = settings.diagnostics_dir / "errors"
    path = directory / f"{fingerprint[:24]}.json"
    if not path.exists():
        atomic_write_json(path, payload, pretty=True)
    return fingerprint


def list_error_diagnostics(settings: Settings, *, limit: int = 200) -> list[dict[str, Any]]:
    directory = settings.diagnostics_dir / "errors"
    if not directory.exists():
        return []
    paths = sorted(directory.glob("*.json"), key=lambda p: p.name)
    import json

    result: list[dict[str, Any]] = []
    for path in paths[-limit:]:
        try:
            result.append(json.loads(path.read_text(encoding="utf-8")))
        except (OSError, ValueError):
            continue
    return result
