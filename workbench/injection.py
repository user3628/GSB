from __future__ import annotations

import contextlib
from pathlib import Path
from typing import Any

from .manifest import load_injection, validate_injection_operation
from .util import WorkbenchError, atomic_write_json, read_json, relpath


def _injection_path(root: Path, config_path: Path) -> Path:
    if not config_path.exists():
        return root / "configs" / "injection.json"
    data = read_json(config_path)
    value = data.get("dataset", {}).get("injection", "configs/injection.json")
    return (root / value).resolve()


def append_injection(root: Path, operation: dict[str, Any], *, config_path: Path | None = None) -> dict[str, Any]:
    root = root.resolve()
    config_path = config_path or (root / "configs" / "default.json")
    validate_injection_operation(operation)
    path = _injection_path(root, config_path)
    current = load_injection(path) if path.exists() else []
    current.append(operation)
    payload = {
        "format": 1,
        "operations": current,
    }
    atomic_write_json(path, payload, pretty=True)
    return {
        "path": relpath(root, path),
        "operation": operation,
        "operations": current,
    }


def clear_injections(root: Path, *, config_path: Path | None = None) -> dict[str, Any]:
    root = root.resolve()
    config_path = config_path or (root / "configs" / "default.json")
    path = _injection_path(root, config_path)
    payload = {"format": 1, "operations": []}
    atomic_write_json(path, payload, pretty=True)
    return {"path": relpath(root, path), "operations": []}


def list_injections(root: Path, *, config_path: Path | None = None) -> dict[str, Any]:
    root = root.resolve()
    config_path = config_path or (root / "configs" / "default.json")
    path = _injection_path(root, config_path)
    operations = load_injection(path) if path.exists() else []
    if not operations and path.exists():
        with contextlib.suppress(Exception):
            payload = read_json(path)
            if isinstance(payload, list):
                operations = payload
    return {"path": relpath(root, path), "operations": operations}


def build_operation(
    kind: str,
    sample: str,
    *,
    width: int | None = None,
    height: int | None = None,
    label: str | None = None,
    value: str | None = None,
    field: str | None = None,
    lower_bound: float | None = None,
) -> dict[str, Any]:
    operation: dict[str, Any] = {"type": kind, "sample": sample}
    if kind == "dim_mismatch":
        if width is None or height is None:
            raise WorkbenchError("E_INJECTION_INVALID", "dim_mismatch requires width and height")
        operation.update({"width": width, "height": height})
    elif kind == "unknown_label":
        if not label:
            raise WorkbenchError("E_INJECTION_INVALID", "unknown_label requires --label")
        operation["label"] = label
        if lower_bound is not None:
            operation["lower_bound"] = lower_bound
    elif kind == "numeric":
        if not label or value is None:
            raise WorkbenchError("E_INJECTION_INVALID", "numeric requires --label and --value")
        operation.update({"label": label, "value": normalize_numeric_token(value)})
    elif kind == "missing_field":
        if not field:
            raise WorkbenchError("E_INJECTION_INVALID", "missing_field requires --field")
        operation["field"] = field
    elif kind == "score_length":
        if not label:
            raise WorkbenchError("E_INJECTION_INVALID", "score_length requires --label")
        operation["label"] = label
    else:
        raise WorkbenchError("E_INJECTION_INVALID", "unsupported injection type", {"type": kind})
    validate_injection_operation(operation)
    return operation


def normalize_numeric_token(value: str) -> str | float:
    lowered = value.strip().lower()
    if lowered == "nan":
        return "NaN"
    if lowered in {"inf", "+inf", "infinity", "+infinity"}:
        return "Inf"
    if lowered in {"-inf", "-infinity"}:
        return "-Inf"
    try:
        return float(value)
    except ValueError as exc:
        raise WorkbenchError("E_INJECTION_INVALID", "numeric value is not a number", {"value": value}) from exc


__all__ = [
    "append_injection",
    "build_operation",
    "clear_injections",
    "list_injections",
    "normalize_numeric_token",
]
