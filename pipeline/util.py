from __future__ import annotations

import hashlib
import json
import math
import os
import tempfile
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Iterable


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def canonical_json(value: Any) -> str:
    return canonical_bytes(value).decode("utf-8")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_text(text: str) -> str:
    return sha256_bytes(text.encode("utf-8"))


def canonical_digest(value: Any) -> str:
    return sha256_bytes(canonical_bytes(value))


def sha256_file(path: Path | str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def stable_float(value: float) -> float | str:
    if math.isnan(value):
        return "nan"
    if math.isinf(value):
        return "inf" if value > 0 else "-inf"
    if value == 0.0:
        return 0.0
    return value


def stable_number_map(values: dict[str, float]) -> dict[str, float | str]:
    return {key: stable_float(values[key]) for key in sorted(values)}


def json_line(value: Any) -> str:
    return canonical_json(value) + "\n"


def read_json(path: Path | str) -> Any:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def read_jsonl(path: Path | str) -> list[Any]:
    rows: list[Any] = []
    with open(path, "r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid JSONL at {path}:{line_number}: {exc}") from exc
    return rows


def iter_jsonl(path: Path | str) -> Iterable[Any]:
    with open(path, "r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid JSONL at {path}:{line_number}: {exc}") from exc


def _fsync_directory(path: Path) -> None:
    try:
        descriptor = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def atomic_write_bytes(path: Path | str, data: bytes) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temp_name = tempfile.mkstemp(prefix=f".{target.name}.tmp-", dir=target.parent)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, target)
        _fsync_directory(target.parent)
    except BaseException:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass
        raise


def atomic_write_text(path: Path | str, text: str) -> None:
    atomic_write_bytes(path, text.encode("utf-8"))


def atomic_write_json(path: Path | str, value: Any) -> None:
    atomic_write_bytes(path, canonical_bytes(value) + b"\n")


def atomic_write_jsonl(path: Path | str, rows: Iterable[Any]) -> None:
    payload = "".join(json_line(row) for row in rows)
    atomic_write_text(path, payload)


def atomic_move(source: Path | str, target: Path | str) -> None:
    source_path = Path(source)
    target_path = Path(target)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    os.replace(source_path, target_path)
    _fsync_directory(target_path.parent)


def ensure_within(root: Path, relative_path: str) -> Path:
    candidate = (root / relative_path).resolve()
    resolved_root = root.resolve()
    if candidate != resolved_root and resolved_root not in candidate.parents:
        raise ValueError(f"path escapes workspace: {relative_path}")
    return candidate


def first_difference(left: Any, right: Any, path: str = "") -> tuple[str, Any, Any] | None:
    if type(left) is not type(right):
        return path or "$", left, right
    if isinstance(left, dict):
        left_keys = set(left)
        right_keys = set(right)
        if left_keys != right_keys:
            missing = sorted(left_keys ^ right_keys)[0]
            field = f"{path}.{missing}" if path else missing
            return field, left.get(missing), right.get(missing)
        for key in sorted(left):
            child = f"{path}.{key}" if path else key
            result = first_difference(left[key], right[key], child)
            if result is not None:
                return result
        return None
    if isinstance(left, list):
        if len(left) != len(right):
            return f"{path}.length" if path else "length", len(left), len(right)
        for index, (left_item, right_item) in enumerate(zip(left, right)):
            child = f"{path}[{index}]" if path else f"[{index}]"
            result = first_difference(left_item, right_item, child)
            if result is not None:
                return result
        return None
    if left != right:
        return path or "$", left, right
    return None


def parse_fixed_decimal(text: str, scale: int = 1_000_000_000) -> int:
    value = Decimal(text.strip()) * Decimal(scale)
    return int(value.to_integral_value(rounding=ROUND_HALF_UP))
