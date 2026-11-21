from __future__ import annotations

import hashlib
import json
import os
from decimal import Decimal, InvalidOperation, Overflow, localcontext
from pathlib import Path
from typing import Any, Iterable


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def canonical_line(value: Any) -> bytes:
    return (canonical_json(value) + "\n").encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_value(value: Any) -> str:
    return sha256_bytes(canonical_json(value).encode("utf-8"))


def short_hash(value: Any, length: int = 12) -> str:
    return sha256_value(value)[:length]


def read_json(path: str | Path) -> Any:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def read_jsonl(path: str | Path) -> list[Any]:
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


def atomic_write_bytes(path: str | Path, data: bytes) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    with open(temporary, "wb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, target)


def atomic_write_json(path: str | Path, value: Any) -> None:
    atomic_write_bytes(path, (canonical_json(value) + "\n").encode("utf-8"))


def atomic_write_jsonl(path: str | Path, rows: Iterable[Any]) -> None:
    data = b"".join(canonical_line(row) for row in rows)
    atomic_write_bytes(path, data)


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def fixed_decimal(
    text: str | int | float | Decimal,
    scale: int,
    *,
    max_abs: str = "1000000000000000000",
) -> str:
    if scale < 0 or scale > 18:
        raise ValueError("scale must be between 0 and 18")
    raw = str(text).strip().replace(",", "").replace("_", "")
    if not raw or raw.lower() in {"nan", "+nan", "-nan", "inf", "+inf", "-inf", "infinity"}:
        raise ValueError("non-finite numeric value")
    try:
        with localcontext() as context:
            context.prec = 80
            number = Decimal(raw)
            if not number.is_finite():
                raise ValueError("non-finite numeric value")
            limit = Decimal(max_abs)
            if abs(number) > limit:
                raise Overflow("numeric value exceeds configured fixed-point bound")
            quantum = Decimal(1).scaleb(-scale)
            quantized = number.quantize(quantum)
            if abs(quantized) > limit:
                raise Overflow("quantized numeric value exceeds configured bound")
            return format(quantized, f".{scale}f")
    except (InvalidOperation, Overflow, ValueError) as exc:
        raise ValueError(str(exc) or "invalid numeric value") from exc

