from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable

from .errors import BusinessError

MAX_I64 = 9_223_372_036_854_775_807
_SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")
_SESSION_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_POLICIES = {"reject", "allow_generation"}


@dataclass(frozen=True)
class PartInput:
    seq: int
    size: int
    sha256: str
    arrival_ms: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "size": self.size,
            "sha256": self.sha256,
            "arrival_ms": self.arrival_ms,
        }


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def parse_int(value: Any, field: str, *, minimum: int = 0, maximum: int = MAX_I64) -> int:
    if not _is_int(value):
        raise BusinessError("E_PART_INVALID", f"{field} must be a JSON integer")
    if value < minimum or value > maximum:
        raise BusinessError(
            "E_PART_INVALID",
            f"{field} must be in [{minimum}, {maximum}]",
            details={"field": field, "value": value},
        )
    return value


def parse_part(raw: Any, *, position: int | None = None) -> PartInput:
    prefix = f"parts[{position}]" if position is not None else "part"
    if not isinstance(raw, dict):
        raise BusinessError("E_PART_INVALID", f"{prefix} must be an object")

    try:
        seq = parse_int(raw.get("seq"), f"{prefix}.seq", minimum=1)
        size = parse_int(raw.get("size"), f"{prefix}.size", minimum=0)
        arrival_ms = parse_int(raw.get("arrival_ms"), f"{prefix}.arrival_ms", minimum=0)
    except BusinessError as exc:
        exc.details.setdefault("part_index", position)
        raise

    sha = raw.get("sha256")
    if not isinstance(sha, str) or _SHA256_RE.fullmatch(sha) is None:
        raise BusinessError(
            "E_PART_INVALID",
            f"{prefix}.sha256 must be exactly 64 hexadecimal characters",
            details={"part_index": position},
        )
    return PartInput(seq=seq, size=size, sha256=sha.lower(), arrival_ms=arrival_ms)


def parse_parts(raw: Any, *, allow_empty: bool = False) -> list[PartInput]:
    if not isinstance(raw, list):
        raise BusinessError("E_PART_INVALID", "parts must be an array")
    if not raw and not allow_empty:
        raise BusinessError("E_PART_INVALID", "parts must not be empty")

    parsed: list[PartInput] = []
    total = 0
    for index, item in enumerate(raw):
        part = parse_part(item, position=index)
        total += part.size
        if total > MAX_I64:
            raise BusinessError(
                "E_PART_INVALID",
                "batch total size overflows signed 64-bit range",
                details={"part_index": index, "max": MAX_I64},
            )
        parsed.append(part)
    return parsed


def parse_object_key(value: Any) -> str:
    if not isinstance(value, str) or not value or len(value) > 1024:
        raise BusinessError("E_REQUEST_INVALID", "object_key must be a non-empty string of at most 1024 characters")
    return value


def parse_session_id(value: Any, *, required: bool = False) -> str | None:
    if value is None and not required:
        return None
    if not isinstance(value, str) or _SESSION_ID_RE.fullmatch(value) is None:
        raise BusinessError(
            "E_REQUEST_INVALID",
            "session_id must match [A-Za-z0-9][A-Za-z0-9._:-]{0,127}",
        )
    return value


def parse_policy(value: Any) -> str:
    if value is None:
        return "reject"
    if value not in _POLICIES:
        raise BusinessError(
            "E_REQUEST_INVALID",
            "conflict_policy must be one of: reject, allow_generation",
        )
    return str(value)


def parse_version(value: Any) -> int:
    if not _is_int(value) or value < 0:
        raise BusinessError("E_REQUEST_INVALID", "version must be a non-negative integer")
    return value


def parse_manifest(raw: Any) -> list[PartInput] | None:
    if raw is None:
        return None
    if not isinstance(raw, list):
        raise BusinessError("E_PART_INVALID", "parts manifest must be an array")
    relaxed: list[Any] = []
    for item in raw:
        if isinstance(item, dict):
            relaxed.append({**item, "arrival_ms": item.get("arrival_ms", 0)})
        else:
            relaxed.append(item)
    return parse_parts(relaxed, allow_empty=True)


def validate_unique_indexes(parts: Iterable[PartInput]) -> None:
    seen: dict[int, str] = {}
    for part in parts:
        previous = seen.get(part.seq)
        if previous is not None and previous != part.sha256:
            raise BusinessError(
                "E_PART_MISMATCH",
                f"batch contains conflicting digests for seq {part.seq}",
                details={"seq": part.seq, "first_sha256": previous, "new_sha256": part.sha256},
            )
        seen[part.seq] = part.sha256

