from __future__ import annotations

import base64
import binascii
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from .errors import ErrorCode, ServiceError


class CheckpointState(str, Enum):
    WRITING = "WRITING"
    COMMITTED = "COMMITTED"
    INVALID = "INVALID"
    PRUNED = "PRUNED"


class ShardKind(str, Enum):
    PARAMS = "params"
    OPTIMIZER = "optimizer"


_SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")
_REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_SAFE_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True, slots=True)
class ShardInput:
    kind: ShardKind
    ordinal: int
    name: str
    data: bytes
    sha256: str


@dataclass(frozen=True, slots=True)
class ParsedCheckpointRequest:
    request_id: str
    round_number: int
    scheduler_position: int
    data_cursor: Any
    params: tuple[ShardInput, ...]
    optimizer: tuple[ShardInput, ...]

    @property
    def all_shards(self) -> tuple[ShardInput, ...]:
        return self.params + self.optimizer


def _parse_shard(raw: Any, kind: ShardKind, seen: set[tuple[str, int]]) -> ShardInput:
    if not isinstance(raw, dict):
        raise ServiceError(ErrorCode.INVALID_REQUEST, f"{kind.value} shard must be an object")
    ordinal = raw.get("ordinal")
    if not isinstance(ordinal, int) or ordinal < 0:
        raise ServiceError(ErrorCode.INVALID_REQUEST, f"{kind.value} shard ordinal must be an integer >= 0")
    name = raw.get("name") or f"{kind.value}-{ordinal:03d}.bin"
    if not isinstance(name, str) or not _SAFE_NAME_RE.fullmatch(name):
        raise ServiceError(ErrorCode.INVALID_REQUEST, "shard name contains unsafe characters", {"name": name})
    key = (kind.value, ordinal)
    if key in seen:
        raise ServiceError(ErrorCode.INVALID_REQUEST, "duplicate shard ordinal", {"kind": kind.value, "ordinal": ordinal})
    seen.add(key)
    encoded = raw.get("data_base64")
    if not isinstance(encoded, str):
        raise ServiceError(ErrorCode.INVALID_REQUEST, "data_base64 is required")
    try:
        data = base64.b64decode(encoded, validate=True)
    except (ValueError, binascii.Error) as exc:
        raise ServiceError(ErrorCode.INVALID_REQUEST, "data_base64 is not valid base64") from exc
    if not data:
        raise ServiceError(ErrorCode.INVALID_REQUEST, "shard must not be empty", {"name": name})
    actual = hashlib.sha256(data).hexdigest()
    declared = raw.get("sha256")
    if declared is not None:
        if not isinstance(declared, str) or not _SHA256_RE.fullmatch(declared):
            raise ServiceError(ErrorCode.INVALID_REQUEST, "sha256 must be 64 hexadecimal characters")
        if declared.lower() != actual:
            raise ServiceError(
                ErrorCode.CHECKSUM_MISMATCH,
                "declared shard digest does not match submitted bytes",
                {
                    "kind": kind.value,
                    "ordinal": ordinal,
                    "name": name,
                    "declared_sha256": declared.lower(),
                    "actual_sha256": actual,
                },
            )
    return ShardInput(kind=kind, ordinal=ordinal, name=name, data=data, sha256=actual)


def parse_checkpoint_request(raw: Any) -> ParsedCheckpointRequest:
    if not isinstance(raw, dict):
        raise ServiceError(ErrorCode.INVALID_REQUEST, "request body must be a JSON object")
    request_id = raw.get("request_id")
    if not isinstance(request_id, str) or not _REQUEST_ID_RE.fullmatch(request_id):
        raise ServiceError(ErrorCode.INVALID_REQUEST, "request_id must match [A-Za-z0-9][A-Za-z0-9._:-]{0,127}")
    round_number = raw.get("round")
    if not isinstance(round_number, int) or isinstance(round_number, bool) or round_number < 0:
        raise ServiceError(ErrorCode.INVALID_REQUEST, "round must be an integer >= 0")
    scheduler_position = raw.get("scheduler_position")
    if not isinstance(scheduler_position, int) or isinstance(scheduler_position, bool) or scheduler_position < 0:
        raise ServiceError(ErrorCode.INVALID_REQUEST, "scheduler_position must be an integer >= 0")
    if "data_cursor" not in raw or raw["data_cursor"] is None:
        raise ServiceError(ErrorCode.INVALID_REQUEST, "data_cursor is required")
    data_cursor = raw["data_cursor"]
    try:
        canonical_json(data_cursor)
    except (TypeError, ValueError) as exc:
        raise ServiceError(ErrorCode.INVALID_REQUEST, "data_cursor must be JSON serializable") from exc
    params_raw = raw.get("params")
    optimizer_raw = raw.get("optimizer")
    if not isinstance(params_raw, list) or not params_raw:
        raise ServiceError(ErrorCode.INVALID_REQUEST, "params must be a non-empty array")
    if not isinstance(optimizer_raw, list) or not optimizer_raw:
        raise ServiceError(ErrorCode.INVALID_REQUEST, "optimizer must be a non-empty array")
    seen: set[tuple[str, int]] = set()
    params = tuple(_parse_shard(item, ShardKind.PARAMS, seen) for item in params_raw)
    optimizer = tuple(_parse_shard(item, ShardKind.OPTIMIZER, seen) for item in optimizer_raw)
    return ParsedCheckpointRequest(
        request_id=request_id,
        round_number=round_number,
        scheduler_position=scheduler_position,
        data_cursor=data_cursor,
        params=tuple(sorted(params, key=lambda shard: shard.ordinal)),
        optimizer=tuple(sorted(optimizer, key=lambda shard: shard.ordinal)),
    )

