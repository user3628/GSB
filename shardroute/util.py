from __future__ import annotations

import hashlib
import json
import math
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SAFE_VERSION = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def snapshot_checksum(document: dict[str, Any]) -> str:
    body = {key: value for key, value in document.items() if key != "checksum"}
    return sha256_text(canonical_json(body))


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def read_json(path: str | os.PathLike[str]) -> Any:
    with Path(path).open("r", encoding="utf-8") as handle:
        return json.load(handle)


def atomic_write_json(path: str | os.PathLike[str], value: Any) -> None:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".tmp", dir=str(destination.parent)
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(value, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def require_plain_int(value: Any, field_name: str, context: dict[str, Any] | None = None) -> int:
    from .errors import error

    if isinstance(value, bool) or not isinstance(value, int):
        raise error(
            "E_BAD_REQUEST",
            f"{field_name} must be an integer",
            field=field_name,
            value_type=type(value).__name__,
            **(context or {}),
        )
    return value


def normalize_integer(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and re.fullmatch(r"-?\d+", value.strip()):
        return int(value.strip())
    return None


def normalize_shard_value(value: Any, value_type: str) -> Any:
    from .errors import error

    if value_type == "integer":
        normalized = normalize_integer(value)
        if normalized is None:
            raise error(
                "E_SHARD_KEY_INVALID",
                "shard key must be an integer",
                shard_key_type=value_type,
                value=value,
            )
        return normalized
    if value_type == "string":
        if not isinstance(value, str) or not value:
            raise error(
                "E_SHARD_KEY_INVALID",
                "shard key must be a non-empty string",
                shard_key_type=value_type,
                value=value,
            )
        return value
    raise error(
        "E_SNAPSHOT_INVALID",
        f"unsupported shard key type: {value_type}",
        shard_key_type=value_type,
    )


def type_rank(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, bool):
        return 1
    if isinstance(value, (int, float)):
        return 2
    if isinstance(value, str):
        return 3
    if isinstance(value, bytes):
        return 4
    return 5


def stable_value_key(value: Any) -> tuple[Any, ...]:
    rank = type_rank(value)
    if value is None:
        return (rank,)
    if isinstance(value, bool):
        return (rank, int(value))
    if isinstance(value, float):
        if math.isnan(value):
            return (rank, "nan")
        return (rank, value)
    if isinstance(value, bytes):
        return (rank, value.hex())
    if isinstance(value, (int, str)):
        return (rank, value)
    return (rank, canonical_json(value))


def validate_identifier(value: Any, field_name: str) -> str:
    from .errors import error

    if not isinstance(value, str) or not SAFE_IDENTIFIER.fullmatch(value):
        raise error(
            "E_SNAPSHOT_INVALID",
            f"{field_name} is not a safe SQL identifier",
            field=field_name,
            value=value,
        )
    return value


def fingerprint(value: Any) -> str:
    return sha256_text(canonical_json(value))

