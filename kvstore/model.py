from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from .errors import InvalidArgument, KeyNotFound


def canonical_json(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError) as exc:
        raise InvalidArgument("value is not JSON serializable", {"error": str(exc)}) from exc


def clamp_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise InvalidArgument(f"{name} must be an integer", {"field": name})
    return value


@dataclass(frozen=True)
class Record:
    key: str
    value: Any
    version: int
    written_at_us: int
    expires_at_us: Optional[int]
    deleted: bool = False
    extras: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.key, str) or not self.key:
            raise InvalidArgument("key must be a non-empty string", {"field": "key"})
        if isinstance(self.version, bool) or not isinstance(self.version, int) or self.version < 0:
            raise InvalidArgument("version must be a non-negative integer", {"field": "version"})
        if isinstance(self.written_at_us, bool) or not isinstance(self.written_at_us, int):
            raise InvalidArgument("written_at_us must be an integer", {"field": "written_at_us"})
        if self.expires_at_us is not None:
            if isinstance(self.expires_at_us, bool) or not isinstance(self.expires_at_us, int):
                raise InvalidArgument("expires_at_us must be an integer or null", {"field": "expires_at_us"})
            if self.expires_at_us < self.written_at_us:
                raise InvalidArgument(
                    "expires_at_us must not be earlier than written_at_us",
                    {"field": "expires_at_us", "written_at_us": self.written_at_us},
                )
        if not isinstance(self.extras, dict):
            raise InvalidArgument("extras must be an object", {"field": "extras"})
        canonical_json(self.value)
        canonical_json(self.extras)

    def payload(self) -> Dict[str, Any]:
        return {
            "key": self.key,
            "value": self.value,
            "version": self.version,
            "written_at_us": self.written_at_us,
            "expires_at_us": self.expires_at_us,
            "deleted": self.deleted,
            "extras": self.extras,
        }

    @classmethod
    def from_payload(cls, payload: Dict[str, Any]) -> "Record":
        try:
            return cls(
                key=payload["key"],
                value=payload.get("value"),
                version=payload["version"],
                written_at_us=payload["written_at_us"],
                expires_at_us=payload.get("expires_at_us"),
                deleted=bool(payload.get("deleted", False)),
                extras=payload.get("extras") or {},
            )
        except KeyError as exc:
            raise InvalidArgument("internal record is missing a required field", {"field": str(exc)}) from exc


@dataclass(frozen=True)
class IndexEntry:
    key: str
    version: int
    written_at_us: int
    expires_at_us: Optional[int]
    deleted: bool
    segment_path: str
    offset: int
    frame_size: int


@dataclass(frozen=True)
class ImportedRecord:
    key: str
    value: Any
    version: int
    written_at_us: int
    expires_at_us: Optional[int]
    source_index: int
    extras: Dict[str, Any] = field(default_factory=dict)
    filled_fields: tuple[str, ...] = ()
    transformations: tuple[str, ...] = ()
    unknown_fields: tuple[str, ...] = ()

    def to_record(self) -> Record:
        return Record(
            key=self.key,
            value=self.value,
            version=self.version,
            written_at_us=self.written_at_us,
            expires_at_us=self.expires_at_us,
            deleted=False,
            extras=self.extras,
        )

    def summary(self) -> Dict[str, Any]:
        return {
            "key": self.key,
            "version": self.version,
            "written_at_us": self.written_at_us,
            "expires_at_us": self.expires_at_us,
            "value": self.value,
        }


@dataclass(frozen=True)
class DecodedBatch:
    input_format: str
    records: tuple[ImportedRecord, ...]
    negotiation: Dict[str, Any]
    warnings: tuple[str, ...] = ()


def ensure_key_found(key: str, expired: bool = False) -> None:
    if expired:
        raise KeyNotFound("key has expired", {"key": key, "expired": True})
    raise KeyNotFound("key does not exist", {"key": key})
