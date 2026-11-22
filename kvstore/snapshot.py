from __future__ import annotations

import base64
import binascii
import hashlib
import json
from dataclasses import dataclass
from typing import Any, Iterable

from .errors import StoreError, snapshot_invalid
from .model import Record, Tombstone


SNAPSHOT_MAGIC = "KVSTORE-SNAPSHOT"
SNAPSHOT_FORMAT_VERSION = 1
SUPPORTED_SNAPSHOT_VERSIONS = [SNAPSHOT_FORMAT_VERSION]
INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1


def _canonical_bytes(doc: dict[str, Any]) -> bytes:
    body = {key: value for key, value in doc.items() if key != "checksum"}
    return json.dumps(
        body, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def _checksum(doc: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical_bytes(doc)).hexdigest()


def _record_object(record: Record) -> dict[str, Any]:
    try:
        value = record.value.decode("utf-8")
        encoding = "utf8"
    except UnicodeDecodeError:
        value = base64.b64encode(record.value).decode("ascii")
        encoding = "base64"
    obj: dict[str, Any] = {
        "key": record.key,
        "value": value,
        "value_encoding": encoding,
        "version": record.version,
        "write_timestamp_ms": record.write_ts_ms,
        "expires_at_ms": record.expires_at_ms,
        "deleted": record.deleted,
    }
    if record.deleted:
        obj["op"] = "delete"
    return obj


def build_snapshot_document(
    *,
    records: Iterable[Record],
    tombstones: Iterable[Tombstone],
    snapshot_seq: int,
    generated_at_ms: int,
    state_version: int,
    base_snapshot_seq: int,
) -> dict[str, Any]:
    if snapshot_seq < 1:
        raise ValueError("snapshot_seq must be >= 1")
    # Tombstones precede live records so a delete/recreate sequence remains
    # unambiguous if a consumer replays the array literally.
    record_objects: list[dict[str, Any]] = []
    for tombstone in sorted(tombstones, key=lambda item: item.key):
        record_objects.append(
            _record_object(
                Record(
                    key=tombstone.key,
                    value=b"",
                    version=tombstone.version,
                    write_ts_ms=tombstone.write_ts_ms,
                    expires_at_ms=None,
                    deleted=True,
                )
            )
        )
    for record in sorted(records, key=lambda item: item.key):
        record_objects.append(_record_object(record))
    doc: dict[str, Any] = {
        "magic": SNAPSHOT_MAGIC,
        "format_version": SNAPSHOT_FORMAT_VERSION,
        "snapshot_seq": snapshot_seq,
        "base_snapshot_seq": base_snapshot_seq,
        "generated_at_ms": generated_at_ms,
        "state_version": state_version,
        "records": record_objects,
    }
    doc["checksum"] = _checksum(doc)
    return doc


def snapshot_bytes(doc: dict[str, Any]) -> bytes:
    return (
        json.dumps(doc, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        + "\n"
    ).encode("utf-8")


@dataclass(frozen=True, slots=True)
class SnapshotData:
    snapshot_seq: int
    base_snapshot_seq: int
    generated_at_ms: int
    state_version: int
    records: tuple[Record, ...]
    checksum: str


def parse_snapshot(data: bytes | str | dict[str, Any]) -> SnapshotData:
    if isinstance(data, dict):
        doc = data
    else:
        raw = data.encode("utf-8") if isinstance(data, str) else bytes(data)
        try:
            doc = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise snapshot_invalid(
                "snapshot is not valid UTF-8 JSON",
                stage="json_decode",
                error=str(exc),
            ) from exc
    if not isinstance(doc, dict):
        raise snapshot_invalid("snapshot root must be a JSON object", stage="schema")
    if doc.get("magic") != SNAPSHOT_MAGIC:
        raise snapshot_invalid(
            "snapshot magic mismatch",
            stage="magic",
            expected_magic=SNAPSHOT_MAGIC,
            actual_magic=doc.get("magic"),
        )
    version = doc.get("format_version")
    if version not in SUPPORTED_SNAPSHOT_VERSIONS:
        raise snapshot_invalid(
            "snapshot format version is not compatible",
            stage="format_version",
            format_version=version,
            supported_versions=SUPPORTED_SNAPSHOT_VERSIONS,
        )
    declared = doc.get("checksum")
    actual = _checksum(doc)
    if not isinstance(declared, str) or declared != actual:
        raise snapshot_invalid(
            "snapshot checksum mismatch",
            stage="checksum",
            declared_checksum=declared,
            actual_checksum=actual,
        )
    snapshot_seq = doc.get("snapshot_seq")
    if not isinstance(snapshot_seq, int) or isinstance(snapshot_seq, bool) or snapshot_seq < 1:
        raise snapshot_invalid(
            "snapshot_seq must be a positive integer", stage="snapshot_seq"
        )
    base_seq = doc.get("base_snapshot_seq", 0)
    if not isinstance(base_seq, int) or isinstance(base_seq, bool) or base_seq < 0:
        raise snapshot_invalid(
            "base_snapshot_seq must be a non-negative integer",
            stage="base_snapshot_seq",
        )
    generated = doc.get("generated_at_ms")
    if not isinstance(generated, int) or isinstance(generated, bool):
        raise snapshot_invalid(
            "generated_at_ms must be an integer", stage="generated_at_ms"
        )
    state_version = doc.get("state_version", 0)
    if not isinstance(state_version, int) or isinstance(state_version, bool):
        raise snapshot_invalid("state_version must be an integer", stage="state_version")
    raw_records = doc.get("records")
    if not isinstance(raw_records, list):
        raise snapshot_invalid("snapshot records must be an array", stage="records")
    records: list[Record] = []
    for index, obj in enumerate(raw_records):
        if not isinstance(obj, dict):
            raise snapshot_invalid(
                "snapshot record must be an object", stage="record", record_index=index
            )
        key = obj.get("key")
        if not isinstance(key, str) or not key:
            raise snapshot_invalid(
                "snapshot record key must be a non-empty string",
                stage="record",
                record_index=index,
            )
        encoding = obj.get("value_encoding", "utf8")
        value_raw = obj.get("value", "")
        if not isinstance(value_raw, str):
            raise snapshot_invalid(
                "snapshot record value must be a string",
                stage="record",
                record_index=index,
            )
        if encoding in ("utf8", "text", None):
            value = value_raw.encode("utf-8")
        elif encoding == "base64":
            try:
                value = base64.b64decode(value_raw, validate=True)
            except (ValueError, binascii.Error) as exc:
                raise snapshot_invalid(
                    "snapshot record base64 value is invalid",
                    stage="record",
                    record_index=index,
                ) from exc
        else:
            raise snapshot_invalid(
                "snapshot record value_encoding is unsupported",
                stage="record",
                record_index=index,
                value_encoding=encoding,
            )
        version_value = obj.get("version")
        if (
            not isinstance(version_value, int)
            or isinstance(version_value, bool)
            or version_value < 1
            or version_value > 0xFFFFFFFFFFFFFFFF
        ):
            raise snapshot_invalid(
                "snapshot record version must be a positive integer",
                stage="record",
                record_index=index,
            )
        write_ms = obj.get("write_timestamp_ms")
        if (
            not isinstance(write_ms, int)
            or isinstance(write_ms, bool)
            or write_ms < INT64_MIN
            or write_ms > INT64_MAX
        ):
            raise snapshot_invalid(
                "snapshot record write_timestamp_ms must be an integer",
                stage="record",
                record_index=index,
            )
        expires = obj.get("expires_at_ms")
        if expires is not None and (
            not isinstance(expires, int)
            or isinstance(expires, bool)
            or expires < INT64_MIN
            or expires > INT64_MAX
        ):
            raise snapshot_invalid(
                "snapshot record expires_at_ms must be null or an integer",
                stage="record",
                record_index=index,
            )
        deleted = bool(obj.get("deleted", False)) or obj.get("op") in ("delete", "d")
        records.append(
            Record(
                key=key,
                value=value,
                version=version_value,
                write_ts_ms=write_ms,
                expires_at_ms=expires,
                deleted=deleted,
            )
        )
    return SnapshotData(
        snapshot_seq=snapshot_seq,
        base_snapshot_seq=base_seq,
        generated_at_ms=generated,
        state_version=state_version,
        records=tuple(records),
        checksum=actual,
    )

