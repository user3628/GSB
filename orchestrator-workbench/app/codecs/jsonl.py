"""Sharded JSON-lines snapshot codec.

The first line is a snapshot header object; every following line is one
resource record. Records may omit optional fields (budget / weight /
expected_version) and the missing-field names are preserved so that the
negotiation layer can record degradations.
"""
from __future__ import annotations

import json

from app.errors import ErrorCode, WorkbenchError
from app.models import DecodedPayload, FormatName, RawRecord, SnapshotHeader

SUPPORTED_VERSION = 1
HIGHEST_KNOWN_VERSION = 2
HEADER_KEYS = {"snapshot", "format", "format_version", "snapshot_seq", "generated_at"}
RECORD_KEYS = {"resource_id", "parents", "budget", "weight", "expected_version"}


def encode(payload: DecodedPayload) -> bytes:
    if payload.header.format is not FormatName.JSONL:
        raise WorkbenchError(ErrorCode.E_FORMAT_INVALID, "jsonl codec requires jsonl format")
    if payload.header.format_version != SUPPORTED_VERSION:
        raise WorkbenchError(
            ErrorCode.E_VERSION_UNSUPPORTED,
            f"writer only emits jsonl v{SUPPORTED_VERSION}, got v{payload.header.format_version}",
        )
    lines = [
        json.dumps(
            {
                "snapshot": True,
                "format": "jsonl",
                "format_version": payload.header.format_version,
                "snapshot_seq": payload.header.snapshot_seq,
                "generated_at": payload.header.generated_at,
            },
            sort_keys=True,
        )
    ]
    for record in payload.records:
        obj: dict = {"resource_id": record.resource_id, "parents": list(record.parents)}
        if record.budget is not None:
            obj["budget"] = record.budget
        if record.weight_float is not None:
            obj["weight"] = record.weight_float
        elif record.weight_permille is not None:
            obj["weight"] = record.weight_permille / 1000.0
        if record.expected_version is not None:
            obj["expected_version"] = record.expected_version
        lines.append(json.dumps(obj, sort_keys=True))
    return ("\n".join(lines) + "\n").encode("utf-8")


def decode(data: bytes) -> DecodedPayload:
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise WorkbenchError(ErrorCode.E_FORMAT_INVALID, f"invalid utf-8: {exc}") from exc

    lines = [line for line in text.splitlines() if line.strip()]
    if not lines:
        raise WorkbenchError(ErrorCode.E_FORMAT_INVALID, "empty jsonl snapshot")
    try:
        first = json.loads(lines[0])
    except json.JSONDecodeError as exc:
        raise WorkbenchError(ErrorCode.E_FORMAT_INVALID, f"malformed header line: {exc}") from exc
    if not isinstance(first, dict) or not first.get("snapshot"):
        raise WorkbenchError(ErrorCode.E_FORMAT_INVALID, "first jsonl line must be a snapshot header")

    format_version = first.get("format_version")
    if not isinstance(format_version, int) or format_version < 1:
        raise WorkbenchError(ErrorCode.E_FORMAT_INVALID, "header missing integer format_version")
    snapshot_seq = first.get("snapshot_seq")
    if not isinstance(snapshot_seq, int) or snapshot_seq < 0:
        raise WorkbenchError(ErrorCode.E_FORMAT_INVALID, "header missing integer snapshot_seq")
    generated_at = str(first.get("generated_at", ""))
    unknown_required = sorted(k for k in first.keys() - HEADER_KEYS if k.startswith("required_"))

    records: list[RawRecord] = []
    for index, line in enumerate(lines[1:], start=1):
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as exc:
            raise WorkbenchError(ErrorCode.E_FORMAT_INVALID, f"malformed record on line {index}: {exc}") from exc
        if not isinstance(obj, dict) or not isinstance(obj.get("resource_id"), str):
            raise WorkbenchError(ErrorCode.E_FORMAT_INVALID, f"line {index} missing string resource_id")
        extra_required = sorted(k for k in obj.keys() - RECORD_KEYS if k.startswith("required_"))
        unknown_required.extend(name for name in extra_required if name not in unknown_required)

        parents = obj.get("parents", [])
        if not isinstance(parents, list) or not all(isinstance(p, str) for p in parents):
            raise WorkbenchError(ErrorCode.E_FORMAT_INVALID, f"line {index} parents must be a list of strings")
        budget = obj.get("budget")
        if budget is not None and not isinstance(budget, int):
            raise WorkbenchError(ErrorCode.E_FORMAT_INVALID, f"line {index} budget must be an integer")
        weight = obj.get("weight")
        if weight is not None and (isinstance(weight, bool) or not isinstance(weight, (int, float))):
            raise WorkbenchError(ErrorCode.E_FORMAT_INVALID, f"line {index} weight must be numeric")
        expected_version = obj.get("expected_version")
        if expected_version is not None and not isinstance(expected_version, int):
            raise WorkbenchError(ErrorCode.E_FORMAT_INVALID, f"line {index} expected_version must be integer")

        present = [name for name in ("budget", "weight", "expected_version") if name in obj]
        records.append(
            RawRecord(
                resource_id=obj["resource_id"],
                parents=list(parents),
                budget=budget,
                weight_float=float(weight) if weight is not None else None,
                expected_version=expected_version,
                present_fields=present,
            )
        )

    header = SnapshotHeader(
        format=FormatName.JSONL,
        format_version=format_version,
        snapshot_seq=snapshot_seq,
        generated_at=generated_at,
        unknown_required_fields=unknown_required,
    )
    return DecodedPayload(header=header, records=records)
