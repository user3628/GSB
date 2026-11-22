from __future__ import annotations

import json
from typing import Any, Iterable

from ..errors import E_FORMAT_CORRUPT, E_FORMAT_UNSUPPORTED, BusinessError, corrupt
from ..models import ProbeResultRecord, RegistrationRecord, Snapshot


FORMAT_NAME = "svc-snapshot-jsonl"


def _line_bytes(value: dict[str, Any]) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _registration_line(
    record: RegistrationRecord,
    *,
    version: int,
    sequence: int,
    shard_index: int,
    shard_count: int,
) -> dict[str, Any]:
    if record.time_unit != "ms":
        raise BusinessError(
            E_FORMAT_CORRUPT,
            "JSONL registration encoding requires canonical millisecond timestamps",
            details={"service_name": record.service_name, "instance_id": record.instance_id, "time_unit": record.time_unit},
        )
    item: dict[str, Any] = {
        "format_name": FORMAT_NAME,
        "format_version": version,
        "kind": "registration",
        "sequence": sequence,
        "shard_index": shard_index,
        "shard_count": shard_count,
        "service_name": record.service_name,
        "instance_id": record.instance_id,
        "endpoint": record.endpoint,
        "weight": record.weight,
        "lease_ttl_ms": record.lease_ttl_ms,
        "probe_method": record.probe_method,
        "probe_params": record.probe_params,
        "registered_at_us": record.registered_at * 1000,
        "lease_version": record.lease_version,
        "probe_window_ms": record.probe_window_ms,
        "last_heartbeat_at_us": (
            record.last_heartbeat_at * 1000 if record.last_heartbeat_at is not None else None
        ),
    }
    return item


def _probe_line(
    record: ProbeResultRecord,
    *,
    version: int,
    sequence: int,
    shard_index: int,
    shard_count: int,
) -> dict[str, Any]:
    if record.time_unit != "ms":
        raise BusinessError(
            E_FORMAT_CORRUPT,
            "JSONL probe-result encoding requires canonical millisecond timestamps",
            details={"service_name": record.service_name, "instance_id": record.instance_id, "time_unit": record.time_unit},
        )
    return {
        "format_name": FORMAT_NAME,
        "format_version": version,
        "kind": "probe_result",
        "sequence": sequence,
        "shard_index": shard_index,
        "shard_count": shard_count,
        "service_name": record.service_name,
        "instance_id": record.instance_id,
        "endpoint": record.endpoint,
        "weight": record.weight,
        "probe_method": record.probe_method,
        "probe_params": record.probe_params,
        "checked_at_us": record.checked_at * 1000,
        "success": record.success,
        "status": record.status,
        "latency_ms": record.latency_ms,
        "outcome": record.outcome,
        "lease_version": record.lease_version,
        "probe_window_ms": record.probe_window_ms,
        "error_code": record.error_code,
        "detail": record.detail,
    }


def encode_jsonl(snapshot: Snapshot, *, version: int = 2, shards: int = 1) -> list[bytes]:
    if version not in (1, 2):
        raise BusinessError(
            E_FORMAT_UNSUPPORTED,
            "JSONL encoder only supports versions 1 and 2",
            details={"version": version, "supported": [1, 2]},
        )
    if shards < 1:
        raise BusinessError(E_FORMAT_CORRUPT, "shards must be at least 1", details={"shards": shards})
    records = list(snapshot.records)
    buckets: list[list[tuple[int, Any]]] = [[] for _ in range(shards)]
    for sequence, record in enumerate(records):
        buckets[sequence % shards].append((sequence, record))
    output: list[bytes] = []
    for shard_index, bucket in enumerate(buckets):
        lines: list[bytes] = []
        for sequence, record in bucket:
            if snapshot.kind == "registration":
                if not isinstance(record, RegistrationRecord):
                    raise BusinessError(E_FORMAT_CORRUPT, "record type does not match snapshot kind")
                value = _registration_line(
                    record,
                    version=version,
                    sequence=sequence,
                    shard_index=shard_index,
                    shard_count=shards,
                )
            else:
                if not isinstance(record, ProbeResultRecord):
                    raise BusinessError(E_FORMAT_CORRUPT, "record type does not match snapshot kind")
                value = _probe_line(
                    record,
                    version=version,
                    sequence=sequence,
                    shard_index=shard_index,
                    shard_count=shards,
                )
            lines.append(_line_bytes(value))
        output.append(b"\n".join(lines) + (b"\n" if lines else b""))
    return output


def _line_texts(data: bytes | str) -> list[str]:
    if isinstance(data, str):
        text = data
    else:
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise corrupt(0, "JSONL shard is not valid UTF-8") from exc
    return text.splitlines()


def _required_string(item: dict[str, Any], name: str, record_index: int) -> str:
    value = item.get(name)
    if not isinstance(value, str) or not value:
        raise corrupt(record_index, f"required field {name} is missing or empty")
    return value


def _timestamp(
    item: dict[str, Any],
    base_name: str,
    record_index: int,
) -> tuple[int, str]:
    us_key = f"{base_name}_us"
    ms_key = f"{base_name}_ms"
    if us_key in item and ms_key in item:
        raise corrupt(record_index, f"both {us_key} and {ms_key} are present")
    if us_key in item:
        value = item[us_key]
        unit = "us"
    elif ms_key in item:
        value = item[ms_key]
        unit = "ms"
    else:
        raise corrupt(record_index, f"required timestamp {us_key} is missing")
    if isinstance(value, bool) or not isinstance(value, int):
        raise corrupt(record_index, f"{base_name} timestamp must be an integer")
    return value, unit


def _parse_jsonl_items(data: bytes | str | list[bytes | str]) -> list[tuple[int, list[str], dict[str, Any]]]:
    shard_inputs: Iterable[bytes | str]
    if isinstance(data, list):
        shard_inputs = data
    else:
        shard_inputs = [data]
    parsed: list[tuple[int, list[str], dict[str, Any]]] = []
    record_index = 0
    for shard_index, shard in enumerate(shard_inputs):
        for line_number, line in enumerate(_line_texts(shard), start=1):
            if not line.strip():
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError as exc:
                raise corrupt(
                    record_index,
                    f"invalid JSON on shard {shard_index} line {line_number}: {exc.msg}",
                    shard_index=shard_index,
                    line_number=line_number,
                ) from exc
            if not isinstance(item, dict):
                raise corrupt(record_index, "JSONL record must be a JSON object")
            parsed.append((record_index, [str(shard_index), str(line_number)], item))
            record_index += 1
    if not parsed:
        raise corrupt(0, "JSONL snapshot contains no records")
    sequential = [item for _, _, item in parsed]
    if all(isinstance(item.get("sequence"), int) and not isinstance(item.get("sequence"), bool) for item in sequential):
        parsed.sort(key=lambda entry: entry[2]["sequence"])
        sequences = [item["sequence"] for _, _, item in parsed]
        if len(sequences) != len(set(sequences)):
            raise corrupt(0, "JSONL sequence values must be unique")
        parsed = [
            (index, location, item)
            for index, (_, location, item) in enumerate(parsed)
        ]
    return parsed


def decode_jsonl(data: bytes | str | list[bytes | str]) -> Snapshot:
    parsed = _parse_jsonl_items(data)
    versions: set[int] = set()
    kinds: set[str] = set()
    records: list[RegistrationRecord | ProbeResultRecord] = []
    for record_index, location, item in parsed:
        version = item.get("format_version", 1)
        if isinstance(version, bool) or not isinstance(version, int):
            raise corrupt(record_index, "format_version must be an integer")
        if version > 2:
            raise BusinessError(
                E_FORMAT_UNSUPPORTED,
                "JSONL snapshot version is newer than supported",
                details={
                    "record_index": record_index,
                    "version": version,
                    "supported": [1, 2],
                    "unknown_required_fields": item.get("unknown_required_fields", []),
                },
                http_status=409,
            )
        if version < 1:
            raise corrupt(record_index, "format_version is invalid", version=version)
        kind = item.get("kind", "registration")
        if kind not in ("registration", "probe_result"):
            raise corrupt(record_index, "kind is invalid", kind=kind)
        versions.add(version)
        kinds.add(kind)
        service_name = _required_string(item, "service_name", record_index)
        instance_id = _required_string(item, "instance_id", record_index)
        endpoint = _required_string(item, "endpoint", record_index)
        method = item.get("probe_method", "")
        if not isinstance(method, str):
            raise corrupt(record_index, "probe_method must be a string")
        weight = item.get("weight")
        if weight is None:
            raise corrupt(record_index, "required field weight is missing")
        probe_params = item.get("probe_params")
        if probe_params is not None and not isinstance(probe_params, dict):
            raise corrupt(record_index, "probe_params must be an object or null")
        lease_version = item.get("lease_version")
        probe_window = item.get("probe_window_ms")
        if kind == "registration":
            ttl = item.get("lease_ttl_ms")
            if isinstance(ttl, bool) or not isinstance(ttl, int):
                raise corrupt(record_index, "lease_ttl_ms must be an integer")
            registered_at, unit = _timestamp(item, "registered_at", record_index)
            heartbeat = None
            if "last_heartbeat_at_us" in item or "last_heartbeat_at_ms" in item:
                heartbeat, heartbeat_unit = _timestamp(item, "last_heartbeat_at", record_index)
                if heartbeat_unit != unit:
                    raise corrupt(record_index, "record timestamps must use the same unit")
            record: RegistrationRecord | ProbeResultRecord = RegistrationRecord(
                service_name=service_name,
                instance_id=instance_id,
                endpoint=endpoint,
                weight=weight,
                weight_encoding="ratio" if version == 1 else "permille",
                lease_ttl_ms=ttl,
                probe_method=method,
                probe_params=probe_params,
                registered_at=registered_at,
                time_unit=unit,
                lease_version=lease_version,
                probe_window_ms=probe_window,
                last_heartbeat_at=heartbeat,
                source_version=version,
            )
        else:
            checked_at, unit = _timestamp(item, "checked_at", record_index)
            success = item.get("success")
            if not isinstance(success, bool):
                raise corrupt(record_index, "success must be boolean")
            status = item.get("status")
            outcome = item.get("outcome")
            latency_ms = item.get("latency_ms")
            if not isinstance(status, str) or not isinstance(outcome, str):
                raise corrupt(record_index, "status and outcome must be strings")
            if isinstance(latency_ms, bool) or not isinstance(latency_ms, int):
                raise corrupt(record_index, "latency_ms must be an integer")
            record = ProbeResultRecord(
                service_name=service_name,
                instance_id=instance_id,
                endpoint=endpoint,
                weight=weight,
                weight_encoding="ratio" if version == 1 else "permille",
                probe_method=method,
                probe_params=probe_params,
                checked_at=checked_at,
                time_unit=unit,
                success=success,
                status=status,
                latency_ms=latency_ms,
                outcome=outcome,
                lease_version=lease_version,
                probe_window_ms=probe_window,
                error_code=item.get("error_code"),
                detail=item.get("detail"),
                source_version=version,
            )
        records.append(record)
    if len(versions) != 1:
        raise BusinessError(
            E_FORMAT_UNSUPPORTED,
            "one JSONL snapshot must not mix format versions",
            details={"versions": sorted(versions)},
            http_status=409,
        )
    if len(kinds) != 1:
        raise BusinessError(
            E_FORMAT_CORRUPT,
            "one JSONL snapshot must not mix record kinds",
            details={"kinds": sorted(kinds)},
        )
    return Snapshot(
        kind=next(iter(kinds)),
        version=next(iter(versions)),
        records=tuple(records),
        source_format="jsonl",
    )
