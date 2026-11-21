from __future__ import annotations

from collections import defaultdict
from typing import Any

from .errors import BusinessError
from .models import MirrorRecord, sha256_json


def _group_records(selected: list[MirrorRecord]) -> dict[str, list[MirrorRecord]]:
    grouped: dict[str, list[MirrorRecord]] = defaultdict(list)
    for record in sorted(selected, key=lambda item: (item.timestamp_ms, item.sampling_sequence, item.record_key)):
        grouped[record.connection_id].append(record)
    return dict(grouped)


def _fits(records: list[MirrorRecord], multiplier: int, policy: dict[str, Any]) -> bool:
    if not records:
        return True
    return (
        len(records) <= policy["max_batch_samples"]
        and sum(record.response_bytes for record in records) * multiplier <= policy["batch_max_bytes"]
        and sum(record.duration_us for record in records) * multiplier <= policy["batch_max_duration_us"]
    )


def _operations_for(
    records: list[MirrorRecord],
    multiplier: int,
    *,
    connection_chunk: int | None = None,
    connection_chunk_count: int | None = None,
) -> list[dict[str, Any]]:
    operations: list[dict[str, Any]] = []
    for record in records:
        for copy_index in range(1, multiplier + 1):
            operations.append(
                {
                    "record_key": record.record_key,
                    "sampling_sequence": record.sampling_sequence,
                    "copy_index": copy_index,
                    "connection_id": record.connection_id,
                    "timestamp_ms": record.timestamp_ms,
                    "source_service": record.source_service,
                    "destination_service": record.destination_service,
                    "method": record.method,
                    "path_template": record.path_template,
                    "original_status_code": record.status_code,
                    "original_response_bytes": record.response_bytes,
                    "original_duration_us": record.duration_us,
                    "connection_chunk": connection_chunk,
                    "connection_chunk_count": connection_chunk_count,
                }
            )
    return operations


def _budget_error(record: MirrorRecord, policy: dict[str, Any]) -> BusinessError:
    return BusinessError(
        "E_BATCH_BUDGET_EXCEEDED",
        "A single mirrored sample cannot fit the declared batch byte/duration budget",
        {
            "record_key": record.record_key,
            "response_bytes": record.response_bytes,
            "duration_us": record.duration_us,
            "batch_max_bytes": policy["batch_max_bytes"],
            "batch_max_duration_us": policy["batch_max_duration_us"],
        },
    )


def _split_group(records: list[MirrorRecord], policy: dict[str, Any], connection_id: str) -> list[dict[str, Any]]:
    chunks: list[list[MirrorRecord]] = []
    current: list[MirrorRecord] = []
    for record in records:
        if not _fits([record], 1, policy):
            raise _budget_error(record, policy)
        if current and not _fits([*current, record], 1, policy):
            chunks.append(current)
            current = [record]
        else:
            current.append(record)
    if current:
        chunks.append(current)
    units = []
    for index, chunk in enumerate(chunks, start=1):
        units.append(
            {
                "connection_id": connection_id,
                "records": chunk,
                "operations": _operations_for(
                    chunk,
                    1,
                    connection_chunk=index,
                    connection_chunk_count=len(chunks),
                ),
                "multiplier": 1,
                "split": len(chunks) > 1,
            }
        )
    return units


def build_batches(selected: list[MirrorRecord], policy: dict[str, Any]) -> dict[str, Any]:
    groups = _group_records(selected)
    target_multiplier = int(policy["replay_multiplier"])
    effective_multiplier = target_multiplier
    for candidate in range(target_multiplier, 0, -1):
        if all(_fits(records, candidate, policy) or not _fits(records, 1, policy) for records in groups.values()):
            effective_multiplier = candidate
            break
    oversized_connections = sorted(
        connection_id for connection_id, records in groups.items() if not _fits(records, 1, policy)
    )
    if oversized_connections:
        effective_multiplier = 1

    units: list[dict[str, Any]] = []
    split_connections: list[dict[str, Any]] = []
    for connection_id in sorted(groups):
        records = groups[connection_id]
        if _fits(records, effective_multiplier, policy):
            units.append(
                {
                    "connection_id": connection_id,
                    "records": records,
                    "operations": _operations_for(records, effective_multiplier),
                    "multiplier": effective_multiplier,
                    "split": False,
                }
            )
        else:
            for unit in _split_group(records, policy, connection_id):
                units.append(unit)
            split_connections.append(
                {
                    "connection_id": connection_id,
                    "sample_count": len(records),
                    "reason": "connection_group_exceeds_batch_limits",
                }
            )

    def unit_sort_key(unit: dict[str, Any]) -> tuple[int, str, int]:
        first = unit["operations"][0]
        return (first["timestamp_ms"], unit["connection_id"], first.get("connection_chunk") or 0)

    units.sort(key=unit_sort_key)

    batches_raw: list[dict[str, Any]] = []
    current: list[dict[str, Any]] = []

    def current_totals() -> tuple[int, int, int]:
        sample_keys = set()
        estimated_bytes = 0
        estimated_duration_us = 0
        for unit in current:
            sample_keys.update(record.record_key for record in unit["records"])
            multiplier = unit["multiplier"]
            estimated_bytes += sum(record.response_bytes for record in unit["records"]) * multiplier
            estimated_duration_us += sum(record.duration_us for record in unit["records"]) * multiplier
        return len(sample_keys), estimated_bytes, estimated_duration_us

    def flush() -> None:
        if not current:
            return
        samples, estimated_bytes, estimated_duration_us = current_totals()
        operations: list[dict[str, Any]] = []
        for unit in current:
            operations.extend(unit["operations"])
        operations.sort(
            key=lambda op: (
                op["connection_id"],
                op["timestamp_ms"],
                op["sampling_sequence"],
                op["record_key"],
                op["copy_index"],
            )
        )
        connection_ids = sorted({inst["connection_id"] for inst in current})
        batches_raw.append(
            {
                "sequence": len(batches_raw) + 1,
                "connection_ids": connection_ids,
                "sample_count": samples,
                "operation_count": len(operations),
                "estimated_bytes": estimated_bytes,
                "estimated_duration_us": estimated_duration_us,
                "max_workers": min(policy["max_batch_concurrency"], max(1, len(connection_ids))),
                "operations": operations,
                "split_connections": sorted({inst["connection_id"] for inst in current if inst["split"]}),
            }
        )

    for unit in units:
        if not current:
            current = [unit]
            continue
        samples, estimated_bytes, estimated_duration_us = current_totals()
        unit_bytes = sum(record.response_bytes for record in unit["records"]) * unit["multiplier"]
        unit_duration = sum(record.duration_us for record in unit["records"]) * unit["multiplier"]
        unit_samples = len(unit["records"])
        if (
            samples + unit_samples <= policy["max_batch_samples"]
            and estimated_bytes + unit_bytes <= policy["batch_max_bytes"]
            and estimated_duration_us + unit_duration <= policy["batch_max_duration_us"]
        ):
            current.append(unit)
        else:
            flush()
            current = [unit]
    flush()

    batches: list[dict[str, Any]] = []
    for raw in batches_raw:
        content = {
            "sequence": raw["sequence"],
            "operations": raw["operations"],
            "estimated_bytes": raw["estimated_bytes"],
            "estimated_duration_us": raw["estimated_duration_us"],
        }
        batch_id = f"b-{raw['sequence']:04d}-{sha256_json(content)[:12]}"
        operations = []
        for index, operation in enumerate(raw["operations"], start=1):
            operations.append({**operation, "operation_id": f"{batch_id}:op:{index:04d}"})
        batches.append(
            {
                "batch_id": batch_id,
                **{key: value for key, value in raw.items() if key != "operations"},
                "operations": operations,
                "state": "ready",
            }
        )

    downshift_applied = effective_multiplier < target_multiplier
    affected_samples = len(selected) if downshift_applied else 0
    affected_operations = affected_samples * (target_multiplier - effective_multiplier)
    result = {
        "artifact_type": "batches",
        "policy_version": policy["policy_version"],
        "target_multiplier": target_multiplier,
        "effective_multiplier": effective_multiplier,
        "downshift": {
            "applied": downshift_applied,
            "reason": "batch_limits_exceeded" if downshift_applied else None,
            "affected_samples": affected_samples,
            "affected_operations": affected_operations,
        },
        "batch_count": len(batches),
        "selected_count": len(selected),
        "operation_count": sum(batch["operation_count"] for batch in batches),
        "split_connections": split_connections,
        "batches": batches,
    }
    result["artifact_id"] = f"bat-{sha256_json(result)[:20]}"
    return result
