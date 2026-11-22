from __future__ import annotations

import json
import zlib
from dataclasses import dataclass
from typing import Any

from ..compat import DEGRADE_ACCEPT, decide, degradation, ensure_supported
from ..errors import BusinessError, E_FORMAT_CORRUPT, E_STAT_INVALID
from ..fixed import MAX_ROWS
from ..model import (
    RelationStats,
    StatsSnapshot,
    canonical_json,
    finalize_snapshot,
    normalize_relation,
)

FORMAT_NAME = "jsonl"
CURRENT_VERSION = 2
SCHEMA = "jcs.stats.jsonl"


@dataclass(frozen=True)
class DecodedSnapshot:
    snapshot: StatsSnapshot
    report: dict[str, Any]


def _crc32(data: bytes) -> str:
    return f"{zlib.crc32(data) & 0xFFFFFFFF:08x}"


def _record_without_crc(record: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in record.items() if key != "record_crc32"}


def encode_jsonl(snapshot: StatsSnapshot, *, format_version: int = CURRENT_VERSION) -> bytes:
    if format_version not in {1, 2}:
        raise ValueError("encoder supports format versions 1 and 2")
    record_lines: list[str] = []
    for relation in snapshot.relations:
        record: dict[str, Any] = {
            "type": "relation",
            "relation": relation.name,
            "row_count": relation.row_count,
            "stale_keys": list(relation.stale_keys),
            "record_crc32": "",
        }
        if format_version == 1:
            percent = (relation.ndv_ratio_fixed + 5_000_000) // 10_000_000
            record["ndv_percent"] = max(0, min(100, percent))
        else:
            permille = (relation.ndv_ratio_fixed + 500_000) // 1_000_000
            record["ndv_permille"] = max(0, min(1000, permille))
            record["correlation_coefficient"] = relation.correlation_fixed / 1_000_000_000
        record["record_crc32"] = _crc32(canonical_json(_record_without_crc(record)).encode())
        record_lines.append(canonical_json(record))
    records_blob = ("\n".join(record_lines) + "\n").encode("utf-8")
    manifest = {
        "type": "manifest",
        "schema": SCHEMA,
        "format_version": format_version,
        "snapshot_version": snapshot.version,
        "record_count": len(record_lines),
        "records_crc32": _crc32(records_blob),
        "required_features": ["crc32", "stale_keys"],
    }
    return (canonical_json(manifest) + "\n").encode("utf-8") + records_blob


def _corrupt(message: str, *, line: int | None = None) -> BusinessError:
    details: dict[str, Any] = {"format": FORMAT_NAME}
    if line is not None:
        details["line"] = line
    return BusinessError(E_FORMAT_CORRUPT, message, details)


def _invalid_stat(
    message: str, *, line: int | None = None, relation: str | None = None
) -> BusinessError:
    details: dict[str, Any] = {"format": FORMAT_NAME}
    if line is not None:
        details["line"] = line
    if relation is not None:
        details["relation"] = relation
    return BusinessError(E_STAT_INVALID, message, details)


def _base_report(
    *,
    format_version: int,
    decision: str,
    compatibility: dict[str, Any],
    snapshot_version: int | None,
    record_count: int | None,
) -> dict[str, Any]:
    return {
        "detected_format": FORMAT_NAME,
        "format_version": format_version,
        "decision": decision,
        "compatibility": compatibility,
        "degradations": [],
        "snapshot_version": snapshot_version,
        "digest": None,
        "record_count": record_count,
        "wire": {"schema": SCHEMA, "records_crc32": None},
    }


def decode_jsonl(data: bytes) -> DecodedSnapshot:
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise _corrupt("JSONL snapshot is not valid UTF-8") from exc
    if text.startswith("\ufeff"):
        text = text[1:]
    if not text.strip():
        raise _corrupt("JSONL snapshot is empty")
    raw_lines = text.splitlines()
    objects: list[tuple[int, dict[str, Any]]] = []
    for index, line in enumerate(raw_lines, start=1):
        if not line.strip():
            continue
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError as exc:
            raise _corrupt("invalid JSON line", line=index) from exc
        if not isinstance(parsed, dict):
            raise _corrupt("JSONL line must be an object", line=index)
        objects.append((index, parsed))
    if not objects:
        raise _corrupt("JSONL snapshot contains no records")
    first_index, first = objects[0]
    has_manifest = first.get("type") in {"manifest", "snapshot"} or (
        "relation" not in first and "name" not in first
    )
    if has_manifest:
        manifest = first
        manifest_line = first_index
        record_objects = objects[1:]
        format_version = manifest.get("format_version", 2)
        if isinstance(format_version, bool) or not isinstance(format_version, int):
            raise _corrupt("manifest format_version must be an integer", line=manifest_line)
        required_features = manifest.get("required_features", [])
        if required_features is None:
            required_features = []
        if not isinstance(required_features, list) or not all(
            isinstance(item, str) for item in required_features
        ):
            raise _corrupt("required_features must be an array of strings", line=manifest_line)
        snapshot_version = manifest.get("snapshot_version")
        if isinstance(snapshot_version, bool) or not isinstance(snapshot_version, int):
            raise _corrupt("manifest snapshot_version must be an integer", line=manifest_line)
        declared_count = manifest.get("record_count")
        if isinstance(declared_count, bool) or not isinstance(declared_count, int):
            raise _corrupt("manifest record_count must be an integer", line=manifest_line)
        declared_crc = manifest.get("records_crc32")
        if declared_crc is not None and not isinstance(declared_crc, str):
            raise _corrupt("manifest records_crc32 must be a string", line=manifest_line)
    else:
        format_version = first.get("format_version", 2)
        if isinstance(format_version, bool) or not isinstance(format_version, int):
            raise _corrupt("format_version must be an integer", line=first_index)
        required_features = []
        snapshot_version = first.get("snapshot_version")
        if isinstance(snapshot_version, bool) or not isinstance(snapshot_version, int):
            raise _corrupt(
                "snapshot_version must be present on an implicit-manifest record",
                line=first_index,
            )
        declared_count = len(objects)
        declared_crc = None
        record_objects = objects
    verdict = decide(FORMAT_NAME, format_version, required_features)
    report = _base_report(
        format_version=format_version,
        decision=verdict.decision,
        compatibility=verdict.public_dict(),
        snapshot_version=snapshot_version,
        record_count=declared_count,
    )
    ensure_supported(verdict, report)
    if declared_count != len(record_objects):
        raise _corrupt(
            "manifest record_count does not match the number of JSONL records",
            line=first_index,
        )
    canonical_records: list[str] = []
    correlations_defaulted: list[tuple[int, str]] = []
    relations: list[RelationStats] = []
    for line_number, raw in record_objects:
        if raw.get("type") not in {None, "relation"}:
            raise _corrupt("unexpected JSONL record type", line=line_number)
        relation_name = raw.get("relation", raw.get("name"))
        if not isinstance(relation_name, str):
            raise _corrupt("record is missing relation name", line=line_number)
        line_version = raw.get("format_version", format_version)
        if line_version != format_version:
            raise _corrupt("record format_version disagrees with the manifest", line=line_number)
        line_snapshot_version = raw.get("snapshot_version", snapshot_version)
        if line_snapshot_version != snapshot_version:
            raise _corrupt("record snapshot_version disagrees with the manifest", line=line_number)
        row_count = raw.get("row_count")
        if row_count is None:
            raise _corrupt("record is missing row_count", line=line_number)
        if isinstance(row_count, bool) or not isinstance(row_count, int):
            raise _invalid_stat(
                "record row_count must be an integer",
                line=line_number,
                relation=relation_name,
            )
        if row_count < 0 or row_count > MAX_ROWS:
            raise _invalid_stat(
                "record row_count is outside the supported non-negative range",
                line=line_number,
                relation=relation_name,
            )
        if format_version == 1:
            if "ndv_percent" in raw:
                ndv_raw = raw["ndv_percent"]
                if isinstance(ndv_raw, bool) or not isinstance(ndv_raw, int):
                    raise _invalid_stat(
                        "ndv_percent must be an integer",
                        line=line_number,
                        relation=relation_name,
                    )
                if ndv_raw < 0 or ndv_raw > 100:
                    raise _invalid_stat(
                        "ndv_percent must be in [0, 100]",
                        line=line_number,
                        relation=relation_name,
                    )
                ndv_ratio = ndv_raw / 100
                precision = 2
                ratio_kind = "legacy_percent_integer"
            elif "ndv_permille" in raw:
                ndv_raw = raw["ndv_permille"]
                if isinstance(ndv_raw, bool) or not isinstance(ndv_raw, int):
                    raise _invalid_stat(
                        "ndv_permille must be an integer",
                        line=line_number,
                        relation=relation_name,
                    )
                if ndv_raw < 0 or ndv_raw > 1000:
                    raise _invalid_stat(
                        "ndv_permille must be in [0, 1000]",
                        line=line_number,
                        relation=relation_name,
                    )
                ndv_ratio = ndv_raw / 1000
                precision = 3
                ratio_kind = "permille_integer"
            else:
                raise _invalid_stat(
                    "legacy record is missing ndv_percent",
                    line=line_number,
                    relation=relation_name,
                )
        else:
            ndv_raw = raw.get("ndv_permille")
            if isinstance(ndv_raw, bool) or not isinstance(ndv_raw, int):
                raise _invalid_stat(
                    "ndv_permille must be an integer",
                    line=line_number,
                    relation=relation_name,
                )
            if ndv_raw < 0 or ndv_raw > 1000:
                raise _invalid_stat(
                    "ndv_permille must be in [0, 1000]",
                    line=line_number,
                    relation=relation_name,
                )
            ndv_ratio = ndv_raw / 1000
            precision = 3
            ratio_kind = "permille_integer"
        stale_marker = raw.get("stale_keys", raw.get("stale", []))
        if isinstance(stale_marker, bool):
            stale_keys: Any = ("*",) if stale_marker else ()
        elif isinstance(stale_marker, str):
            stale_keys = (stale_marker,)
        elif isinstance(stale_marker, list) and all(
            isinstance(item, str) for item in stale_marker
        ):
            stale_keys = tuple(stale_marker)
        else:
            raise _corrupt("stale_keys must be a boolean, string, or string array", line=line_number)
        correlation_present = "correlation_coefficient" in raw
        correlation = raw.get("correlation_coefficient", 1.0)
        if correlation_present:
            if isinstance(correlation, bool) or not isinstance(correlation, (int, float)):
                raise _invalid_stat(
                    "correlation_coefficient must be numeric",
                    line=line_number,
                    relation=relation_name,
                )
        else:
            correlations_defaulted.append((line_number, relation_name))
        record_crc = raw.get("record_crc32")
        if record_crc is not None:
            if not isinstance(record_crc, str):
                raise _corrupt("record_crc32 must be a string", line=line_number)
            expected = _crc32(canonical_json(_record_without_crc(raw)).encode())
            if record_crc.lower() != expected:
                raise _corrupt("record CRC32 mismatch", line=line_number)
        canonical_records.append(canonical_json(raw))
        try:
            relation = normalize_relation(
                name=relation_name,
                row_count=row_count,
                ndv_ratio_value=ndv_ratio,
                correlation_value=correlation,
                stale_keys=stale_keys,
                source_kind=ratio_kind,
                source_precision_places=precision,
            )
        except BusinessError as exc:
            exc.details.setdefault("line", line_number)
            raise
        relations.append(relation)
    if declared_crc is not None:
        actual_crc = _crc32(("\n".join(canonical_records) + "\n").encode())
        if actual_crc.lower() != declared_crc.lower():
            raise _corrupt("snapshot records CRC32 mismatch", line=first_index)
    degradations: list[dict[str, Any]] = []
    if format_version == 1:
        degradations.append(
            degradation(
                relation=None,
                field_name="ndv_ratio",
                action="accept_reduced_precision",
                reason="format v1 uses percent precision instead of permille",
            )
        )
    for line_number, relation_name in correlations_defaulted:
        degradations.append(
            degradation(
                relation=relation_name,
                field_name="correlation_coefficient",
                action="default",
                reason="correlation coefficient omitted",
                new_value="1.000000000",
            )
        )
    effective_decision = DEGRADE_ACCEPT if degradations else verdict.decision
    snapshot = finalize_snapshot(
        version=snapshot_version,
        relations=relations,
        format_name=FORMAT_NAME,
        format_version=format_version,
        decision=effective_decision,
        degradations=degradations,
    )
    report = _base_report(
        format_version=format_version,
        decision=effective_decision,
        compatibility=verdict.public_dict(),
        snapshot_version=snapshot_version,
        record_count=declared_count,
    )
    report["degradations"] = degradations
    report["digest"] = snapshot.digest
    report["wire"]["records_crc32"] = declared_crc
    report["normalization"] = snapshot.normalization
    return DecodedSnapshot(snapshot=snapshot, report=report)

