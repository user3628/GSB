from __future__ import annotations

import hmac
import json
import re
from pathlib import Path
from typing import Any

from .errors import invalid_sample, invalid_snapshot
from .sql_analysis import validate_readonly_sql
from .util import canonical_json, parse_timestamp, sha256_canonical, sha256_text


MAX_SQLITE_INTEGER = 2**63 - 1
_DIGEST_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")


def load_json_document(path: str | Path, error_kind: str = "snapshot") -> Any:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        if error_kind == "sample":
            raise invalid_sample(f"Unable to read sample JSON: {exc}") from exc
        raise invalid_snapshot(f"Unable to read snapshot JSON: {exc}") from exc


def require_nonnegative_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise invalid_sample(f"{field_name} must be a non-negative integer")
    if value < 0:
        raise invalid_sample(f"{field_name} must not be negative", field=field_name, value=value)
    if value > MAX_SQLITE_INTEGER:
        raise invalid_sample(
            f"{field_name} exceeds the supported 64-bit integer range",
            field=field_name,
            value=str(value),
            max_value=str(MAX_SQLITE_INTEGER),
        )
    return value


def _first_present(raw: dict[str, Any], names: tuple[str, ...]) -> Any:
    for name in names:
        if name in raw:
            return raw[name]
    return None


def normalize_sample(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise invalid_sample("each sample must be a JSON object")
    sample_id = _first_present(raw, ("sample_id", "id"))
    if not isinstance(sample_id, str) or not sample_id.strip():
        raise invalid_sample("sample_id must be a non-empty string")
    sample_id = sample_id.strip()
    if len(sample_id) > 160:
        raise invalid_sample("sample_id must be at most 160 characters", sample_id=sample_id)

    sql_raw = _first_present(raw, ("sql", "query", "query_sql", "statement"))
    try:
        sql = validate_readonly_sql(sql_raw)
    except Exception as exc:
        if hasattr(exc, "code"):
            raise
        raise invalid_sample(f"invalid sql for sample {sample_id}: {exc}", sample_id=sample_id) from exc

    duration_value = _first_present(raw, ("duration_ms", "elapsed_ms", "execution_ms"))
    scanned_value = _first_present(raw, ("scanned_rows", "rows_scanned"))
    lock_value = _first_present(raw, ("lock_wait_ms", "lock_wait", "lock_time_ms"))
    if lock_value is None:
        lock_value = 0
    try:
        duration_ms = require_nonnegative_int(duration_value, "duration_ms")
    except Exception as exc:
        exc.details["sample_id"] = sample_id
        raise
    try:
        scanned_rows = require_nonnegative_int(scanned_value, "scanned_rows")
    except Exception as exc:
        exc.details["sample_id"] = sample_id
        raise
    try:
        lock_wait_ms = require_nonnegative_int(lock_value, "lock_wait_ms")
    except Exception as exc:
        exc.details["sample_id"] = sample_id
        raise

    occurred_raw = _first_present(raw, ("occurred_at", "occurred"))
    try:
        occurred_at = parse_timestamp(occurred_raw, "occurred_at")
    except ValueError as exc:
        raise invalid_sample(str(exc), sample_id=sample_id) from exc

    normalized = {
        "sample_id": sample_id,
        "sql": sql,
        "duration_ms": duration_ms,
        "scanned_rows": scanned_rows,
        "lock_wait_ms": lock_wait_ms,
        "occurred_at": occurred_at,
    }
    normalized["payload_sha256"] = sha256_text(canonical_json(normalized))
    return normalized


def normalize_sample_batch(raw: Any) -> list[dict[str, Any]]:
    if isinstance(raw, dict) and "samples" in raw:
        raw_samples = raw["samples"]
    else:
        raw_samples = raw
    if not isinstance(raw_samples, list) or not raw_samples:
        raise invalid_sample("samples must be a non-empty JSON array")
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    errors: list[dict[str, Any]] = []
    for index, item in enumerate(raw_samples):
        try:
            sample = normalize_sample(item)
        except Exception as exc:
            if hasattr(exc, "code") and exc.code == "E_SAMPLE_INVALID":
                errors.append({"index": index, **exc.details, "message": exc.message})
                continue
            raise
        if sample["sample_id"] in seen:
            errors.append(
                {
                    "index": index,
                    "sample_id": sample["sample_id"],
                    "message": "duplicate sample_id within batch",
                }
            )
            continue
        seen.add(sample["sample_id"])
        normalized.append(sample)
    if errors:
        raise invalid_sample(
            "The sample batch is invalid; no sample was persisted.",
            errors=errors,
            sample_count=len(raw_samples),
        )
    return normalized


def _snapshot_invalid(message: str, **details: Any):
    return invalid_snapshot(message, **details)


def _snapshot_int(value: Any, field_name: str, *, allow_zero: bool = True) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise _snapshot_invalid(f"{field_name} must be an integer")
    if value < 0 or (not allow_zero and value == 0):
        raise _snapshot_invalid(f"{field_name} must be {'non-negative' if allow_zero else 'positive'}")
    if value > MAX_SQLITE_INTEGER:
        raise _snapshot_invalid(f"{field_name} exceeds the 64-bit integer range")
    return value


def normalize_snapshot(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict) and "snapshot" in raw and len(raw) == 1:
        raw = raw["snapshot"]
    if not isinstance(raw, dict):
        raise _snapshot_invalid("snapshot must be a JSON object")
    version = _snapshot_int(raw.get("version"), "snapshot.version", allow_zero=False)
    try:
        generated_at = parse_timestamp(raw.get("generated_at"), "snapshot.generated_at")
    except ValueError as exc:
        raise _snapshot_invalid(str(exc)) from exc

    digest = raw.get("digest")
    if not isinstance(digest, str) or not _DIGEST_PATTERN.match(digest):
        raise _snapshot_invalid("snapshot.digest must be 'sha256:<64 lowercase hex characters>'")
    payload_without_digest = {key: value for key, value in raw.items() if key != "digest"}
    calculated_digest = "sha256:" + sha256_canonical(payload_without_digest)
    if not hmac.compare_digest(digest, calculated_digest):
        raise _snapshot_invalid(
            "snapshot digest mismatch",
            expected=calculated_digest,
            supplied=digest,
        )

    tables_raw = raw.get("tables")
    if not isinstance(tables_raw, dict) or not tables_raw:
        raise _snapshot_invalid("snapshot.tables must be a non-empty object")

    normalized_tables: dict[str, Any] = {}
    total_rows = 0
    total_indexes = 0
    for raw_table_name, raw_table in tables_raw.items():
        if not isinstance(raw_table_name, str) or not raw_table_name.strip():
            raise _snapshot_invalid("table names must be non-empty strings")
        table_name = raw_table_name.strip().lower()
        if not isinstance(raw_table, dict):
            raise _snapshot_invalid(f"snapshot table {table_name!r} must be an object")
        row_count = _snapshot_int(raw_table.get("row_count"), f"tables.{table_name}.row_count")
        columns_raw = raw_table.get("columns")
        if not isinstance(columns_raw, dict) or not columns_raw:
            raise _snapshot_invalid(f"tables.{table_name}.columns must be a non-empty object")
        columns: dict[str, dict[str, int]] = {}
        for raw_column, raw_stats in columns_raw.items():
            if not isinstance(raw_column, str) or not raw_column.strip():
                raise _snapshot_invalid(f"tables.{table_name}.columns has an invalid column name")
            column_name = raw_column.strip().lower()
            if not isinstance(raw_stats, dict):
                raise _snapshot_invalid(
                    f"tables.{table_name}.columns.{column_name} must be an object"
                )
            ndv = _snapshot_int(
                raw_stats.get("ndv"), f"tables.{table_name}.columns.{column_name}.ndv"
            )
            if ndv > row_count:
                raise _snapshot_invalid(
                    f"tables.{table_name}.columns.{column_name}.ndv cannot exceed row_count"
                )
            columns[column_name] = {"ndv": ndv}
        indexes_raw = raw_table.get("indexes", [])
        if not isinstance(indexes_raw, list):
            raise _snapshot_invalid(f"tables.{table_name}.indexes must be an array")
        indexes: list[dict[str, Any]] = []
        seen_index_names: set[str] = set()
        for raw_index in indexes_raw:
            if not isinstance(raw_index, dict):
                raise _snapshot_invalid(f"tables.{table_name}.indexes entries must be objects")
            name = raw_index.get("name")
            if not isinstance(name, str) or not name.strip():
                raise _snapshot_invalid(f"tables.{table_name} has an index without a name")
            name = name.strip()
            if name.lower() in seen_index_names:
                raise _snapshot_invalid(f"duplicate index name {name!r} on table {table_name!r}")
            seen_index_names.add(name.lower())
            index_columns = raw_index.get("columns")
            if not isinstance(index_columns, list) or not index_columns:
                raise _snapshot_invalid(f"index {name!r} must have at least one column")
            normalized_columns: list[str] = []
            for column in index_columns:
                if not isinstance(column, str) or column.strip().lower() not in columns:
                    raise _snapshot_invalid(
                        f"index {name!r} references an unknown column {column!r}"
                    )
                normalized_columns.append(column.strip().lower())
            unique = raw_index.get("unique", False)
            if not isinstance(unique, bool):
                raise _snapshot_invalid(f"index {name!r}.unique must be boolean")
            indexes.append({"name": name, "columns": normalized_columns, "unique": unique})
        normalized_tables[table_name] = {
            "row_count": row_count,
            "columns": dict(sorted(columns.items())),
            "indexes": indexes,
        }
        total_rows += row_count
        total_indexes += len(indexes)

    summary_raw = raw.get("summary")
    if not isinstance(summary_raw, dict):
        raise _snapshot_invalid("snapshot.summary is required")
    summary = {
        "table_count": len(normalized_tables),
        "total_rows": total_rows,
        "index_count": total_indexes,
    }
    for key, expected in summary.items():
        supplied = summary_raw.get(key)
        if supplied != expected:
            raise _snapshot_invalid(
                f"snapshot.summary.{key} does not match the snapshot content",
                expected=expected,
                supplied=supplied,
            )

    normalized = {
        "version": version,
        "generated_at": generated_at,
        "summary": summary,
        "tables": dict(sorted(normalized_tables.items())),
        "digest": digest,
    }
    normalized["normalized_sha256"] = sha256_text(canonical_json(normalized))
    return normalized


def load_samples(path: str | Path) -> list[dict[str, Any]]:
    return normalize_sample_batch(load_json_document(path, error_kind="sample"))


def load_snapshot(path: str | Path) -> dict[str, Any]:
    return normalize_snapshot(load_json_document(path, error_kind="snapshot"))
