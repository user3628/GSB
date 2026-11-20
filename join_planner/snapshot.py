from __future__ import annotations

import hashlib
import hmac
import json
import math
import threading
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping

from .errors import PlannerError, snapshot_invalid, version_retired


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON number is not allowed: {value}")


def _canonical_bytes(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def checksum_for_snapshot(payload: Mapping[str, Any]) -> str:
    unsealed = {key: value for key, value in payload.items() if key != "checksum"}
    return hashlib.sha256(_canonical_bytes(unsealed)).hexdigest()


def _require_positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise snapshot_invalid(f"{field} must be a positive integer", field=field, value=value)
    return value


@dataclass(frozen=True)
class ColumnStats:
    name: str
    ndv: int
    min_value: float | None = None
    max_value: float | None = None


@dataclass(frozen=True)
class CorrelationStats:
    columns: tuple[str, ...]
    coefficient: float


@dataclass(frozen=True)
class RelationStats:
    name: str
    rows: int
    columns: Mapping[str, ColumnStats]
    correlations: tuple[CorrelationStats, ...]


@dataclass(frozen=True)
class Snapshot:
    version: int
    generated_at: str
    checksum: str
    relations: Mapping[str, RelationStats]
    source_file: str

    def public_metadata(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "generated_at": self.generated_at,
            "checksum": self.checksum,
            "source_file": self.source_file,
            "relations": {
                name: {"rows": relation.rows}
                for name, relation in sorted(self.relations.items())
            },
        }


def _parse_column(name: str, data: Any) -> ColumnStats:
    if not isinstance(data, dict):
        raise snapshot_invalid("column statistics must be an object", relation_column=name)
    ndv = _require_positive_int(data.get("ndv"), f"columns.{name}.ndv")
    min_value = data.get("min")
    max_value = data.get("max")
    if min_value is not None:
        if isinstance(min_value, bool) or not isinstance(min_value, (int, float)):
            raise snapshot_invalid(f"columns.{name}.min must be numeric")
        min_value = float(min_value)
        if not math.isfinite(min_value):
            raise snapshot_invalid(f"columns.{name}.min must be finite")
    if max_value is not None:
        if isinstance(max_value, bool) or not isinstance(max_value, (int, float)):
            raise snapshot_invalid(f"columns.{name}.max must be numeric")
        max_value = float(max_value)
        if not math.isfinite(max_value):
            raise snapshot_invalid(f"columns.{name}.max must be finite")
    if min_value is not None and max_value is not None and min_value > max_value:
        raise snapshot_invalid(
            f"columns.{name} min cannot exceed max",
            column=name,
            min=min_value,
            max=max_value,
        )
    return ColumnStats(name=name, ndv=ndv, min_value=min_value, max_value=max_value)


def _parse_relation(name: str, data: Any) -> RelationStats:
    if not isinstance(data, dict):
        raise snapshot_invalid("relation statistics must be an object", relation=name)
    rows = _require_positive_int(data.get("rows"), f"relations.{name}.rows")
    raw_columns = data.get("columns")
    if not isinstance(raw_columns, dict) or not raw_columns:
        raise snapshot_invalid(
            "relation must contain a non-empty columns object",
            relation=name,
        )
    columns = {column_name: _parse_column(column_name, raw) for column_name, raw in raw_columns.items()}

    raw_correlations = data.get("correlations", [])
    if not isinstance(raw_correlations, list):
        raise snapshot_invalid("relation correlations must be an array", relation=name)
    correlations: list[CorrelationStats] = []
    for index, raw in enumerate(raw_correlations):
        if not isinstance(raw, dict):
            raise snapshot_invalid("correlation must be an object", relation=name, index=index)
        raw_columns_list = raw.get("columns")
        if (
            not isinstance(raw_columns_list, list)
            or len(raw_columns_list) < 2
            or any(not isinstance(column, str) or not column for column in raw_columns_list)
            or len(set(raw_columns_list)) != len(raw_columns_list)
        ):
            raise snapshot_invalid(
                "correlation columns must contain at least two unique names",
                relation=name,
                index=index,
            )
        missing = [column for column in raw_columns_list if column not in columns]
        if missing:
            raise snapshot_invalid(
                "correlation references unknown column",
                relation=name,
                missing_columns=missing,
            )
        coefficient = raw.get("coefficient")
        if isinstance(coefficient, bool) or not isinstance(coefficient, (int, float)):
            raise snapshot_invalid("correlation coefficient must be numeric", relation=name, index=index)
        coefficient = float(coefficient)
        if not math.isfinite(coefficient) or coefficient <= 0:
            raise snapshot_invalid(
                "correlation coefficient must be finite and positive",
                relation=name,
                index=index,
            )
        correlations.append(
            CorrelationStats(columns=tuple(raw_columns_list), coefficient=coefficient)
        )

    return RelationStats(
        name=name,
        rows=rows,
        columns=MappingProxyType(columns),
        correlations=tuple(correlations),
    )


def parse_snapshot_text(text: str, source_file: str = "<memory>") -> Snapshot:
    try:
        payload = json.loads(text, parse_constant=_reject_json_constant)
    except (json.JSONDecodeError, ValueError) as exc:
        raise snapshot_invalid("snapshot is not valid finite JSON", source_file=source_file, error=str(exc)) from exc
    if not isinstance(payload, dict):
        raise snapshot_invalid("snapshot root must be an object", source_file=source_file)

    version = _require_positive_int(payload.get("version"), "version")
    generated_at = payload.get("generated_at")
    if not isinstance(generated_at, str) or not generated_at:
        raise snapshot_invalid("generated_at must be a non-empty ISO-8601 string")
    try:
        datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise snapshot_invalid("generated_at is not a valid ISO-8601 timestamp") from exc

    expected_checksum = payload.get("checksum")
    algorithm = payload.get("checksum_algorithm", "sha256")
    if algorithm != "sha256":
        raise snapshot_invalid(
            "checksum_algorithm must be sha256",
            checksum_algorithm=algorithm,
        )
    if not isinstance(expected_checksum, str) or len(expected_checksum) != 64:
        raise snapshot_invalid("checksum must be a 64-character SHA-256 hex digest")
    try:
        int(expected_checksum, 16)
    except ValueError as exc:
        raise snapshot_invalid("checksum must be hexadecimal") from exc
    actual_checksum = checksum_for_snapshot(payload)
    if not hmac.compare_digest(expected_checksum.lower(), actual_checksum):
        raise snapshot_invalid(
            "snapshot checksum mismatch",
            expected_checksum=expected_checksum,
            actual_checksum=actual_checksum,
        )

    raw_relations = payload.get("relations")
    if not isinstance(raw_relations, dict) or not raw_relations:
        raise snapshot_invalid("snapshot must contain a non-empty relations object")
    relations = {
        name: _parse_relation(name, raw)
        for name, raw in raw_relations.items()
        if isinstance(name, str) and name
    }
    if len(relations) != len(raw_relations):
        raise snapshot_invalid("relation names must be non-empty strings")

    return Snapshot(
        version=version,
        generated_at=generated_at,
        checksum=actual_checksum,
        relations=MappingProxyType(relations),
        source_file=source_file,
    )


class SnapshotStore:
    """Thread-safe, monotonic snapshot registry.

    Snapshots are immutable. A request binds to the object captured at submission;
    replacing the current snapshot cannot mutate a build that is already running.
    """

    def __init__(self, snapshot_dir: Path) -> None:
        self.snapshot_dir = snapshot_dir.resolve()
        self._lock = threading.RLock()
        self._current: Snapshot | None = None
        self._history: dict[int, Snapshot] = {}

    @property
    def current(self) -> Snapshot | None:
        with self._lock:
            return self._current

    def _safe_path(self, filename: str) -> Path:
        if not isinstance(filename, str) or not filename.strip():
            raise snapshot_invalid("snapshot filename must be a non-empty string")
        candidate = (self.snapshot_dir / filename).resolve()
        try:
            candidate.relative_to(self.snapshot_dir)
        except ValueError as exc:
            raise snapshot_invalid("snapshot path must stay inside the snapshot directory") from exc
        if candidate.suffix.lower() != ".json":
            raise snapshot_invalid("snapshot filename must end in .json")
        return candidate

    def load_file(self, filename: str) -> Snapshot:
        path = self._safe_path(filename)
        try:
            text = path.read_text(encoding="utf-8")
        except OSError as exc:
            raise snapshot_invalid(
                "snapshot file cannot be read",
                filename=filename,
                error=str(exc),
            ) from exc
        snapshot = parse_snapshot_text(text, source_file=filename)
        return self._activate(snapshot)

    def load_absolute_file(self, path: Path) -> Snapshot:
        resolved = path.resolve()
        try:
            resolved.relative_to(self.snapshot_dir)
        except ValueError as exc:
            raise snapshot_invalid("snapshot path must stay inside the snapshot directory") from exc
        try:
            text = resolved.read_text(encoding="utf-8")
        except OSError as exc:
            raise snapshot_invalid("snapshot file cannot be read", error=str(exc)) from exc
        snapshot = parse_snapshot_text(text, source_file=resolved.name)
        return self._activate(snapshot)

    def _activate(self, incoming: Snapshot) -> Snapshot:
        with self._lock:
            current = self._current
            if current is not None and incoming.version <= current.version:
                raise snapshot_invalid(
                    "snapshot version must be strictly newer than the active version",
                    incoming_version=incoming.version,
                    current_version=current.version,
                    memory_unchanged=True,
                )
            self._current = incoming
            self._history[incoming.version] = incoming
            return incoming

    def for_submission(self, version: int) -> Snapshot:
        with self._lock:
            current = self._current
            if current is not None and current.version == version:
                return current
            known = sorted(self._history)
            if current is None and version not in self._history:
                raise PlannerError(
                    "E_VERSION_RETIRED",
                    "The requested snapshot version has never been active.",
                    {
                        "requested_version": version,
                        "current_version": None,
                        "known_versions": known,
                    },
                )
            raise version_retired(version, current.version if current else None)

    def is_still_current(self, snapshot: Snapshot) -> bool:
        with self._lock:
            return self._current is snapshot

