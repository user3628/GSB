"""Deterministic CSV shard loading, manifest verification and fingerprints.

Only the Python standard library plus numpy is used.  A shard is a CSV file
with columns ``timestamp_ms,series_id,target,exo_0,exo_1,...``.  Missing
target values are represented by an empty field; missing timestamps are
slots that exist on the regular time grid but have no CSV row at all.
"""
from __future__ import annotations

import csv
import hashlib
import json
import math
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

from .config import canonical_json
from .errors import ManifestError


TARGET_COLUMN = "target"
TIMESTAMP_COLUMN = "timestamp_ms"
SERIES_COLUMN = "series_id"
EXO_PREFIX = "exo_"


@dataclass
class SeriesData:
    series_id: str
    timestamps: List[int]
    target: np.ndarray            # NaN marks any kind of missing point
    missing_timestamp: np.ndarray  # grid slot without a CSV row
    missing_target: np.ndarray     # row present but target empty
    exogenous: Dict[str, np.ndarray]

    @property
    def slot_count(self) -> int:
        return len(self.timestamps)


@dataclass
class ShardData:
    shard_id: str
    path: str
    cadence_ms: int
    series: Dict[str, SeriesData]
    raw_rows: List[Dict[str, Any]] = field(repr=False, default_factory=list)
    content_fingerprint: str = ""

    @property
    def series_ids(self) -> List[str]:
        return sorted(self.series)


def shard_fingerprint(rows: List[Dict[str, Any]]) -> str:
    """SHA-256 over the canonical encoding of the raw shard rows."""
    payload = canonical_json(rows)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def load_manifest(manifest_path: str) -> Dict[str, Any]:
    with open(manifest_path, "r", encoding="utf-8") as handle:
        manifest = json.load(handle)
    required = {"cadence_ms", "shards"}
    missing = required - set(manifest)
    if missing:
        raise ManifestError(f"manifest missing keys: {sorted(missing)}",
                            context={"path": manifest_path})
    return manifest


def _parse_float(value: str) -> Optional[float]:
    value = value.strip()
    if value == "":
        return None
    number = float(value)
    if math.isnan(number):
        return None
    return number


def _read_csv_rows(path: str) -> Tuple[List[str], List[Dict[str, Any]]]:
    with open(path, "r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        try:
            header = next(reader)
        except StopIteration:
            raise ManifestError("shard CSV is empty", context={"path": path})
        header = [column.strip() for column in header]
        for required in (TIMESTAMP_COLUMN, SERIES_COLUMN, TARGET_COLUMN):
            if required not in header:
                raise ManifestError(f"CSV missing column {required!r}",
                                    context={"path": path})
        rows: List[Dict[str, Any]] = []
        for line_number, raw in enumerate(reader, start=2):
            if not raw or all(cell.strip() == "" for cell in raw):
                continue
            if len(raw) != len(header):
                raise ManifestError(
                    "column count mismatch",
                    context={"path": path, "line": line_number})
            record: Dict[str, Any] = {}
            for column, cell in zip(header, raw):
                if column == TIMESTAMP_COLUMN:
                    record[column] = int(cell)
                elif column == SERIES_COLUMN:
                    record[column] = cell.strip()
                elif column == TARGET_COLUMN:
                    record[column] = _parse_float(cell)
                else:
                    record[column] = _parse_float(cell)
            rows.append(record)
    return header, rows


def load_shard(shard_entry: Dict[str, Any], base_dir: str) -> ShardData:
    """Load one shard and verify it against the manifest entry."""
    path = shard_entry["path"]
    if not os.path.isabs(path):
        path = os.path.normpath(os.path.join(base_dir, path))
    header, rows = _read_csv_rows(path)

    fingerprint = shard_fingerprint(rows)
    declared = shard_entry.get("content_fingerprint")
    if declared is not None and declared != fingerprint:
        raise ManifestError(
            "shard content fingerprint mismatch",
            context={"shard_id": shard_entry["shard_id"],
                     "declared": declared, "actual": fingerprint})
    if shard_entry.get("rows") is not None and shard_entry["rows"] != len(rows):
        raise ManifestError(
            "shard row count mismatch",
            context={"shard_id": shard_entry["shard_id"],
                     "declared": shard_entry["rows"], "actual": len(rows)})

    cadence_ms = int(shard_entry["cadence_ms"])
    start_ms = int(shard_entry["timestamp_start_ms"])
    end_ms = int(shard_entry["timestamp_end_ms"])
    default_grid = list(range(start_ms, end_ms + 1, cadence_ms))
    # optional per-series grid length: lets a genuinely short series differ
    # from a full-grid series with heavy missing rows
    grid_lengths = dict(shard_entry.get("series_grid_slots", {}))
    declared_series = sorted(shard_entry["series"])
    observed_series = sorted({row[SERIES_COLUMN] for row in rows})
    if declared_series != observed_series:
        raise ManifestError(
            "series set mismatch",
            context={"shard_id": shard_entry["shard_id"],
                     "declared": declared_series, "actual": observed_series})

    exo_columns = [c for c in header if c.startswith(EXO_PREFIX)]
    # index rows by (series_id, timestamp)
    indexed: Dict[Tuple[str, int], Dict[str, Any]] = {}
    for row in rows:
        key = (row[SERIES_COLUMN], row[TIMESTAMP_COLUMN])
        if key in indexed:
            raise ManifestError("duplicate (series_id, timestamp) row",
                                context={"key": list(key)})
        indexed[key] = row

    series_data: Dict[str, SeriesData] = {}
    for series_id in declared_series:
        grid_size = int(grid_lengths.get(series_id, len(default_grid)))
        expected_grid = default_grid[:grid_size]
        target = np.full(len(expected_grid), np.nan, dtype=np.float64)
        missing_ts = np.zeros(len(expected_grid), dtype=bool)
        missing_target = np.zeros(len(expected_grid), dtype=bool)
        exogenous = {name: np.full(len(expected_grid), np.nan, dtype=np.float64)
                     for name in exo_columns}
        for slot, timestamp in enumerate(expected_grid):
            row = indexed.get((series_id, timestamp))
            if row is None:
                missing_ts[slot] = True
                continue
            value = row[TARGET_COLUMN]
            if value is None:
                missing_target[slot] = True
            else:
                target[slot] = value
            for name in exo_columns:
                if row.get(name) is not None:
                    exogenous[name][slot] = row[name]
        series_data[series_id] = SeriesData(
            series_id=series_id, timestamps=list(expected_grid),
            target=target, missing_timestamp=missing_ts,
            missing_target=missing_target, exogenous=exogenous)

    return ShardData(
        shard_id=shard_entry["shard_id"], path=path,
        cadence_ms=cadence_ms, series=series_data,
        raw_rows=rows, content_fingerprint=fingerprint)


def load_shards(manifest_path: str) -> Tuple[Dict[str, Any], List[ShardData]]:
    manifest = load_manifest(manifest_path)
    base_dir = os.path.dirname(os.path.abspath(manifest_path))
    shards = [load_shard(entry, base_dir)
              for entry in sorted(manifest["shards"],
                                  key=lambda item: item["shard_id"])]
    return manifest, shards


def estimate_slots(shard_entry: Dict[str, Any]) -> int:
    """Number of grid slots per series implied by manifest time range."""
    start_ms = int(shard_entry["timestamp_start_ms"])
    end_ms = int(shard_entry["timestamp_end_ms"])
    cadence_ms = int(shard_entry["cadence_ms"])
    return (end_ms - start_ms) // cadence_ms + 1


def write_csv_shard(path: str, header: List[str],
                    rows: List[List[Any]]) -> None:
    """Deterministic CSV writer used by the sample generator."""
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)
