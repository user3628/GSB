"""Sharded JSON-lines persistence format (ETJL).

Each non-empty line is one run object. Every line self-describes its
``format_version`` so a shard may be inspected without out-of-band metadata.

Format versions
~~~~~~~~~~~~~~~
* ``JSON_V1 = 1``: ``params`` is optional (an absent/null ``params`` means
  the producer omitted it); metric timestamps are integer microseconds.
* ``JSON_V2 = 2`` (current): same payload, ``params`` is still optional to
  exercise default back-fill, adds an optional downgrade log.

Every line carries the relative retention period (``retention_seconds``) plus
the record's own ``base_time_us`` so the absolute expiry instant can be
recomputed deterministically.
"""

from __future__ import annotations

import json
from typing import Dict, Iterator, List, Tuple

from .errors import ErrorCode, TrackerError
from .models import Artifact, MetricPoint, Run
from .timeutil import retention_to_expiry

JSON_V1 = 1
JSON_V2 = 2
JSON_CURRENT = JSON_V2
SUPPORTED_JSON_VERSIONS = (JSON_V1, JSON_V2)
FORMAT_KIND = "jsonl"


def _run_to_line(run: Run, version: int) -> dict:
    line = {
        "format_version": version,
        "run_id": run.run_id,
        "experiment": run.experiment,
        "params": dict(run.params),
        "metrics": [point.to_dict() for point in run.metrics],
        "toolchain_version": run.toolchain_version,
        "last_write_us": run.last_write_us,
        "base_time_us": run.base_time_us,
        "retention_seconds": run.retention_seconds,
        "expires_at_us": run.expires_at_us,
        "artifacts": [artifact.to_dict() for artifact in run.artifacts],
    }
    if version >= JSON_V2:
        line["downgrades"] = list(run.downgrades)
    return line


def encode_file(runs: List[Run], path: str, version: int = JSON_CURRENT,
                omit_params: bool = False) -> None:
    """Write a JSON-lines shard.

    ``omit_params`` reproduces the legacy producer behavior where the
    parameter block is dropped entirely from each line.
    """
    if version not in SUPPORTED_JSON_VERSIONS:
        raise TrackerError(
            ErrorCode.FORMAT_UNSUPPORTED,
            "cannot encode unsupported jsonl format version",
            {"version": version, "supported": list(SUPPORTED_JSON_VERSIONS)},
        )
    with open(path, "w", encoding="utf-8", newline="\n") as handle:
        for run in runs:
            line = _run_to_line(run, version)
            if omit_params:
                line.pop("params", None)
            handle.write(json.dumps(line, sort_keys=True, separators=(",", ":")))
            handle.write("\n")


def _line_to_run(raw: dict, line_index: int) -> Run:
    try:
        metrics = [MetricPoint.from_dict(point) for point in raw.get("metrics", [])]
        artifacts = [Artifact.from_dict(a) for a in raw.get("artifacts", [])]
        base_time_us = raw.get("base_time_us")
        retention_seconds = raw.get("retention_seconds")
        expires_at_us = raw.get("expires_at_us")
        if base_time_us is not None and retention_seconds is not None:
            recomputed = retention_to_expiry(int(base_time_us), float(retention_seconds))
            if expires_at_us is None:
                expires_at_us = recomputed
        return Run(
            run_id=str(raw["run_id"]),
            experiment=str(raw["experiment"]),
            params=dict(raw.get("params") or {}),
            metrics=metrics,
            toolchain_version=str(raw["toolchain_version"]),
            last_write_us=int(raw["last_write_us"]),
            base_time_us=base_time_us,
            retention_seconds=retention_seconds,
            expires_at_us=expires_at_us,
            artifacts=artifacts,
            downgrades=list(raw.get("downgrades", [])),
        )
    except KeyError as exc:
        raise TrackerError(
            ErrorCode.RECORD_CORRUPT,
            "jsonl line missing required field",
            {"record_index": line_index, "field": str(ex).strip("'")},
        )
    except (TypeError, ValueError) as exc:
        raise TrackerError(
            ErrorCode.RECORD_CORRUPT,
            "jsonl line contains malformed numeric data",
            {"record_index": line_index, "detail": str(exc)},
        )


def inspect_file(path: str) -> Dict[str, object]:
    """Inspect a shard via its first non-empty line; never mutates state."""
    version = None
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                raw = json.loads(line)
            except json.JSONDecodeError as exc:
                raise TrackerError(
                    ErrorCode.RECORD_CORRUPT,
                    "invalid JSON in shard header line",
                    {"record_index": 0, "detail": exc.msg},
                )
            version = raw.get("format_version")
            break
    if version is None:
        raise TrackerError(
            ErrorCode.RECORD_CORRUPT,
            "empty jsonl shard contains no records",
            {"path": path},
        )
    return {"kind": FORMAT_KIND, "format_version": int(version)}


def decode_file(path: str) -> Iterator[Tuple[int, Run]]:
    """Yield ``(record_index, run)`` for each non-empty line."""
    with open(path, "r", encoding="utf-8") as handle:
        line_index = -1
        for raw_line in handle:
            raw_line = raw_line.strip()
            if not raw_line:
                continue
            line_index += 1
            try:
                raw = json.loads(raw_line)
            except json.JSONDecodeError as exc:
                raise TrackerError(
                    ErrorCode.RECORD_CORRUPT,
                    "invalid JSON line",
                    {"record_index": line_index, "detail": exc.msg},
                )
            if not isinstance(raw, dict) or "run_id" not in raw:
                raise TrackerError(
                    ErrorCode.RECORD_CORRUPT,
                    "jsonl line is not a run object",
                    {"record_index": line_index},
                )
            yield line_index, _line_to_run(raw, line_index)
