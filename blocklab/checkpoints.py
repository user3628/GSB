"""Atomic, sequenced, recoverable block checkpoint publication."""

from __future__ import annotations

import json
import re
import shutil
import uuid
from pathlib import Path
from typing import Any, Callable

import numpy as np

from .errors import E_STALE_CHECKPOINT, BusinessError
from .paths import FileLock, JobPaths
from .tensorio import npy_bytes, npy_from_bytes
from .util import (
    atomic_write_bytes,
    atomic_write_json,
    canonical_json,
    digest_bytes,
    digest_json,
    read_json,
)

CHECKPOINT_SCHEMA = "blocklab.checkpoint.v1"
RECORD_RE = re.compile(r"^seq-(\d{8})-[^/]+\.json$")


def checkpoint_filename(sequence: int, key: str) -> str:
    return f"seq-{sequence:08d}-{key}.json"


def artifact_filename(sequence: int, key: str, kind: str) -> str:
    suffix = "npy" if kind == "npy" else "json"
    return f"blocks/{sequence:08d}-{key}.{suffix}"


def _record_file_map(stage_dir: Path) -> dict[int, list[Path]]:
    result: dict[int, list[Path]] = {}
    if not stage_dir.exists():
        return result
    for path in stage_dir.glob("seq-*.json"):
        match = RECORD_RE.match(path.name)
        if not match:
            continue
        filename_sequence = int(match.group(1))
        try:
            record = read_json(path)
            internal = int(record.get("sequence", filename_sequence))
        except Exception:
            # Keep an unreadable/truncated record attached to its filename sequence so
            # recovery can report the exact damaged position instead of a silent gap.
            internal = filename_sequence
        result.setdefault(internal, []).append(path)
    return result


def _validate_record(
    record: Any,
    *,
    stage_dir: Path,
    sequence: int,
    epoch: int,
    stage: str,
    job_id: str,
    plan_fingerprint: str,
    input_fingerprint: str,
    config_version: str,
    schedule_item: dict[str, Any],
) -> tuple[dict[str, Any] | None, str | None]:
    if not isinstance(record, dict):
        return None, "record_not_object"
    required = {
        "schema",
        "sequence",
        "epoch",
        "job_id",
        "shard_id",
        "stage",
        "block",
        "input_fingerprint",
        "stage_fingerprint",
        "config_version",
        "product_digest",
        "product_kind",
        "artifact",
        "status",
    }
    if not required.issubset(record):
        return None, "record_incomplete"
    if record.get("schema") != CHECKPOINT_SCHEMA:
        return None, "record_schema_invalid"
    if record.get("sequence") != sequence or record.get("epoch") != epoch:
        return None, "sequence_or_epoch_mismatch"
    if record.get("stage") != stage or record.get("job_id") != job_id:
        return None, "record_identity_mismatch"
    if record.get("stage_fingerprint") != plan_fingerprint:
        return None, "stage_fingerprint_mismatch"
    if record.get("input_fingerprint") != input_fingerprint:
        return None, "input_fingerprint_mismatch"
    if record.get("config_version") != config_version:
        return None, "config_version_mismatch"
    block = record.get("block")
    if not isinstance(block, dict):
        return None, "block_missing"
    if block.get("key") != schedule_item.get("key"):
        return None, "block_key_mismatch"
    if int(block.get("index", -1)) != int(schedule_item.get("index", -2)):
        return None, "block_index_mismatch"
    artifact_rel = record.get("artifact")
    if not isinstance(artifact_rel, str) or not artifact_rel:
        return None, "artifact_missing"
    artifact = (stage_dir / artifact_rel).resolve()
    try:
        artifact.relative_to(stage_dir.resolve())
    except ValueError:
        return None, "artifact_path_escape"
    if not artifact.is_file():
        return None, "artifact_not_found"
    data = artifact.read_bytes()
    if digest_bytes(data) != record.get("product_digest"):
        return None, "artifact_digest_mismatch"
    if record.get("product_kind") == "npy":
        try:
            npy_from_bytes(data)
        except Exception:
            return None, "artifact_not_npy"
    elif record.get("product_kind") == "json":
        try:
            json.loads(data.decode("utf-8"))
        except Exception:
            return None, "artifact_not_json"
    else:
        return None, "artifact_kind_invalid"
    return record, None


def scan_stage(
    paths: JobPaths,
    *,
    stage: str,
    epoch: int,
    plan_fingerprint: str,
    input_fingerprint: str,
    config_version: str,
    schedule: list[dict[str, Any]],
) -> dict[str, Any]:
    stage_dir = paths.stage_dir(stage, epoch)
    record_map = _record_file_map(stage_dir)
    valid: list[dict[str, Any]] = []
    ignored: list[dict[str, Any]] = []
    stop_reason = None
    max_seen = max(record_map, default=0)
    for sequence in range(1, len(schedule) + 1):
        candidates = record_map.get(sequence, [])
        if not candidates:
            stop_reason = "missing_sequence"
            break
        if len(candidates) > 1:
            ignored.append(
                {
                    "sequence": sequence,
                    "reason": "duplicate_sequence",
                    "files": [path.name for path in candidates],
                }
            )
            stop_reason = "duplicate_sequence"
            break
        filename_match = RECORD_RE.match(candidates[0].name)
        if filename_match is None or int(filename_match.group(1)) != sequence:
            ignored.append(
                {
                    "sequence": sequence,
                    "reason": "filename_sequence_mismatch",
                    "file": candidates[0].name,
                }
            )
            stop_reason = "filename_sequence_mismatch"
            break
        record, reason = _validate_record(
            _safe_read(candidates[0]),
            stage_dir=stage_dir,
            sequence=sequence,
            epoch=epoch,
            stage=stage,
            job_id=paths.job_id,
            plan_fingerprint=plan_fingerprint,
            input_fingerprint=input_fingerprint,
            config_version=config_version,
            schedule_item=schedule[sequence - 1],
        )
        if record is None:
            ignored.append(
                {"sequence": sequence, "reason": reason, "file": candidates[0].name}
            )
            stop_reason = reason
            break
        valid.append(record)
    return {
        "stage": stage,
        "epoch": epoch,
        "expected_blocks": len(schedule),
        "valid_blocks": len(valid),
        "records": valid,
        "ignored": ignored,
        "max_sequence_seen": max_seen,
        "stop_reason": stop_reason,
        "complete": len(valid) == len(schedule),
        "stage_dir": str(stage_dir),
    }


def _safe_read(path: Path) -> Any:
    try:
        return read_json(path)
    except Exception:
        return None


def stage_content_digest(
    records: list[dict[str, Any]], *, stage: str, plan_fingerprint: str
) -> str:
    return digest_json(
        {
            "schema": "blocklab.stage-content.v1",
            "stage": stage,
            "stage_fingerprint": plan_fingerprint,
            "blocks": [
                {
                    "sequence": int(record["sequence"]),
                    "key": record["block"]["key"],
                    "product_digest": record["product_digest"],
                    "status": record["status"],
                }
                for record in records
            ],
        }
    )


def _quarantine_invalid_tail(paths: JobPaths, stage_dir: Path, valid_count: int) -> None:
    candidates = list(stage_dir.glob("seq-*.json"))
    if not candidates:
        return
    destination = paths.quarantine_dir / f"{stage_dir.parent.name}-{uuid.uuid4().hex[:12]}"
    moved = False
    for path in candidates:
        match = RECORD_RE.match(path.name)
        if not match:
            continue
        sequence = int(match.group(1))
        if sequence <= valid_count:
            continue
        destination.mkdir(parents=True, exist_ok=True)
        try:
            record = read_json(path)
            artifact_rel = record.get("artifact") if isinstance(record, dict) else None
            if isinstance(artifact_rel, str):
                artifact = (stage_dir / artifact_rel).resolve()
                try:
                    artifact.relative_to(stage_dir.resolve())
                except ValueError:
                    artifact = None
                if artifact is not None and artifact.exists():
                    target = destination / artifact.name
                    shutil.move(str(artifact), str(target))
        except Exception:
            pass
        shutil.move(str(path), str(destination / path.name))
        moved = True
    if moved:
        (destination / "QUARANTINE_REASON.txt").write_text(
            f"ignored non-prefix checkpoint records after sequence {valid_count}\n",
            encoding="utf-8",
        )


def publish_block(
    paths: JobPaths,
    *,
    stage: str,
    epoch: int,
    requested_sequence: int,
    plan_fingerprint: str,
    input_fingerprint: str,
    config_version: str,
    workspace_config_version: str,
    schedule: list[dict[str, Any]],
    schedule_item: dict[str, Any],
    artifact_bytes: bytes,
    product_kind: str,
    status: str,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    """Publish one complete block under a true file lock.

    Sequence and schedule position must both match the current valid prefix. Any
    stale, duplicate, or cross-block publish is rejected with E_STALE_CHECKPOINT.
    """

    with FileLock(paths.stage_lock(stage)):
        current = scan_stage(
            paths,
            stage=stage,
            epoch=epoch,
            plan_fingerprint=plan_fingerprint,
            input_fingerprint=input_fingerprint,
            config_version=config_version,
            schedule=schedule,
        )
        expected_sequence = current["valid_blocks"] + 1
        if requested_sequence != expected_sequence:
            raise BusinessError(
                E_STALE_CHECKPOINT,
                "checkpoint sequence is stale or non-contiguous",
                stage=stage,
                requested_sequence=requested_sequence,
                expected_sequence=expected_sequence,
                current_available_sequence=current["valid_blocks"],
                epoch=epoch,
                block=schedule_item["key"],
            )
        expected_item = schedule[expected_sequence - 1]
        if schedule_item["key"] != expected_item["key"]:
            raise BusinessError(
                E_STALE_CHECKPOINT,
                "checkpoint crosses an unpublished block",
                stage=stage,
                requested_block=schedule_item["key"],
                expected_block=expected_item["key"],
                expected_sequence=expected_sequence,
                current_available_sequence=current["valid_blocks"],
                epoch=epoch,
            )

        stage_dir = paths.stage_dir(stage, epoch)
        _quarantine_invalid_tail(paths, stage_dir, current["valid_blocks"])
        artifact_rel = artifact_filename(expected_sequence, schedule_item["key"], product_kind)
        artifact_path = stage_dir / artifact_rel
        atomic_write_bytes(artifact_path, artifact_bytes)
        product_digest = digest_bytes(artifact_bytes)
        try:
            job_record = read_json(paths.job_file)
            shard_id = job_record.get("shard_id") or paths.job_id
        except Exception:
            shard_id = paths.job_id
        record = {
            "schema": CHECKPOINT_SCHEMA,
            "sequence": expected_sequence,
            "epoch": epoch,
            "job_id": paths.job_id,
            "shard_id": shard_id,
            "stage": stage,
            "block": {
                "index": int(schedule_item["index"]),
                "key": schedule_item["key"],
                "coords": schedule_item.get("coords", {}),
                "row_range": schedule_item.get("row_range"),
                "col_range": schedule_item.get("col_range"),
            },
            "input_fingerprint": input_fingerprint,
            "stage_fingerprint": plan_fingerprint,
            "config_version": config_version,
            "workspace_config_version": workspace_config_version,
            "product_digest": product_digest,
            "product_kind": product_kind,
            "artifact": artifact_rel,
            "artifact_size": len(artifact_bytes),
            "status": status,
            "metadata": metadata,
            "artifact_shape": metadata.get("shape"),
        }
        record_path = stage_dir / checkpoint_filename(expected_sequence, schedule_item["key"])
        atomic_write_json(record_path, record)
        return record


def load_artifact(paths: JobPaths, stage: str, epoch: int, record: dict[str, Any]) -> bytes:
    return (paths.stage_dir(stage, epoch) / record["artifact"]).read_bytes()


def load_block_array(paths: JobPaths, stage: str, epoch: int, record: dict[str, Any]) -> np.ndarray:
    return npy_from_bytes(load_artifact(paths, stage, epoch, record))


def json_artifact(value: Any) -> bytes:
    return (canonical_json(value) + "\n").encode("utf-8")

