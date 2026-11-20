"""Atomic stage checkpoints, monotonic sequencing and file-lock publishing."""
from __future__ import annotations

import contextlib
import fcntl
import json
import os
from pathlib import Path

from .errors import InvalidStageError, StaleCheckpointError
from .hashing import canonical_json
from .paths import ensure_dir
from .store import atomic_write_json

STAGE_ORDER = ["normalize", "quality", "features", "sampling"]


class FileLock:
    """Exclusive cross-process lock backed by fcntl flock."""

    def __init__(self, lock_path: Path):
        self.lock_path = ensure_dir(lock_path.parent) / lock_path.name
        self._handle = None

    def __enter__(self):
        self._handle = open(self.lock_path, "a+")
        fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX)
        return self

    def __exit__(self, exc_type, exc, tb):
        if self._handle is not None:
            fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
            self._handle.close()
            self._handle = None


def checkpoint_path(checkpoints_dir: Path, stage: str) -> Path:
    return checkpoints_dir / f"{stage}.checkpoint.json"


def load_checkpoint(checkpoints_dir: Path, stage: str) -> dict | None:
    """Return a checkpoint only when complete and parseable; else ignore it."""
    path = checkpoint_path(checkpoints_dir, stage)
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as handle:
            raw = handle.read()
        record = json.loads(raw)
    except (json.JSONDecodeError, OSError):
        return None
    required = {"stage", "seq", "input_summary", "config_version", "config_fingerprint",
                "artifact_digest", "stage_fingerprint"}
    if not required.issubset(record) or record.get("stage") != stage:
        return None
    return record


def latest_consistent(checkpoints_dir: Path) -> tuple[str | None, int]:
    """Find the longest intact prefix of stage checkpoints and its max seq."""
    last_stage = None
    last_seq = 0
    for index, stage in enumerate(STAGE_ORDER):
        record = load_checkpoint(checkpoints_dir, stage)
        if record is None:
            break
        if record["seq"] != index + 1:
            break
        last_stage = stage
        last_seq = record["seq"]
    return last_stage, last_seq


def publish_checkpoint(checkpoints_dir: Path, record: dict) -> dict:
    """Atomically publish a checkpoint enforcing monotonic per-stage sequence.

    Must be called while holding the workspace publish lock.
    """
    stage = record["stage"]
    if stage not in STAGE_ORDER:
        raise InvalidStageError(f"unknown stage: {stage}", details={"stage": stage})
    expected_seq = STAGE_ORDER.index(stage) + 1
    if record["seq"] != expected_seq:
        raise StaleCheckpointError(
            f"stage {stage} requires seq {expected_seq}, got {record['seq']}",
            details={"stage": stage, "expected_seq": expected_seq, "actual_seq": record["seq"]},
        )
    if expected_seq > 1:
        predecessor = STAGE_ORDER[expected_seq - 2]
        if load_checkpoint(checkpoints_dir, predecessor) is None:
            raise InvalidStageError(
                f"missing predecessor artifact for stage {stage}",
                details={"stage": stage, "missing": predecessor},
            )
    existing = load_checkpoint(checkpoints_dir, stage)
    if existing is not None:
        # Monotonic publication: a stage checkpoint is written exactly once and
        # never overwritten; duplicate (even identical) publication is stale.
        raise StaleCheckpointError(
            "duplicate checkpoint publication",
            details={"stage": stage, "existing_seq": existing["seq"],
                     "conflicting_seq": record["seq"]},
        )
    atomic_write_json(checkpoint_path(checkpoints_dir, stage), record)
    return record


def remove_checkpoint(checkpoints_dir: Path, stage: str) -> None:
    path = checkpoint_path(checkpoints_dir, stage)
    with contextlib.suppress(FileNotFoundError):
        os.unlink(path)


def truncate_from(checkpoints_dir: Path, stage: str) -> None:
    """Invalidate a stage and every downstream checkpoint."""
    start = STAGE_ORDER.index(stage)
    for downstream in STAGE_ORDER[start:]:
        remove_checkpoint(checkpoints_dir, downstream)


def write_marker(path: Path) -> None:
    ensure_dir(path.parent)
    atomic_write_json(path, {"requested": True})


def marker_present(path: Path) -> bool:
    return path.exists()


def clear_marker(path: Path) -> None:
    with contextlib.suppress(FileNotFoundError):
        path.unlink()


def checkpoint_record(stage: str, seq: int, input_summary: dict, config_version: int,
                      config_fingerprint: str, artifact_digest: str,
                      stage_fingerprint: str) -> dict:
    return {
        "stage": stage,
        "seq": seq,
        "input_summary": input_summary,
        "config_version": config_version,
        "config_fingerprint": config_fingerprint,
        "artifact_digest": artifact_digest,
        "stage_fingerprint": stage_fingerprint,
    }
