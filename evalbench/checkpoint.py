"""Persistent checkpoint store with atomic writes and file-lock publishing.

Layout:  <workspace>/checkpoints/<shard_id>/<seq:03d>_<stage>.json

Each checkpoint record carries: shard id, stage, monotonic per-shard
sequence number, record content fingerprint, config version fingerprint,
artifact digest and the artifact itself. Writes are atomic (tmp file +
fsync + os.replace) so an interrupted write never leaves a half-written
record. Publishing takes a real file lock and enforces strictly
consecutive per-shard sequence numbers; stale, duplicated or cross-shard
publishes are rejected with E_STALE_CHECKPOINT, and illegal stage jumps
or missing prerequisites with E_INVALID_STAGE.
"""
from __future__ import annotations

import fcntl
import json
import os
from pathlib import Path

from .errors import InvalidStageError, StaleCheckpointError
from .fingerprint import fingerprint_of_obj

STAGE_ORDER = ("parse", "calibrate", "diagnose")
STAGE_SEQ = {stage: i + 1 for i, stage in enumerate(STAGE_ORDER)}


def _atomic_write_json(path: Path, obj: dict) -> None:
    """Write JSON deterministically and atomically; no half-written files."""
    payload = json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=2)
    tmp = path.with_name(path.name + f".tmp.{os.getpid()}")
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(payload)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)
    dir_fd = os.open(str(path.parent), os.O_DIRECTORY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


class CheckpointStore:
    def __init__(self, workspace: str | Path):
        self.workspace = Path(workspace)
        self.root = self.workspace / "checkpoints"
        self.lock_path = self.workspace / ".publish.lock"
        self.root.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------ helpers
    def _shard_dir(self, shard_id: str) -> Path:
        return self.root / shard_id

    def _checkpoint_path(self, shard_id: str, stage: str) -> Path:
        return self._shard_dir(shard_id) / f"{STAGE_SEQ[stage]:03d}_{stage}.json"

    def _list_checkpoints(self, shard_id: str) -> list[Path]:
        shard_dir = self._shard_dir(shard_id)
        if not shard_dir.is_dir():
            return []
        return sorted(p for p in shard_dir.glob("???_*.json")
                      if not p.name.endswith(".tmp"))

    def latest_seq(self, shard_id: str) -> int:
        seqs = [int(p.name.split("_", 1)[0]) for p in self._list_checkpoints(shard_id)]
        return max(seqs, default=0)

    def get(self, shard_id: str, stage: str) -> dict | None:
        path = self._checkpoint_path(shard_id, stage)
        if not path.is_file():
            return None
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)

    def shard_ids(self) -> list[str]:
        if not self.root.is_dir():
            return []
        return sorted(p.name for p in self.root.iterdir() if p.is_dir())

    # ------------------------------------------------------------ publish
    def publish(self, shard_id: str, stage: str, *,
                record_fingerprint: str, config_fingerprint: str,
                artifact: dict, extra: dict | None = None) -> dict:
        """Publish one stage artifact under the workspace file lock.

        Enforces: stage must follow the completed predecessor (else
        E_INVALID_STAGE); per-shard sequence must advance by exactly one
        and the checkpoint body must target this shard (else
        E_STALE_CHECKPOINT).
        """
        if stage not in STAGE_ORDER:
            raise InvalidStageError("unknown stage", stage=stage,
                                    known_stages=list(STAGE_ORDER))
        expected_seq = STAGE_SEQ[stage]
        record = {
            "shard_id": shard_id,
            "stage": stage,
            "seq": expected_seq,
            "record_fingerprint": record_fingerprint,
            "config_fingerprint": config_fingerprint,
            "artifact_digest": fingerprint_of_obj(artifact),
            "artifact": artifact,
        }
        if extra:
            record.update(extra)

        self.workspace.mkdir(parents=True, exist_ok=True)
        with open(self.lock_path, "a+") as lock_fh:
            fcntl.flock(lock_fh, fcntl.LOCK_EX)
            try:
                if record["shard_id"] != shard_id:
                    raise StaleCheckpointError(
                        "checkpoint body targets a different shard (cross-shard publish)",
                        shard_id=shard_id, body_shard_id=record["shard_id"],
                    )
                current = self.latest_seq(shard_id)
                if expected_seq <= current:
                    raise StaleCheckpointError(
                        "stale or duplicated per-shard sequence",
                        shard_id=shard_id, stage=stage,
                        expected_seq=current + 1, got_seq=expected_seq,
                    )
                if expected_seq > current + 1:
                    raise InvalidStageError(
                        "illegal stage jump: stages must advance consecutively",
                        shard_id=shard_id, stage=stage,
                        expected_seq=current + 1, got_seq=expected_seq,
                    )
                if expected_seq > 1:
                    predecessor = STAGE_ORDER[expected_seq - 2]
                    if self.get(shard_id, predecessor) is None:
                        raise InvalidStageError(
                            "missing prerequisite stage artifact",
                            shard_id=shard_id, stage=stage,
                            missing_prerequisite=predecessor,
                        )
                shard_dir = self._shard_dir(shard_id)
                shard_dir.mkdir(parents=True, exist_ok=True)
                path = self._checkpoint_path(shard_id, stage)
                _atomic_write_json(path, record)
            finally:
                fcntl.flock(lock_fh, fcntl.LOCK_UN)
        return record

    def publish_record(self, record: dict, *, target_shard_id: str | None = None) -> dict:
        """Publish an externally built checkpoint record (validates body).

        When target_shard_id is given, the record body must name the same
        shard; a mismatch is a cross-shard publish (E_STALE_CHECKPOINT).
        """
        shard_id = record.get("shard_id")
        if target_shard_id is not None and shard_id != target_shard_id:
            raise StaleCheckpointError(
                "checkpoint body targets a different shard (cross-shard publish)",
                shard_id=target_shard_id, body_shard_id=shard_id,
            )
        stage = record.get("stage")
        seq = record.get("seq")
        if stage not in STAGE_ORDER or seq != STAGE_SEQ.get(stage):
            raise InvalidStageError(
                "checkpoint record has inconsistent stage/seq",
                shard_id=shard_id, stage=stage, seq=seq,
            )
        return self.publish(
            shard_id, stage,
            record_fingerprint=record.get("record_fingerprint", ""),
            config_fingerprint=record.get("config_fingerprint", ""),
            artifact=record.get("artifact", {}),
            extra={k: v for k, v in record.items()
                   if k not in {"shard_id", "stage", "seq", "record_fingerprint",
                                "config_fingerprint", "artifact_digest", "artifact"}},
        )

    def invalidate_from(self, shard_id: str, stage: str) -> list[str]:
        """Remove a stage and all its downstream checkpoints under the lock.

        Used before rebuilding invalidated artifacts so that publishing
        always appends strictly consecutive sequence numbers.
        """
        if stage not in STAGE_ORDER:
            raise InvalidStageError("unknown stage", stage=stage,
                                    known_stages=list(STAGE_ORDER))
        removed = []
        self.workspace.mkdir(parents=True, exist_ok=True)
        with open(self.lock_path, "a+") as lock_fh:
            fcntl.flock(lock_fh, fcntl.LOCK_EX)
            try:
                for victim in STAGE_ORDER[STAGE_SEQ[stage] - 1:]:
                    path = self._checkpoint_path(shard_id, victim)
                    if path.is_file():
                        path.unlink()
                        removed.append(victim)
            finally:
                fcntl.flock(lock_fh, fcntl.LOCK_UN)
        return removed

    # ------------------------------------------------------------- status
    def status(self) -> dict:
        out = {}
        for shard_id in self.shard_ids():
            stages = {}
            for stage in STAGE_ORDER:
                rec = self.get(shard_id, stage)
                if rec is not None:
                    stages[stage] = {
                        "seq": rec["seq"],
                        "record_fingerprint": rec["record_fingerprint"],
                        "config_fingerprint": rec["config_fingerprint"],
                        "artifact_digest": rec["artifact_digest"],
                    }
            out[shard_id] = {"latest_seq": self.latest_seq(shard_id), "stages": stages}
        return out
