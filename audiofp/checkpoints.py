"""Atomic stage checkpoints with per-shard monotonic publish sequences."""

from __future__ import annotations

import hashlib
import re
import shutil
from pathlib import Path
from typing import Any

from .errors import E_INVALID_STAGE, E_STALE_CHECKPOINT, FingerprintError
from .jsonutil import atomic_write_json, read_json
from .locking import FileLock


STAGES = ("peaks", "pairs", "quantize", "audit")
STAGE_INDEX = {stage: index for index, stage in enumerate(STAGES)}


def _safe_shard_id(shard_id: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", shard_id).strip("._")
    if not cleaned:
        cleaned = "shard"
    digest = hashlib.sha256(shard_id.encode("utf-8")).hexdigest()[:10]
    return f"{cleaned[:48]}-{digest}"


def safe_shard_id(shard_id: str) -> str:
    return _safe_shard_id(shard_id)


def _initial_state(shard_id: str) -> dict[str, Any]:
    return {
        "schema": "audiofp.checkpoint-state.v1",
        "shard_id": shard_id,
        "generation": 0,
        "next_seq": 1,
        "stages": {},
        "last_invalidation": None,
    }


class CheckpointStore:
    """Durable checkpoint store scoped to one workspace.

    ``seq`` is monotonic inside a shard and never resets on invalidation.  When
    a stage is invalidated, that stage and all downstream stages are removed,
    but ``next_seq`` advances only when a replacement record is published.
    """

    def __init__(self, workspace: str | Path) -> None:
        self.workspace = Path(workspace)
        self.root = self.workspace / "checkpoints"
        self.locks = self.workspace / ".locks"
        self.root.mkdir(parents=True, exist_ok=True)
        self.locks.mkdir(parents=True, exist_ok=True)

    def shard_dir(self, shard_id: str) -> Path:
        return self.root / safe_shard_id(shard_id)

    def state_path(self, shard_id: str) -> Path:
        return self.shard_dir(shard_id) / "state.json"

    def record_path(self, shard_id: str, seq: int, stage: str) -> Path:
        return self.shard_dir(shard_id) / "records" / f"{seq:08d}-{stage}.json"

    def _lock(self, shard_id: str) -> FileLock:
        key = hashlib.sha256(shard_id.encode("utf-8")).hexdigest()[:24]
        return FileLock(self.locks / f"checkpoint-{key}.lock")

    def _read_state(self, shard_id: str) -> dict[str, Any]:
        path = self.state_path(shard_id)
        if not path.exists():
            return _initial_state(shard_id)
        state = read_json(path)
        if not isinstance(state, dict) or state.get("shard_id") != shard_id:
            raise FingerprintError(
                E_STALE_CHECKPOINT,
                "checkpoint state belongs to a different shard",
                shard_id=shard_id,
                stage="checkpoint",
                details={"reason": "cross_shard_state"},
            )
        return state

    def _write_state(self, state: dict[str, Any]) -> None:
        atomic_write_json(self.state_path(state["shard_id"]), state)

    def read_state(self, shard_id: str) -> dict[str, Any]:
        with self._lock(shard_id):
            return self._read_state(shard_id)

    def _expected_stage(self, state: dict[str, Any]) -> str | None:
        for stage in STAGES:
            if stage not in state["stages"]:
                return stage
        return None

    def publish(
        self,
        shard_id: str,
        record: dict[str, Any],
        expected_seq: int | None = None,
        artifact_path: str | Path | None = None,
    ) -> dict[str, Any]:
        """Atomically publish one checkpoint record.

        The caller writes the stage artifact first.  If *artifact_path* is
        supplied, its SHA-256 must equal ``record['artifact_digest']`` while
        the shard lock is held.
        """

        if not isinstance(shard_id, str) or not shard_id:
            raise FingerprintError(E_INVALID_STAGE, "invalid shard id", stage="checkpoint")
        stage = record.get("stage")
        if stage not in STAGES:
            raise FingerprintError(
                E_INVALID_STAGE,
                "unknown checkpoint stage",
                shard_id=shard_id,
                stage=str(stage) if stage is not None else "checkpoint",
                details={"allowed_stages": list(STAGES)},
            )
        with self._lock(shard_id):
            state = self._read_state(shard_id)
            record_shard = record.get("shard_id")
            if record_shard != shard_id:
                raise FingerprintError(
                    E_STALE_CHECKPOINT,
                    "checkpoint record is misaligned with the target shard",
                    shard_id=shard_id,
                    stage=stage,
                    details={"reason": "cross_shard_record", "record_shard_id": record_shard},
                )
            if stage in state["stages"]:
                raise FingerprintError(
                    E_STALE_CHECKPOINT,
                    "stage checkpoint already exists and cannot be republished in place",
                    shard_id=shard_id,
                    stage=stage,
                    details={
                        "reason": "duplicate_stage",
                        "existing_seq": state["stages"][stage]["seq"],
                        "next_seq": state["next_seq"],
                    },
                )
            expected_stage = self._expected_stage(state)
            if stage != expected_stage:
                raise FingerprintError(
                    E_INVALID_STAGE,
                    "checkpoint would skip a required predecessor stage",
                    shard_id=shard_id,
                    stage=stage,
                    details={
                        "expected_stage": expected_stage,
                        "received_stage": stage,
                        "present_stages": [name for name in STAGES if name in state["stages"]],
                    },
                )
            next_seq = int(state["next_seq"])
            supplied_seq = record.get("seq")
            if expected_seq is not None and supplied_seq is not None and int(expected_seq) != int(supplied_seq):
                raise FingerprintError(
                    E_STALE_CHECKPOINT,
                    "checkpoint sequence disagrees with the expected sequence",
                    shard_id=shard_id,
                    stage=stage,
                    details={"reason": "sequence_conflict", "expected_seq": expected_seq, "provided_seq": supplied_seq},
                )
            if supplied_seq is None:
                seq = int(expected_seq) if expected_seq is not None else next_seq
            else:
                seq = int(supplied_seq)
            if seq != next_seq:
                raise FingerprintError(
                    E_STALE_CHECKPOINT,
                    "checkpoint sequence is stale, duplicated, or advanced out of order",
                    shard_id=shard_id,
                    stage=stage,
                    details={
                        "reason": "non_consecutive_sequence",
                        "expected_seq": next_seq,
                        "provided_seq": seq,
                    },
                )
            if artifact_path is not None:
                artifact = Path(artifact_path)
                if not artifact.exists():
                    raise FingerprintError(
                        E_INVALID_STAGE,
                        "stage artifact is missing before checkpoint publication",
                        shard_id=shard_id,
                        stage=stage,
                        details={"reason": "missing_artifact"},
                    )
                actual_digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
                if actual_digest != record.get("artifact_digest"):
                    raise FingerprintError(
                        E_STALE_CHECKPOINT,
                        "stage artifact digest does not match the checkpoint",
                        shard_id=shard_id,
                        stage=stage,
                        details={"reason": "artifact_digest_mismatch"},
                    )

            stored = dict(record)
            stored["seq"] = seq
            stored["generation"] = int(state["generation"])
            stored.setdefault("schema", "audiofp.checkpoint.v1")
            atomic_write_json(self.record_path(shard_id, seq, stage), stored)
            state["stages"][stage] = stored
            state["next_seq"] = seq + 1
            self._write_state(state)
            return stored

    def invalidate_from(
        self,
        shard_id: str,
        stage: str,
        reason: str,
        changed_fields: list[str] | None = None,
        old_versions: dict[str, Any] | None = None,
        new_versions: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if stage not in STAGES:
            raise FingerprintError(
                E_INVALID_STAGE,
                "cannot invalidate an unknown stage",
                shard_id=shard_id,
                stage=str(stage),
                details={"allowed_stages": list(STAGES)},
            )
        with self._lock(shard_id):
            state = self._read_state(shard_id)
            affected = STAGES[STAGE_INDEX[stage] :]
            removed = [name for name in affected if name in state["stages"]]
            for name in affected:
                state["stages"].pop(name, None)
            state["generation"] = int(state["generation"]) + 1
            state["last_invalidation"] = {
                "from_stage": stage,
                "reason": reason,
                "changed_fields": list(changed_fields or []),
                "removed_stages": removed,
                "old_versions": old_versions or {},
                "new_versions": new_versions or {},
                "generation": state["generation"],
                "next_seq": state["next_seq"],
            }
            self._write_state(state)
            return state["last_invalidation"]

    def clear_invalidation_marker(self, shard_id: str) -> None:
        with self._lock(shard_id):
            state = self._read_state(shard_id)
            state["last_invalidation"] = None
            self._write_state(state)

    def reset(self, shard_id: str) -> None:
        with self._lock(shard_id):
            directory = self.shard_dir(shard_id)
            if directory.exists():
                shutil.rmtree(directory)

