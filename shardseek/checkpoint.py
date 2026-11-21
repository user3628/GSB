"""Atomic, lock-protected stage checkpoint log with strict ordering checks."""

from __future__ import annotations

import contextlib
import fcntl
import os
from pathlib import Path
from typing import Any, Iterator

from .errors import (
    E_INVALID_STAGE,
    E_STALE_CHECKPOINT,
    error,
)
from .util import (
    canonical_bytes,
    digest_obj,
    jsonl_bytes,
    read_json,
    write_atomic,
    write_json_atomic,
)


STAGES = ("feature_extract", "project_quantize", "index_build", "recall_verify")
STAGE_INDEX = {name: index + 1 for index, name in enumerate(STAGES)}


class CheckpointStore:
    def __init__(self, state_dir: str | Path) -> None:
        self.state_dir = Path(state_dir)
        self.lock_path = self.state_dir / "locks" / "checkpoints.lock"
        self.session_path = self.state_dir / "session.json"
        self.checkpoint_path = self.state_dir / "checkpoints.jsonl"
        self.archive_dir = self.state_dir / "checkpoints_archive"
        self.archive_dir.mkdir(parents=True, exist_ok=True)
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)

    @contextlib.contextmanager
    def _locked(self) -> Iterator[None]:
        with open(self.lock_path, "a+", encoding="utf-8") as lock_handle:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)

    def _read_records_unlocked(self) -> list[dict[str, Any]]:
        if not self.checkpoint_path.exists():
            return []
        data = self.checkpoint_path.read_bytes()
        if not data:
            return []
        records: list[dict[str, Any]] = []
        for line_number, line in enumerate(data.decode("utf-8").splitlines(), start=1):
            if not line:
                continue
            import json

            try:
                records.append(json.loads(line))
            except Exception as exc:
                raise error(
                    E_STALE_CHECKPOINT,
                    "Checkpoint log contains an invalid record",
                    stage="checkpoint",
                    details={"line": line_number, "reason": type(exc).__name__},
                ) from exc
        return records

    def read_records(self) -> list[dict[str, Any]]:
        with self._locked():
            return self._read_records_unlocked()

    def initialize_session(self, session: dict[str, Any], *, force_new: bool = False) -> dict[str, Any]:
        required = {"session_id", "expected_pairs", "start_stage", "ordered_shards"}
        missing = sorted(required - set(session))
        if missing:
            raise error(
                E_INVALID_STAGE,
                "Checkpoint session is missing required fields",
                stage="checkpoint",
                details={"missing": missing},
            )
        with self._locked():
            existing: dict[str, Any] | None = None
            if self.session_path.exists():
                existing = read_json(self.session_path)
            if existing and existing.get("session_id") == session["session_id"] and not force_new:
                # Retain partial progress for a resume of the same deterministic build.
                return existing

            if self.checkpoint_path.exists() and self.checkpoint_path.stat().st_size > 0:
                archive_name = f"{(existing or {}).get('session_id', 'unknown')}.jsonl"
                archive_path = self.archive_dir / archive_name
                os.replace(self.checkpoint_path, archive_path)
            write_atomic(self.checkpoint_path, b"")
            write_json_atomic(self.session_path, session, pretty=True)
            return session

    def load_session(self) -> dict[str, Any] | None:
        if not self.session_path.exists():
            return None
        return read_json(self.session_path)

    def _ordering_error(
        self,
        *,
        shard_id: str,
        stage_index: int,
        expected_shard: str,
        expected_stage: int,
        records: list[dict[str, Any]],
    ) -> Exception:
        same_shard = [r for r in records if r.get("shard_id") == shard_id]
        if stage_index > 1 and not same_shard and shard_id == expected_shard:
            return error(
                E_INVALID_STAGE,
                "Prerequisite checkpoint stage is missing",
                stage="checkpoint",
                item_id=shard_id,
                details={
                    "requested_stage_index": stage_index,
                    "expected_stage_index": expected_stage,
                    "prerequisite_stage": STAGES[stage_index - 2],
                },
            )
        return error(
            E_STALE_CHECKPOINT,
            "Checkpoint publication is stale, duplicate, or cross-shard out of order",
            stage="checkpoint",
            item_id=shard_id,
            details={
                "requested_stage_index": stage_index,
                "expected_shard": expected_shard,
                "expected_stage_index": expected_stage,
                "records_published": len(records),
            },
        )

    def publish(self, record: dict[str, Any]) -> dict[str, Any]:
        shard_id = str(record.get("shard_id", ""))
        stage = str(record.get("stage", ""))
        if stage not in STAGE_INDEX:
            raise error(
                E_INVALID_STAGE,
                "Unknown checkpoint stage",
                stage="checkpoint",
                item_id=shard_id or None,
                details={"stage": stage, "allowed": list(STAGES)},
            )
        stage_index = int(record.get("stage_index", STAGE_INDEX[stage]))
        if stage_index != STAGE_INDEX[stage]:
            raise error(
                E_INVALID_STAGE,
                "Stage name and stage index disagree",
                stage="checkpoint",
                item_id=shard_id or None,
                details={"stage": stage, "stage_index": stage_index, "expected": STAGE_INDEX[stage]},
            )
        for field in (
            "input_fingerprint",
            "config_version",
            "quantization_fingerprint",
            "artifact_digest",
        ):
            if not record.get(field):
                raise error(
                    E_INVALID_STAGE,
                    "Checkpoint record is missing a required fingerprint",
                    stage="checkpoint",
                    item_id=shard_id or None,
                    details={"field": field},
                )

        with self._locked():
            session = self.load_session()
            if not session:
                raise error(
                    E_INVALID_STAGE,
                    "Checkpoint session has not been initialized",
                    stage="checkpoint",
                    item_id=shard_id or None,
                    details={},
                )
            records = self._read_records_unlocked()
            expected_pairs = session["expected_pairs"]
            if len(records) >= len(expected_pairs):
                raise error(
                    E_STALE_CHECKPOINT,
                    "Checkpoint sequence is already complete",
                    stage="checkpoint",
                    item_id=shard_id or None,
                    details={"published": len(records), "expected": len(expected_pairs)},
                )
            expected_shard, expected_stage = expected_pairs[len(records)]
            expected_stage = int(expected_stage)
            if shard_id != expected_shard or stage_index != expected_stage:
                raise self._ordering_error(
                    shard_id=shard_id,
                    stage_index=stage_index,
                    expected_shard=str(expected_shard),
                    expected_stage=expected_stage,
                    records=records,
                )

            next_seq = len(records) + 1
            supplied_seq = record.get("seq")
            if supplied_seq is not None and int(supplied_seq) != next_seq:
                raise error(
                    E_STALE_CHECKPOINT,
                    "Checkpoint sequence is not the next continuous value",
                    stage="checkpoint",
                    item_id=shard_id,
                    details={"supplied_seq": supplied_seq, "required_seq": next_seq},
                )
            normalized = {
                "seq": next_seq,
                "session_id": session["session_id"],
                "shard_id": shard_id,
                "stage": stage,
                "stage_index": stage_index,
                "input_fingerprint": record["input_fingerprint"],
                "config_version": record["config_version"],
                "quantization_fingerprint": record["quantization_fingerprint"],
                "artifact_digest": record["artifact_digest"],
                "snapshot_sequence": int(session.get("snapshot_sequence", 0)),
            }
            records.append(normalized)
            write_atomic(self.checkpoint_path, jsonl_bytes(records))
            return normalized

    def mark_session_complete(self) -> dict[str, Any]:
        with self._locked():
            session = self.load_session()
            if not session:
                raise error(E_INVALID_STAGE, "No active checkpoint session", stage="checkpoint")
            session["completed"] = True
            session["final_seq"] = len(self._read_records_unlocked())
            write_json_atomic(self.session_path, session, pretty=True)
            return session

    def status(self) -> dict[str, Any]:
        with self._locked():
            session = self.load_session()
            records = self._read_records_unlocked()
            expected_pairs = session.get("expected_pairs", []) if session else []
            next_pair = expected_pairs[len(records)] if len(records) < len(expected_pairs) else None
            return {
                "session_id": session.get("session_id") if session else None,
                "start_stage": session.get("start_stage") if session else None,
                "completed": session.get("completed", False) if session else False,
                "record_count": len(records),
                "expected_count": len(expected_pairs),
                "last_seq": records[-1]["seq"] if records else 0,
                "next_expected": (
                    {"shard_id": next_pair[0], "stage_index": int(next_pair[1]), "stage": STAGES[int(next_pair[1]) - 1]}
                    if next_pair
                    else None
                ),
                "checkpoint_digest": digest_obj(records),
                "last_record": records[-1] if records else None,
            }

