"""Atomic monotonic checkpoints and cooperative stop control."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from .errors import ToneLabError
from .fingerprints import STAGES
from .util import (
    atomic_write_json,
    canonical_json_bytes,
    file_lock,
    read_json,
    sha256_bytes,
)


class CheckpointStore:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.records_dir = self.root / "records"
        self.records_dir.mkdir(parents=True, exist_ok=True)
        self.lock_path = self.root / ".publish.lock"

    def _record_files(self) -> list[Path]:
        return sorted(self.records_dir.glob("*.json"))

    def records(self) -> list[dict[str, Any]]:
        output = []
        for path in self._record_files():
            output.append(read_json(path))
        return output

    def last_sequence(self) -> int:
        files = self._record_files()
        if not files:
            return 0
        return int(files[-1].stem)

    def _assert_continuous(self) -> None:
        sequences = [int(path.stem) for path in self._record_files()]
        expected = list(range(1, len(sequences) + 1))
        if sequences != expected:
            raise ToneLabError(
                "E_INVALID_STAGE",
                "checkpoint sequence files are not continuous",
                context={"sequences": sequences, "expected": expected},
            )

    def _latest(self, records: list[dict[str, Any]], stage: str, shard_id: str) -> dict[str, Any] | None:
        for record in reversed(records):
            if record.get("stage") == stage and record.get("shard_id") == shard_id:
                return record
        return None

    def publish(
        self,
        *,
        stage: str,
        shard_id: str,
        shard_fingerprint: str,
        structure_version_fingerprint: str,
        config_version: str,
        artifact_summary: dict[str, Any],
        artifact_paths: list[str | Path] | None = None,
        sequence: int | None = None,
        project_id: str = "",
        stage_fingerprint: str | None = None,
    ) -> dict[str, Any]:
        if stage not in STAGES:
            raise ToneLabError("E_INVALID_STAGE", "unknown checkpoint stage", shard_id=shard_id, context={"stage": stage})
        with file_lock(self.lock_path):
            self._assert_continuous()
            records = self.records()
            last = self.last_sequence()
            if stage != "feature":
                previous_stage = STAGES[STAGES.index(stage) - 1]
                previous_record = self._latest(records, previous_stage, shard_id)
                if previous_record is None:
                    raise ToneLabError(
                        "E_INVALID_STAGE",
                        "missing prerequisite stage checkpoint",
                        shard_id=shard_id,
                        context={"stage": stage, "required_stage": previous_stage},
                    )
                missing_predecessor = [
                    self._resolve_artifact_path(relative)
                    for relative in previous_record.get("artifact_files", [])
                    if not self._resolve_artifact_path(relative).exists()
                ]
                if missing_predecessor:
                    raise ToneLabError(
                        "E_INVALID_STAGE",
                        "prerequisite stage artifact is missing",
                        shard_id=shard_id,
                        context={
                            "stage": stage,
                            "required_stage": previous_stage,
                            "missing": [str(path) for path in missing_predecessor],
                        },
                    )
            latest_same_stage = self._latest(records, stage, shard_id)
            artifact_digest = sha256_bytes(canonical_json_bytes(artifact_summary))
            duplicate = False
            if latest_same_stage is not None:
                if stage_fingerprint is not None:
                    duplicate = latest_same_stage.get("stage_fingerprint") == stage_fingerprint
                else:
                    duplicate = latest_same_stage.get("artifact_digest") == artifact_digest
            if duplicate:
                raise ToneLabError(
                    "E_STALE_CHECKPOINT",
                    "duplicate publication for an unchanged shard stage",
                    shard_id=shard_id,
                    context={
                        "stage": stage,
                        "last_sequence": latest_same_stage.get("sequence"),
                        "shard_fingerprint": shard_fingerprint,
                    },
                )
            if sequence is None:
                sequence = last + 1
            else:
                sequence = int(sequence)
                if sequence <= last:
                    raise ToneLabError(
                        "E_STALE_CHECKPOINT",
                        "checkpoint sequence is stale or duplicated",
                        shard_id=shard_id,
                        context={
                            "stage": stage,
                            "requested_sequence": sequence,
                            "last_sequence": last,
                        },
                    )
                if sequence != last + 1:
                    raise ToneLabError(
                        "E_STALE_CHECKPOINT",
                        "checkpoint sequence must advance continuously",
                        shard_id=shard_id,
                        context={
                            "stage": stage,
                            "requested_sequence": sequence,
                            "expected_sequence": last + 1,
                        },
                    )
            record_path = self.records_dir / f"{sequence:012d}.json"
            if record_path.exists():
                raise ToneLabError(
                    "E_STALE_CHECKPOINT",
                    "checkpoint sequence already exists",
                    shard_id=shard_id,
                    context={"sequence": sequence, "path": str(record_path)},
                )
            for artifact_path in artifact_paths or []:
                if not Path(artifact_path).exists():
                    raise ToneLabError(
                        "E_INVALID_STAGE",
                        "checkpoint artifact file is missing",
                        shard_id=shard_id,
                        context={
                            "stage": stage,
                            "artifact_path": str(artifact_path),
                        },
                    )
            record = {
                "format": "tonelab.checkpoint",
                "schema": 1,
                "sequence": sequence,
                "project_id": project_id,
                "stage": stage,
                "shard_id": shard_id,
                "shard_fingerprint": shard_fingerprint,
                "stage_fingerprint": stage_fingerprint,
                "structure_version_fingerprint": structure_version_fingerprint,
                "config_version": config_version,
                "artifact_summary": artifact_summary,
                "artifact_files": [self._store_artifact_path(path) for path in (artifact_paths or [])],
                "artifact_digest": artifact_digest,
            }
            atomic_write_json(record_path, record)
            return record

    def _store_artifact_path(self, path: str | Path) -> str:
        resolved = Path(path).resolve()
        try:
            return resolved.relative_to(self.root.parent.resolve()).as_posix()
        except ValueError:
            return str(resolved)

    def _resolve_artifact_path(self, stored: str) -> Path:
        path = Path(stored)
        if path.is_absolute():
            return path
        return self.root.parent / path


class StopController:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.path = self.root / "STOP_REQUEST.json"

    def request(self, reason: str = "user requested stop") -> dict[str, Any]:
        payload = {"requested": True, "reason": reason}
        atomic_write_json(self.path, payload)
        return payload

    def is_requested(self) -> bool:
        if not self.path.exists():
            return False
        try:
            return bool(read_json(self.path).get("requested"))
        except Exception:
            return True

    def clear(self) -> None:
        self.path.unlink(missing_ok=True)

    def status(self) -> dict[str, Any]:
        if not self.path.exists():
            return {"requested": False}
        try:
            return read_json(self.path)
        except Exception:
            return {"requested": True, "reason": "unreadable stop marker"}

