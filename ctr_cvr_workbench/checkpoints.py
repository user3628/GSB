"""Atomic monotonic checkpoints and stage-level validity/invalidation."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .errors import ErrorCode, PipelineError
from .util import (
    FileLock,
    atomic_write_bytes,
    canonical_json_line,
    portable_relative,
    sha256_bytes,
    sha256_file,
)


STAGES = ("features", "train", "funnel", "infer")
_RECORD_NAME = re.compile(r"^(\d{8})\.json$")


@dataclass
class CheckpointRecord:
    seq: int
    kind: str
    stage: str | None
    status: str
    stage_signature: str | None
    config_version: str
    shard_input_fingerprint: str | None
    artifact_digest: str | None
    artifact_path: str | None
    details: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "kind": self.kind,
            "stage": self.stage,
            "status": self.status,
            "stage_signature": self.stage_signature,
            "config_version": self.config_version,
            "shard_input_fingerprint": self.shard_input_fingerprint,
            "artifact_digest": self.artifact_digest,
            "artifact_path": self.artifact_path,
            "details": self.details,
        }


class CheckpointStore:
    def __init__(self, root: Path, workspace_root: Path):
        self.root = Path(root).resolve()
        self.workspace_root = Path(workspace_root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def _record_path(self, seq: int) -> Path:
        return self.root / f"{seq:08d}.json"

    def records(self) -> list[CheckpointRecord]:
        found: list[CheckpointRecord] = []
        for path in sorted(self.root.glob("*.json")):
            match = _RECORD_NAME.match(path.name)
            if not match:
                continue
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                found.append(
                    CheckpointRecord(
                        seq=int(payload["seq"]),
                        kind=str(payload["kind"]),
                        stage=payload.get("stage"),
                        status=str(payload.get("status", "completed")),
                        stage_signature=payload.get("stage_signature"),
                        config_version=str(payload.get("config_version", "")),
                        shard_input_fingerprint=payload.get("shard_input_fingerprint"),
                        artifact_digest=payload.get("artifact_digest"),
                        artifact_path=payload.get("artifact_path"),
                        details=dict(payload.get("details", {})),
                    )
                )
            except (OSError, KeyError, ValueError, json.JSONDecodeError) as exc:
                raise PipelineError(
                    ErrorCode.E_CHECKPOINT_CORRUPT,
                    "checkpoint record is unreadable",
                    stage="checkpoint",
                    details={"path": path.as_posix(), "reason": str(exc)},
                ) from exc
        found.sort(key=lambda record: record.seq)
        sequences = [record.seq for record in found]
        if len(set(sequences)) != len(sequences):
            raise PipelineError(
                ErrorCode.E_CHECKPOINT_CORRUPT,
                "checkpoint sequence is not monotonic or unique",
                stage="checkpoint",
            )
        return found

    def _next_seq_locked(self) -> int:
        maximum = 0
        for path in self.root.glob("*.json"):
            match = _RECORD_NAME.match(path.name)
            if match:
                maximum = max(maximum, int(match.group(1)))
        return maximum + 1

    def write_record(
        self,
        *,
        kind: str,
        stage: str | None,
        status: str,
        stage_signature: str | None,
        config_version: str,
        shard_input_fingerprint: str | None,
        artifact_digest: str | None,
        artifact_path: str | None,
        details: dict[str, Any] | None = None,
    ) -> CheckpointRecord:
        with FileLock(self.root / ".lock"):
            seq = self._next_seq_locked()
            record = CheckpointRecord(
                seq=seq,
                kind=kind,
                stage=stage,
                status=status,
                stage_signature=stage_signature,
                config_version=config_version,
                shard_input_fingerprint=shard_input_fingerprint,
                artifact_digest=artifact_digest,
                artifact_path=artifact_path,
                details=details or {},
            )
            target = self._record_path(seq)
            data = canonical_json_line(record.to_dict())
            # The final checkpoint name is produced by one atomic replace.
            atomic_write_bytes(target, data)
            directory = target.parent
            if os.name != "nt":
                try:
                    descriptor = os.open(directory, os.O_RDONLY)
                    try:
                        os.fsync(descriptor)
                    finally:
                        os.close(descriptor)
                except OSError:
                    pass
            return record

    def write_artifact_and_stage(
        self,
        *,
        stage: str,
        stage_signature: str,
        config_version: str,
        shard_input_fingerprint: str,
        artifact_path: Path,
        artifact_bytes: bytes,
        details: dict[str, Any] | None = None,
        status: str = "completed",
    ) -> CheckpointRecord:
        artifact_path = Path(artifact_path)
        atomic_write_bytes(artifact_path, artifact_bytes)
        digest = sha256_bytes(artifact_bytes)
        return self.write_record(
            kind="stage_complete",
            stage=stage,
            status=status,
            stage_signature=stage_signature,
            config_version=config_version,
            shard_input_fingerprint=shard_input_fingerprint,
            artifact_digest=digest,
            artifact_path=portable_relative(artifact_path, self.workspace_root),
            details=details,
        )

    def write_progress(
        self,
        *,
        stage: str,
        stage_signature: str,
        config_version: str,
        shard_input_fingerprint: str,
        artifact_path: Path,
        artifact_bytes: bytes,
        status: str,
        details: dict[str, Any] | None = None,
    ) -> CheckpointRecord:
        artifact_path = Path(artifact_path)
        atomic_write_bytes(artifact_path, artifact_bytes)
        digest = sha256_bytes(artifact_bytes)
        return self.write_record(
            kind="stage_progress",
            stage=stage,
            status=status,
            stage_signature=stage_signature,
            config_version=config_version,
            shard_input_fingerprint=shard_input_fingerprint,
            artifact_digest=digest,
            artifact_path=portable_relative(artifact_path, self.workspace_root),
            details=details,
        )

    def invalidate(
        self,
        *,
        earliest_stage: str,
        invalidated_stages: list[str],
        reason: str,
        config_version: str,
        details: dict[str, Any] | None = None,
    ) -> CheckpointRecord:
        return self.write_record(
            kind="invalidation",
            stage=earliest_stage,
            status="invalidated",
            stage_signature=None,
            config_version=config_version,
            shard_input_fingerprint=None,
            artifact_digest=None,
            artifact_path=None,
            details={
                "earliest_stage": earliest_stage,
                "invalidated_stages": invalidated_stages,
                "reason": reason,
                **(details or {}),
            },
        )

    def _latest_invalidation_seq(self, stage: str, records: list[CheckpointRecord]) -> int:
        latest = 0
        for record in records:
            if record.kind == "invalidation" and stage in record.details.get("invalidated_stages", []):
                latest = max(latest, record.seq)
        return latest

    def latest_completion(
        self,
        stage: str,
        *,
        signature: str | None = None,
        verify_artifact: bool = True,
    ) -> CheckpointRecord | None:
        records = self.records()
        invalidation_seq = self._latest_invalidation_seq(stage, records)
        candidates = [
            record
            for record in records
            if record.kind == "stage_complete" and record.stage == stage and record.seq > invalidation_seq
        ]
        if signature is not None:
            candidates = [record for record in candidates if record.stage_signature == signature]
        if not candidates:
            return None
        record = max(candidates, key=lambda item: item.seq)
        if verify_artifact:
            if not record.artifact_path or record.artifact_digest is None:
                return None
            artifact = self.resolve_artifact(record.artifact_path)
            if not artifact.exists():
                return None
            if sha256_file(artifact) != record.artifact_digest:
                return None
        return record

    def latest_progress(
        self,
        stage: str,
        *,
        signature: str,
        statuses: tuple[str, ...] = ("running", "stopped"),
    ) -> CheckpointRecord | None:
        records = self.records()
        invalidation_seq = self._latest_invalidation_seq(stage, records)
        completion = self.latest_completion(stage, verify_artifact=False)
        completion_seq = completion.seq if completion else 0
        candidates = [
            record
            for record in records
            if record.kind == "stage_progress"
            and record.stage == stage
            and record.stage_signature == signature
            and record.status in statuses
            and record.seq > invalidation_seq
            and record.seq > completion_seq
        ]
        if not candidates:
            return None
        record = max(candidates, key=lambda item: item.seq)
        if not record.artifact_path or record.artifact_digest is None:
            return None
        artifact = self.resolve_artifact(record.artifact_path)
        if not artifact.exists() or sha256_file(artifact) != record.artifact_digest:
            return None
        return record

    def resolve_artifact(self, relative_path: str) -> Path:
        path = Path(relative_path)
        if path.is_absolute():
            return path
        return (self.workspace_root / path).resolve()

    def invalidations(self) -> list[CheckpointRecord]:
        return [record for record in self.records() if record.kind == "invalidation"]

    def stage_status(self, stage: str, expected_signature: str | None) -> dict[str, Any]:
        record = self.latest_completion(stage, signature=expected_signature)
        if record is not None:
            return {
                "stage": stage,
                "status": "valid",
                "seq": record.seq,
                "signature": record.stage_signature,
                "artifact_digest": record.artifact_digest,
                "artifact_path": record.artifact_path,
            }
        any_record = self.latest_completion(stage)
        progress = None
        if expected_signature:
            progress = self.latest_progress(stage, signature=expected_signature)
        if progress is not None:
            return {
                "stage": stage,
                "status": "stopped" if progress.status == "stopped" else "running",
                "seq": progress.seq,
                "signature": progress.stage_signature,
                "artifact_digest": progress.artifact_digest,
                "artifact_path": progress.artifact_path,
            }
        if any_record is not None:
            return {
                "stage": stage,
                "status": "stale",
                "seq": any_record.seq,
                "signature": any_record.stage_signature,
                "artifact_digest": any_record.artifact_digest,
                "artifact_path": any_record.artifact_path,
            }
        return {"stage": stage, "status": "missing", "seq": None, "signature": None}
