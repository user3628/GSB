from __future__ import annotations

import fcntl
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .canonical import atomic_write_json, digest_value, read_json
from .errors import PipelineError


STAGES = ("schedule", "compose", "transition", "encode")


class CheckpointStore:
    def __init__(self, workspace: Path) -> None:
        self.workspace = workspace
        self.directory = workspace / "checkpoints"
        self.lock_path = workspace / ".checkpoint.lock"

    @contextmanager
    def _locked(self) -> Iterator[None]:
        self.workspace.mkdir(parents=True, exist_ok=True)
        with self.lock_path.open("a+b") as lock_handle:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)

    def _record_path(self, stage: str) -> Path:
        if stage not in STAGES:
            raise PipelineError(
                "E_INVALID_STAGE", f"unknown stage {stage}", stage=stage
            )
        return self.directory / f"{stage}.json"

    def read(self, stage: str) -> dict[str, Any] | None:
        path = self._record_path(stage)
        if not path.exists():
            return None
        try:
            record = read_json(path)
        except Exception as exc:
            raise PipelineError(
                "E_CHECKPOINT_CORRUPT",
                f"checkpoint for {stage} is unreadable",
                stage=stage,
                details={"error": str(exc)},
            ) from exc
        if not isinstance(record, dict) or record.get("stage") != stage:
            raise PipelineError(
                "E_CHECKPOINT_CORRUPT",
                f"checkpoint for {stage} has invalid content",
                stage=stage,
            )
        return record

    def read_all(self) -> dict[str, dict[str, Any] | None]:
        return {stage: self.read(stage) for stage in STAGES}

    def publish(
        self,
        *,
        stage: str,
        input_fingerprint: str,
        config_version: str,
        artifact_digest: str,
        artifact_path: str,
        components: dict[str, Any],
        sequence: int | None = None,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self._record_path(stage)
        with self._locked():
            records = self.read_all()
            stage_index = STAGES.index(stage)
            missing_prerequisites = [
                prior
                for prior in STAGES[:stage_index]
                if records[prior] is None
            ]
            if missing_prerequisites:
                raise PipelineError(
                    "E_INVALID_STAGE",
                    f"cannot publish {stage} before {missing_prerequisites[0]}",
                    stage=stage,
                    details={
                        "required_stage": missing_prerequisites[0],
                        "missing_prerequisites": missing_prerequisites,
                    },
                )
            existing = records[stage]
            if (
                existing is not None
                and existing.get("input_fingerprint") == input_fingerprint
                and existing.get("artifact_digest") == artifact_digest
            ):
                raise PipelineError(
                    "E_STALE_CHECKPOINT",
                    f"duplicate checkpoint publication for {stage}",
                    stage=stage,
                    details={"sequence": existing.get("sequence")},
                )
            sequences = [
                int(record["sequence"])
                for record in records.values()
                if record is not None and isinstance(record.get("sequence"), int)
            ]
            max_sequence = max(sequences, default=0)
            if sequence is None:
                sequence = max_sequence + 1
            else:
                if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence <= 0:
                    raise PipelineError(
                        "E_STALE_CHECKPOINT",
                        "sequence must be a positive integer",
                        stage=stage,
                    )
                if existing is not None and sequence <= int(existing.get("sequence", 0)):
                    raise PipelineError(
                        "E_STALE_CHECKPOINT",
                        f"sequence {sequence} is behind the current {stage} checkpoint",
                        stage=stage,
                        details={"current_sequence": existing.get("sequence")},
                    )
                if sequence <= max_sequence:
                    raise PipelineError(
                        "E_STALE_CHECKPOINT",
                        f"sequence {sequence} is behind published sequence {max_sequence}",
                        stage=stage,
                        details={"latest_sequence": max_sequence},
                    )
            record = {
                "stage": stage,
                "sequence": sequence,
                "input_fingerprint": input_fingerprint,
                "config_version": config_version,
                "artifact_digest": artifact_digest,
                "artifact_path": artifact_path,
                "components": components,
            }
            if extra:
                record.update(extra)
            path = self._record_path(stage)
            atomic_write_json(path, record)
            return record

    def repository_digest(self) -> str:
        records = {
            stage: record
            for stage, record in self.read_all().items()
            if record is not None
        }
        return digest_value(records)
