"""Atomic stage checkpoints, revision fencing, and artifact verification."""

from __future__ import annotations

import fcntl
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .canonical import (
    atomic_write_bytes,
    atomic_write_json,
    canonical_json_bytes,
    digest_object,
    file_sha256,
    read_json,
    sha256_bytes,
)
from .errors import WorkbenchError


STAGE_ORDER = ("bandwidth", "density", "metrics", "drift")
STAGE_PREREQUISITES: dict[str, tuple[str, ...]] = {
    "bandwidth": (),
    "density": ("bandwidth",),
    "metrics": ("density",),
    "drift": ("metrics",),
}


class CheckpointStore:
    def __init__(self, workdir: str | Path) -> None:
        self.workdir = Path(workdir)
        self.checkpoints_dir = self.workdir / "checkpoints"
        self.artifacts_dir = self.workdir / "artifacts"
        self.lock_path = self.workdir / ".publish.lock"
        self.checkpoints_dir.mkdir(parents=True, exist_ok=True)
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)
        self.workdir.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def _exclusive_lock(self) -> Iterator[None]:
        with open(self.lock_path, "a+b") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def _record_path(self, stage: str) -> Path:
        return self.checkpoints_dir / f"{stage}.json"

    def read_record(self, stage: str) -> dict[str, Any] | None:
        path = self._record_path(stage)
        if not path.exists():
            return None
        try:
            record = read_json(path)
        except (OSError, ValueError) as exc:
            raise WorkbenchError(
                "E_INVALID_STAGE",
                "checkpoint record is unreadable",
                stage=stage,
                location=str(path),
                details={"reason": str(exc)},
            ) from exc
        if record.get("stage") != stage:
            raise WorkbenchError(
                "E_INVALID_STAGE",
                "checkpoint stage identity does not match its filename",
                stage=stage,
                location=str(path),
                details={"recorded_stage": record.get("stage")},
            )
        return record

    def records(self) -> dict[str, dict[str, Any] | None]:
        return {stage: self.read_record(stage) for stage in STAGE_ORDER}

    def load_artifact(self, record: dict[str, Any]) -> dict[str, Any]:
        artifact = record.get("artifact", {})
        relative = artifact.get("path")
        if not relative:
            raise WorkbenchError(
                "E_INVALID_STAGE",
                "checkpoint has no artifact pointer",
                stage=record.get("stage"),
                location="checkpoint.artifact",
            )
        path = self.workdir / relative
        if not path.exists():
            raise WorkbenchError(
                "E_INVALID_STAGE",
                "checkpoint artifact is missing",
                stage=record.get("stage"),
                location=str(path),
            )
        digest = file_sha256(path)
        if digest != artifact.get("sha256"):
            raise WorkbenchError(
                "E_INVALID_STAGE",
                "checkpoint artifact digest does not match its record",
                stage=record.get("stage"),
                location=str(path),
                details={"expected": artifact.get("sha256"), "actual": digest},
            )
        try:
            payload = read_json(path)
        except (OSError, ValueError) as exc:
            raise WorkbenchError(
                "E_INVALID_STAGE",
                "checkpoint artifact is unreadable",
                stage=record.get("stage"),
                location=str(path),
                details={"reason": str(exc)},
            ) from exc
        if digest_object(payload) != record.get("result_fingerprint"):
            raise WorkbenchError(
                "E_INVALID_STAGE",
                "checkpoint artifact content fingerprint is inconsistent",
                stage=record.get("stage"),
                location=str(path),
            )
        return payload

    def _latest_sequence_unlocked(self) -> int:
        sequences: list[int] = []
        for stage in STAGE_ORDER:
            record = self.read_record(stage)
            if record is not None:
                sequences.append(int(record.get("sequence", 0)))
        return max(sequences, default=0)

    def latest_sequence(self) -> int:
        with self._exclusive_lock():
            return self._latest_sequence_unlocked()

    def publish(
        self,
        *,
        stage: str,
        payload: dict[str, Any],
        expected_previous_revision: int,
        config_version: str,
        stage_input_fingerprint: str,
        dataset_fingerprint: str,
        stage_config: dict[str, Any],
        upstream: dict[str, str],
        accumulator: dict[str, Any],
    ) -> dict[str, Any]:
        """Atomically publish a stage result under a revision fence."""

        if stage not in STAGE_ORDER:
            raise WorkbenchError("E_INVALID_STAGE", f"unknown stage: {stage}", stage=stage)
        with self._exclusive_lock():
            current = self.read_record(stage)
            current_revision = int(current.get("stage_revision", 0)) if current else 0
            if expected_previous_revision != current_revision:
                raise WorkbenchError(
                    "E_STALE_CHECKPOINT",
                    "stage revision fence rejected a stale or duplicate publication",
                    stage=stage,
                    location="checkpoint.stage_revision",
                    details={
                        "expected_previous_revision": expected_previous_revision,
                        "current_revision": current_revision,
                    },
                )
            for prerequisite in STAGE_PREREQUISITES[stage]:
                expected_fingerprint = upstream.get(prerequisite)
                if not expected_fingerprint:
                    raise WorkbenchError(
                        "E_INVALID_STAGE",
                        "publisher did not identify a required upstream stage",
                        stage=stage,
                        location=f"checkpoint.prerequisite.{prerequisite}",
                    )
                prerequisite_record = self.read_record(prerequisite)
                if prerequisite_record is None:
                    raise WorkbenchError(
                        "E_INVALID_STAGE",
                        "required prerequisite stage artifact is missing",
                        stage=stage,
                        location=f"checkpoint.prerequisite.{prerequisite}",
                    )
                if prerequisite_record.get("result_fingerprint") != expected_fingerprint:
                    raise WorkbenchError(
                        "E_INVALID_STAGE",
                        "required prerequisite stage fingerprint does not match",
                        stage=stage,
                        location=f"checkpoint.prerequisite.{prerequisite}",
                        details={
                            "expected": expected_fingerprint,
                            "actual": prerequisite_record.get("result_fingerprint"),
                        },
                    )

            sequence = self._latest_sequence_unlocked() + 1
            revision = current_revision + 1
            artifact_name = f"{stage}-r{revision:06d}-s{sequence:06d}.json"
            artifact_relative = f"artifacts/{artifact_name}"
            artifact_path = self.workdir / artifact_relative
            artifact_bytes = canonical_json_bytes(payload, strict=True)
            atomic_write_bytes(artifact_path, artifact_bytes)
            result_fingerprint = digest_object(payload)
            artifact_sha = sha256_bytes(artifact_bytes)
            record = {
                "schema_version": 1,
                "dataset_fingerprint": dataset_fingerprint,
                "stage": stage,
                "sequence": sequence,
                "stage_revision": revision,
                "config_version": config_version,
                "stage_input_fingerprint": stage_input_fingerprint,
                "result_fingerprint": result_fingerprint,
                "stage_config": stage_config,
                "upstream": {
                    name: {
                        "result_fingerprint": fingerprint,
                        "sequence": (
                            int(self.read_record(name).get("sequence", 0))
                            if self.read_record(name) is not None
                            else 0
                        ),
                    }
                    for name, fingerprint in upstream.items()
                },
                "accumulator": accumulator,
                "artifact": {
                    "path": artifact_relative,
                    "sha256": artifact_sha,
                    "bytes": len(artifact_bytes),
                },
            }
            atomic_write_json(self._record_path(stage), record, strict=False)
            return record

    def public_record(self, record: dict[str, Any] | None) -> dict[str, Any] | None:
        if record is None:
            return None
        return {
            "stage": record.get("stage"),
            "sequence": record.get("sequence"),
            "stage_revision": record.get("stage_revision"),
            "config_version": record.get("config_version"),
            "stage_input_fingerprint": record.get("stage_input_fingerprint"),
            "result_fingerprint": record.get("result_fingerprint"),
            "dataset_fingerprint": record.get("dataset_fingerprint"),
            "upstream": record.get("upstream", {}),
            "accumulator": record.get("accumulator", {}),
            "artifact": record.get("artifact", {}),
        }
