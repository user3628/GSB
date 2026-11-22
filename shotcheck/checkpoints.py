from __future__ import annotations

import fcntl
import json
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .errors import (
    BusinessError,
    E_STALE_CHECKPOINT,
    invalid_stage_error,
    stale_checkpoint_error,
)
from .util import (
    atomic_write_bytes,
    atomic_write_json,
    canonical_bytes,
    ensure_dir,
    read_json,
    sha256_bytes,
)
from .version import SCHEMA_VERSION


STAGES: tuple[tuple[str, int, str], ...] = (
    ("normalize", 1, "stage-01-normalize.json"),
    ("duration", 2, "stage-02-duration.json"),
    ("coverage", 3, "stage-03-coverage.json"),
    ("strategy", 4, "stage-04-strategy.json"),
)
STAGE_BY_NAME = {name: (index, filename) for name, index, filename in STAGES}
STAGE_BY_INDEX = {index: name for name, index, _ in STAGES}


class CheckpointStore:
    """Atomic checkpoint store guarded by a real POSIX file lock."""

    def __init__(self, workspace: str | Path):
        self.workspace = Path(workspace)
        self.checkpoints_dir = ensure_dir(self.workspace / "checkpoints")
        self.objects_dir = ensure_dir(self.workspace / "objects")
        self.locks_dir = ensure_dir(self.workspace / "locks")
        self.lock_path = self.locks_dir / "checkpoints.lock"

    @contextmanager
    def _locked(self, exclusive: bool) -> Iterator[None]:
        ensure_dir(self.locks_dir)
        fd = os.open(str(self.lock_path), os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
            yield
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)

    def validate_stage_name(self, stage: str) -> tuple[int, str]:
        if stage not in STAGE_BY_NAME:
            raise invalid_stage_error(stage, reason="unknown_stage")
        return STAGE_BY_NAME[stage]

    def _checkpoint_path(self, stage: str) -> Path:
        _, filename = self.validate_stage_name(stage)
        return self.checkpoints_dir / filename

    def _read_checkpoint_unlocked(self, stage: str) -> dict[str, Any] | None:
        path = self._checkpoint_path(stage)
        if not path.exists():
            return None
        try:
            value = read_json(path)
        except (json.JSONDecodeError, UnicodeDecodeError, OSError) as exc:
            raise BusinessError(
                E_STALE_CHECKPOINT,
                "stored checkpoint is unreadable; refusing to overwrite it",
                stage=stage,
                details={"reason": "checkpoint_corrupt", "path": path.name},
            ) from exc
        if not isinstance(value, dict):
            raise BusinessError(
                E_STALE_CHECKPOINT,
                "stored checkpoint is not an object; refusing to overwrite it",
                stage=stage,
                details={"reason": "checkpoint_corrupt", "path": path.name},
            )
        if value.get("stage") != stage:
            raise BusinessError(
                E_STALE_CHECKPOINT,
                "checkpoint stage name does not match its storage slot",
                stage=stage,
                details={
                    "reason": "cross_stage_mismatch",
                    "stored_stage": value.get("stage"),
                },
            )
        return value

    def read_checkpoint(self, stage: str) -> dict[str, Any] | None:
        self.validate_stage_name(stage)
        with self._locked(exclusive=False):
            return self._read_checkpoint_unlocked(stage)

    def read_all(self) -> dict[str, dict[str, Any] | None]:
        return {name: self.read_checkpoint(name) for name, _, _ in STAGES}

    def upstream_descriptor(self, stage: str) -> dict[str, Any]:
        index, _ = self.validate_stage_name(stage)
        if index == 1:
            return {}
        previous_stage = STAGE_BY_INDEX[index - 1]
        current = self.read_checkpoint(previous_stage)
        if current is None:
            raise invalid_stage_error(
                stage,
                reason="missing_prerequisite",
                prerequisite_stage=previous_stage,
            )
        return {
            "stage": previous_stage,
            "sequence": current["sequence"],
            "input_fingerprint": current["input_fingerprint"],
            "artifact_digest": current["artifact_digest"],
        }

    def _artifact_path(self, stage: str, digest: str) -> Path:
        index, _ = self.validate_stage_name(stage)
        return self.objects_dir / f"{index:02d}-{stage}-{digest}.json"

    def verify_artifact(self, checkpoint: dict[str, Any]) -> dict[str, Any]:
        stage = checkpoint["stage"]
        digest = checkpoint["artifact_digest"]
        path = self._artifact_path(stage, digest)
        if not path.exists():
            raise BusinessError(
                E_STALE_CHECKPOINT,
                "checkpoint artifact is missing",
                stage=stage,
                details={
                    "reason": "artifact_missing",
                    "artifact_digest": digest,
                    "sequence": checkpoint.get("sequence"),
                },
            )
        data = path.read_bytes()
        actual_digest = sha256_bytes(data)
        if actual_digest != digest:
            raise BusinessError(
                E_STALE_CHECKPOINT,
                "checkpoint artifact digest does not match",
                stage=stage,
                details={
                    "reason": "artifact_digest_mismatch",
                    "expected_digest": digest,
                    "actual_digest": actual_digest,
                    "sequence": checkpoint.get("sequence"),
                },
            )
        try:
            value = json.loads(data.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise BusinessError(
                E_STALE_CHECKPOINT,
                "checkpoint artifact is unreadable",
                stage=stage,
                details={"reason": "artifact_corrupt", "artifact_digest": digest},
            ) from exc
        return value

    def load_artifact(self, stage: str) -> dict[str, Any] | None:
        checkpoint = self.read_checkpoint(stage)
        if checkpoint is None:
            return None
        return self.verify_artifact(checkpoint)

    def _normalise_upstream(self, value: dict[str, Any] | None) -> dict[str, Any] | None:
        if value is None:
            return None
        required = {"stage", "sequence", "input_fingerprint", "artifact_digest"}
        if not required.issubset(value):
            raise invalid_stage_error(
                value.get("stage", "unknown"),
                reason="malformed_upstream_descriptor",
                missing=sorted(required - set(value)),
            )
        return {key: value[key] for key in required}

    def publish(
        self,
        stage: str,
        artifact: dict[str, Any],
        *,
        input_fingerprint: str,
        config_version: str,
        config_fingerprint: str,
        expected_prev_sequence: int,
        upstream: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Publish a complete stage artifact and checkpoint under one file lock."""
        index, _ = self.validate_stage_name(stage)
        if expected_prev_sequence < 0:
            raise invalid_stage_error(stage, reason="negative_expected_sequence")
        upstream = self._normalise_upstream(upstream)

        artifact_bytes = canonical_bytes(artifact)
        artifact_digest = sha256_bytes(artifact_bytes)
        artifact_path = self._artifact_path(stage, artifact_digest)
        if artifact_path.exists():
            if sha256_bytes(artifact_path.read_bytes()) != artifact_digest:
                raise BusinessError(
                    E_STALE_CHECKPOINT,
                    "content-addressed artifact path contains different bytes",
                    stage=stage,
                    details={"reason": "artifact_collision", "artifact_digest": artifact_digest},
                )
        else:
            atomic_write_bytes(artifact_path, artifact_bytes)

        with self._locked(exclusive=True):
            current = self._read_checkpoint_unlocked(stage)
            expected_sequence = expected_prev_sequence + 1
            if current is None:
                if expected_prev_sequence != 0:
                    raise stale_checkpoint_error(
                        stage,
                        reason="expected_predecessor_missing",
                        expected=expected_sequence,
                        current=0,
                    )
            else:
                if current["sequence"] != expected_prev_sequence:
                    raise stale_checkpoint_error(
                        stage,
                        reason="sequence_not_contiguous",
                        expected=expected_sequence,
                        current=int(current["sequence"]),
                    )

            expected_upstream: list[dict[str, Any]] = []
            if index > 1:
                previous_stage = STAGE_BY_INDEX[index - 1]
                previous_checkpoint = self._read_checkpoint_unlocked(previous_stage)
                if previous_checkpoint is None:
                    raise invalid_stage_error(
                        stage,
                        reason="missing_prerequisite",
                        prerequisite_stage=previous_stage,
                    )
                current_upstream = {
                    "stage": previous_stage,
                    "sequence": previous_checkpoint["sequence"],
                    "input_fingerprint": previous_checkpoint["input_fingerprint"],
                    "artifact_digest": previous_checkpoint["artifact_digest"],
                }
                if upstream is None:
                    upstream = current_upstream
                elif upstream != current_upstream:
                    raise stale_checkpoint_error(
                        stage,
                        reason="upstream_lineage_mismatch",
                        expected=expected_sequence,
                        current=int(current["sequence"]) if current else 0,
                        supplied_upstream=upstream,
                        current_upstream=current_upstream,
                    )
                expected_upstream = [current_upstream]
            elif upstream is not None:
                raise invalid_stage_error(
                    stage,
                    reason="first_stage_cannot_have_upstream",
                )

            if current is not None and (
                current.get("input_fingerprint") == input_fingerprint
                and current.get("config_fingerprint") == config_fingerprint
                and current.get("artifact_digest") == artifact_digest
                and current.get("upstream") == expected_upstream
            ):
                raise stale_checkpoint_error(
                    stage,
                    reason="duplicate_publish",
                    expected=expected_sequence,
                    current=int(current["sequence"]),
                )

            record = {
                "schema_version": SCHEMA_VERSION,
                "stage": stage,
                "stage_index": index,
                "sequence": expected_sequence,
                "input_fingerprint": input_fingerprint,
                "config_version": config_version,
                "config_fingerprint": config_fingerprint,
                "artifact_digest": artifact_digest,
                "artifact_ref": artifact_path.name,
                "upstream": expected_upstream,
            }
            atomic_write_json(self._checkpoint_path(stage), record)
            return record

    def status(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for stage, index, _ in STAGES:
            checkpoint = self.read_checkpoint(stage)
            result[stage] = {
                "stage_index": index,
                "present": checkpoint is not None,
                "sequence": checkpoint.get("sequence") if checkpoint else None,
                "artifact_digest": checkpoint.get("artifact_digest") if checkpoint else None,
                "config_version": checkpoint.get("config_version") if checkpoint else None,
            }
        return result
