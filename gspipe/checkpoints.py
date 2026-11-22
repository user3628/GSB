"""Append-only stage checkpoints with OS file locking and sequence checks."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .artifacts import ArtifactStore
from .canonical import sha256_bytes
from .errors import (
    E_ARTIFACT_CORRUPT,
    E_INVALID_STAGE,
    E_STALE_CHECKPOINT,
    PipelineError,
    invalid_stage,
    stale_checkpoint,
)
from .fsutil import atomic_write_json, safe_relative_path
from .locking import FileLock
from .stages import STAGE_INDEX, STAGES, validate_stage


CHECKPOINT_SCHEMA = "gspipe-checkpoint-v1"


@dataclass(frozen=True)
class CheckpointState:
    task_id: str
    records: tuple[dict[str, Any], ...]
    latest_by_stage: dict[str, dict[str, Any]]
    last_seq: int
    generation: int
    valid: dict[str, bool]

    def latest(self, stage: str) -> dict[str, Any] | None:
        return self.latest_by_stage.get(stage)

    def is_valid(self, stage: str) -> bool:
        return bool(self.valid.get(stage, False))

    def current_artifact_digest(self, stage: str) -> str | None:
        record = self.latest(stage)
        if record is None or not self.is_valid(stage):
            return None
        return record.get("artifact_digest")


class CheckpointStore:
    def __init__(self, workspace_root: str | Path, artifacts: ArtifactStore) -> None:
        self.workspace_root = Path(workspace_root)
        self.root = self.workspace_root / ".gspipe" / "checkpoints"
        self.lock_path = self.workspace_root / ".gspipe" / "state" / "locks" / "checkpoints.lock"
        self.artifacts = artifacts

    def records_dir(self, task_id: str) -> Path:
        return self.root / task_id / "records"

    def load_records(self, task_id: str) -> list[dict[str, Any]]:
        directory = self.records_dir(task_id)
        if not directory.exists():
            return []
        records: list[dict[str, Any]] = []
        for path in sorted(directory.glob("*.json")):
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise PipelineError(
                    E_ARTIFACT_CORRUPT,
                    f"Corrupt checkpoint record {path}: {exc}",
                    task_id=task_id,
                    details={"path": str(path)},
                ) from exc
            if not isinstance(data, dict) or data.get("schema") != CHECKPOINT_SCHEMA:
                raise PipelineError(
                    E_ARTIFACT_CORRUPT,
                    f"Invalid checkpoint schema in {path}",
                    task_id=task_id,
                    details={"path": str(path)},
                )
            records.append(data)
        records.sort(key=lambda item: int(item.get("seq", 0)))
        return records

    def _artifact_verified(self, record: Mapping[str, Any]) -> bool:
        digest = record.get("artifact_digest")
        kind = record.get("artifact_type")
        relative = record.get("artifact_path")
        if not isinstance(digest, str) or kind not in {"json", "png"} or not isinstance(relative, str):
            return False
        try:
            path = safe_relative_path(self.workspace_root, relative)
            data = path.read_bytes()
        except (OSError, PipelineError):
            return False
        return sha256_bytes(data) == digest

    def state(self, task_id: str) -> CheckpointState:
        records = self.load_records(task_id)
        latest_by_stage: dict[str, dict[str, Any]] = {}
        for record in records:
            stage = record.get("stage")
            if stage in STAGES:
                latest_by_stage[stage] = record
        last_seq = max((int(record.get("seq", 0)) for record in records), default=0)
        generation = max((int(record.get("generation", 0)) for record in records), default=0)
        valid: dict[str, bool] = {}
        for stage in STAGES:
            record = latest_by_stage.get(stage)
            if record is None or not self._artifact_verified(record):
                valid[stage] = False
                continue
            index = STAGE_INDEX[stage]
            if index == 0:
                valid[stage] = True
                continue
            prior_stage = STAGES[index - 1]
            prior = latest_by_stage.get(prior_stage)
            if prior is None or not valid.get(prior_stage, False):
                valid[stage] = False
                continue
            dependencies = record.get("dependencies")
            expected = {prior_stage: prior.get("artifact_digest")}
            valid[stage] = dependencies == expected
        return CheckpointState(
            task_id=task_id,
            records=tuple(records),
            latest_by_stage=latest_by_stage,
            last_seq=last_seq,
            generation=generation,
            valid=valid,
        )

    def next_sequence(self, task_id: str) -> int:
        with FileLock(self.lock_path):
            return self.state(task_id).last_seq + 1

    def _validate_record_shape(self, record: Mapping[str, Any], expected_task_id: str) -> str:
        if not isinstance(record, Mapping):
            raise stale_checkpoint(None, None, "invalid_record", "Checkpoint record must be an object")
        if record.get("task_id") != expected_task_id:
            raise stale_checkpoint(
                record.get("task_id") if isinstance(record.get("task_id"), str) else None,
                record.get("stage") if isinstance(record.get("stage"), str) else None,
                "cross_task_publish",
                "Checkpoint record task_id does not match its target task",
                expected_task_id=expected_task_id,
                actual_task_id=record.get("task_id"),
            )
        if record.get("schema") != CHECKPOINT_SCHEMA:
            raise stale_checkpoint(expected_task_id, None, "invalid_schema", "Unsupported checkpoint schema")
        stage = record.get("stage")
        if not isinstance(stage, str) or stage not in STAGES:
            raise invalid_stage(
                stage if isinstance(stage, str) else None,
                "unknown_stage",
                f"Unknown checkpoint stage {stage!r}",
                task_id=expected_task_id,
            )
        for key in ("seq", "generation"):
            value = record.get(key)
            if isinstance(value, bool) or not isinstance(value, int):
                raise stale_checkpoint(expected_task_id, stage, "invalid_sequence", f"{key} must be an integer")
        if record["seq"] < 1 or record["generation"] < 0:
            raise stale_checkpoint(expected_task_id, stage, "invalid_sequence", "seq/generation is out of range")
        for key in ("input_fingerprint", "config_version", "artifact_digest"):
            if not isinstance(record.get(key), str) or not record.get(key):
                raise stale_checkpoint(expected_task_id, stage, "invalid_record", f"{key} must be a non-empty string")
        if record.get("artifact_type") not in {"json", "png"}:
            raise stale_checkpoint(expected_task_id, stage, "invalid_record", "artifact_type must be json or png")
        if not isinstance(record.get("artifact_path"), str):
            raise stale_checkpoint(expected_task_id, stage, "invalid_record", "artifact_path must be a string")
        if not isinstance(record.get("fingerprint_components"), Mapping):
            raise stale_checkpoint(expected_task_id, stage, "invalid_record", "fingerprint_components must be an object")
        dependencies = record.get("dependencies")
        if not isinstance(dependencies, Mapping):
            raise stale_checkpoint(expected_task_id, stage, "invalid_record", "dependencies must be an object")
        return stage

    def _validate_dependency(
        self,
        task_id: str,
        stage: str,
        record: Mapping[str, Any],
        latest_by_stage: Mapping[str, Mapping[str, Any]],
    ) -> None:
        index = STAGE_INDEX[stage]
        dependencies = dict(record.get("dependencies") or {})
        if index == 0:
            if dependencies:
                raise invalid_stage(
                    stage,
                    "unexpected_dependency",
                    "parse stage must not depend on a predecessor artifact",
                    task_id=task_id,
                )
            return
        prior_stage = STAGES[index - 1]
        prior = latest_by_stage.get(prior_stage)
        if prior is None:
            raise invalid_stage(
                stage,
                "missing_predecessor",
                f"Stage {stage} requires a completed {prior_stage} checkpoint",
                task_id=task_id,
                predecessor=prior_stage,
            )
        if not self._artifact_verified(prior):
            raise invalid_stage(
                stage,
                "missing_predecessor_artifact",
                f"Stage {stage} predecessor {prior_stage} artifact is missing or corrupt",
                task_id=task_id,
                predecessor=prior_stage,
                predecessor_artifact=prior.get("artifact_digest"),
            )
        expected = {prior_stage: prior.get("artifact_digest")}
        if dependencies != expected:
            raise invalid_stage(
                stage,
                "predecessor_fingerprint_mismatch",
                f"Stage {stage} does not depend on the current {prior_stage} artifact",
                task_id=task_id,
                expected_dependencies=expected,
                actual_dependencies=dependencies,
            )

    def publish(self, expected_task_id: str, record: Mapping[str, Any]) -> dict[str, Any]:
        """Validate and atomically append one checkpoint under an OS file lock."""

        stage = self._validate_record_shape(record, expected_task_id)
        if not self._artifact_verified(record):
            raise stale_checkpoint(
                expected_task_id,
                stage,
                "artifact_missing_or_corrupt",
                "Checkpoint artifact is missing or does not match its digest",
                artifact_digest=record.get("artifact_digest"),
            )
        with FileLock(self.lock_path):
            state = self.state(expected_task_id)
            seq = int(record["seq"])
            generation = int(record["generation"])
            if seq <= state.last_seq:
                raise stale_checkpoint(
                    expected_task_id,
                    stage,
                    "stale_sequence",
                    "Checkpoint sequence has already advanced",
                    actual_seq=seq,
                    last_seq=state.last_seq,
                )
            expected_seq = state.last_seq + 1
            if seq != expected_seq:
                raise stale_checkpoint(
                    expected_task_id,
                    stage,
                    "sequence_gap",
                    "Checkpoint sequence must advance by exactly one",
                    actual_seq=seq,
                    expected_seq=expected_seq,
                )

            if not state.records:
                if generation != 0 or stage != "parse":
                    raise invalid_stage(
                        stage,
                        "invalid_initial_stage",
                        "A task must begin with parse in generation 0",
                        task_id=expected_task_id,
                    )
            else:
                if generation < state.generation:
                    raise stale_checkpoint(
                        expected_task_id,
                        stage,
                        "stale_generation",
                        "Checkpoint generation is older than the task generation",
                        actual_generation=generation,
                        current_generation=state.generation,
                    )
                if generation > state.generation + 1:
                    raise stale_checkpoint(
                        expected_task_id,
                        stage,
                        "generation_gap",
                        "Checkpoint generation may advance by at most one",
                        actual_generation=generation,
                        current_generation=state.generation,
                    )
                latest_record = max(state.records, key=lambda item: int(item["seq"]))
                if generation == state.generation and stage == latest_record.get("stage"):
                    raise stale_checkpoint(
                        expected_task_id,
                        stage,
                        "duplicate_stage",
                        f"Stage {stage} was already published in the current generation",
                        last_seq=state.last_seq,
                    )
                if generation == state.generation:
                    latest_index = STAGE_INDEX[latest_record["stage"]]
                    expected_stage = STAGES[latest_index + 1] if latest_index + 1 < len(STAGES) else None
                    if expected_stage is None or stage != expected_stage:
                        raise invalid_stage(
                            stage,
                            "stage_order",
                            f"Next stage must be {expected_stage or 'a new generation'}; got {stage}",
                            task_id=expected_task_id,
                            expected_stage=expected_stage,
                        )

            self._validate_dependency(expected_task_id, stage, record, state.latest_by_stage)
            directory = self.records_dir(expected_task_id)
            filename = f"{seq:06d}-{stage}.json"
            target = directory / filename
            if target.exists():
                raise stale_checkpoint(
                    expected_task_id,
                    stage,
                    "record_already_exists",
                    "Checkpoint record file already exists",
                    path=str(target),
                )
            payload = dict(record)
            atomic_write_json(target, payload)
            return payload


def build_checkpoint_record(
    *,
    task_id: str,
    stage: str,
    seq: int,
    generation: int,
    input_fingerprint: str,
    config_version: str,
    fingerprint_components: Mapping[str, Any],
    artifact_digest: str,
    artifact_type: str,
    artifact_path: str,
    dependencies: Mapping[str, str],
) -> dict[str, Any]:
    validate_stage(stage)
    return {
        "schema": CHECKPOINT_SCHEMA,
        "task_id": task_id,
        "stage": stage,
        "seq": int(seq),
        "generation": int(generation),
        "input_fingerprint": input_fingerprint,
        "config_version": config_version,
        "fingerprint_components": dict(fingerprint_components),
        "dependencies": dict(dependencies),
        "artifact_digest": artifact_digest,
        "artifact_type": artifact_type,
        "artifact_path": artifact_path,
    }
