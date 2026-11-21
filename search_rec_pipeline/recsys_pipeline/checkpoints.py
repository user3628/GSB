"""Atomic per-stage checkpoints with real OS locks and contiguous sequence checks."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

from .config import STAGE_INDEX, STAGES
from .errors import E_INVALID_STAGE, E_STALE_CHECKPOINT, PipelineError
from .util import (
    canonical_json,
    hash_json,
    locked_file,
    sha256_bytes,
    sha256_file,
    write_json_atomic,
)


CHECKPOINT_SCHEMA_VERSION = 1


class CheckpointStore:
    def __init__(self, workdir: Path) -> None:
        self.workdir = Path(workdir).resolve()
        self.checkpoint_root = self.workdir / "checkpoints"
        self.lock_root = self.workdir / "locks"
        self.artifact_root = self.workdir / "scales"

    def scale_dir(self, scale: str) -> Path:
        return self.checkpoint_root / scale

    def checkpoint_lock_path(self, scale: str) -> Path:
        return self.lock_root / f"{scale}.checkpoint.lock"

    def run_lock_path(self, scale: str) -> Path:
        return self.lock_root / f"{scale}.run.lock"

    def stage_artifact_dir(self, scale: str, stage: str) -> Path:
        return self.artifact_root / scale / f"{STAGE_INDEX[stage] + 1:02d}_{stage}"

    def checkpoint_path(self, scale: str, seq: int, stage: str) -> Path:
        return self.scale_dir(scale) / f"{seq:04d}_{stage}.json"

    def _load_checkpoint_file(self, path: Path) -> tuple[dict[str, Any] | None, str | None]:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            return None, f"unreadable_json:{exc}"
        if not isinstance(data, dict):
            return None, "record_not_object"
        checksum = data.get("record_checksum")
        if not isinstance(checksum, str):
            return None, "missing_record_checksum"
        body = dict(data)
        body.pop("record_checksum", None)
        if hash_json(body) != checksum:
            return None, "record_checksum_mismatch"
        required = (
            "schema_version",
            "stage",
            "scale",
            "seq",
            "input_fingerprint",
            "config_version",
            "stage_config_fingerprint",
            "stage_fingerprint",
            "artifact_digest",
            "artifacts",
        )
        missing = [key for key in required if key not in data]
        if missing:
            return None, f"missing_fields:{','.join(missing)}"
        if data["stage"] not in STAGE_INDEX:
            return None, "unknown_stage"
        if not isinstance(data["seq"], int) or data["seq"] <= 0:
            return None, "invalid_seq"
        if not isinstance(data["artifacts"], list):
            return None, "invalid_artifacts"
        return data, None

    def scan(self, scale: str) -> dict[str, Any]:
        """Return only the contiguous, checksum-valid stage records."""

        directory = self.scale_dir(scale)
        files = sorted(directory.glob("*.json")) if directory.exists() else []
        parsed: dict[int, tuple[Path, dict[str, Any]]] = {}
        invalid: list[dict[str, str]] = []
        for path in files:
            record, error = self._load_checkpoint_file(path)
            if record is None:
                invalid.append({"path": self._display_path(path), "reason": str(error)})
                continue
            if record["scale"] != scale:
                invalid.append({"path": self._display_path(path), "reason": "cross_scale_record"})
                continue
            seq = int(record["seq"])
            if seq in parsed:
                previous_path, previous = parsed[seq]
                invalid.append(
                    {"path": self._display_path(previous_path), "reason": f"duplicate_seq:{seq}"}
                )
                invalid.append(
                    {"path": self._display_path(path), "reason": f"duplicate_seq:{seq}"}
                )
                parsed.pop(seq, None)
                continue
            parsed[seq] = (path, record)

        contiguous: list[dict[str, Any]] = []
        for expected_seq, expected_stage in enumerate(STAGES, start=1):
            item = parsed.get(expected_seq)
            if item is None:
                break
            path, record = item
            if record["stage"] != expected_stage:
                invalid.append(
                    {
                        "path": self._display_path(path),
                        "reason": f"stage_order_mismatch:expected={expected_stage}:actual={record['stage']}",
                    }
                )
                break
            contiguous.append(record)

        contiguous_paths = {
            self.checkpoint_path(scale, record["seq"], record["stage"]) for record in contiguous
        }
        for path in files:
            if path in contiguous_paths:
                continue
            if not any(item.get("path") == self._display_path(path) for item in invalid):
                invalid.append(
                    {"path": self._display_path(path), "reason": "non_contiguous_or_orphaned_record"}
                )
        return {
            "records": contiguous,
            "invalid_files": invalid,
            "current_sequence": len(contiguous),
            "next_stage": STAGES[len(contiguous)] if len(contiguous) < len(STAGES) else None,
        }

    def _display_path(self, path: Path) -> str:
        try:
            return path.resolve().relative_to(self.workdir).as_posix()
        except ValueError:
            return str(path)

    def verify_artifacts(self, record: dict[str, Any]) -> list[dict[str, Any]]:
        errors: list[dict[str, Any]] = []
        artifact_entries: list[dict[str, Any]] = []
        for artifact in record.get("artifacts", []):
            if not isinstance(artifact, dict) or not isinstance(artifact.get("path"), str):
                errors.append({"reason": "invalid_artifact_entry", "artifact": artifact})
                continue
            path = self.workdir / artifact["path"]
            if not path.is_file():
                errors.append({"reason": "artifact_missing", "path": artifact["path"]})
                continue
            actual_digest = sha256_file(path)
            actual_size = path.stat().st_size
            artifact_entries.append(
                {"path": artifact["path"], "sha256": actual_digest, "size": actual_size}
            )
            if actual_digest != artifact.get("sha256"):
                errors.append(
                    {
                        "reason": "artifact_digest_mismatch",
                        "path": artifact["path"],
                        "expected": artifact.get("sha256"),
                        "actual": actual_digest,
                    }
                )
            if "size" in artifact and actual_size != artifact["size"]:
                errors.append(
                    {
                        "reason": "artifact_size_mismatch",
                        "path": artifact["path"],
                        "expected": artifact["size"],
                        "actual": actual_size,
                    }
                )
        expected_aggregate = hash_json(
            [
                {
                    "path": item["path"],
                    "sha256": item["sha256"],
                    "size": item["size"],
                }
                for item in artifact_entries
            ]
        )
        if not errors and expected_aggregate != record.get("artifact_digest"):
            errors.append(
                {
                    "reason": "artifact_digest_aggregate_mismatch",
                    "expected": record.get("artifact_digest"),
                    "actual": expected_aggregate,
                }
            )
        return errors

    def artifact_entries(self, paths: list[Path]) -> list[dict[str, Any]]:
        entries: list[dict[str, Any]] = []
        for path in paths:
            path = Path(path)
            if not path.is_file():
                raise PipelineError(
                    E_INVALID_STAGE,
                    "cannot publish a checkpoint with a missing artifact",
                    path=str(path),
                )
            try:
                relative = path.resolve().relative_to(self.workdir).as_posix()
            except ValueError as exc:
                raise PipelineError(
                    E_INVALID_STAGE,
                    "checkpoint artifacts must live under the configured workdir",
                    path=str(path),
                    workdir=str(self.workdir),
                ) from exc
            entries.append(
                {
                    "path": relative,
                    "sha256": sha256_file(path),
                    "size": path.stat().st_size,
                }
            )
        entries.sort(key=lambda item: item["path"])
        return entries

    def publish(
        self,
        scale: str,
        stage: str,
        *,
        expected_seq: int | None,
        input_fingerprint: str,
        config_version: str,
        stage_config_fingerprint: str,
        stage_fingerprint: str,
        artifact_paths: list[Path],
        record_scale: str | None = None,
    ) -> dict[str, Any]:
        if record_scale is not None and record_scale != scale:
            raise PipelineError(
                E_STALE_CHECKPOINT,
                "cross-scale checkpoint publication is rejected",
                lock_scale=scale,
                record_scale=record_scale,
            )
        if stage not in STAGE_INDEX:
            raise PipelineError(E_INVALID_STAGE, "unknown stage", stage=stage, scale=scale)
        artifact_entries = self.artifact_entries(artifact_paths)
        artifact_digest = hash_json(artifact_entries)
        with locked_file(self.checkpoint_lock_path(scale)):
            state = self.scan(scale)
            current = int(state["current_sequence"])
            next_sequence = current + 1
            if expected_seq is not None and expected_seq != next_sequence:
                raise PipelineError(
                    E_STALE_CHECKPOINT,
                    "checkpoint expected_seq is not the next contiguous sequence",
                    scale=scale,
                    stage=stage,
                    current_sequence=current,
                    expected_seq=expected_seq,
                    next_sequence=next_sequence,
                )
            if current >= len(STAGES):
                raise PipelineError(
                    E_STALE_CHECKPOINT,
                    "all stages are already published; duplicate publication is rejected",
                    scale=scale,
                    stage=stage,
                    current_sequence=current,
                )
            expected_stage = STAGES[current]
            if stage != expected_stage:
                requested_index = STAGE_INDEX[stage]
                if requested_index <= current:
                    raise PipelineError(
                        E_STALE_CHECKPOINT,
                        "stage is behind or duplicates an already published stage",
                        scale=scale,
                        stage=stage,
                        expected_stage=expected_stage,
                        current_sequence=current,
                    )
                raise PipelineError(
                    E_INVALID_STAGE,
                    "illegal stage jump; the prerequisite stage is missing",
                    scale=scale,
                    stage=stage,
                    expected_stage=expected_stage,
                    current_sequence=current,
                    missing_prerequisite=expected_stage,
                )
            for prerequisite in state["records"]:
                prerequisite_errors = self.verify_artifacts(prerequisite)
                if prerequisite_errors:
                    raise PipelineError(
                        E_INVALID_STAGE,
                        "cannot advance the checkpoint sequence while a prerequisite artifact is missing or corrupt",
                        scale=scale,
                        stage=stage,
                        prerequisite_stage=prerequisite["stage"],
                        prerequisite_seq=prerequisite["seq"],
                        artifact_errors=prerequisite_errors,
                    )
            target_path = self.checkpoint_path(scale, next_sequence, stage)
            if target_path.exists():
                state_paths = {item.get("path") for item in state["invalid_files"]}
                if self._display_path(target_path) not in state_paths:
                    raise PipelineError(
                        E_STALE_CHECKPOINT,
                        "target checkpoint sequence slot is already occupied",
                        scale=scale,
                        stage=stage,
                        seq=next_sequence,
                        path=self._display_path(target_path),
                    )
                target_path.unlink()

            body: dict[str, Any] = {
                "schema_version": CHECKPOINT_SCHEMA_VERSION,
                "stage": stage,
                "scale": scale,
                "seq": next_sequence,
                "input_fingerprint": input_fingerprint,
                "config_version": config_version,
                "stage_config_fingerprint": stage_config_fingerprint,
                "stage_fingerprint": stage_fingerprint,
                "artifact_digest": artifact_digest,
                "artifacts": artifact_entries,
            }
            record = dict(body)
            record["record_checksum"] = hash_json(body)
            write_json_atomic(target_path, record, pretty=True)
            return record

    def invalidate_from(self, scale: str, stage: str) -> dict[str, Any]:
        if stage not in STAGE_INDEX:
            raise PipelineError(E_INVALID_STAGE, "unknown invalidation stage", stage=stage, scale=scale)
        target_index = STAGE_INDEX[stage]
        target_seq = target_index + 1
        removed_checkpoints: list[str] = []
        removed_artifacts: list[str] = []
        with locked_file(self.checkpoint_lock_path(scale)):
            directory = self.scale_dir(scale)
            if directory.exists():
                for path in sorted(directory.glob("*")):
                    seq: int | None = None
                    parts = path.name.split("_", 1)
                    if parts and parts[0].isdigit():
                        seq = int(parts[0])
                    if seq is not None and seq >= target_seq:
                        path.unlink()
                        removed_checkpoints.append(self._display_path(path))
                    elif seq is None and path.suffix == ".json":
                        record, _error = self._load_checkpoint_file(path)
                        if record is not None and STAGE_INDEX.get(record.get("stage"), -1) >= target_index:
                            path.unlink()
                            removed_checkpoints.append(self._display_path(path))
            for stage_name in STAGES[target_index:]:
                artifact_dir = self.stage_artifact_dir(scale, stage_name)
                if artifact_dir.exists():
                    shutil.rmtree(artifact_dir)
                    removed_artifacts.append(self._display_path(artifact_dir))
        return {
            "scale": scale,
            "from_stage": stage,
            "removed_checkpoints": removed_checkpoints,
            "removed_artifact_directories": removed_artifacts,
        }

    def checkpoint_digest(self, scale: str) -> str:
        state = self.scan(scale)
        return hash_json(
            [
                {
                    "seq": item["seq"],
                    "stage": item["stage"],
                    "stage_fingerprint": item["stage_fingerprint"],
                    "artifact_digest": item["artifact_digest"],
                }
                for item in state["records"]
            ]
        )


def canonical_checkpoint_summary(record: dict[str, Any]) -> str:
    return canonical_json(
        {
            "scale": record.get("scale"),
            "stage": record.get("stage"),
            "seq": record.get("seq"),
            "stage_fingerprint": record.get("stage_fingerprint"),
            "artifact_digest": record.get("artifact_digest"),
        }
    )
