from __future__ import annotations

import fcntl
import json
import os
import re
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .errors import (
    E_CHECKPOINT_CORRUPT,
    E_INVALID_STAGE,
    E_MISSING_ARTIFACT,
    E_STALE_CHECKPOINT,
    PipelineError,
)
from .util import (
    atomic_write_json,
    canonical_digest,
    ensure_within,
    read_json,
    sha256_file,
)


STAGES = ("features", "train_score", "calibration", "summarize")
STAGE_INDEX = {stage: index for index, stage in enumerate(STAGES)}
_RECORD_PATTERN = re.compile(r"^(\d{12})\.json$")


class CheckpointManager:
    def __init__(self, workspace: Path | str) -> None:
        self.workspace = Path(workspace).resolve()
        self.checkpoint_dir = self.workspace / "checkpoints"
        self.record_dir = self.checkpoint_dir / "records"
        self.lock_path = self.checkpoint_dir / "publish.lock"
        self.record_dir.mkdir(parents=True, exist_ok=True)
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def _locked(self) -> Iterator[None]:
        descriptor = os.open(self.lock_path, os.O_CREAT | os.O_RDWR, 0o644)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    def _record_paths(self) -> list[Path]:
        paths: list[tuple[int, Path]] = []
        for path in self.record_dir.iterdir():
            if path.name.startswith("."):
                continue
            match = _RECORD_PATTERN.match(path.name)
            if match is None:
                continue
            paths.append((int(match.group(1)), path))
        paths.sort(key=lambda item: item[0])
        return [path for _sequence, path in paths]

    def _load_records_locked(self) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        paths = self._record_paths()
        expected = 1
        for path in paths:
            try:
                record = read_json(path)
            except Exception as exc:
                raise PipelineError(
                    E_CHECKPOINT_CORRUPT,
                    "checkpoint record is not valid JSON",
                    {"path": str(path), "reason": str(exc)},
                ) from exc
            sequence = record.get("sequence")
            if sequence != expected or sequence != int(path.stem):
                raise PipelineError(
                    E_CHECKPOINT_CORRUPT,
                    "checkpoint sequence is not contiguous",
                    {"path": str(path), "expected": expected, "actual": sequence},
                )
            if record.get("action") == "publish":
                record["artifact_digest"] = canonical_digest(
                    [
                        {"path": artifact["path"], "digest": artifact["digest"]}
                        for artifact in record.get("artifacts", [])
                    ]
                )
            records.append(record)
            expected += 1
        return records

    @staticmethod
    def _state_from_records(records: list[dict[str, Any]]) -> dict[str, Any]:
        batches: dict[str, Any] = {}
        for record in records:
            batch_id = record["batch_id"]
            info = batches.setdefault(
                batch_id,
                {
                    "next_stage_index": 0,
                    "stage_records": {},
                    "invalidations": [],
                    "last_sequence": 0,
                },
            )
            info["last_sequence"] = record["sequence"]
            if record["action"] == "publish":
                info["stage_records"][record["stage_index"]] = record
                info["next_stage_index"] = max(info["next_stage_index"], record["stage_index"] + 1)
            elif record["action"] == "invalidate":
                stage_index = record["stage_index"]
                for stale_index in list(info["stage_records"]):
                    if stale_index >= stage_index:
                        del info["stage_records"][stale_index]
                info["next_stage_index"] = stage_index
                info["invalidations"].append(record)
        return batches

    def state(self) -> dict[str, Any]:
        with self._locked():
            records = self._load_records_locked()
            batches = self._state_from_records(records)
        return {
            "sequence": records[-1]["sequence"] if records else 0,
            "batches": batches,
        }

    def next_stage_index(self, batch_id: str) -> int:
        return int(self.state()["batches"].get(batch_id, {}).get("next_stage_index", 0))

    def stage_record(self, batch_id: str, stage: str) -> dict[str, Any] | None:
        if stage not in STAGE_INDEX:
            raise PipelineError(E_INVALID_STAGE, "unknown stage", {"stage": stage})
        state = self.state()
        record = state["batches"].get(batch_id, {}).get("stage_records", {}).get(STAGE_INDEX[stage])
        return record

    def _validate_artifacts(self, batch_id: str, artifacts: list[dict[str, Any]]) -> None:
        if not artifacts:
            raise PipelineError(E_INVALID_STAGE, "checkpoint must reference at least one artifact")
        for artifact in artifacts:
            relative = artifact.get("path")
            digest = artifact.get("digest")
            if not isinstance(relative, str) or not relative or not isinstance(digest, str):
                raise PipelineError(E_INVALID_STAGE, "artifact must contain path and digest")
            try:
                path = ensure_within(self.workspace, relative)
            except ValueError as exc:
                raise PipelineError(E_INVALID_STAGE, "artifact path is invalid", {"path": relative}) from exc
            if not path.is_file():
                raise PipelineError(
                    E_MISSING_ARTIFACT,
                    "checkpoint artifact is missing",
                    {"batch_id": batch_id, "path": relative},
                )
            actual_digest = sha256_file(path)
            if actual_digest != digest:
                raise PipelineError(
                    E_STALE_CHECKPOINT,
                    "artifact digest does not match checkpoint",
                    {"batch_id": batch_id, "path": relative, "expected": digest, "actual": actual_digest},
                )
        primary_path = ensure_within(self.workspace, artifacts[0]["path"])
        claimed_batch_id: str | None = None
        try:
            with open(primary_path, "r", encoding="utf-8") as handle:
                first_line = handle.readline()
            if first_line.strip():
                value = json.loads(first_line)
                if isinstance(value, dict):
                    claimed_batch_id = value.get("batch_id")
        except (OSError, json.JSONDecodeError):
            claimed_batch_id = None
        if claimed_batch_id is not None and claimed_batch_id != batch_id:
            raise PipelineError(
                E_STALE_CHECKPOINT,
                "artifact belongs to a different batch",
                {"batch_id": batch_id, "artifact_batch_id": claimed_batch_id, "path": artifacts[0]["path"]},
            )

    def publish(
        self,
        batch_id: str,
        stage: str,
        input_fingerprint: str,
        config_version: str,
        artifacts: list[dict[str, Any]],
        expected_sequence: int | None = None,
    ) -> dict[str, Any]:
        if stage not in STAGE_INDEX:
            raise PipelineError(E_INVALID_STAGE, "unknown stage", {"stage": stage})
        stage_index = STAGE_INDEX[stage]
        if not isinstance(batch_id, str) or not batch_id:
            raise PipelineError(E_INVALID_STAGE, "batch_id must be a non-empty string")
        if not isinstance(input_fingerprint, str) or not input_fingerprint:
            raise PipelineError(E_INVALID_STAGE, "input_fingerprint must be a non-empty string")
        if not isinstance(config_version, str) or not config_version:
            raise PipelineError(E_INVALID_STAGE, "config_version must be a non-empty string")
        with self._locked():
            records = self._load_records_locked()
            sequence = records[-1]["sequence"] if records else 0
            if expected_sequence is not None and expected_sequence != sequence:
                raise PipelineError(
                    E_STALE_CHECKPOINT,
                    "publisher observed a non-current checkpoint sequence",
                    {"expected_sequence": expected_sequence, "actual_sequence": sequence},
                )
            batches = self._state_from_records(records)
            info = batches.get(
                batch_id,
                {"next_stage_index": 0, "stage_records": {}, "invalidations": [], "last_sequence": 0},
            )
            next_index = int(info["next_stage_index"])
            if stage_index < next_index:
                raise PipelineError(
                    E_STALE_CHECKPOINT,
                    "stage was already published or is behind the current batch sequence",
                    {"batch_id": batch_id, "stage": stage, "next_stage_index": next_index},
                )
            if stage_index > next_index:
                raise PipelineError(
                    E_INVALID_STAGE,
                    "stage jump would skip a prerequisite stage",
                    {
                        "batch_id": batch_id,
                        "requested_stage": stage,
                        "expected_stage": STAGES[next_index] if next_index < len(STAGES) else None,
                        "next_stage_index": next_index,
                    },
                )
            for prior_index in range(next_index):
                prior_record = info["stage_records"].get(prior_index)
                if prior_record is None:
                    raise PipelineError(
                        E_INVALID_STAGE,
                        "prerequisite stage artifact is missing",
                        {"batch_id": batch_id, "prerequisite_stage_index": prior_index},
                    )
                try:
                    self._validate_artifacts(batch_id, prior_record.get("artifacts", []))
                except (PipelineError, OSError) as exc:
                    raise PipelineError(
                        E_INVALID_STAGE,
                        "prerequisite stage artifact is unavailable",
                        {
                            "batch_id": batch_id,
                            "prerequisite_stage": STAGES[prior_index],
                            "reason": getattr(exc, "code", type(exc).__name__),
                        },
                    ) from exc
            self._validate_artifacts(batch_id, artifacts)
            new_sequence = sequence + 1
            record = {
                "sequence": new_sequence,
                "previous_sequence": sequence,
                "action": "publish",
                "batch_id": batch_id,
                "stage": stage,
                "stage_index": stage_index,
                "input_fingerprint": input_fingerprint,
                "config_version": config_version,
                "artifacts": artifacts,
                "artifact_digest": canonical_digest(
                    [
                        {"path": artifact["path"], "digest": artifact["digest"]}
                        for artifact in artifacts
                    ]
                ),
            }
            path = self.record_dir / f"{new_sequence:012d}.json"
            atomic_write_json(path, record)
            self._write_latest_locked(records + [record])
            return record

    def invalidate(
        self,
        batch_id: str,
        earliest_stage: str,
        reason: str,
        old_fingerprint: str | None = None,
        new_fingerprint: str | None = None,
        config_version: str = "",
    ) -> dict[str, Any]:
        if earliest_stage not in STAGE_INDEX:
            raise PipelineError(E_INVALID_STAGE, "unknown invalidation stage", {"stage": earliest_stage})
        if not isinstance(reason, str) or not reason:
            raise PipelineError(E_INVALID_STAGE, "invalidation reason must be non-empty")
        with self._locked():
            records = self._load_records_locked()
            sequence = records[-1]["sequence"] if records else 0
            batches = self._state_from_records(records)
            info = batches.get(batch_id)
            if info is None or not info["stage_records"]:
                raise PipelineError(
                    E_INVALID_STAGE,
                    "cannot invalidate a batch without published stage artifacts",
                    {"batch_id": batch_id},
                )
            stage_index = STAGE_INDEX[earliest_stage]
            if not any(index >= stage_index for index in info["stage_records"]):
                raise PipelineError(
                    E_INVALID_STAGE,
                    "invalidation range has no published artifacts",
                    {"batch_id": batch_id, "earliest_stage": earliest_stage},
                )
            new_sequence = sequence + 1
            record = {
                "sequence": new_sequence,
                "previous_sequence": sequence,
                "action": "invalidate",
                "batch_id": batch_id,
                "stage": earliest_stage,
                "stage_index": stage_index,
                "reason": reason,
                "old_fingerprint": old_fingerprint,
                "new_fingerprint": new_fingerprint,
                "config_version": config_version,
            }
            path = self.record_dir / f"{new_sequence:012d}.json"
            atomic_write_json(path, record)
            self._write_latest_locked(records + [record])
            return record

    def _write_latest_locked(self, records: list[dict[str, Any]]) -> None:
        latest = {
            "sequence": records[-1]["sequence"] if records else 0,
            "last_record": records[-1] if records else None,
        }
        atomic_write_json(self.checkpoint_dir / "latest.json", latest)

    def verify_integrity(self) -> dict[str, Any]:
        with self._locked():
            records = self._load_records_locked()
        return {
            "status": "ok",
            "record_count": len(records),
            "first_sequence": records[0]["sequence"] if records else None,
            "last_sequence": records[-1]["sequence"] if records else None,
            "contiguous": all(record["sequence"] == index + 1 for index, record in enumerate(records)),
        }
