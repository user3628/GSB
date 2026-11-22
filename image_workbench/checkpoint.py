"""Atomic per-task four-stage checkpoint journal with cross-process locking."""

from __future__ import annotations

import os
import re
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .errors import (
    E_ARTIFACT_INVALID,
    E_INVALID_STAGE,
    E_STALE_CHECKPOINT,
    WorkbenchError,
)
from .filelock import FileLock
from .util import (
    atomic_write_json,
    ensure_dir,
    path_within,
    read_json_file,
    relative_posix,
    safe_name,
    sha256_bytes,
    sha256_json,
)
from .versioning import STAGES, STAGE_INDEX


RECORD_RE = re.compile(r"^seq-(\d{8})-(parsed|executed|size_checked|exported)\.json$")
SCHEMA_VERSION = 1


@dataclass(frozen=True)
class ScanResult:
    records: list[dict[str, Any]]
    ignored: list[dict[str, Any]]
    by_seq: dict[int, list[tuple[Path, dict[str, Any] | None, str | None]]]


def _record_hash(record: dict[str, Any]) -> str:
    body = {key: value for key, value in record.items() if key != "record_sha256"}
    return sha256_json(body)


class CheckpointStore:
    def __init__(self, workspace: Path, task_id: str):
        self.workspace = Path(workspace).resolve()
        self.task_id = safe_name(task_id, "task_id")
        self.directory = self.workspace / "checkpoints" / self.task_id
        self.lock_path = self.workspace / ".locks" / "checkpoints" / f"{self.task_id}.lock"

    def _filename(self, seq: int, stage: str) -> Path:
        return self.directory / f"seq-{seq:08d}-{stage}.json"

    def _scan_files(self) -> list[tuple[int, str, Path]]:
        if not self.directory.is_dir():
            return []
        entries: list[tuple[int, str, Path]] = []
        for path in sorted(self.directory.iterdir()):
            if not path.is_file():
                continue
            match = RECORD_RE.fullmatch(path.name)
            if not match:
                continue
            entries.append((int(match.group(1)), match.group(2), path))
        return entries

    def _validate_record(
        self,
        path: Path,
        expected_seq: int,
        expected_stage: str,
    ) -> tuple[dict[str, Any] | None, str | None]:
        try:
            record = read_json_file(path)
        except Exception as exc:
            return None, f"UNREADABLE_JSON:{type(exc).__name__}"
        if not isinstance(record, dict):
            return None, "RECORD_NOT_OBJECT"
        required = {
            "schema_version",
            "task_id",
            "stage",
            "stage_index",
            "seq",
            "input_digest",
            "config_version",
            "stage_fingerprint",
            "upstream_fingerprint",
            "fingerprint_components",
            "artifact_path",
            "artifact_sha256",
            "output_digest",
            "output_size",
            "ok",
            "error",
            "record_sha256",
        }
        missing = sorted(required - set(record))
        if missing:
            return None, f"MISSING_FIELDS:{','.join(missing)}"
        if record.get("schema_version") != SCHEMA_VERSION:
            return None, "SCHEMA_VERSION_MISMATCH"
        if record.get("task_id") != self.task_id:
            return None, "TASK_ID_MISMATCH"
        if record.get("seq") != expected_seq or path.name != self._filename(expected_seq, expected_stage).name:
            return None, "SEQUENCE_OR_STAGE_FILENAME_MISMATCH"
        if record.get("stage") != expected_stage:
            return None, "STAGE_FILENAME_MISMATCH"
        if record.get("stage_index") != STAGE_INDEX.get(expected_stage):
            return None, "STAGE_INDEX_MISMATCH"
        if not isinstance(record.get("ok"), bool):
            return None, "OK_NOT_BOOLEAN"
        if record.get("record_sha256") != _record_hash(record):
            return None, "RECORD_CHECKSUM_MISMATCH"
        return record, None

    def scan(self) -> ScanResult:
        entries = self._scan_files()
        by_seq: dict[int, list[tuple[Path, dict[str, Any] | None, str | None]]] = {}
        for seq, stage, path in entries:
            record, error = self._validate_record(path, seq, stage)
            by_seq.setdefault(seq, []).append((path, record, error))
        records: list[dict[str, Any]] = []
        ignored: list[dict[str, Any]] = []
        expected_seq = 1
        stopped = False
        for seq in sorted(by_seq):
            candidates = by_seq[seq]
            if stopped or seq != expected_seq:
                for path, _, error in candidates:
                    ignored.append(
                        {
                            "path": relative_posix(self.workspace, path),
                            "seq": seq,
                            "reason": error or "SEQUENCE_GAP",
                        }
                    )
                stopped = True
                continue
            if len(candidates) != 1:
                for path, _, error in candidates:
                    ignored.append(
                        {
                            "path": relative_posix(self.workspace, path),
                            "seq": seq,
                            "reason": error or "DUPLICATE_SEQUENCE",
                        }
                    )
                stopped = True
                continue
            path, record, error = candidates[0]
            if record is None:
                ignored.append(
                    {
                        "path": relative_posix(self.workspace, path),
                        "seq": seq,
                        "reason": error or "INVALID_RECORD",
                    }
                )
                stopped = True
                continue
            records.append(record)
            expected_seq += 1
        return ScanResult(records=records, ignored=ignored, by_seq=by_seq)

    def read_consistent(self) -> list[dict[str, Any]]:
        return self.scan().records

    @staticmethod
    def latest_by_stage(records: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        latest: dict[str, dict[str, Any]] = {}
        for record in records:
            stage = record.get("stage")
            if stage in STAGES:
                latest[stage] = record
        return latest

    def snapshot(self) -> dict[str, Any]:
        scan = self.scan()
        latest = self.latest_by_stage(scan.records)
        return {
            "task_id": self.task_id,
            "next_seq": len(scan.records) + 1,
            "records": [
                {
                    "seq": record["seq"],
                    "stage": record["stage"],
                    "fingerprint": record["stage_fingerprint"],
                    "artifact_path": record["artifact_path"],
                    "ok": record["ok"],
                }
                for record in scan.records
            ],
            "latest_by_stage": {
                stage: {
                    "seq": record["seq"],
                    "fingerprint": record["stage_fingerprint"],
                    "artifact_path": record["artifact_path"],
                    "ok": record["ok"],
                }
                for stage, record in latest.items()
            },
            "ignored": scan.ignored,
        }

    def next_seq(self) -> int:
        with FileLock(self.lock_path):
            return len(self.scan().records) + 1

    def artifact_ok(self, record: dict[str, Any]) -> bool:
        try:
            relative = Path(record["artifact_path"])
        except (KeyError, TypeError):
            return False
        if relative.is_absolute():
            return False
        path = self.workspace / relative
        if not path_within(self.workspace, path) or not path.is_file():
            return False
        try:
            return sha256_bytes(path.read_bytes()) == record.get("artifact_sha256")
        except OSError:
            return False

    def _quarantine_locked(self, paths: list[Path], reason: str) -> None:
        if not paths:
            return
        target_dir = ensure_dir(
            self.workspace / "checkpoints" / "_quarantine" / self.task_id
        )
        token = uuid.uuid4().hex
        for index, path in enumerate(paths):
            if not path.exists():
                continue
            target = target_dir / f"{token}-{index:03d}-{reason[:32]}-{path.name}"
            try:
                os.replace(path, target)
            except OSError:
                pass

    def _repair_locked(self, scan: ScanResult) -> list[dict[str, Any]]:
        if not scan.ignored:
            return scan.records
        first_bad_seq = len(scan.records) + 1
        paths: list[Path] = []
        for seq, candidates in scan.by_seq.items():
            if seq >= first_bad_seq:
                paths.extend(path for path, _, _ in candidates)
        self._quarantine_locked(sorted(paths), "recovery")
        repaired = self.scan()
        if repaired.ignored:
            raise WorkbenchError(
                E_ARTIFACT_INVALID,
                "checkpoint journal could not be repaired",
                task_id=self.task_id,
                stage="checkpoint",
                reason="CHECKPOINT_REPAIR_FAILED",
                details={"ignored": repaired.ignored[:10]},
            )
        return repaired.records

    def publish(
        self,
        stage: str,
        payload: dict[str, Any],
        *,
        expected_seq: int | None = None,
    ) -> dict[str, Any]:
        if stage not in STAGES:
            raise WorkbenchError(
                E_INVALID_STAGE,
                f"unknown checkpoint stage {stage!r}",
                task_id=self.task_id,
                stage=stage,
                reason="STAGE_UNKNOWN",
                details={"known_stages": list(STAGES)},
            )
        with FileLock(self.lock_path):
            scan = self.scan()
            records = self._repair_locked(scan)
            next_seq = len(records) + 1
            if expected_seq is not None and expected_seq != next_seq:
                raise WorkbenchError(
                    E_STALE_CHECKPOINT,
                    "checkpoint sequence is stale or out of order",
                    task_id=self.task_id,
                    stage=stage,
                    reason="EXPECTED_SEQUENCE_MISMATCH",
                    details={"expected_seq": expected_seq, "actual_next_seq": next_seq},
                )
            stage_index = STAGE_INDEX[stage]
            previous = self.latest_by_stage(records).get(STAGES[stage_index - 1]) if stage_index else None
            if stage_index > 0 and previous is None:
                raise WorkbenchError(
                    E_INVALID_STAGE,
                    f"cannot publish {stage!r} before stage {STAGES[stage_index - 1]!r}",
                    task_id=self.task_id,
                    stage=stage,
                    reason="PREDECESSOR_MISSING",
                )
            upstream_fingerprint = payload.get("upstream_fingerprint")
            if stage_index > 0 and previous is not None:
                if upstream_fingerprint != previous.get("stage_fingerprint"):
                    raise WorkbenchError(
                        E_INVALID_STAGE,
                        f"cannot publish {stage!r}: predecessor fingerprint does not match",
                        task_id=self.task_id,
                        stage=stage,
                        reason="PREDECESSOR_FINGERPRINT_MISMATCH",
                        details={
                            "expected": previous.get("stage_fingerprint"),
                            "provided": upstream_fingerprint,
                        },
                    )
                if not self.artifact_ok(previous):
                    raise WorkbenchError(
                        E_INVALID_STAGE,
                        f"cannot publish {stage!r}: predecessor artifact is missing or corrupt",
                        task_id=self.task_id,
                        stage=stage,
                        reason="PREDECESSOR_ARTIFACT_INVALID",
                        details={"predecessor_stage": STAGES[stage_index - 1]},
                    )
            artifact_path_value = payload.get("artifact_path")
            artifact_sha = payload.get("artifact_sha256")
            if not isinstance(artifact_path_value, str) or not isinstance(artifact_sha, str):
                raise WorkbenchError(
                    E_ARTIFACT_INVALID,
                    "checkpoint payload requires artifact_path and artifact_sha256",
                    task_id=self.task_id,
                    stage=stage,
                    reason="ARTIFACT_FIELDS_MISSING",
                )
            artifact_relative = Path(artifact_path_value)
            artifact_absolute = (self.workspace / artifact_relative).resolve()
            if (
                artifact_relative.is_absolute()
                or not path_within(self.workspace, artifact_absolute)
                or not artifact_absolute.is_file()
            ):
                raise WorkbenchError(
                    E_ARTIFACT_INVALID,
                    "artifact is missing or outside the workspace",
                    task_id=self.task_id,
                    stage=stage,
                    reason="ARTIFACT_PATH_INVALID",
                    details={"artifact_path": artifact_path_value},
                )
            if artifact_absolute.stem != self.task_id:
                raise WorkbenchError(
                    E_STALE_CHECKPOINT,
                    "artifact belongs to a different task",
                    task_id=self.task_id,
                    stage=stage,
                    reason="CROSS_TASK_ARTIFACT",
                    details={"artifact_path": artifact_path_value},
                )
            actual_sha = sha256_bytes(artifact_absolute.read_bytes())
            if actual_sha != artifact_sha:
                raise WorkbenchError(
                    E_ARTIFACT_INVALID,
                    "artifact sha256 does not match checkpoint payload",
                    task_id=self.task_id,
                    stage=stage,
                    reason="ARTIFACT_DIGEST_MISMATCH",
                    details={"declared": artifact_sha, "actual": actual_sha},
                )
            record: dict[str, Any] = {
                "schema_version": SCHEMA_VERSION,
                "task_id": self.task_id,
                "stage": stage,
                "stage_index": stage_index,
                "seq": next_seq,
                "input_digest": payload.get("input_digest"),
                "config_version": payload.get("config_version"),
                "stage_fingerprint": payload.get("stage_fingerprint"),
                "upstream_fingerprint": upstream_fingerprint,
                "fingerprint_components": payload.get("fingerprint_components"),
                "artifact_path": relative_posix(self.workspace, artifact_absolute),
                "artifact_sha256": artifact_sha,
                "output_digest": payload.get("output_digest"),
                "output_size": payload.get("output_size"),
                "ok": bool(payload.get("ok", True)),
                "error": payload.get("error"),
            }
            record["record_sha256"] = _record_hash(record)
            record_path = self._filename(next_seq, stage)
            ensure_dir(record_path.parent)
            atomic_write_json(record_path, record)
            return record


def status_for_task(workspace: Path, task_id: str) -> dict[str, Any]:
    return CheckpointStore(workspace, task_id).snapshot()
