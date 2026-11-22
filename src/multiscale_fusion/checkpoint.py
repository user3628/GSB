from __future__ import annotations

import errno
import fcntl
import json
import os
import shutil
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

from .errors import FusionError
from .util import (
    canonical_json_bytes,
    fsync_dir,
    new_tmp_name,
    pretty_json_bytes,
    read_json,
    sha256_bytes,
    sha256_file,
    sha256_json,
    write_bytes,
    write_json,
)


class FileLock:
    """Advisory file lock backed by flock(2)."""

    def __init__(self, path: str | Path, *, exclusive: bool = True, timeout: float = 20.0) -> None:
        self.path = Path(path)
        self.exclusive = exclusive
        self.timeout = timeout
        self._handle: Any = None

    def __enter__(self) -> "FileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = open(self.path, "a+", encoding="utf-8")
        operation = fcntl.LOCK_EX if self.exclusive else fcntl.LOCK_SH
        deadline = time.monotonic() + self.timeout
        while True:
            try:
                fcntl.flock(self._handle.fileno(), operation | fcntl.LOCK_NB)
                return self
            except OSError as exc:
                if exc.errno not in {errno.EACCES, errno.EAGAIN}:
                    self._handle.close()
                    self._handle = None
                    raise
                if time.monotonic() >= deadline:
                    self._handle.close()
                    self._handle = None
                    raise FusionError(
                        "E_LOCK_TIMEOUT",
                        f"timed out waiting for workspace lock {self.path}",
                        details={"path": str(self.path), "timeout_seconds": self.timeout},
                    )
                time.sleep(0.01)

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self._handle is not None:
            try:
                fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
            finally:
                self._handle.close()
                self._handle = None


@dataclass
class CheckpointState:
    task_id: str
    records: list[dict[str, Any]]
    head_sequence: int
    head_digest: str | None
    head_repair_needed: bool
    classification: str = "normal"

    @property
    def head(self) -> dict[str, Any] | None:
        return self.records[-1] if self.records else None

    def latest_for_layer(self, layer: str) -> dict[str, Any] | None:
        for record in reversed(self.records):
            if record.get("layer") == layer:
                return record
        return None

    def find_stage(self, layer: str, stage_version: str) -> dict[str, Any] | None:
        for record in reversed(self.records):
            if record.get("layer") == layer and record.get("stage_version") == stage_version:
                return record
        return None


class CheckpointStore:
    def __init__(self, workspace: str | Path, task_id: str, *, lock_timeout: float = 20.0) -> None:
        self.workspace = Path(workspace).resolve()
        self.task_id = task_id
        self.state_root = self.workspace / "state"
        self.task_root = self.state_root / "tasks" / task_id
        self.checkpoints_dir = self.task_root / "checkpoints"
        self.artifacts_dir = self.task_root / "artifacts"
        self.reports_dir = self.task_root / "reports"
        self.stop_path = self.task_root / "STOP.json"
        self.head_path = self.task_root / "HEAD.json"
        self.lock_path = self.state_root / ".lock"
        self.lock_timeout = lock_timeout
        for path in (self.checkpoints_dir, self.artifacts_dir, self.reports_dir):
            path.mkdir(parents=True, exist_ok=True)

    def lock(self, *, exclusive: bool = True) -> FileLock:
        return FileLock(self.lock_path, exclusive=exclusive, timeout=self.lock_timeout)

    @staticmethod
    def record_digest(record: Mapping[str, Any]) -> str:
        public = {key: value for key, value in record.items() if not key.startswith("_")}
        return sha256_json(public)

    @staticmethod
    def _public_record(record: Mapping[str, Any]) -> dict[str, Any]:
        return {key: value for key, value in record.items() if not key.startswith("_")}

    def _stale(self, message: str, *, reason: str, classification: str, **details: Any) -> FusionError:
        payload = {"task_id": self.task_id, **details}
        return FusionError.stale_checkpoint(message, reason=reason, classification=classification, **payload)

    def _read_head(self) -> dict[str, Any] | None:
        if not self.head_path.exists():
            return None
        try:
            value = read_json(self.head_path)
        except Exception as exc:  # noqa: BLE001
            raise self._stale(
                "checkpoint HEAD is unreadable",
                reason="head_unreadable",
                classification="tamper",
                path=str(self.head_path),
                error=str(exc),
            )
        if not isinstance(value, dict):
            raise self._stale("checkpoint HEAD must be an object", reason="head_invalid", classification="tamper")
        return value

    def _verify_artifacts(self, record: Mapping[str, Any]) -> None:
        stage_version = record.get("stage_version")
        summary = record.get("artifact_summary")
        if not isinstance(stage_version, str) or not isinstance(summary, dict):
            raise self._stale(
                "checkpoint record has invalid artifact metadata",
                reason="record_schema_invalid",
                classification="tamper",
                sequence=record.get("sequence"),
            )
        files = summary.get("files")
        if not isinstance(files, dict) or not files:
            raise self._stale(
                "checkpoint record has no artifact summary",
                reason="artifact_summary_missing",
                classification="tamper",
                sequence=record.get("sequence"),
                stage_version=stage_version,
            )
        stage_dir = self.artifacts_dir / stage_version
        for relative, expected_digest in files.items():
            if not isinstance(relative, str) or Path(relative).is_absolute() or ".." in Path(relative).parts:
                raise self._stale(
                    "checkpoint artifact path is unsafe",
                    reason="artifact_path_invalid",
                    classification="tamper",
                    sequence=record.get("sequence"),
                    artifact=relative,
                )
            artifact_path = stage_dir / relative
            if not artifact_path.is_file():
                raise self._stale(
                    "checkpoint artifact is missing",
                    reason="artifact_missing",
                    classification="tamper",
                    sequence=record.get("sequence"),
                    artifact=relative,
                    path=str(artifact_path),
                )
            actual_digest = sha256_file(artifact_path)
            if actual_digest != expected_digest:
                raise self._stale(
                    "checkpoint artifact digest does not match its record",
                    reason="artifact_digest_mismatch",
                    classification="tamper",
                    sequence=record.get("sequence"),
                    artifact=relative,
                    expected=expected_digest,
                    actual=actual_digest,
                )

    def _scan_locked(self) -> CheckpointState:
        records: list[dict[str, Any]] = []
        files: list[tuple[int, Path]] = []
        if self.checkpoints_dir.exists():
            for path in self.checkpoints_dir.iterdir():
                if not path.is_file() or not path.suffix == ".json" or path.name.startswith("."):
                    continue
                try:
                    file_sequence = int(path.stem)
                except ValueError:
                    continue
                files.append((file_sequence, path))
        files.sort(key=lambda item: item[0])
        seen_sequences: set[int] = set()
        for file_sequence, path in files:
            if file_sequence in seen_sequences:
                raise self._stale(
                    "duplicate checkpoint sequence",
                    reason="duplicate_sequence",
                    classification="tamper",
                    sequence=file_sequence,
                )
            seen_sequences.add(file_sequence)
            if file_sequence != len(records) + 1:
                raise self._stale(
                    "checkpoint sequence is not contiguous",
                    reason="sequence_gap_or_rollback",
                    classification="rollback",
                    expected_sequence=len(records) + 1,
                    actual_sequence=file_sequence,
                    path=str(path),
                )
            try:
                record = read_json(path)
            except Exception as exc:  # noqa: BLE001
                raise self._stale(
                    "checkpoint record is unreadable",
                    reason="record_unreadable",
                    classification="tamper",
                    path=str(path),
                    error=str(exc),
                )
            if not isinstance(record, dict):
                raise self._stale(
                    "checkpoint record must be an object",
                    reason="record_invalid",
                    classification="tamper",
                    path=str(path),
                )
            if record.get("task_id") != self.task_id:
                raise self._stale(
                    "checkpoint record belongs to a different task",
                    reason="cross_task_record",
                    classification="rollback",
                    path=str(path),
                    expected_task=self.task_id,
                    actual_task=record.get("task_id"),
                )
            if record.get("sequence") != file_sequence:
                raise self._stale(
                    "checkpoint sequence field does not match filename",
                    reason="sequence_field_mismatch",
                    classification="tamper",
                    path=str(path),
                    expected_sequence=file_sequence,
                    actual_sequence=record.get("sequence"),
                )
            if record.get("status") != "committed":
                raise self._stale(
                    "checkpoint record is not committed",
                    reason="record_not_committed",
                    classification="tamper",
                    sequence=file_sequence,
                )
            expected_previous = records[-1]["_digest"] if records else None
            actual_previous = record.get("previous_record_digest")
            if expected_previous != actual_previous:
                raise self._stale(
                    "checkpoint chain predecessor digest mismatch",
                    reason="chain_digest_mismatch",
                    classification="tamper",
                    sequence=file_sequence,
                    expected_previous=expected_previous,
                    actual_previous=actual_previous,
                )
            digest = self.record_digest(record)
            stored = record.get("_digest")
            if stored is not None and stored != digest:
                raise self._stale(
                    "checkpoint self digest mismatch",
                    reason="record_digest_mismatch",
                    classification="tamper",
                    sequence=file_sequence,
                )
            record["_digest"] = digest
            record["_path"] = str(path)
            self._verify_artifacts(record)
            records.append(record)

        head = self._read_head()
        head_sequence = 0
        head_digest: str | None = None
        repair_needed = False
        if head is not None:
            if head.get("task_id") != self.task_id:
                raise self._stale(
                    "checkpoint HEAD belongs to a different task",
                    reason="cross_task_head",
                    classification="rollback",
                    expected_task=self.task_id,
                    actual_task=head.get("task_id"),
                )
            try:
                head_sequence = int(head.get("sequence", 0))
            except (TypeError, ValueError):
                raise self._stale("checkpoint HEAD sequence is invalid", reason="head_invalid", classification="tamper")
            head_digest = head.get("record_digest")
            if head_sequence > len(records):
                raise self._stale(
                    "checkpoint HEAD is ahead of committed records (snapshot rollback)",
                    reason="head_ahead_of_records",
                    classification="rollback",
                    head_sequence=head_sequence,
                    record_count=len(records),
                )
            if head_sequence > 0:
                referenced = records[head_sequence - 1]
                if head_digest != referenced["_digest"]:
                    raise self._stale(
                        "checkpoint HEAD digest does not match the referenced record",
                        reason="head_digest_mismatch",
                        classification="tamper",
                        head_sequence=head_sequence,
                        expected=head_digest,
                        actual=referenced["_digest"],
                    )
            if head_sequence < len(records):
                repair_needed = True
        elif records:
            repair_needed = True
        return CheckpointState(
            task_id=self.task_id,
            records=records,
            head_sequence=head_sequence,
            head_digest=head_digest,
            head_repair_needed=repair_needed,
        )

    def _write_head_locked(self, state: CheckpointState) -> None:
        if state.records:
            last = state.records[-1]
            payload = {
                "schema_version": 1,
                "task_id": self.task_id,
                "sequence": int(last["sequence"]),
                "record_digest": last["_digest"],
            }
        else:
            payload = {"schema_version": 1, "task_id": self.task_id, "sequence": 0, "record_digest": None}
        write_json(self.head_path, payload)

    def _repair_head_locked(self, state: CheckpointState) -> CheckpointState:
        if state.head_repair_needed:
            self._write_head_locked(state)
            state.head_repair_needed = False
            if state.records:
                state.head_sequence = int(state.records[-1]["sequence"])
                state.head_digest = state.records[-1]["_digest"]
        return state

    def scan(self, *, repair_head: bool = False) -> CheckpointState:
        exclusive = repair_head
        with self.lock(exclusive=exclusive):
            state = self._scan_locked()
            if repair_head:
                state = self._repair_head_locked(state)
            return state

    def verify(self) -> dict[str, Any]:
        state = self.scan(repair_head=False)
        return {
            "ok": True,
            "classification": "normal",
            "task_id": self.task_id,
            "record_count": len(state.records),
            "head_sequence": state.head_sequence,
            "head_repair_pending": state.head_repair_needed,
            "records": [
                {
                    "sequence": record["sequence"],
                    "layer": record["layer"],
                    "stage_index": record.get("stage_index"),
                    "stage_version": record["stage_version"],
                    "digest": record["_digest"],
                }
                for record in state.records
            ],
        }

    def _publish_artifacts_locked(
        self,
        stage_version: str,
        files: Mapping[str, bytes],
    ) -> dict[str, Any]:
        if not stage_version or "/" in stage_version or ".." in Path(stage_version).parts:
            raise FusionError.invalid_param("<checkpoint>", None, "stage_version", "must be a safe digest string")
        file_hashes = {relative: sha256_bytes(data) for relative, data in sorted(files.items())}
        target = self.artifacts_dir / stage_version
        if target.exists():
            for relative, expected in file_hashes.items():
                path = target / relative
                if not path.is_file() or sha256_file(path) != expected:
                    raise FusionError.incremental_mismatch(
                        "<artifact>",
                        reason="orphan_or_corrupt_stage_artifact",
                        expected=expected,
                        actual=sha256_file(path) if path.is_file() else None,
                        artifact=relative,
                        stage_version=stage_version,
                    )
            return {"files": file_hashes, "primary": "raw.npy"}
        tmp_dir = self.artifacts_dir / new_tmp_name("stage")
        tmp_dir.mkdir(parents=True, exist_ok=False)
        try:
            for relative, data in sorted(files.items()):
                if Path(relative).is_absolute() or ".." in Path(relative).parts:
                    raise FusionError.invalid_param("<checkpoint>", None, "artifact", "unsafe relative artifact path", artifact=relative)
                path = tmp_dir / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                with open(path, "wb") as handle:
                    handle.write(data)
                    handle.flush()
                    os.fsync(handle.fileno())
            fsync_dir(tmp_dir)
            os.rename(tmp_dir, target)
            fsync_dir(self.artifacts_dir)
        except BaseException:
            shutil.rmtree(tmp_dir, ignore_errors=True)
            raise
        return {"files": file_hashes, "primary": "raw.npy"}

    def publish_stage(
        self,
        *,
        layer: str,
        stage_index: int,
        stage_version: str,
        own_fingerprint: str,
        dependency_version: str | None,
        component_fingerprints: Mapping[str, str],
        artifact_files: Mapping[str, bytes],
        prerequisites: Iterable[tuple[str, str]] = (),
        expected_sequence: int | None = None,
        task_id_claim: str | None = None,
    ) -> dict[str, Any]:
        with self.lock(exclusive=True):
            state = self._scan_locked()
            state = self._repair_head_locked(state)
            if task_id_claim is not None and task_id_claim != self.task_id:
                raise self._stale(
                    "cross-task stage publication rejected",
                    reason="cross_task_publish",
                    classification="rollback",
                    claimed_task=task_id_claim,
                    expected_task=self.task_id,
                )
            as_sequence = state.head_sequence + 1
            if expected_sequence is not None:
                if expected_sequence < as_sequence:
                    raise self._stale(
                        "stage publication sequence is behind the committed head",
                        reason="publication_rollback_or_duplicate",
                        classification="rollback",
                        expected_sequence=expected_sequence,
                        required_sequence=as_sequence,
                    )
                if expected_sequence > as_sequence:
                    raise self._stale(
                        "stage publication sequence would create a gap",
                        reason="publication_gap",
                        classification="rollback",
                        expected_sequence=expected_sequence,
                        required_sequence=as_sequence,
                    )
            for prerequisite_layer, prerequisite_version in prerequisites:
                if state.find_stage(prerequisite_layer, prerequisite_version) is None:
                    raise FusionError.invalid_stage(
                        f"missing prerequisite stage {prerequisite_layer!r} ({prerequisite_version})",
                        layer=layer,
                        prerequisite_layer=prerequisite_layer,
                        prerequisite_stage_version=prerequisite_version,
                    )
            latest = state.latest_for_layer(layer)
            if latest is not None and latest.get("stage_version") == stage_version:
                return {**self._public_record(latest), "publication": "reused"}
            artifact_summary = self._publish_artifacts_locked(stage_version, artifact_files)
            sequence = as_sequence
            previous_digest = state.records[-1]["_digest"] if state.records else None
            record = {
                "schema_version": 1,
                "task_id": self.task_id,
                "sequence": sequence,
                "layer": layer,
                "stage_index": stage_index,
                "stage_version": stage_version,
                "own_fingerprint": own_fingerprint,
                "dependency_version": dependency_version,
                "component_fingerprints": dict(sorted(component_fingerprints.items())),
                "input_fingerprint": component_fingerprints.get("input"),
                "attention_fingerprint": component_fingerprints.get("attention"),
                "config_version": component_fingerprints.get("config"),
                "artifact_summary": artifact_summary,
                "status": "committed",
                "previous_record_digest": previous_digest,
            }
            digest = self.record_digest(record)
            record["_digest"] = digest
            path = self.checkpoints_dir / f"{sequence:08d}.json"
            if path.exists():
                raise self._stale(
                    "refusing to overwrite an existing checkpoint sequence",
                    reason="duplicate_sequence",
                    classification="rollback",
                    sequence=sequence,
                )
            write_bytes(path, pretty_json_bytes(self._public_record(record)))
            new_state = CheckpointState(
                task_id=self.task_id,
                records=state.records + [record],
                head_sequence=sequence,
                head_digest=digest,
                head_repair_needed=False,
            )
            self._write_head_locked(new_state)
            return {**self._public_record(record), "publication": "committed"}

    def write_stop(self, *, reason: str = "requested") -> Path:
        payload = {"schema_version": 1, "task_id": self.task_id, "stop_requested": True, "reason": reason}
        with self.lock(exclusive=True):
            write_json(self.stop_path, payload)
        return self.stop_path

    def clear_stop(self) -> bool:
        with self.lock(exclusive=True):
            existed = self.stop_path.exists()
            if existed:
                self.stop_path.unlink()
                fsync_dir(self.stop_path.parent)
            return existed

    def stop_requested(self) -> bool:
        if not self.stop_path.exists():
            return False
        try:
            value = read_json(self.stop_path)
        except Exception:
            return True
        return bool(value.get("stop_requested")) if isinstance(value, dict) else True

    def load_artifact(self, record: Mapping[str, Any], relative: str = "raw.npy") -> Path:
        stage_version = record["stage_version"]
        path = self.artifacts_dir / stage_version / relative
        expected = record.get("artifact_summary", {}).get("files", {}).get(relative)
        if expected is None:
            raise FusionError.invalid_stage(
                f"artifact {relative!r} is absent from checkpoint record",
                layer=record.get("layer"),
                stage_version=stage_version,
            )
        if not path.is_file() or sha256_file(path) != expected:
            raise FusionError.incremental_mismatch(
                str(record.get("layer")),
                reason="checkpoint_artifact_diff",
                artifact=relative,
                stage_version=stage_version,
                expected=expected,
                actual=sha256_file(path) if path.is_file() else None,
            )
        return path

    def write_report(self, name: str, payload: Mapping[str, Any]) -> Path:
        path = self.reports_dir / name
        with self.lock(exclusive=True):
            write_json(path, payload)
        return path

    def read_report(self, name: str) -> dict[str, Any] | None:
        path = self.reports_dir / name
        if not path.exists():
            return None
        with self.lock(exclusive=False):
            try:
                value = read_json(path)
            except Exception:
                return None
        return value if isinstance(value, dict) else None
