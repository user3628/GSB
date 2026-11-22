"""Atomic block checkpoints, monotonic publishing, and workspace queries."""

from __future__ import annotations

import io
import json
import os
import re
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np

from .errors import (
    BlockGemmError,
    E_WORKSPACE,
    invalid_shape,
    stale_checkpoint,
)
from .fingerprint import array_payload_digest, canonical_json_bytes, sha256_bytes, sha256_json


RECORD_NAME_RE = re.compile(r"^(\d{10})\.json$")
REQUIRED_RECORD_FIELDS = {
    "schema_version",
    "sequence",
    "shard_id",
    "block_coord",
    "row_range",
    "col_range",
    "input_shape",
    "input_shape_fingerprint",
    "plan",
    "plan_fingerprint",
    "accumulation_strategy",
    "cache_budget",
    "version_fingerprint",
    "static_version",
    "block_input_digest",
    "product_digest",
    "artifact",
    "artifact_file_sha256",
    "tail_flags",
    "record_checksum",
}


def _fsync_directory(path: Path) -> None:
    try:
        descriptor = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        pass
    finally:
        os.close(descriptor)


def atomic_write_bytes(path: Path, data: bytes) -> None:
    """Write bytes through a same-directory temp file and atomic replace."""

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.tmp-", dir=str(path.parent)
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
        _fsync_directory(path.parent)
    finally:
        if temporary_path.exists():
            try:
                temporary_path.unlink()
            except OSError:
                pass


def atomic_write_json(path: Path, value: Any) -> None:
    atomic_write_bytes(path, canonical_json_bytes(value) + b"\n")


def npy_bytes(array: np.ndarray) -> bytes:
    buffer = io.BytesIO()
    np.save(buffer, np.ascontiguousarray(array), allow_pickle=False)
    return buffer.getvalue()


def atomic_save_npy(path: Path, array: np.ndarray) -> str:
    data = npy_bytes(array)
    atomic_write_bytes(path, data)
    return sha256_bytes(data)


_PROCESS_LOCK_GUARD = threading.Lock()
_PROCESS_LOCKS: dict[str, threading.Lock] = {}


def _process_lock_for(path: Path) -> threading.Lock:
    key = os.path.normcase(str(path.resolve()))
    with _PROCESS_LOCK_GUARD:
        lock = _PROCESS_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _PROCESS_LOCKS[key] = lock
        return lock


class WorkspaceLock:
    """Process and thread-safe lock using fcntl or msvcrt."""

    def __init__(self, lock_path: Path, timeout: float = 30.0) -> None:
        self.lock_path = Path(lock_path)
        self.timeout = float(timeout)
        self._process_lock = _process_lock_for(self.lock_path)
        self._handle: Any = None
        self._is_windows = os.name == "nt"

    def __enter__(self) -> "WorkspaceLock":
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        if not self._process_lock.acquire(timeout=self.timeout):
            raise BlockGemmError(
                E_WORKSPACE,
                "timed out acquiring in-process workspace lock",
                reason="workspace_lock_timeout",
                details={"lock_path": str(self.lock_path)},
            )
        try:
            handle = open(self.lock_path, "a+b")
            self._handle = handle
            handle.seek(0, os.SEEK_END)
            if handle.tell() == 0:
                handle.write(b"\0")
                handle.flush()
            handle.seek(0)
            deadline = time.monotonic() + self.timeout
            while True:
                try:
                    if self._is_windows:
                        import msvcrt

                        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                    else:
                        import fcntl

                        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except OSError as error:
                    if time.monotonic() >= deadline:
                        raise BlockGemmError(
                            E_WORKSPACE,
                            "timed out acquiring workspace file lock",
                            reason="workspace_lock_timeout",
                            details={"lock_path": str(self.lock_path), "error": str(error)},
                        ) from error
                    time.sleep(0.01)
            return self
        except Exception:
            if self._handle is not None:
                self._handle.close()
                self._handle = None
            self._process_lock.release()
            raise

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        handle = self._handle
        self._handle = None
        try:
            if handle is not None:
                handle.seek(0)
                if self._is_windows:
                    import msvcrt

                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                handle.close()
        finally:
            self._process_lock.release()


@dataclass(frozen=True)
class WorkspacePaths:
    root: Path

    @property
    def meta(self) -> Path:
        return self.root / "meta"

    @property
    def checkpoints(self) -> Path:
        return self.root / "checkpoints"

    @property
    def blocks(self) -> Path:
        return self.root / "blocks"

    @property
    def output_dir(self) -> Path:
        return self.root / "output"

    @property
    def reports(self) -> Path:
        return self.root / "reports"

    @property
    def lock_file(self) -> Path:
        return self.meta / "lock"

    @property
    def stop_file(self) -> Path:
        return self.meta / "STOP.json"

    @property
    def run_spec_file(self) -> Path:
        return self.meta / "run-spec.json"

    @property
    def invalidation_file(self) -> Path:
        return self.meta / "invalidation.json"

    @property
    def summary_file(self) -> Path:
        return self.reports / "summary.json"

    @property
    def output_file(self) -> Path:
        return self.output_dir / "result.npy"

    def checkpoint(self, sequence: int) -> Path:
        return self.checkpoints / f"{sequence:010d}.json"

    def block(self, sequence: int) -> Path:
        return self.blocks / f"{sequence:010d}.npy"

    def block_relative(self, sequence: int) -> str:
        return f"blocks/{sequence:010d}.npy"

    def ensure(self) -> None:
        for directory in (
            self.root,
            self.meta,
            self.checkpoints,
            self.blocks,
            self.output_dir,
            self.reports,
        ):
            directory.mkdir(parents=True, exist_ok=True)


@dataclass
class ScanResult:
    records: dict[int, dict[str, Any]] = field(default_factory=dict)
    issues: dict[int, list[str]] = field(default_factory=dict)
    valid_prefix: int = 0
    max_observed_sequence: int = -1
    artifact_verified: bool = False

    def issue_summary(self) -> list[dict[str, Any]]:
        return [
            {"sequence": sequence, "reasons": reasons}
            for sequence, reasons in sorted(self.issues.items())
        ]


class CheckpointStore:
    def __init__(self, workspace: str | os.PathLike[str], *, timeout: float = 30.0) -> None:
        self.paths = WorkspacePaths(Path(workspace))
        self.timeout = timeout
        self.paths.ensure()

    def lock(self) -> WorkspaceLock:
        return WorkspaceLock(self.paths.lock_file, timeout=self.timeout)

    def _record_checksum(self, record: dict[str, Any]) -> str:
        payload = {key: value for key, value in record.items() if key != "record_checksum"}
        return sha256_json(payload)

    def _seal_record(self, record: dict[str, Any]) -> dict[str, Any]:
        sealed = dict(record)
        sealed["record_checksum"] = self._record_checksum(sealed)
        return sealed

    def _artifact_path_from_record(self, record: dict[str, Any]) -> Path | None:
        artifact = record.get("artifact")
        if not isinstance(artifact, str):
            return None
        if not artifact.startswith("blocks/") or "/../" in artifact or artifact.startswith("../"):
            return None
        return self.paths.root / Path(artifact)

    def _validate_record_object(
        self,
        record: Any,
        *,
        expected_sequence: int | None = None,
        verify_artifact: bool,
    ) -> tuple[bool, str]:
        if not isinstance(record, dict):
            return False, "record_is_not_an_object"
        missing = sorted(REQUIRED_RECORD_FIELDS - set(record))
        if missing:
            return False, f"missing_fields:{','.join(missing)}"
        sequence = record.get("sequence")
        if not isinstance(sequence, int) or sequence < 0:
            return False, "invalid_sequence"
        if expected_sequence is not None and sequence != expected_sequence:
            return False, "sequence_filename_mismatch"
        checksum = record.get("record_checksum")
        if not isinstance(checksum, str) or checksum != self._record_checksum(record):
            return False, "record_checksum_mismatch"
        artifact_path = self._artifact_path_from_record(record)
        if artifact_path is None:
            return False, "invalid_artifact_path"
        if artifact_path != self.paths.block(sequence):
            return False, "artifact_path_does_not_match_sequence"
        row_range = record.get("row_range")
        col_range = record.get("col_range")
        if (
            not isinstance(row_range, list)
            or len(row_range) != 2
            or not isinstance(col_range, list)
            or len(col_range) != 2
        ):
            return False, "invalid_block_ranges"
        if not artifact_path.is_file():
            return False, "missing_artifact"
        if verify_artifact:
            file_digest = sha256_bytes(artifact_path.read_bytes())
            if file_digest != record.get("artifact_file_sha256"):
                return False, "artifact_file_digest_mismatch"
            try:
                tile = np.load(artifact_path, allow_pickle=False)
            except Exception as error:  # malformed npy is an integrity failure
                return False, f"artifact_load_error:{type(error).__name__}"
            expected_shape = (row_range[1] - row_range[0], col_range[1] - col_range[0])
            if tile.shape != expected_shape:
                return False, "artifact_shape_mismatch"
            if array_payload_digest(tile) != record.get("product_digest"):
                return False, "product_digest_mismatch"
        return True, "ok"

    def scan(self, *, verify_artifacts: bool = False, lock: bool = True) -> ScanResult:
        if lock:
            with self.lock():
                return self._scan_unlocked(verify_artifacts=verify_artifacts)
        return self._scan_unlocked(verify_artifacts=verify_artifacts)

    def _scan_unlocked(self, *, verify_artifacts: bool) -> ScanResult:
        result = ScanResult(artifact_verified=verify_artifacts)
        raw_records: dict[int, list[dict[str, Any]]] = {}
        path_records: list[tuple[int, Path, dict[str, Any] | None, str | None]] = []
        sequence_occurrences: dict[int, int] = {}

        candidates = sorted(self.paths.checkpoints.glob("*.json"))
        for path in candidates:
            match = RECORD_NAME_RE.match(path.name)
            if match is None:
                continue
            filename_sequence = int(match.group(1))
            result.max_observed_sequence = max(result.max_observed_sequence, filename_sequence)
            try:
                record = json.loads(path.read_text(encoding="utf-8"))
            except Exception as error:
                result.issues.setdefault(filename_sequence, []).append(
                    f"record_json_error:{type(error).__name__}"
                )
                path_records.append((filename_sequence, path, None, None))
                continue
            path_records.append((filename_sequence, path, record, None))
            if isinstance(record, dict) and isinstance(record.get("sequence"), int):
                record_sequence = int(record["sequence"])
                sequence_occurrences[record_sequence] = sequence_occurrences.get(record_sequence, 0) + 1
                raw_records.setdefault(record_sequence, []).append(record)

        duplicate_sequences = {
            sequence for sequence, count in sequence_occurrences.items() if count > 1
        }
        for filename_sequence, path, record, _ in path_records:
            if record is None:
                continue
            sequence = record.get("sequence") if isinstance(record, dict) else None
            if isinstance(sequence, int) and sequence in duplicate_sequences:
                result.issues.setdefault(min(filename_sequence, sequence), []).append(
                    "duplicate_sequence"
                )
                continue
            ok, reason = self._validate_record_object(
                record,
                expected_sequence=filename_sequence,
                verify_artifact=verify_artifacts,
            )
            if not ok:
                result.issues.setdefault(filename_sequence, []).append(reason)
                continue
            result.records[filename_sequence] = record

        # A valid record after the first gap is visible for diagnostics but never reusable.
        cursor = 0
        while cursor in result.records and cursor not in result.issues:
            cursor += 1
        result.valid_prefix = cursor
        return result

    def _valid_prefix_unlocked(self) -> tuple[int, ScanResult]:
        scan = self._scan_unlocked(verify_artifacts=False)
        return scan.valid_prefix, scan

    def publish_block(
        self,
        record_base: dict[str, Any],
        product: np.ndarray,
        *,
        expected_plan_fingerprint: str | None = None,
    ) -> dict[str, Any]:
        """Atomically publish one complete block.

        The artifact is installed first, then the sealed record. The record is
        the commit marker. Sequence checks and both installs happen under the
        workspace file lock.
        """

        sequence = int(record_base.get("sequence", -1))
        shard_id = record_base.get("shard_id")
        block_coord = record_base.get("block_coord")
        if sequence < 0:
            raise invalid_shape(
                "checkpoint record has an invalid sequence",
                shard_id=shard_id,
                block_coord=block_coord,
                reason="invalid_sequence",
            )
        product = np.ascontiguousarray(product)
        row_range = record_base.get("row_range")
        col_range = record_base.get("col_range")
        if not isinstance(row_range, list) or not isinstance(col_range, list):
            raise invalid_shape(
                "checkpoint record lacks explicit output ranges",
                shard_id=shard_id,
                block_coord=block_coord,
                reason="missing_block_ranges",
            )
        expected_shape = (int(row_range[1]) - int(row_range[0]), int(col_range[1]) - int(col_range[0]))
        if product.shape != expected_shape:
            raise invalid_shape(
                "block product shape does not match its declared ranges",
                shard_id=shard_id,
                block_coord=block_coord,
                reason="product_shape_mismatch",
                details={"expected_shape": list(expected_shape), "actual_shape": list(product.shape)},
            )

        with self.lock():
            prefix, scan = self._valid_prefix_unlocked()
            if sequence != prefix:
                raise stale_checkpoint(
                    "checkpoint sequence is not the next contiguous block",
                    shard_id=shard_id,
                    block_coord=block_coord,
                    reason="sequence_not_contiguous",
                    details={
                        "attempted_sequence": sequence,
                        "expected_sequence": prefix,
                        "valid_prefix": prefix,
                        "issues": scan.issue_summary(),
                    },
                )
            if self.paths.checkpoint(sequence).exists():
                raise stale_checkpoint(
                    "checkpoint record already exists for this sequence",
                    shard_id=shard_id,
                    block_coord=block_coord,
                    reason="duplicate_checkpoint",
                    details={"sequence": sequence},
                )
            previous = scan.records.get(sequence - 1)
            if previous is not None:
                previous_plan = previous.get("plan_fingerprint")
                current_plan = record_base.get("plan_fingerprint")
                if previous_plan != current_plan:
                    raise stale_checkpoint(
                        "checkpoint plan differs from the preceding block",
                        shard_id=shard_id,
                        block_coord=block_coord,
                        reason="cross_plan_publish",
                        details={
                            "previous_plan_fingerprint": previous_plan,
                            "current_plan_fingerprint": current_plan,
                        },
                    )
                previous_artifact = self._artifact_path_from_record(previous)
                if previous_artifact is None or not previous_artifact.is_file():
                    raise invalid_shape(
                        "the preceding block artifact is missing",
                        shard_id=shard_id,
                        block_coord=block_coord,
                        reason="missing_predecessor_artifact",
                        details={"missing_sequence": sequence - 1},
                    )
            if (
                expected_plan_fingerprint is not None
                and record_base.get("plan_fingerprint") != expected_plan_fingerprint
            ):
                raise stale_checkpoint(
                    "checkpoint plan does not match the active executor plan",
                    shard_id=shard_id,
                    block_coord=block_coord,
                    reason="cross_plan_publish",
                    details={
                        "expected_plan_fingerprint": expected_plan_fingerprint,
                        "current_plan_fingerprint": record_base.get("plan_fingerprint"),
                    },
                )

            artifact_data = npy_bytes(product)
            artifact_path = self.paths.block(sequence)
            atomic_write_bytes(artifact_path, artifact_data)
            record = dict(record_base)
            record["artifact"] = self.paths.block_relative(sequence)
            record["artifact_file_sha256"] = sha256_bytes(artifact_data)
            record["product_digest"] = array_payload_digest(product)
            sealed = self._seal_record(record)
            atomic_write_json(self.paths.checkpoint(sequence), sealed)
            return sealed

    def invalidate_from(
        self,
        start_sequence: int,
        *,
        block_count: int,
        reason: str,
        details: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        start_sequence = max(0, int(start_sequence))
        with self.lock():
            removed_records: list[int] = []
            for path in sorted(self.paths.checkpoints.glob("*.json")):
                match = RECORD_NAME_RE.match(path.name)
                if match is None:
                    continue
                sequence = int(match.group(1))
                if sequence >= start_sequence:
                    try:
                        path.unlink()
                    except FileNotFoundError:
                        pass
                    removed_records.append(sequence)
            for path in sorted(self.paths.blocks.glob("*.npy")):
                match = re.match(r"^(\d{10})\.npy$", path.name)
                if match is None:
                    continue
                if int(match.group(1)) >= start_sequence:
                    try:
                        path.unlink()
                    except FileNotFoundError:
                        pass
            # A final artifact from an older version must never remain visible
            # while the new checkpoint prefix is being rebuilt.
            for stale_final in (
                self.paths.output_file,
                self.paths.output_dir / "result.json",
                self.paths.summary_file,
            ):
                try:
                    stale_final.unlink()
                except FileNotFoundError:
                    pass
            report = {
                "schema_version": 1,
                "reason": reason,
                "affected_start_sequence": start_sequence,
                "affected_end_sequence": max(start_sequence, int(block_count) - 1),
                "affected_block_count": max(0, int(block_count) - start_sequence),
                "downstream_invalidated": True,
                "removed_record_sequences": removed_records,
                "details": details or {},
            }
            atomic_write_json(self.paths.invalidation_file, report)
            return report

    def record_invalidation(
        self,
        *,
        start_sequence: int,
        block_count: int,
        reason: str,
        details: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        report = {
            "schema_version": 1,
            "reason": reason,
            "affected_start_sequence": max(0, int(start_sequence)),
            "affected_end_sequence": max(int(start_sequence), int(block_count) - 1),
            "affected_block_count": max(0, int(block_count) - int(start_sequence)),
            "downstream_invalidated": True,
            "removed_record_sequences": [],
            "details": details or {},
        }
        with self.lock():
            atomic_write_json(self.paths.invalidation_file, report)
        return report

    def require_block(
        self,
        sequence: int,
        *,
        expected_shard_id: str | None = None,
        expected_block_coord: list[int] | None = None,
        verify_artifact: bool = True,
    ) -> tuple[dict[str, Any], np.ndarray]:
        with self.lock():
            scan = self._scan_unlocked(verify_artifacts=verify_artifact)
            record = scan.records.get(int(sequence))
            if int(sequence) >= scan.valid_prefix or record is None:
                raise invalid_shape(
                    "requested block has no complete contiguous predecessor state",
                    shard_id=expected_shard_id,
                    block_coord=expected_block_coord,
                    reason="missing_prerequisite_block",
                    details={
                        "requested_sequence": int(sequence),
                        "valid_prefix": scan.valid_prefix,
                        "issues": scan.issue_summary(),
                    },
                )
            if expected_shard_id is not None and record.get("shard_id") != expected_shard_id:
                raise invalid_shape(
                    "checkpoint shard id does not match the requested block",
                    shard_id=expected_shard_id,
                    block_coord=expected_block_coord,
                    reason="checkpoint_shard_mismatch",
                    details={"record_shard_id": record.get("shard_id")},
                )
            if expected_block_coord is not None and record.get("block_coord") != expected_block_coord:
                raise invalid_shape(
                    "checkpoint block coordinate does not match the requested block",
                    shard_id=expected_shard_id,
                    block_coord=expected_block_coord,
                    reason="checkpoint_coord_mismatch",
                    details={"record_block_coord": record.get("block_coord")},
                )
            artifact_path = self._artifact_path_from_record(record)
            if artifact_path is None or not artifact_path.is_file():
                raise invalid_shape(
                    "checkpoint artifact is missing",
                    shard_id=expected_shard_id,
                    block_coord=expected_block_coord,
                    reason="missing_block_artifact",
                    details={"sequence": int(sequence)},
                )
            tile = np.load(artifact_path, allow_pickle=False)
            row_range = record["row_range"]
            col_range = record["col_range"]
            expected_shape = (row_range[1] - row_range[0], col_range[1] - col_range[0])
            if tile.shape != expected_shape:
                raise invalid_shape(
                    "checkpoint artifact shape does not match its record",
                    shard_id=expected_shard_id,
                    block_coord=expected_block_coord,
                    reason="artifact_shape_mismatch",
                    details={"expected_shape": list(expected_shape), "actual_shape": list(tile.shape)},
                )
            return record, tile

    def assemble_from_checkpoints(
        self,
        *,
        output_shape: tuple[int, ...],
        dtype: np.dtype,
        block_count: int,
        descriptors: list[Any],
    ) -> tuple[np.ndarray, str, dict[int, dict[str, Any]]]:
        with self.lock():
            scan = self._scan_unlocked(verify_artifacts=True)
            if scan.valid_prefix < block_count:
                missing = scan.valid_prefix
                descriptor = descriptors[missing] if missing < len(descriptors) else None
                raise invalid_shape(
                    "final assembly requires the complete contiguous checkpoint prefix",
                    shard_id=getattr(descriptor, "shard_id", None),
                    block_coord=getattr(descriptor, "block_coord", None),
                    reason="missing_prerequisite_block",
                    details={
                        "missing_sequence": missing,
                        "valid_prefix": scan.valid_prefix,
                        "block_count": block_count,
                        "issues": scan.issue_summary(),
                    },
                )
            output = np.zeros(output_shape, dtype=dtype)
            for descriptor in descriptors:
                record = scan.records[descriptor.sequence]
                expected_fields = {
                    "shard_id": descriptor.shard_id,
                    "batch_index": descriptor.batch_index,
                    "block_coord": descriptor.block_coord,
                    "row_range": descriptor.row_range,
                    "col_range": descriptor.col_range,
                    "plan_fingerprint": descriptor.plan_fingerprint,
                    "input_shape_fingerprint": descriptor.shape_fingerprint,
                    "static_version": descriptor.static_version,
                    "accumulation_strategy": descriptor.accumulation_strategy,
                    "block_input_digest": descriptor.block_input_digest,
                }
                mismatches = [
                    field
                    for field, expected_value in expected_fields.items()
                    if record.get(field) != expected_value
                ]
                if mismatches:
                    raise stale_checkpoint(
                        "final assembly encountered a checkpoint from a different block state",
                        shard_id=descriptor.shard_id,
                        block_coord=descriptor.block_coord,
                        reason="assembly_checkpoint_state_mismatch",
                        details={"sequence": descriptor.sequence, "mismatched_fields": mismatches},
                    )
                artifact_path = self._artifact_path_from_record(record)
                if artifact_path is None:
                    raise invalid_shape(
                        "checkpoint artifact path is invalid",
                        shard_id=descriptor.shard_id,
                        block_coord=descriptor.block_coord,
                        reason="invalid_artifact_path",
                    )
                tile = np.load(artifact_path, allow_pickle=False)
                if tile.shape != (descriptor.row_end - descriptor.row_start, descriptor.col_end - descriptor.col_start):
                    raise invalid_shape(
                        "checkpoint artifact shape does not match its block descriptor",
                        shard_id=descriptor.shard_id,
                        block_coord=descriptor.block_coord,
                        reason="artifact_shape_mismatch",
                        details={"actual_shape": list(tile.shape)},
                    )
                if descriptor.batch_index is None:
                    output[
                        descriptor.row_start : descriptor.row_end,
                        descriptor.col_start : descriptor.col_end,
                    ] = tile
                else:
                    output[
                        descriptor.batch_index,
                        descriptor.row_start : descriptor.row_end,
                        descriptor.col_start : descriptor.col_end,
                    ] = tile
        return output, array_payload_digest(output), scan.records

    def save_output(self, output: np.ndarray) -> str:
        file_digest = atomic_save_npy(self.paths.output_file, output)
        metadata = {
            "schema_version": 1,
            "file": "output/result.npy",
            "shape": list(output.shape),
            "dtype": output.dtype.str,
            "array_payload_digest": array_payload_digest(output),
            "file_sha256": file_digest,
        }
        atomic_write_json(self.paths.output_dir / "result.json", metadata)
        return file_digest

    def write_run_spec(self, spec: dict[str, Any]) -> None:
        with self.lock():
            atomic_write_json(self.paths.run_spec_file, spec)

    def read_run_spec(self) -> dict[str, Any] | None:
        try:
            return json.loads(self.paths.run_spec_file.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
        except Exception:
            return None

    def write_summary(self, summary: dict[str, Any]) -> None:
        atomic_write_json(self.paths.summary_file, summary)

    def read_summary(self) -> dict[str, Any] | None:
        try:
            return json.loads(self.paths.summary_file.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
        except Exception:
            return None

    def read_invalidation(self) -> dict[str, Any] | None:
        try:
            return json.loads(self.paths.invalidation_file.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
        except Exception:
            return None

    def request_stop(self, reason: str = "external_request") -> dict[str, Any]:
        marker = {"schema_version": 1, "requested": True, "reason": reason}
        atomic_write_json(self.paths.stop_file, marker)
        return marker

    def stop_requested(self) -> bool:
        return self.paths.stop_file.is_file()

    def read_stop(self) -> dict[str, Any] | None:
        try:
            return json.loads(self.paths.stop_file.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
        except Exception:
            return {"schema_version": 1, "requested": True, "reason": "unreadable_stop_marker"}

    def clear_stop(self) -> None:
        try:
            self.paths.stop_file.unlink()
        except FileNotFoundError:
            pass

    def status(self) -> dict[str, Any]:
        with self.lock():
            scan = self._scan_unlocked(verify_artifacts=True)
        spec = self.read_run_spec()
        total_blocks = spec.get("block_count") if isinstance(spec, dict) else None
        last_record = scan.records.get(scan.valid_prefix - 1) if scan.valid_prefix else None
        return {
            "schema_version": 1,
            "workspace": str(self.paths.root),
            "valid_prefix": scan.valid_prefix,
            "next_sequence": scan.valid_prefix,
            "block_count": total_blocks,
            "complete": bool(total_blocks is not None and scan.valid_prefix == total_blocks),
            "checkpoint_count": len(scan.records),
            "max_observed_sequence": scan.max_observed_sequence,
            "artifact_verified": True,
            "issues": scan.issue_summary(),
            "last_record": {
                "sequence": last_record.get("sequence"),
                "shard_id": last_record.get("shard_id"),
                "block_coord": last_record.get("block_coord"),
                "product_digest": last_record.get("product_digest"),
                "version_fingerprint": last_record.get("version_fingerprint"),
            }
            if last_record
            else None,
            "run_spec": spec,
            "last_invalidation": self.read_invalidation(),
            "stop_requested": self.stop_requested(),
            "stop_marker": self.read_stop(),
            "summary_available": self.paths.summary_file.is_file(),
            "output_available": self.paths.output_file.is_file(),
        }
