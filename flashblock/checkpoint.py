"""Versioned, atomic checkpoint publication and recovery."""

from __future__ import annotations

import json
import os
import re
import shutil
import uuid
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Iterable

import numpy as np

from .config import AttentionConfig
from .digest import canonical_json, sha256_arrays
from .errors import fail
from .locking import FileLock
from .state import RowBlockState


RECORD_SUFFIX = ".ckpt.npz"
_RECORD_NAME_RE = re.compile(r"^r(?P<row>\d{6})-s(?P<seq>\d{10})\.ckpt\.npz$")
_UNSET = object()


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    try:
        descriptor = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    try:
        with open(temporary, "xb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        if temporary.exists():
            try:
                temporary.unlink()
            except OSError:
                pass


def atomic_write_json(path: Path, value: Any) -> None:
    atomic_write_bytes(path, json.dumps(value, indent=2, sort_keys=True).encode("utf-8"))


def _shard_directory(root: Path, shard_id: str) -> Path:
    import hashlib

    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", shard_id).strip("._") or "shard"
    suffix = hashlib.sha256(shard_id.encode("utf-8")).hexdigest()[:12]
    return root / f"shard-{safe[:36]}-{suffix}"


@dataclass
class RunHeader:
    format: str
    run_number: int
    run_id: str
    shard_id: str
    mode: str
    parent_run_id: str | None
    base_seq: int
    expected_row_blocks: int
    config: dict[str, Any]
    config_fingerprint: str
    q_fingerprint: str
    kv_fingerprint: str
    input_fingerprint: str
    q_shape: list[int]
    k_shape: list[int]
    v_shape: list[int]
    invalidation_id: str | None = None
    invalidation_reasons: list[str] | None = None

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "RunHeader":
        if value.get("format") != "flashblock/run-v1":
            raise ValueError("not a flashblock run header")
        return cls(**value)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class RecordInfo:
    path: Path
    metadata: dict[str, Any]
    sequence: int
    row_block: int


@dataclass
class ScanResult:
    header: RunHeader
    records: list[RecordInfo]
    anomalies: list[dict[str, Any]]
    blocking_reason: str | None

    @property
    def committed_row_blocks(self) -> int:
        return len(self.records)

    @property
    def available_sequence(self) -> int:
        if not self.records:
            return self.header.base_seq - 1
        return self.records[-1].sequence

    @property
    def complete(self) -> bool:
        return len(self.records) == self.header.expected_row_blocks


class CheckpointStore:
    def __init__(self, workspace: str | Path, shard_id: str) -> None:
        self.workspace = Path(workspace).resolve()
        self.shard_id = shard_id
        self.root = _shard_directory(self.workspace, shard_id)
        self.runs_root = self.root / "runs"
        self.quarantine_root = self.root / "quarantine"
        self.lock_path = self.root / ".publish.lock"
        self.runs_root.mkdir(parents=True, exist_ok=True)
        self.quarantine_root.mkdir(parents=True, exist_ok=True)

    def _lock(self) -> FileLock:
        return FileLock(self.lock_path)

    def _run_directory(self, run_id: str) -> Path:
        return self.runs_root / run_id

    def _next_run_number(self, headers: list[RunHeader]) -> int:
        maximum = max((header.run_number for header in headers), default=0)
        if self.runs_root.exists():
            for child in self.runs_root.iterdir():
                match = re.match(r"^run-(\d+)$", child.name)
                if child.is_dir() and match is not None:
                    maximum = max(maximum, int(match.group(1)))
        return maximum + 1

    def _header_path(self, run_id: str) -> Path:
        return self._run_directory(run_id) / "run.json"

    def read_header(self, run_id: str) -> RunHeader:
        path = self._header_path(run_id)
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
            header = RunHeader.from_dict(value)
        except Exception as exc:
            fail(
                "E_INVALID_STAGE",
                f"Unreadable or invalid run header {run_id}",
                shard_id=self.shard_id,
                run_id=run_id,
                error=str(exc),
            )
        if header.shard_id != self.shard_id:
            fail(
                "E_INVALID_STAGE",
                "Run header belongs to a different shard",
                expected_shard_id=self.shard_id,
                actual_shard_id=header.shard_id,
            )
        return header

    def list_headers(self) -> list[RunHeader]:
        headers: list[RunHeader] = []
        for child in sorted(self.runs_root.iterdir()) if self.runs_root.exists() else []:
            if not child.is_dir() or not child.name.startswith("run-"):
                continue
            try:
                headers.append(self.read_header(child.name))
            except Exception:
                continue
        headers.sort(key=lambda item: (item.run_number, item.run_id))
        return headers

    def latest_header(self) -> RunHeader | None:
        headers = self.list_headers()
        return headers[-1] if headers else None

    @staticmethod
    def _record_sort_key(record: RecordInfo) -> tuple[int, int]:
        return record.sequence, record.row_block

    def _read_record(self, path: Path, *, deep: bool) -> tuple[RecordInfo, RowBlockState | None]:
        try:
            with np.load(path, allow_pickle=False) as payload:
                if "meta_json" not in payload.files:
                    raise ValueError("missing meta_json")
                metadata = json.loads(str(payload["meta_json"].item()))
                if metadata.get("format") != "flashblock/checkpoint-v1":
                    raise ValueError("unknown checkpoint format")
                expected_keys = set(metadata.get("array_shapes", {}))
                missing = sorted(expected_keys - set(payload.files))
                if missing:
                    raise ValueError(f"missing arrays: {missing}")
                if not deep:
                    state = None
                else:
                    arrays = {key: np.ascontiguousarray(payload[key]) for key in expected_keys}
                    if sha256_arrays(arrays) != metadata.get("payload_sha256"):
                        raise ValueError("payload checksum mismatch")
                    state = RowBlockState.from_payload(metadata, arrays)
        except Exception as exc:
            raise ValueError(f"invalid checkpoint {path.name}: {exc}") from exc
        info = RecordInfo(
            path=path,
            metadata=metadata,
            sequence=int(metadata["sequence"]),
            row_block=int(metadata["row_block"]),
        )
        return info, state

    def scan_run(self, run_id: str | None = None) -> ScanResult:
        header = self.read_header(run_id) if run_id is not None else self.latest_header()
        if header is None:
            fail("E_INVALID_STAGE", "No checkpoint run is available", shard_id=self.shard_id)
        run_dir = self._run_directory(header.run_id)
        anomalies: list[dict[str, Any]] = []
        by_sequence: dict[int, list[RecordInfo]] = {}
        for path in sorted(run_dir.glob(f"*{RECORD_SUFFIX}")) if run_dir.exists() else []:
            try:
                info, _ = self._read_record(path, deep=False)
            except Exception as exc:
                anomalies.append({"type": "unreadable_record", "path": path.name, "error": str(exc)})
                continue
            if info.metadata.get("run_id") != header.run_id:
                anomalies.append(
                    {
                        "type": "wrong_run_record",
                        "path": path.name,
                        "record_run_id": info.metadata.get("run_id"),
                    }
                )
                continue
            if not (header.base_seq <= info.sequence < header.base_seq + header.expected_row_blocks):
                anomalies.append(
                    {
                        "type": "sequence_out_of_range",
                        "path": path.name,
                        "sequence": info.sequence,
                    }
                )
                continue
            by_sequence.setdefault(info.sequence, []).append(info)

        records: list[RecordInfo] = []
        blocking_reason: str | None = None
        for offset in range(header.expected_row_blocks):
            sequence = header.base_seq + offset
            candidates = by_sequence.get(sequence, [])
            if not candidates:
                blocking_reason = "missing_next_record"
                break
            if len(candidates) > 1:
                anomalies.append(
                    {
                        "type": "duplicate_sequence",
                        "sequence": sequence,
                        "paths": sorted(item.path.name for item in candidates),
                    }
                )
                blocking_reason = "duplicate_sequence"
                break
            candidate = candidates[0]
            if candidate.row_block != offset:
                anomalies.append(
                    {
                        "type": "row_block_misaligned",
                        "sequence": sequence,
                        "expected_row_block": offset,
                        "actual_row_block": candidate.row_block,
                    }
                )
                blocking_reason = "row_block_misaligned"
                break
            records.append(candidate)
        if len(records) == header.expected_row_blocks:
            for sequence, candidates in sorted(by_sequence.items()):
                if sequence >= header.base_seq + header.expected_row_blocks:
                    anomalies.append({"type": "extra_sequence", "sequence": sequence})
        records.sort(key=self._record_sort_key)
        return ScanResult(
            header=header,
            records=records,
            anomalies=anomalies,
            blocking_reason=blocking_reason,
        )

    def _quarantine_suffix(self, run_id: str, start_row: int) -> list[str]:
        run_dir = self._run_directory(run_id)
        target = self.quarantine_root / f"{run_id}-{uuid.uuid4().hex}"
        target.mkdir(parents=True, exist_ok=True)
        moved: list[str] = []
        if not run_dir.exists():
            return moved
        for path in sorted(run_dir.glob(f"*{RECORD_SUFFIX}")):
            should_move = False
            match = _RECORD_NAME_RE.match(path.name)
            if match is None:
                should_move = True
            elif int(match.group("row")) >= start_row:
                should_move = True
            if should_move:
                destination = target / path.name
                os.replace(path, destination)
                moved.append(path.name)
        if not moved:
            try:
                target.rmdir()
            except OSError:
                pass
        _fsync_directory(run_dir)
        return moved

    def recover_prefix(self, run_id: str | None = None) -> tuple[list[RowBlockState], ScanResult, list[dict[str, Any]]]:
        with self._lock():
            scan = self.scan_run(run_id)
            if scan.blocking_reason in {"duplicate_sequence", "row_block_misaligned"}:
                moved = self._quarantine_suffix(scan.header.run_id, scan.committed_row_blocks)
                anomalies = list(scan.anomalies)
                if moved:
                    anomalies.append(
                        {
                            "type": "quarantined_suffix",
                            "start_row_block": scan.committed_row_blocks,
                            "files": moved,
                        }
                    )
                scan = self.scan_run(scan.header.run_id)
            else:
                anomalies = list(scan.anomalies)

            states: list[RowBlockState] = []
            for record in scan.records:
                try:
                    _, state = self._read_record(record.path, deep=True)
                except Exception as exc:
                    moved = self._quarantine_suffix(scan.header.run_id, record.row_block)
                    anomalies.append(
                        {
                            "type": "quarantined_corrupt_payload",
                            "row_block": record.row_block,
                            "error": str(exc),
                            "files": moved,
                        }
                    )
                    scan = self.scan_run(scan.header.run_id)
                    break
                assert state is not None
                states.append(state)
            return states, scan, anomalies

    def load_states(self, run_id: str | None = None, *, require_complete: bool = True) -> list[RowBlockState]:
        states, scan, _ = self.recover_prefix(run_id)
        if require_complete and not scan.complete:
            fail(
                "E_INVALID_STAGE",
                "Checkpoint run is incomplete",
                shard_id=self.shard_id,
                run_id=scan.header.run_id,
                committed_row_blocks=scan.committed_row_blocks,
                expected_row_blocks=scan.header.expected_row_blocks,
                available_sequence=scan.available_sequence,
            )
        return states

    def create_run(
        self,
        *,
        config: AttentionConfig,
        q_shape: tuple[int, ...],
        k_shape: tuple[int, ...],
        v_shape: tuple[int, ...],
        fingerprints: dict[str, str],
        mode: str,
        parent_run_id: str | None = None,
        invalidation_id: str | None = None,
        invalidation_reasons: list[str] | None = None,
        expected_latest_run_id: str | None | object = _UNSET,
    ) -> RunHeader:
        with self._lock():
            headers = self.list_headers()
            if expected_latest_run_id is not _UNSET:
                actual_latest = headers[-1].run_id if headers else None
                if actual_latest != expected_latest_run_id:
                    fail(
                        "E_STALE_CHECKPOINT",
                        "Latest run changed before a new run could be created",
                        shard_id=self.shard_id,
                        expected_latest_run_id=expected_latest_run_id,
                        actual_latest_run_id=actual_latest,
                    )
            if headers:
                latest = headers[-1]
                try:
                    latest_scan = self.scan_run(latest.run_id)
                except Exception:
                    latest_scan = None
                if latest_scan is not None and not latest_scan.complete:
                    fail(
                        "E_INVALID_STAGE",
                        "Cannot start a new run while the latest run is incomplete",
                        shard_id=self.shard_id,
                        latest_run_id=latest.run_id,
                        committed_row_blocks=latest_scan.committed_row_blocks,
                        expected_row_blocks=latest_scan.header.expected_row_blocks,
                        available_sequence=latest_scan.available_sequence,
                    )
            run_number = self._next_run_number(headers)
            run_id = f"run-{run_number:04d}"
            last_sequence = self._maximum_sequence()
            base_seq = last_sequence + 1
            expected_row_blocks = (q_shape[2] + config.row_block_size - 1) // config.row_block_size
            header = RunHeader(
                format="flashblock/run-v1",
                run_number=run_number,
                run_id=run_id,
                shard_id=self.shard_id,
                mode=mode,
                parent_run_id=parent_run_id,
                base_seq=base_seq,
                expected_row_blocks=expected_row_blocks,
                config=config.to_dict(),
                config_fingerprint=fingerprints["config_fingerprint"],
                q_fingerprint=fingerprints["q_fingerprint"],
                kv_fingerprint=fingerprints["kv_fingerprint"],
                input_fingerprint=fingerprints["input_fingerprint"],
                q_shape=list(q_shape),
                k_shape=list(k_shape),
                v_shape=list(v_shape),
                invalidation_id=invalidation_id,
                invalidation_reasons=invalidation_reasons,
            )
            run_dir = self._run_directory(run_id)
            run_dir.mkdir(parents=True, exist_ok=False)
            atomic_write_json(run_dir / "run.json", header.to_dict())
            return header

    def _maximum_sequence(self) -> int:
        maximum = 0
        for header in self.list_headers():
            run_dir = self._run_directory(header.run_id)
            for path in run_dir.glob(f"*{RECORD_SUFFIX}") if run_dir.exists() else []:
                try:
                    info, _ = self._read_record(path, deep=False)
                except Exception:
                    continue
                if info.metadata.get("run_id") == header.run_id:
                    maximum = max(maximum, info.sequence)
        return maximum

    def _write_record(self, run_dir: Path, state: RowBlockState, run_id: str, sequence: int) -> Path:
        metadata, arrays = state.to_payload(sequence, run_id)
        metadata["payload_sha256"] = sha256_arrays(arrays)
        metadata["array_shapes"] = {key: list(value.shape) for key, value in arrays.items()}
        final_path = run_dir / f"r{state.row_block:06d}-s{sequence:010d}{RECORD_SUFFIX}"
        temporary = run_dir / f".{final_path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
        try:
            with open(temporary, "xb") as handle:
                np.savez(
                    handle,
                    meta_json=np.asarray(json.dumps(metadata, sort_keys=True, separators=(",", ":"))),
                    **arrays,
                )
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, final_path)
            _fsync_directory(run_dir)
        finally:
            if temporary.exists():
                try:
                    temporary.unlink()
                except OSError:
                    pass
        return final_path

    def publish_next(self, state: RowBlockState, *, run_id: str | None = None) -> dict[str, Any]:
        with self._lock():
            header = self.read_header(run_id) if run_id is not None else self.latest_header()
            if header is None:
                fail("E_INVALID_STAGE", "No active checkpoint run", shard_id=self.shard_id)
            scan = self.scan_run(header.run_id)
            if scan.blocking_reason in {"duplicate_sequence", "row_block_misaligned"}:
                fail(
                    "E_STALE_CHECKPOINT",
                    "Checkpoint stream is blocked by a duplicate or misaligned record",
                    shard_id=self.shard_id,
                    run_id=header.run_id,
                    reason=scan.blocking_reason,
                    available_sequence=scan.available_sequence,
                    committed_row_blocks=scan.committed_row_blocks,
                )
            expected_row = scan.committed_row_blocks
            expected_sequence = header.base_seq + expected_row
            if state.row_block != expected_row:
                fail(
                    "E_STALE_CHECKPOINT",
                    "Checkpoint publication is stale or crosses row blocks",
                    shard_id=self.shard_id,
                    run_id=header.run_id,
                    submitted_row_block=state.row_block,
                    expected_row_block=expected_row,
                    available_sequence=scan.available_sequence,
                    expected_sequence=expected_sequence,
                )
            if state.sequence not in (0, expected_sequence):
                fail(
                    "E_STALE_CHECKPOINT",
                    "Checkpoint sequence is not the next monotonic sequence",
                    shard_id=self.shard_id,
                    run_id=header.run_id,
                    submitted_sequence=state.sequence,
                    expected_sequence=expected_sequence,
                    available_sequence=scan.available_sequence,
                )
            if state.run_id not in ("", header.run_id):
                fail(
                    "E_STALE_CHECKPOINT",
                    "Checkpoint belongs to a different run",
                    shard_id=self.shard_id,
                    submitted_run_id=state.run_id,
                    expected_run_id=header.run_id,
                )
            if (
                state.config_fingerprint != header.config_fingerprint
                or state.input_fingerprint != header.input_fingerprint
            ):
                fail(
                    "E_STALE_CHECKPOINT",
                    "Checkpoint fingerprint does not match the active run",
                    shard_id=self.shard_id,
                    run_id=header.run_id,
                    state_config_fingerprint=state.config_fingerprint,
                    run_config_fingerprint=header.config_fingerprint,
                    state_input_fingerprint=state.input_fingerprint,
                    run_input_fingerprint=header.input_fingerprint,
                )
            path = self._write_record(self._run_directory(header.run_id), state, header.run_id, expected_sequence)
            return {
                "run_id": header.run_id,
                "row_block": state.row_block,
                "sequence": expected_sequence,
                "path": str(path),
            }

    def publish_snapshot(
        self,
        *,
        config: AttentionConfig,
        q_shape: tuple[int, ...],
        k_shape: tuple[int, ...],
        v_shape: tuple[int, ...],
        fingerprints: dict[str, str],
        states: list[RowBlockState],
        mode: str,
        parent_run_id: str | None,
        invalidation: dict[str, Any] | None,
        schedule_decisions: Iterable[dict[str, Any]] = (),
    ) -> RunHeader:
        expected_rows = (q_shape[2] + config.row_block_size - 1) // config.row_block_size
        if len(states) != expected_rows or [item.row_block for item in states] != list(range(expected_rows)):
            fail(
                "E_INVALID_STAGE",
                "Snapshot requires one complete, ordered state per row block",
                expected_row_blocks=expected_rows,
                supplied_row_blocks=[item.row_block for item in states],
            )
        with self._lock():
            latest = self.latest_header()
            if parent_run_id is not None:
                if latest is None or latest.run_id != parent_run_id:
                    fail(
                        "E_STALE_CHECKPOINT",
                        "Parent run changed while an incremental snapshot was being prepared",
                        shard_id=self.shard_id,
                        expected_parent_run_id=parent_run_id,
                        actual_latest_run_id=latest.run_id if latest else None,
                    )
            headers = self.list_headers()
            run_number = self._next_run_number(headers)
            run_id = f"run-{run_number:04d}"
            base_seq = self._maximum_sequence() + 1
            invalidation_id = invalidation.get("invalidation_id") if invalidation else None
            reasons = invalidation.get("reasons") if invalidation else None
            header = RunHeader(
                format="flashblock/run-v1",
                run_number=run_number,
                run_id=run_id,
                shard_id=self.shard_id,
                mode=mode,
                parent_run_id=parent_run_id,
                base_seq=base_seq,
                expected_row_blocks=expected_rows,
                config=config.to_dict(),
                config_fingerprint=fingerprints["config_fingerprint"],
                q_fingerprint=fingerprints["q_fingerprint"],
                kv_fingerprint=fingerprints["kv_fingerprint"],
                input_fingerprint=fingerprints["input_fingerprint"],
                q_shape=list(q_shape),
                k_shape=list(k_shape),
                v_shape=list(v_shape),
                invalidation_id=invalidation_id,
                invalidation_reasons=reasons,
            )
            staging = self.runs_root / f".staging-{uuid.uuid4().hex}"
            final = self._run_directory(run_id)
            staging.mkdir(parents=True, exist_ok=False)
            try:
                atomic_write_json(staging / "run.json", header.to_dict())
                if invalidation is not None:
                    atomic_write_json(staging / "invalidation.json", invalidation)
                schedule_values = list(schedule_decisions)
                if schedule_values:
                    with open(staging / "schedule.jsonl", "w", encoding="utf-8") as handle:
                        for decision in schedule_values:
                            handle.write(canonical_json(decision) + "\n")
                        handle.flush()
                        os.fsync(handle.fileno())
                for offset, state in enumerate(states):
                    self._write_record(staging, state, run_id, base_seq + offset)
                _fsync_directory(staging)
                os.replace(staging, final)
                _fsync_directory(self.runs_root)
            except BaseException:
                if staging.exists():
                    shutil.rmtree(staging, ignore_errors=True)
                raise
            return header

    def append_schedule(self, decision: dict[str, Any], *, run_id: str | None = None) -> None:
        with self._lock():
            header = self.read_header(run_id) if run_id is not None else self.latest_header()
            if header is None:
                return
            path = self._run_directory(header.run_id) / "schedule.jsonl"
            with open(path, "a", encoding="utf-8") as handle:
                handle.write(canonical_json(decision) + "\n")
                handle.flush()
                os.fsync(handle.fileno())

    def load_schedule(self, run_id: str | None = None) -> list[dict[str, Any]]:
        header = self.read_header(run_id) if run_id is not None else self.latest_header()
        if header is None:
            return []
        path = self._run_directory(header.run_id) / "schedule.jsonl"
        if not path.exists():
            return []
        decisions = []
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                decisions.append(value)
        return decisions

    def query_state(self, run_id: str | None = None) -> dict[str, Any]:
        header = self.read_header(run_id) if run_id is not None else self.latest_header()
        if header is None:
            return {
                "shard_id": self.shard_id,
                "has_state": False,
                "available_sequence": 0,
                "committed_row_blocks": 0,
            }
        scan = self.scan_run(header.run_id)
        return {
            "shard_id": self.shard_id,
            "has_state": True,
            "run": header.to_dict(),
            "committed_row_blocks": scan.committed_row_blocks,
            "expected_row_blocks": header.expected_row_blocks,
            "complete": scan.complete,
            "available_sequence": scan.available_sequence,
            "blocking_reason": scan.blocking_reason,
            "records": [
                {
                    "sequence": item.sequence,
                    "row_block": item.row_block,
                    "q_interval": [item.metadata["q_start"], item.metadata["q_end"]],
                    "key_interval": [item.metadata["key_start"], item.metadata["key_end"]],
                    "row_m_sha256": item.metadata["row_m_sha256"],
                    "row_l_sha256": item.metadata["row_l_sha256"],
                    "row_o_sha256": item.metadata["row_o_sha256"],
                    "input_fingerprint": item.metadata["input_fingerprint"],
                }
                for item in scan.records
            ],
            "anomalies": scan.anomalies,
        }

    def query_invalidations(self) -> dict[str, Any]:
        header = self.latest_header()
        if header is None or not header.invalidation_id:
            return {"shard_id": self.shard_id, "has_invalidation": False}
        path = self._run_directory(header.run_id) / "invalidation.json"
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            fail(
                "E_INVALID_STAGE",
                "Invalidation report is missing or unreadable",
                shard_id=self.shard_id,
                run_id=header.run_id,
                error=str(exc),
            )
        value["has_invalidation"] = True
        return value

