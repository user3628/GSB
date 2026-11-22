"""Atomic, chained, file-locked checkpoint storage."""

from __future__ import annotations

import contextlib
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

from .canonical import atomic_write_json, digest
from .config import Settings
from .errors import E_STALE_CHECKPOINT, raise_error

CHECKPOINT_RE = re.compile(r"^checkpoint_(\d{8})\.json$")


class FileLock:
    """Real inter-process exclusive lock (fcntl on POSIX, msvcrt on Windows)."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._handle = None

    def __enter__(self) -> "FileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = self.path.open("a+b")
        try:
            if os.name == "nt":  # pragma: no cover - exercised on Windows
                import msvcrt

                self._handle.seek(0)
                if self._handle.read(1) == b"":
                    self._handle.write(b"\0")
                    self._handle.flush()
                self._handle.seek(0)
                msvcrt.locking(self._handle.fileno(), msvcrt.LK_LOCK, 1)
            else:
                import fcntl

                fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX)
        except Exception:
            self._handle.close()
            self._handle = None
            raise
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        assert self._handle is not None
        try:
            if os.name == "nt":  # pragma: no cover - exercised on Windows
                import msvcrt

                self._handle.seek(0)
                msvcrt.locking(self._handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
        finally:
            self._handle.close()
            self._handle = None


@dataclass
class CheckpointScan:
    sequence_id: str
    records: list[dict[str, Any]]
    issues: list[dict[str, Any]]
    total_checkpoint_files: int
    first_invalid_ordinal: int | None

    @property
    def count(self) -> int:
        return len(self.records)

    @property
    def next_frame_index(self) -> int:
        return self.count

    @property
    def last_record(self) -> dict[str, Any] | None:
        return self.records[-1] if self.records else None

    @property
    def last_state(self) -> dict[str, Any] | None:
        return self.records[-1]["state"] if self.records else None

    @property
    def chain_digest(self) -> str:
        return digest([record["checkpoint_digest"] for record in self.records])

    def complete(self, frame_count: int) -> bool:
        return self.count == frame_count


def checkpoint_filename(ordinal: int) -> str:
    return f"checkpoint_{int(ordinal):08d}.json"


def _record_digest(record: dict[str, Any]) -> str:
    return digest({k: v for k, v in record.items() if k != "checkpoint_digest"})


def finalize_checkpoint(record: dict[str, Any]) -> dict[str, Any]:
    record = dict(record)
    record.pop("checkpoint_digest", None)
    record["checkpoint_digest"] = _record_digest(record)
    return record


def validate_checkpoint_record(record: Any, *, expected_sequence_id: str | None = None) -> str | None:
    if not isinstance(record, dict):
        return "record_not_object"
    if record.get("kind") != "tracklab.checkpoint":
        return "wrong_kind"
    if expected_sequence_id is not None and record.get("sequence_id") != expected_sequence_id:
        return "sequence_mismatch"
    ordinal = record.get("ordinal")
    frame_index = record.get("frame_index")
    if not isinstance(ordinal, int) or ordinal < 1:
        return "invalid_ordinal"
    if not isinstance(frame_index, int) or frame_index != ordinal - 1:
        return "frame_ordinal_mismatch"
    if record.get("checkpoint_digest") != _record_digest(record):
        return "checkpoint_digest_mismatch"
    state = record.get("state")
    if not isinstance(state, dict):
        return "missing_state"
    if record.get("state_digest") != digest(state):
        return "state_digest_mismatch"
    if state.get("sequence_id") != record.get("sequence_id"):
        return "state_sequence_mismatch"
    if state.get("frame_index") != frame_index:
        return "state_frame_mismatch"
    return None


class CheckpointStore:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def directory(self, sequence_id: str) -> Path:
        return self.settings.sequence_checkpoint_dir(sequence_id)

    def lock_path(self, sequence_id: str) -> Path:
        return self.settings.locks_dir / f"checkpoints_{sequence_id}.lock"

    @contextlib.contextmanager
    def locked(self, sequence_id: str) -> Iterator[None]:
        with FileLock(self.lock_path(sequence_id)):
            yield

    def _scan_unlocked(self, sequence_id: str) -> CheckpointScan:
        directory = self.directory(sequence_id)
        issues: list[dict[str, Any]] = []
        parsed: list[tuple[Path, int, dict[str, Any] | None, str | None]] = []
        files = sorted(directory.glob("checkpoint_*.json")) if directory.exists() else []
        for path in files:
            match = CHECKPOINT_RE.fullmatch(path.name)
            filename_ordinal = int(match.group(1)) if match else None
            try:
                record = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError) as exc:
                parsed.append((path, filename_ordinal or -1, None, f"parse_error:{exc.__class__.__name__}"))
                continue
            issue = validate_checkpoint_record(record, expected_sequence_id=sequence_id)
            if issue is None and filename_ordinal != record["ordinal"]:
                issue = "filename_ordinal_mismatch"
            parsed.append((path, record.get("ordinal", -1) if isinstance(record, dict) else -1, record if issue is None else None, issue))

        by_ordinal: dict[int, list[tuple[Path, dict[str, Any] | None, str | None]]] = {}
        for path, ordinal, record, issue in parsed:
            by_ordinal.setdefault(int(ordinal), []).append((path, record, issue))

        records: list[dict[str, Any]] = []
        previous_state_digest: str | None = None
        expected = 1
        first_invalid: int | None = None
        while True:
            entries = by_ordinal.get(expected, [])
            if not entries:
                if any(ordinal > expected for ordinal in by_ordinal):
                    first_invalid = expected
                    issues.append({"ordinal": expected, "frame_index": expected - 1, "reason": "gap_or_missing_prefix"})
                break
            if len(entries) != 1:
                first_invalid = expected
                issues.append({"ordinal": expected, "frame_index": expected - 1, "reason": "duplicate_ordinal"})
                break
            path, record, issue = entries[0]
            if record is None or issue is not None:
                first_invalid = expected
                issues.append(
                    {
                        "ordinal": expected,
                        "frame_index": expected - 1,
                        "reason": issue or "invalid_record",
                        "path": str(path),
                    }
                )
                break
            if record.get("previous_state_digest") != previous_state_digest:
                first_invalid = expected
                issues.append(
                    {
                        "ordinal": expected,
                        "frame_index": expected - 1,
                        "reason": "checkpoint_chain_mismatch",
                        "path": str(path),
                    }
                )
                break
            records.append(record)
            previous_state_digest = record["state_digest"]
            expected += 1
        if first_invalid is None and any(ordinal >= expected for ordinal in by_ordinal):
            first_invalid = expected
        return CheckpointScan(
            sequence_id=sequence_id,
            records=records,
            issues=issues,
            total_checkpoint_files=len(files),
            first_invalid_ordinal=first_invalid,
        )

    def scan(self, sequence_id: str) -> CheckpointScan:
        with self.locked(sequence_id):
            return self._scan_unlocked(sequence_id)

    def publish(self, sequence_id: str, record: dict[str, Any]) -> dict[str, Any]:
        with self.locked(sequence_id):
            scan = self._scan_unlocked(sequence_id)
            expected_ordinal = scan.count + 1
            expected_frame = scan.count
            if record.get("sequence_id") != sequence_id:
                raise_error(
                    E_STALE_CHECKPOINT,
                    "cross-sequence checkpoint publication was rejected",
                    context={
                        "expected_sequence_id": sequence_id,
                        "record_sequence_id": record.get("sequence_id"),
                        "expected_frame_index": expected_frame,
                        "record_frame_index": record.get("frame_index"),
                    },
                )
            record_ordinal = record.get("ordinal")
            record_frame = record.get("frame_index")
            if record_ordinal != expected_ordinal or record_frame != expected_frame:
                raise_error(
                    E_STALE_CHECKPOINT,
                    "checkpoint publication is not the next contiguous state",
                    context={
                        "sequence_id": sequence_id,
                        "expected_ordinal": expected_ordinal,
                        "record_ordinal": record_ordinal,
                        "expected_frame_index": expected_frame,
                        "record_frame_index": record_frame,
                        "reason": (
                            "duplicate_or_rollback"
                            if isinstance(record_frame, int) and record_frame < expected_frame
                            else "gap"
                        ),
                    },
                )
            expected_previous = scan.last_record["state_digest"] if scan.last_record else None
            if record.get("previous_state_digest") != expected_previous:
                raise_error(
                    E_STALE_CHECKPOINT,
                    "checkpoint previous-state digest does not match the published chain",
                    context={
                        "sequence_id": sequence_id,
                        "expected_previous_state_digest": expected_previous,
                        "record_previous_state_digest": record.get("previous_state_digest"),
                        "record_state_digest": record.get("state_digest"),
                        "reason": "state_replacement_or_chain_mismatch",
                    },
                )
            finalized = finalize_checkpoint(record)
            issue = validate_checkpoint_record(finalized, expected_sequence_id=sequence_id)
            if issue is not None:
                raise_error(
                    E_STALE_CHECKPOINT,
                    "checkpoint payload failed integrity validation",
                    context={"sequence_id": sequence_id, "reason": issue, "record_ordinal": record_ordinal},
                )
            path = self.directory(sequence_id) / checkpoint_filename(expected_ordinal)
            atomic_write_json(path, finalized)
            return {
                "ok": True,
                "sequence_id": sequence_id,
                "ordinal": expected_ordinal,
                "frame_index": expected_frame,
                "checkpoint_digest": finalized["checkpoint_digest"],
                "state_digest": finalized["state_digest"],
                "path": str(path),
            }

    def quarantine_from(self, sequence_id: str, from_frame: int, *, token: str) -> list[str]:
        """Atomically move all files at/after from_frame out of the live chain."""
        with self.locked(sequence_id):
            directory = self.directory(sequence_id)
            target_dir = self.settings.invalidated_dir / sequence_id / token
            moved: list[str] = []
            if not directory.exists():
                return moved
            for path in sorted(directory.glob("checkpoint_*.json")):
                match = CHECKPOINT_RE.fullmatch(path.name)
                if not match:
                    continue
                ordinal = int(match.group(1))
                frame = ordinal - 1
                if frame < from_frame:
                    continue
                target_dir.mkdir(parents=True, exist_ok=True)
                target = target_dir / path.name
                if target.exists():
                    target = target_dir / f"{path.stem}.{digest([path.name, str(path.stat().st_mtime_ns)])[:8]}.json"
                os.replace(path, target)
                moved.append(target.relative_to(self.settings.root).as_posix())
            _fsync_directory(directory)
            return moved

    def read_record(self, sequence_id: str, frame_index: int) -> dict[str, Any] | None:
        scan = self.scan(sequence_id)
        if 0 <= frame_index < scan.count:
            return scan.records[frame_index]
        return None

    def read_state(self, sequence_id: str, frame_index: int) -> dict[str, Any]:
        record = self.read_record(sequence_id, frame_index)
        if record is None:
            raise_error(
                E_STALE_CHECKPOINT,
                "requested frame state is not present in the valid checkpoint prefix",
                context={"sequence_id": sequence_id, "frame_index": frame_index},
            )
        return record["state"]


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":  # pragma: no cover - Windows handles directory replacement differently
        return
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
