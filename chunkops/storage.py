"""Atomic files, real file locks, checkpoints, version chains, and result cache."""

from __future__ import annotations

import fcntl
import json
import os
import tempfile
import time
from pathlib import Path
from typing import Any

import numpy as np

from .errors import (
    ChunkOpsError,
    E_INVALID_STATE,
    E_INVALID_SHAPE,
    E_STALE_PUBLISH,
    E_STALE_VERSION,
)
from .fingerprint import (
    canonical_json,
    digest_object,
    sha256_array,
    sha256_file,
    sha256_text,
)

STAGES = ("plan", "reduce", "validate", "summary")


def fsync_directory(path: Path) -> None:
    directory_fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def atomic_write_bytes(path: str | os.PathLike[str], data: bytes) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=target.name + ".", suffix=".tmp", dir=target.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
        fsync_directory(target.parent)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def atomic_write_json(
    path: str | os.PathLike[str], value: Any, *, pretty: bool = True
) -> None:
    if pretty:
        text = json.dumps(
            value,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        )
    else:
        text = canonical_json(value)
    atomic_write_bytes(path, (text + "\n").encode("utf-8"))


def read_json(path: str | os.PathLike[str]) -> Any:
    target = Path(path)
    if not target.exists():
        raise ChunkOpsError(
            E_INVALID_STATE, "expected JSON file is missing", {"path": str(target.resolve())}
        )
    try:
        return json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ChunkOpsError(
            E_INVALID_STATE,
            "JSON file is corrupt",
            {"path": str(target.resolve()), "cause": type(exc).__name__},
        ) from exc


def atomic_save_npy(path: str | os.PathLike[str], array: np.ndarray) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=target.name + ".", suffix=".tmp.npy", dir=target.parent)
    os.close(fd)
    try:
        with open(temporary, "wb") as handle:
            np.save(handle, np.asarray(array), allow_pickle=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
        fsync_directory(target.parent)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


class FileLock:
    """Advisory fcntl flock, exclusive by default."""

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        exclusive: bool = True,
        timeout: float | None = 30.0,
    ) -> None:
        self.path = Path(path)
        self.exclusive = exclusive
        self.timeout = timeout
        self._handle: Any = None

    def __enter__(self) -> "FileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = open(self.path, "a+", encoding="utf-8")
        operation = fcntl.LOCK_EX if self.exclusive else fcntl.LOCK_SH
        if self.timeout is None:
            fcntl.flock(self._handle.fileno(), operation)
            return self
        deadline = time.monotonic() + self.timeout
        while True:
            try:
                fcntl.flock(self._handle.fileno(), operation | fcntl.LOCK_NB)
                return self
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise ChunkOpsError(
                        E_INVALID_STATE,
                        "timed out acquiring file lock",
                        {"path": str(self.path.resolve()), "timeout_seconds": self.timeout},
                    )
                time.sleep(0.01)

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        if self._handle is not None:
            fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
            self._handle.close()
            self._handle = None


class CheckpointManager:
    """Append-only stage checkpoints with digest and predecessor validation."""

    def __init__(self, workspace: str | os.PathLike[str]) -> None:
        self.workspace = Path(workspace)
        self.directory = self.workspace / "checkpoints"
        self.directory.mkdir(parents=True, exist_ok=True)
        self.lock_path = self.directory / ".lock"

    def _path_for(self, stage: str) -> Path:
        index = STAGES.index(stage) + 1
        return self.directory / f"{index:04d}_{stage}.json"

    @staticmethod
    def _record_digest(record: dict[str, Any]) -> str:
        payload = {key: value for key, value in record.items() if key != "digest"}
        return digest_object(payload)

    def discover(self) -> dict[str, Any]:
        with FileLock(self.lock_path):
            return self._discover_locked()

    def _discover_locked(self) -> dict[str, Any]:
        valid: list[dict[str, Any]] = []
        previous_digest = ""
        expected_index = 1
        for stage in STAGES:
            path = self._path_for(stage)
            if not path.exists():
                break
            try:
                record = read_json(path)
            except ChunkOpsError as exc:
                return {
                    "latest_stage": valid[-1]["stage"] if valid else None,
                    "latest_seq": valid[-1]["seq"] if valid else 0,
                    "valid_checkpoints": valid,
                    "resume_stage": STAGES[expected_index - 1],
                    "invalid": {"stage": stage, "path": str(path), "error": exc.to_dict()},
                }
            digest = self._record_digest(record)
            problems = []
            if record.get("digest") != digest:
                problems.append("digest_mismatch")
            if record.get("stage") != stage:
                problems.append("stage_mismatch")
            if int(record.get("seq", -1)) != expected_index:
                problems.append("sequence_mismatch")
            if record.get("prev_digest", "") != previous_digest:
                problems.append("predecessor_mismatch")
            if problems:
                return {
                    "latest_stage": valid[-1]["stage"] if valid else None,
                    "latest_seq": valid[-1]["seq"] if valid else 0,
                    "valid_checkpoints": valid,
                    "resume_stage": STAGES[expected_index - 1],
                    "invalid": {
                        "stage": stage,
                        "path": str(path),
                        "problems": problems,
                    },
                }
            valid.append(record)
            previous_digest = record["digest"]
            expected_index += 1
        latest_stage = valid[-1]["stage"] if valid else None
        resume_stage = STAGES[len(valid)] if len(valid) < len(STAGES) else None
        return {
            "latest_stage": latest_stage,
            "latest_seq": valid[-1]["seq"] if valid else 0,
            "valid_checkpoints": valid,
            "resume_stage": resume_stage,
            "invalid": None,
        }

    def latest(self) -> dict[str, Any] | None:
        info = self.discover()
        records = info["valid_checkpoints"]
        return records[-1] if records else None

    def write(self, stage: str, payload: dict[str, Any]) -> dict[str, Any]:
        if stage not in STAGES:
            raise ChunkOpsError(
                E_INVALID_STATE, f"unknown pipeline stage: {stage}", {"stage": stage}
            )
        with FileLock(self.lock_path):
            info = self._discover_locked()
            expected_seq = STAGES.index(stage) + 1
            previous_digest = ""
            if expected_seq > 1:
                previous_stage = STAGES[expected_seq - 2]
                previous = next(
                    (
                        record
                        for record in info["valid_checkpoints"]
                        if record["stage"] == previous_stage
                    ),
                    None,
                )
                if previous is None:
                    raise ChunkOpsError(
                        E_INVALID_STATE,
                        "cannot write checkpoint before its predecessor",
                        {
                            "stage": stage,
                            "required_predecessor": previous_stage,
                            "latest_stage": info["latest_stage"],
                        },
                    )
                previous_digest = previous["digest"]
            record = {
                "schema_version": 1,
                "stage": stage,
                "seq": expected_seq,
                "prev_digest": previous_digest,
                "payload": payload,
            }
            record["digest"] = self._record_digest(record)
            atomic_write_json(self._path_for(stage), record)
            # Remove stale successors left over from a failed/rolled-back run.
            for successor_index in range(expected_seq, len(STAGES)):
                successor = self.directory / (
                    f"{successor_index + 1:04d}_{STAGES[successor_index]}.json"
                )
                if successor.exists():
                    successor.unlink()
            fsync_directory(self.directory)
            return record

    def reset_after_corruption(self) -> dict[str, Any]:
        """Return discoverable state without deleting evidence."""

        return self.discover()


class VersionStore:
    """Atomic monotonic version chain with an external rollback/tamper guard."""

    def __init__(self, workspace: str | os.PathLike[str], kind: str = "plans") -> None:
        if not kind or any(char in kind for char in "/\\."):
            raise ChunkOpsError(
                E_INVALID_SHAPE, "invalid version store kind", {"kind": kind}
            )
        self.workspace = Path(workspace)
        self.kind = kind
        self.root = self.workspace / "versioned" / kind
        self.records = self.root / "records"
        self.records.mkdir(parents=True, exist_ok=True)
        self.lock_path = self.root / "LOCK"
        self.head_path = self.root / "HEAD.json"
        self.guard_path = self.workspace / "versioned" / ".guards" / f"{kind}.json"

    def _record_path(self, seq: int) -> Path:
        return self.records / f"{int(seq):016d}.json"

    @staticmethod
    def _record_digest(record: dict[str, Any]) -> str:
        return digest_object({key: value for key, value in record.items() if key != "digest"})

    def _read_head(self) -> dict[str, Any] | None:
        if not self.head_path.exists():
            return None
        try:
            return read_json(self.head_path)
        except ChunkOpsError:
            return {"__corrupt__": True}

    def _read_guard(self) -> dict[str, Any] | None:
        if not self.guard_path.exists():
            return None
        try:
            return read_json(self.guard_path)
        except ChunkOpsError:
            return {"__corrupt__": True}

    def _stale(self, reason: str, message: str, details: dict[str, Any] | None = None) -> ChunkOpsError:
        payload = {"kind": self.kind, "stale_reason": reason, **(details or {})}
        return ChunkOpsError(E_STALE_VERSION, message, payload)

    def _verify_locked(self) -> dict[str, Any]:
        head = self._read_head()
        guard = self._read_guard()
        if head is not None and head.get("__corrupt__"):
            raise self._stale("corrupt_head", "version HEAD is corrupt")
        if guard is not None and guard.get("__corrupt__"):
            raise self._stale("corrupt_guard", "version guard is corrupt")
        if head is None:
            if guard and int(guard.get("max_seq", 0)) > 0:
                raise self._stale(
                    "rollback",
                    "version HEAD disappeared after a committed sequence",
                    {"guard_max_seq": int(guard.get("max_seq", 0))},
                )
            return {"status": "empty", "seq": 0, "digest": "", "payload": None, "payload_digest": ""}
        path = Path(head.get("record", ""))
        if not path.is_absolute():
            path = self.root / path
        if not path.exists():
            raise self._stale(
                "rollback",
                "current version record disappeared",
                {"head_seq": int(head.get("seq", 0)), "record": str(path)},
            )
        record = read_json(path)
        recomputed = self._record_digest(record)
        if record.get("digest") != recomputed:
            raise self._stale(
                "tamper",
                "version record digest mismatch",
                {"head_seq": int(head.get("seq", 0)), "record": str(path)},
            )
        if str(record.get("digest")) != str(head.get("digest")):
            raise self._stale(
                "tamper",
                "HEAD digest does not match version record",
                {"head_seq": int(head.get("seq", 0)), "record": str(path)},
            )
        payload = record.get("payload", {})
        payload_digest = digest_object(payload)
        if record.get("payload_digest") != payload_digest:
            raise self._stale(
                "tamper",
                "version payload digest mismatch",
                {"head_seq": int(head.get("seq", 0))},
            )
        prev_seq = int(record.get("prev_seq", 0))
        if prev_seq:
            previous_path = self._record_path(prev_seq)
            if not previous_path.exists():
                raise self._stale(
                    "rollback",
                    "version chain predecessor is missing",
                    {"head_seq": int(record.get("seq", 0)), "prev_seq": prev_seq},
                )
            previous = read_json(previous_path)
            if previous.get("digest") != record.get("prev_digest"):
                raise self._stale(
                    "tamper",
                    "version chain predecessor digest mismatch",
                    {"head_seq": int(record.get("seq", 0)), "prev_seq": prev_seq},
                )
        if guard is None:
            raise self._stale(
                "tamper",
                "external version guard is missing",
                {"head_seq": int(record.get("seq", 0))},
            )
        guard_seq = int(guard.get("max_seq", 0))
        head_seq = int(record.get("seq", 0))
        if head_seq < guard_seq:
            raise self._stale(
                "rollback",
                "version sequence rolled back relative to external guard",
                {"head_seq": head_seq, "guard_max_seq": guard_seq},
            )
        if head_seq > guard_seq:
            raise self._stale(
                "tamper",
                "version sequence is ahead of external guard",
                {"head_seq": head_seq, "guard_max_seq": guard_seq},
            )
        if str(record.get("digest")) != str(guard.get("head_digest")):
            raise self._stale(
                "tamper",
                "external guard digest does not match current version",
                {"head_seq": head_seq},
            )
        if record.get("payload_digest") != guard.get("payload_digest"):
            raise self._stale(
                "tamper",
                "external guard payload digest does not match current version",
                {"head_seq": head_seq},
            )
        shape_fp = payload.get("shape_fingerprint")
        if shape_fp != guard.get("shape_fingerprint"):
            raise self._stale(
                "shape_fingerprint_changed",
                "current version shape fingerprint differs from guard",
                {
                    "head_seq": head_seq,
                    "observed_shape_fingerprint": shape_fp,
                    "guarded_shape_fingerprint": guard.get("shape_fingerprint"),
                },
            )
        return {
            "status": "healthy",
            "seq": head_seq,
            "digest": record.get("digest"),
            "payload": payload,
            "payload_digest": record.get("payload_digest"),
            "record_path": str(path),
        }

    def verify(self) -> dict[str, Any]:
        with FileLock(self.lock_path):
            return self._verify_locked()

    def inspect(self, *, include_payload: bool = False) -> dict[str, Any]:
        try:
            result = self.verify()
            value = {
                "kind": self.kind,
                "status": result["status"],
                "latest_seq": result["seq"],
                "latest_digest": result["digest"],
                "payload_digest": result.get("payload_digest", ""),
            }
            if include_payload:
                value["payload"] = result.get("payload")
            return value
        except ChunkOpsError as exc:
            return {
                "kind": self.kind,
                "status": exc.details.get("stale_reason", "invalid"),
                "code": exc.code,
                "message": exc.message,
                "details": exc.details,
            }

    def publish(
        self,
        payload: dict[str, Any],
        *,
        expected_seq: int | None = None,
        scope: dict[str, Any] | None = None,
        publisher: str | None = None,
    ) -> dict[str, Any]:
        with FileLock(self.lock_path):
            current = self._verify_locked()
            current_seq = int(current["seq"])
            if scope is not None:
                payload = dict(payload)
                payload.setdefault("scope", scope)
            if expected_seq is not None and int(expected_seq) != current_seq:
                raise ChunkOpsError(
                    E_STALE_PUBLISH,
                    "publisher compared a stale sequence",
                    {
                        "kind": self.kind,
                        "expected_seq": int(expected_seq),
                        "current_seq": current_seq,
                        "publisher": publisher,
                    },
                )
            if current_seq and scope is not None:
                previous_scope = current["payload"].get("scope")
                if previous_scope is not None and previous_scope != scope:
                    raise ChunkOpsError(
                        E_STALE_PUBLISH,
                        "publisher crossed plan scope",
                        {
                            "kind": self.kind,
                            "current_seq": current_seq,
                            "current_scope": previous_scope,
                            "publisher_scope": scope,
                            "publisher": publisher,
                        },
                    )
            previous_shape = (
                current["payload"].get("shape_fingerprint") if current_seq else None
            )
            observed_shape = payload.get("shape_fingerprint")
            if previous_shape is not None and observed_shape != previous_shape:
                raise self._stale(
                    "shape_fingerprint_changed",
                    "shape fingerprint changed inside one versioned run",
                    {
                        "current_seq": current_seq,
                        "previous_shape_fingerprint": previous_shape,
                        "observed_shape_fingerprint": observed_shape,
                    },
                )
            seq = current_seq + 1
            record = {
                "schema_version": 1,
                "kind": self.kind,
                "seq": seq,
                "prev_seq": current_seq,
                "prev_digest": current["digest"],
                "payload": payload,
                "payload_digest": digest_object(payload),
                "publisher": publisher,
            }
            record["digest"] = self._record_digest(record)
            record_path = self._record_path(seq)
            atomic_write_json(record_path, record)
            head = {
                "kind": self.kind,
                "seq": seq,
                "digest": record["digest"],
                "payload_digest": record["payload_digest"],
                "record": str(record_path.relative_to(self.root)),
            }
            atomic_write_json(self.head_path, head)
            guard = {
                "kind": self.kind,
                "max_seq": seq,
                "head_digest": record["digest"],
                "payload_digest": record["payload_digest"],
                "shape_fingerprint": observed_shape,
                "scope": scope,
            }
            atomic_write_json(self.guard_path, guard)
            return {
                "kind": self.kind,
                "seq": seq,
                "digest": record["digest"],
                "payload_digest": record["payload_digest"],
                "record_path": str(record_path),
                "status": "published",
            }



class ResultCache:
    """Content-addressed deterministic npy cache with integrity checks."""

    def __init__(self, workspace: str | os.PathLike[str]) -> None:
        self.workspace = Path(workspace)
        self.root = self.workspace / "cache" / "results"
        self.root.mkdir(parents=True, exist_ok=True)
        self.lock_path = self.root / "LOCK"

    def _array_path(self, key: str) -> Path:
        return self.root / f"{key}.npy"

    def _meta_path(self, key: str) -> Path:
        return self.root / f"{key}.json"

    def put(self, key: str, array: np.ndarray, metadata: dict[str, Any]) -> dict[str, Any]:
        if len(key) < 32 or any(char not in "0123456789abcdef" for char in key):
            raise ChunkOpsError(
                E_INVALID_SHAPE, "cache key must be a sha256 hex digest", {"cache_key": key}
            )
        digest = sha256_array(array)
        meta = {
            "schema_version": 1,
            "cache_key": key,
            "array_digest": digest,
            "shape": [int(value) for value in array.shape],
            "dtype": str(array.dtype),
            **metadata,
        }
        with FileLock(self.lock_path):
            if self._meta_path(key).exists():
                existing = read_json(self._meta_path(key))
                if existing.get("array_digest") != digest:
                    raise ChunkOpsError(
                        E_STALE_PUBLISH,
                        "conflicting result cache publication for one key",
                        {
                            "cache_key": key,
                            "existing_digest": existing.get("array_digest"),
                            "new_digest": digest,
                        },
                    )
                return existing
            atomic_save_npy(self._array_path(key), array)
            atomic_write_json(self._meta_path(key), meta)
        return meta

    def get(self, key: str) -> tuple[np.ndarray, dict[str, Any]] | None:
        meta_path = self._meta_path(key)
        array_path = self._array_path(key)
        if not meta_path.exists() or not array_path.exists():
            return None
        try:
            with FileLock(self.lock_path):
                meta = read_json(meta_path)
                if meta.get("cache_key") != key:
                    return None
                array = np.load(array_path, allow_pickle=False)
                if [int(value) for value in array.shape] != meta.get("shape"):
                    return None
                if sha256_array(array) != meta.get("array_digest"):
                    return None
                return array, meta
        except (ChunkOpsError, OSError, ValueError):
            return None

    def invalidate(self, key: str) -> None:
        with FileLock(self.lock_path):
            for path in (self._meta_path(key), self._array_path(key)):
                if path.exists():
                    path.unlink()
            fsync_directory(self.root)


def file_artifact(path: str | os.PathLike[str]) -> dict[str, Any]:
    target = Path(path)
    if not target.exists():
        raise ChunkOpsError(
            E_INVALID_STATE, "artifact is missing", {"path": str(target.resolve())}
        )
    return {
        "path": str(target.resolve()),
        "size_bytes": int(target.stat().st_size),
        "sha256": sha256_file(target),
    }


def bytes_artifact_digest(data: bytes) -> dict[str, Any]:
    return {"size_bytes": len(data), "sha256": sha256_text(data.decode("utf-8", errors="replace"))}
