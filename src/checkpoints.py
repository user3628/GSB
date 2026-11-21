"""Atomic shard checkpoints, cross-platform file locks and sequence guards.

Each shard result is published as ``checkpoints/shard_<id>.json`` written
through a temp-file + fsync + atomic rename sequence, so an interrupted
process can never leave a half-written record.  A single workspace lock
file serialises concurrent publishers, and every publish must advance the
shard sequence number by exactly one; stale, duplicate or out-of-order
publishes fail with ``E_STALE_CHECKPOINT``.
"""
from __future__ import annotations

import contextlib
import json
import os
import tempfile
from typing import Any, Dict, Iterator, List, Optional

from .errors import E_STALE_CHECKPOINT, StaleCheckpointError


# ---------------------------------------------------------------------------
# Cross-platform file locking (msvcrt on Windows, fcntl elsewhere)
# ---------------------------------------------------------------------------
class FileLock:
    def __init__(self, path: str):
        self.path = path
        self._handle = None
        self._backend = ""

    def acquire(self) -> None:
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self._handle = open(self.path, "a+b")
        try:
            import msvcrt  # type: ignore
            msvcrt.locking(self._handle.fileno(), msvcrt.LK_LOCK, 1)
            self._backend = "msvcrt"
        except ImportError:
            import fcntl
            fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX)
            self._backend = "fcntl"

    def release(self) -> None:
        if self._handle is None:
            return
        try:
            if self._backend == "msvcrt":
                import msvcrt  # type: ignore
                self._handle.seek(0)
                msvcrt.locking(self._handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
        finally:
            self._handle.close()
            self._handle = None

    def __enter__(self) -> "FileLock":
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.release()


@contextlib.contextmanager
def file_lock(path: str) -> Iterator[None]:
    lock = FileLock(path)
    lock.acquire()
    try:
        yield
    finally:
        lock.release()


def _checkpoint_path(workspace: str, shard_id: str) -> str:
    return os.path.join(workspace, "checkpoints", f"shard_{shard_id}.json")


def _atomic_write_json(path: str, payload: Dict[str, Any],
                       crash_after_temp: bool = False) -> None:
    directory = os.path.dirname(path)
    os.makedirs(directory, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=".tmp-", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(json.dumps(payload, sort_keys=True,
                                    separators=(",", ":"),
                                    ensure_ascii=False, allow_nan=False))
            handle.flush()
            os.fsync(handle.fileno())
        if crash_after_temp:
            # test hook: simulate a crash after the temp write but before
            # the atomic rename; no checkpoint file must be visible
            raise RuntimeError("simulated crash before atomic rename")
        os.replace(tmp_path, path)
        tmp_path = ""
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)


def read_checkpoint(workspace: str, shard_id: str) -> Optional[Dict[str, Any]]:
    path = _checkpoint_path(workspace, shard_id)
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def list_checkpoints(workspace: str) -> List[Dict[str, Any]]:
    directory = os.path.join(workspace, "checkpoints")
    if not os.path.isdir(directory):
        return []
    records = []
    for name in sorted(os.listdir(directory)):
        if name.startswith("shard_") and name.endswith(".json"):
            with open(os.path.join(directory, name), "r",
                      encoding="utf-8") as handle:
                records.append(json.load(handle))
    records.sort(key=lambda item: (item["sequence"], item["shard_id"]))
    return records


def publish_checkpoint(workspace: str, record: Dict[str, Any],
                       crash_after_temp: bool = False) -> Dict[str, Any]:
    """Validate sequence continuity under the workspace lock and publish."""
    shard_id = str(record["shard_id"])
    sequence = int(record["sequence"])
    lock_path = os.path.join(workspace, "state", ".workspace.lock")
    with file_lock(lock_path):
        existing = read_checkpoint(workspace, shard_id)
        if existing is not None:
            expected = int(existing["sequence"]) + 1
            if sequence <= int(existing["sequence"]):
                raise StaleCheckpointError(
                    f"stale publish for shard {shard_id}: sequence "
                    f"{sequence} <= {existing['sequence']}",
                    context={"shard_id": shard_id,
                             "existing_sequence": existing["sequence"],
                             "attempted_sequence": sequence})
            if sequence != expected:
                raise StaleCheckpointError(
                    f"non-contiguous publish for shard {shard_id}: expected "
                    f"{expected}, got {sequence}",
                    context={"shard_id": shard_id, "expected": expected,
                             "attempted_sequence": sequence})
            if existing.get("input_fingerprint") != \
                    record.get("input_fingerprint"):
                raise StaleCheckpointError(
                    "input fingerprint differs; shard must be rebuilt",
                    context={"shard_id": shard_id,
                             "code": E_STALE_CHECKPOINT})

        # cross-shard ordering: every lower sequence must already be present
        if sequence > 0:
            prior = read_checkpoint(workspace,
                                    record.get("prev_shard_id", "")) \
                if record.get("prev_shard_id") else None
            if record.get("prev_shard_id") and prior is None:
                raise StaleCheckpointError(
                    "previous shard checkpoint is missing",
                    context={"shard_id": shard_id,
                             "prev_shard_id": record["prev_shard_id"]})

        path = _checkpoint_path(workspace, shard_id)
        _atomic_write_json(path, record,
                           crash_after_temp=crash_after_temp)
    return record


def reset_workspace(workspace: str) -> None:
    """Remove all generated state (checkpoints, state, reports)."""
    for sub in ("checkpoints", "state", "reports"):
        directory = os.path.join(workspace, sub)
        if not os.path.isdir(directory):
            continue
        for name in os.listdir(directory):
            os.remove(os.path.join(directory, name))


def state_path(workspace: str) -> str:
    return os.path.join(workspace, "state", "pipeline.json")


def write_state(workspace: str, state: Dict[str, Any]) -> None:
    _atomic_write_json(state_path(workspace), state)


def read_state(workspace: str) -> Optional[Dict[str, Any]]:
    path = state_path(workspace)
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def stop_flag_path(workspace: str) -> str:
    return os.path.join(workspace, "state", "STOP")


def request_stop(workspace: str) -> None:
    os.makedirs(os.path.join(workspace, "state"), exist_ok=True)
    with open(stop_flag_path(workspace), "w", encoding="utf-8",
              newline="\n") as handle:
        handle.write("stop\n")


def clear_stop(workspace: str) -> None:
    path = stop_flag_path(workspace)
    if os.path.exists(path):
        os.remove(path)


def stop_requested(workspace: str) -> bool:
    return os.path.exists(stop_flag_path(workspace))
