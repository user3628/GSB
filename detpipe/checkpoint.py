"""Persistent shard records with cross-platform file locking.

Checkpoint layout::

    <root>/
      manifest.json
      STOP                     # presence of this file requests a graceful stop
      shards/
        shard_000000.json ...  # one atomic record per shard, contiguous indices
      _tmp/                    # temporary files used by atomic writes

Locks:

* POSIX   : ``fcntl.flock`` exclusive lock on ``shards/.lock``
* Windows : ``msvcrt.locking`` exclusive byte-range lock on the same file

Both rely only on the Python standard library.  Publication is only allowed
for the exact next contiguous shard index; older indices are rejected with
``E_STALE_CHECKPOINT`` and gaps with ``E_INVALID_STAGE``.
"""
from __future__ import annotations

import contextlib
import json
import os
from pathlib import Path
from typing import Any, Iterator

from .errors import (
    E_INVALID_STAGE,
    E_STALE_CHECKPOINT,
    E_VERSION_MISMATCH,
    PipeError,
)

SHARD_PREFIX = "shard_"
SHARD_SUFFIX = ".json"
MANIFEST_NAME = "manifest.json"
STOP_NAME = "STOP"
LOCK_NAME = ".lock"


class _FileLock:
    """Exclusive cross-process lock, standard library only."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._fh: Any = None
        self._windows = os.name == "nt"

    def acquire(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(self._path, "a+b")
        if self._windows:  # pragma: no cover - exercised on Windows hosts
            import msvcrt

            # One-byte exclusive lock; blocks until every other holder releases.
            self._fh.seek(0)
            while True:
                try:
                    msvcrt.locking(self._fh.fileno(), msvcrt.LK_LOCK, 1)
                    break
                except OSError:
                    continue
        else:
            import fcntl

            fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX)

    def release(self) -> None:
        if self._fh is None:
            return
        if self._windows:  # pragma: no cover
            import msvcrt

            self._fh.seek(0)
            try:
                msvcrt.locking(self._fh.fileno(), msvcrt.LK_UNLCK, 1)
            except OSError:
                pass
        else:
            import fcntl

            fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
        self._fh.close()
        self._fh = None

    def __enter__(self) -> "_FileLock":
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_dir = path.parent.parent / "_tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    tmp_path = tmp_dir / f".{path.name}.{os.getpid()}.tmp"
    data = json.dumps(payload, sort_keys=True, indent=2, ensure_ascii=False)
    with open(tmp_path, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(data)
        fh.write("\n")
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp_path, path)


def shard_filename(index: int) -> str:
    return f"{SHARD_PREFIX}{index:06d}{SHARD_SUFFIX}"


class CheckpointStore:
    def __init__(self, root: str | os.PathLike) -> None:
        self.root = Path(root)
        self.shard_dir = self.root / "shards"
        self.tmp_dir = self.root / "_tmp"
        self.manifest_path = self.root / MANIFEST_NAME
        self.stop_path = self.root / STOP_NAME
        self.lock_path = self.shard_dir / LOCK_NAME
        self._lock = _FileLock(self.lock_path)

    # ---------------------------------------------------------------- workspace
    def exists(self) -> bool:
        return self.manifest_path.exists()

    def write_manifest(self, manifest: dict[str, Any]) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        self.shard_dir.mkdir(parents=True, exist_ok=True)
        self.tmp_dir.mkdir(parents=True, exist_ok=True)
        atomic_write_json(self.manifest_path, manifest)

    def read_manifest(self) -> dict[str, Any]:
        if not self.exists():
            raise FileNotFoundError(f"manifest not found under {self.root}")
        with open(self.manifest_path, "r", encoding="utf-8") as fh:
            return json.load(fh)

    # ---------------------------------------------------------------- stop flag
    def request_stop(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        with open(self.stop_path, "w", encoding="utf-8") as fh:
            fh.write("stop requested\n")

    def clear_stop(self) -> None:
        with contextlib.suppress(FileNotFoundError):
            os.remove(self.stop_path)

    def stop_requested(self) -> bool:
        return self.stop_path.exists()

    # ---------------------------------------------------------------- locking
    @contextlib.contextmanager
    def locked(self) -> Iterator[None]:
        with self._lock:
            yield

    # ---------------------------------------------------------------- shards
    def _shard_path(self, index: int) -> Path:
        return self.shard_dir / shard_filename(index)

    def existing_indices(self) -> list[int]:
        if not self.shard_dir.exists():
            return []
        indices = []
        for entry in self.shard_dir.iterdir():
            name = entry.name
            if name.startswith(SHARD_PREFIX) and name.endswith(SHARD_SUFFIX):
                indices.append(int(name[len(SHARD_PREFIX): -len(SHARD_SUFFIX)]))
        return sorted(indices)

    def read_record(self, index: int) -> dict[str, Any]:
        with open(self._shard_path(index), "r", encoding="utf-8") as fh:
            return json.load(fh)

    def read_all_records(self) -> list[dict[str, Any]]:
        return [self.read_record(i) for i in self.existing_indices()]

    def next_index(self) -> int:
        indices = self.existing_indices()
        expected = 0
        for index in indices:
            if index != expected:
                break
            expected += 1
        return expected

    def validate_record_versions(
        self, record: dict[str, Any], manifest: dict[str, Any]
    ) -> None:
        index = record.get("shard_index")
        if record.get("threshold_version") != manifest["threshold_version"]:
            raise PipeError(
                E_VERSION_MISMATCH,
                "threshold configuration fingerprint does not match manifest",
                shard_index=index,
                field="threshold_version",
            )
        if record.get("budget_version") != manifest["budget_version"]:
            raise PipeError(
                E_VERSION_MISMATCH,
                "budget configuration fingerprint does not match manifest",
                shard_index=index,
                field="budget_version",
            )
        for image_id, fp in record.get("sample_fingerprints", {}).items():
            expected = manifest["image_fingerprints"].get(image_id)
            if expected != fp:
                raise PipeError(
                    E_VERSION_MISMATCH,
                    f"sample content changed for image {image_id!r}",
                    shard_index=index,
                    field=f"sample_fingerprints.{image_id}",
                    details={"stored": fp, "manifest": expected},
                )

    def publish(self, record: dict[str, Any]) -> None:
        """Publish one shard record; callers do not need to hold the lock."""
        manifest = self.read_manifest()
        index = record.get("shard_index")
        if not isinstance(index, int) or index < 0:
            raise PipeError(
                E_INVALID_STAGE,
                "record carries no valid shard_index",
                shard_index=index,
                field="shard_index",
            )
        with self._lock:
            expected = self.next_index()
            if index < expected:
                raise PipeError(
                    E_STALE_CHECKPOINT,
                    f"shard {index} already published; next index is {expected}",
                    shard_index=index,
                    details={"next_index": expected},
                )
            if index > expected:
                raise PipeError(
                    E_INVALID_STAGE,
                    f"shard {index} cannot be published before shard {expected}",
                    shard_index=index,
                    details={"missing_index": expected},
                )
            self.validate_record_versions(record, manifest)
            atomic_write_json(self._shard_path(index), record)

    def truncate_from(self, index: int) -> list[int]:
        """Delete every record at ``index`` or later; returns removed indices."""
        removed = []
        with self._lock:
            for existing in self.existing_indices():
                if existing >= index:
                    os.remove(self._shard_path(existing))
                    removed.append(existing)
        return removed

    def recover(self, manifest: dict[str, Any]) -> int:
        """Return the first index that must be (re)computed.

        A chain is consistent only while indices are contiguous and every
        record's fingerprints still match the manifest.  The first broken or
        missing record truncates the whole tail.
        """
        with self._lock:
            indices = self.existing_indices()
            good_until = 0
            for expected in range(len(indices)):
                if expected >= len(indices) or indices[expected] != expected:
                    break
                record = self.read_record(expected)
                try:
                    self.validate_record_versions(record, manifest)
                except PipeError:
                    break
                good_until = expected + 1
            for stale in [i for i in indices if i >= good_until]:
                os.remove(self._shard_path(stale))
            return good_until

