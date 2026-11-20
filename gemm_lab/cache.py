from __future__ import annotations

import hashlib
import io
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from .blocking import BlockPlan
from .storage import (
    FileLock,
    atomic_write_bytes,
    atomic_write_json,
    digest_bytes,
    digest_value,
    file_sha256,
    read_json,
    stale_publish,
)


CACHE_SCHEMA_VERSION = 1


def make_cache_key(
    *,
    shape_fingerprint: str,
    plan: BlockPlan,
    sum_strategy: str,
    input_digest: str,
    accumulation_mode: str = "auto",
) -> str:
    components = {
        "schema": CACHE_SCHEMA_VERSION,
        "shape_fingerprint": shape_fingerprint,
        "plan": plan.to_dict(),
        "sum_strategy": sum_strategy,
        "accumulation_mode": accumulation_mode,
        "input_digest": input_digest,
    }
    return digest_value(components)


@dataclass(frozen=True)
class CacheLookup:
    status: str
    key: str
    array: np.ndarray | None
    metadata: dict[str, Any] | None
    reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "key": self.key,
            "reason": self.reason,
            "metadata": self.metadata,
            "shape": list(self.array.shape) if self.array is not None else None,
        }


@dataclass(frozen=True)
class CachePublish:
    key: str
    cache_seq: int
    metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "cache_seq": self.cache_seq,
            "metadata": self.metadata,
        }


class ResultCache:
    """Version-aware result cache with a globally monotonic publish sequence."""

    def __init__(
        self,
        root: str | Path,
        *,
        lock_timeout_seconds: float = 10.0,
        lock_path: str | Path | None = None,
    ) -> None:
        self.root = Path(root)
        self.entries_dir = self.root / "entries"
        self.index_path = self.root / "index.json"
        self.lock_path = Path(lock_path) if lock_path is not None else self.root / ".lock"
        self.lock_timeout_seconds = lock_timeout_seconds
        self.entries_dir.mkdir(parents=True, exist_ok=True)

    def _lock(self) -> FileLock:
        return FileLock(self.lock_path, timeout_seconds=self.lock_timeout_seconds)

    def _read_index_unlocked(self) -> dict[str, Any]:
        if not self.index_path.exists():
            return {"schema_version": CACHE_SCHEMA_VERSION, "seq": 0, "entries": {}}
        index = read_json(self.index_path)
        if int(index.get("seq", -1)) < 0 or not isinstance(index.get("entries"), dict):
            stale_publish(
                "cache_index_corrupt",
                "result cache index has an invalid sequence or entries map",
                path=str(self.index_path),
            )
        return index

    def _entry_paths(self, key: str) -> tuple[Path, Path]:
        return self.entries_dir / f"{key}.json", self.entries_dir / f"{key}.npy"

    def _invalidate_unlocked(self, key: str, index: dict[str, Any]) -> None:
        metadata_path, array_path = self._entry_paths(key)
        metadata_path.unlink(missing_ok=True)
        array_path.unlink(missing_ok=True)
        index.get("entries", {}).pop(key, None)
        index["seq"] = int(index.get("seq", 0)) + 1
        atomic_write_json(self.index_path, index)

    def lookup(self, key: str, *, current_config_seq: int) -> CacheLookup:
        with self._lock():
            index = self._read_index_unlocked()
            entry = index.get("entries", {}).get(key)
            if entry is None:
                return CacheLookup(status="miss", key=key, array=None, metadata=None, reason="not_found")
            metadata_path, array_path = self._entry_paths(key)
            if not metadata_path.exists() or not array_path.exists():
                self._invalidate_unlocked(key, index)
                return CacheLookup(
                    status="invalidated",
                    key=key,
                    array=None,
                    metadata=None,
                    reason="missing_entry_file",
                )
            metadata = read_json(metadata_path)
            if int(metadata.get("config_seq", -1)) != int(current_config_seq):
                self._invalidate_unlocked(key, index)
                return CacheLookup(
                    status="invalidated",
                    key=key,
                    array=None,
                    metadata=metadata,
                    reason="config_version_mismatch",
                )
            stored_digest = str(metadata.get("entry_digest", ""))
            metadata_without_digest = {k: v for k, v in metadata.items() if k != "entry_digest"}
            if digest_value(metadata_without_digest) != stored_digest:
                self._invalidate_unlocked(key, index)
                return CacheLookup(
                    status="invalidated",
                    key=key,
                    array=None,
                    metadata=metadata,
                    reason="metadata_digest_mismatch",
                )
            if file_sha256(array_path) != metadata.get("result_sha256"):
                self._invalidate_unlocked(key, index)
                return CacheLookup(
                    status="invalidated",
                    key=key,
                    array=None,
                    metadata=metadata,
                    reason="result_digest_mismatch",
                )
            array = np.load(array_path, allow_pickle=False)
            expected_shape = tuple(int(value) for value in metadata.get("result_shape", []))
            if tuple(array.shape) != expected_shape or array.dtype.name != metadata.get("result_dtype"):
                self._invalidate_unlocked(key, index)
                return CacheLookup(
                    status="invalidated",
                    key=key,
                    array=None,
                    metadata=metadata,
                    reason="shape_or_dtype_mismatch",
                )
            return CacheLookup(status="hit", key=key, array=array, metadata=metadata)

    def publish(
        self,
        *,
        key: str,
        result: np.ndarray,
        key_components: dict[str, Any],
        config_seq: int,
        computation_metadata: dict[str, Any] | None = None,
        expected_seq: int | None = None,
    ) -> CachePublish:
        with self._lock():
            index = self._read_index_unlocked()
            current_seq = int(index.get("seq", 0))
            if expected_seq is not None and expected_seq != current_seq:
                stale_publish(
                    "sequence_conflict",
                    "cache publish requires the exact current cache sequence",
                    expected_seq=expected_seq,
                    current_seq=current_seq,
                )
            if key in index.get("entries", {}):
                stale_publish(
                    "duplicate_publish",
                    "result cache key is already published",
                    key=key,
                    current_seq=current_seq,
                )
            next_seq = current_seq + 1
            buffer = io.BytesIO()
            np.save(buffer, np.ascontiguousarray(result), allow_pickle=False)
            result_bytes = buffer.getvalue()
            metadata_path, array_path = self._entry_paths(key)
            metadata: dict[str, Any] = {
                "schema_version": CACHE_SCHEMA_VERSION,
                "key": key,
                "key_components": key_components,
                "cache_seq": next_seq,
                "config_seq": int(config_seq),
                "result_shape": list(result.shape),
                "result_dtype": result.dtype.name,
                "result_sha256": digest_bytes(result_bytes),
                "computation_metadata": computation_metadata or {},
            }
            metadata["entry_digest"] = digest_value(metadata)
            atomic_write_bytes(array_path, result_bytes)
            atomic_write_json(metadata_path, metadata)
            index["seq"] = next_seq
            index.setdefault("entries", {})[key] = {
                "cache_seq": next_seq,
                "config_seq": int(config_seq),
                "entry_digest": metadata["entry_digest"],
                "result_file": array_path.name,
            }
            atomic_write_json(self.index_path, index)
            return CachePublish(key=key, cache_seq=next_seq, metadata=metadata)

    def status(self) -> dict[str, Any]:
        with self._lock():
            index = self._read_index_unlocked()
            return {
                "schema_version": CACHE_SCHEMA_VERSION,
                "seq": int(index.get("seq", 0)),
                "entry_count": len(index.get("entries", {})),
                "entries": index.get("entries", {}),
            }

    def clear(self, *, expected_seq: int | None = None) -> dict[str, int]:
        with self._lock():
            index = self._read_index_unlocked()
            current_seq = int(index.get("seq", 0))
            if expected_seq is not None and expected_seq != current_seq:
                stale_publish(
                    "sequence_conflict",
                    "cache clear requires the exact current cache sequence",
                    expected_seq=expected_seq,
                    current_seq=current_seq,
                )
            for path in self.entries_dir.glob("*"):
                if path.is_file():
                    path.unlink(missing_ok=True)
            next_seq = current_seq + 1
            atomic_write_json(
                self.index_path,
                {"schema_version": CACHE_SCHEMA_VERSION, "seq": next_seq, "entries": {}},
            )
            return {"previous_seq": current_seq, "seq": next_seq, "entries_removed": len(index.get("entries", {}))}

