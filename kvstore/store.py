from __future__ import annotations

import heapq
import os
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from .codec_binary import encode_binary
from .codec_jsonl import encode_jsonl
from .errors import (
    ConcurrentWrite,
    FormatInvalid,
    InvalidArgument,
    KeyNotFound,
    ReadTimeoutBudget,
    SnapshotInvalid,
    StorageError,
    VersionConflict,
)
from .formats import decode_external
from .model import DecodedBatch, ImportedRecord, IndexEntry, Record, canonical_json, ensure_key_found
from .segment import (
    SegmentHeader,
    append_records,
    create_segment,
    read_header,
    read_record_at,
    rebuild_index,
    record_frame_size,
    sha256_file,
    truncate_to,
)
from .storage_utils import atomic_write_json, fsync_directory, read_json


META_VERSION = 1
INTENT_VERSION = 1
COMMIT_VERSION = 1


def _default_clock_us() -> int:
    return time.time_ns() // 1000


@dataclass
class StoreState:
    index: Dict[str, IndexEntry]
    expiry_heap: List[Tuple[int, str, int]]
    segment_path: Path
    segment_id: str
    snapshot_seq: int
    snapshot_id: Optional[str]
    last_txn_seq: int
    generated_at_us: int
    live_count: int
    _refs: int = 1
    _retired: bool = False
    _lifetime_lock: threading.Lock = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self._lifetime_lock is None:
            self._lifetime_lock = threading.Lock()

    def add_ref(self) -> None:
        with self._lifetime_lock:
            if self._retired and self._refs == 0:
                raise RuntimeError("cannot acquire a fully released state")
            self._refs += 1

    def release_ref(self) -> None:
        with self._lifetime_lock:
            if self._refs <= 0:
                return
            self._refs -= 1
            should_release = self._retired and self._refs == 0
        if should_release:
            self._release_resources()

    def retire_root_ref(self) -> None:
        with self._lifetime_lock:
            self._retired = True
            if self._refs > 0:
                self._refs -= 1
            should_release = self._refs == 0
        if should_release:
            self._release_resources()

    def _release_resources(self) -> None:
        # Readers open segment files per operation, so clearing large in-memory
        # structures is the deferred release. Files are kept for forensic recovery.
        self.index.clear()
        self.expiry_heap.clear()


class KVStore:
    """Segmented key/value store with an in-memory index and expiry heap.

    Writers are serialized by a non-blocking lock. Readers capture an immutable
    StoreState reference and never wait for writer completion.
    """

    def __init__(self, data_dir: str | os.PathLike[str], *, clock_us: Optional[Callable[[], int]] = None) -> None:
        self.data_dir = Path(data_dir).expanduser().resolve()
        self.segments_dir = self.data_dir / "segments"
        self.snapshots_dir = self.data_dir / "snapshots"
        self.intents_dir = self.data_dir / "intents"
        self.recovery_dir = self.data_dir / "recovery"
        self.meta_path = self.data_dir / "meta.json"
        self._clock_us = clock_us or _default_clock_us

        for directory in (self.data_dir, self.segments_dir, self.snapshots_dir, self.intents_dir, self.recovery_dir):
            directory.mkdir(parents=True, exist_ok=True)

        self._write_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._batch_lock = threading.Lock()
        self._inflight_batches: set[str] = set()
        self._completed_batches: set[str] = set()
        self._txn_seq = 0
        self._last_meta_error: Optional[str] = None

        self._recovery_report = self._recover()
        active_segment = self._abs(self._recovery_report["active_segment"])
        header, index, _ = rebuild_index(active_segment)
        live_count = self._live_count(index, self._now_us())
        self._txn_seq = int(self._recovery_report["last_txn_seq"])
        self._state = StoreState(
            index=index,
            expiry_heap=self._heap_from_index(index),
            segment_path=active_segment,
            segment_id=header.segment_id,
            snapshot_seq=int(self._recovery_report.get("snapshot_seq", 0)),
            snapshot_id=self._recovery_report.get("snapshot_id"),
            last_txn_seq=self._txn_seq,
            generated_at_us=header.generated_at_us,
            live_count=live_count,
        )

    @property
    def recovery_report(self) -> Dict[str, Any]:
        return self._recovery_report

    def _now_us(self) -> int:
        return int(self._clock_us())

    def _rel(self, path: Path) -> str:
        try:
            return path.resolve().relative_to(self.data_dir).as_posix()
        except ValueError:
            return str(path.resolve())

    def _abs(self, value: str | os.PathLike[str]) -> Path:
        path = Path(value)
        return path if path.is_absolute() else self.data_dir / path

    def _live_count(self, index: Mapping[str, IndexEntry], now_us: int) -> int:
        return sum(
            1
            for entry in index.values()
            if not entry.deleted and (entry.expires_at_us is None or entry.expires_at_us > now_us)
        )

    @staticmethod
    def _heap_from_index(index: Mapping[str, IndexEntry]) -> List[Tuple[int, str, int]]:
        heap = [
            (entry.expires_at_us, entry.key, entry.version)
            for entry in index.values()
            if not entry.deleted and entry.expires_at_us is not None
        ]
        heapq.heapify(heap)
        return heap

    @staticmethod
    def _compact_heap(index: Mapping[str, IndexEntry], heap: Iterable[Tuple[int, str, int]]) -> List[Tuple[int, str, int]]:
        compacted = []
        for expires_at_us, key, version in heap:
            entry = index.get(key)
            if (
                entry is not None
                and not entry.deleted
                and entry.expires_at_us == expires_at_us
                and entry.version == version
            ):
                compacted.append((expires_at_us, key, version))
        heapq.heapify(compacted)
        return compacted

    def _acquire_state(self) -> StoreState:
        with self._state_lock:
            state = self._state
            state.add_ref()
            return state

    def _release_state(self, state: StoreState) -> None:
        state.release_ref()

    def _publish_state(self, new_state: StoreState) -> None:
        with self._state_lock:
            old_state = self._state
            self._state = new_state
        old_state.retire_root_ref()

    def _begin_write(self) -> None:
        if not self._write_lock.acquire(blocking=False):
            raise ConcurrentWrite(
                "another write operation is in progress",
                {"retryable": True, "policy": "single-writer-nonblocking"},
            )

    def _end_write(self) -> None:
        self._write_lock.release()

    def _check_read_budget(self, started: float, budget_ms: int) -> None:
        elapsed_ms = (time.monotonic() - started) * 1000.0
        if elapsed_ms > budget_ms:
            raise ReadTimeoutBudget(
                "read did not complete within its declared timeout budget",
                {"budget_ms": budget_ms, "elapsed_ms": round(elapsed_ms, 3)},
            )

    def _read_entry(self, entry: IndexEntry, expected_key: str) -> Record:
        record, _ = read_record_at(Path(entry.segment_path), entry.offset, expected_key=expected_key)
        return record

    def get(self, key: str, *, read_timeout_ms: int = 1000) -> Dict[str, Any]:
        if not isinstance(key, str) or not key:
            raise InvalidArgument("key must be a non-empty string", {"field": "key"})
        if isinstance(read_timeout_ms, bool) or not isinstance(read_timeout_ms, int) or read_timeout_ms <= 0:
            raise InvalidArgument("read_timeout_ms must be a positive integer", {"field": "read_timeout_ms"})
        started = time.monotonic()
        state = self._acquire_state()
        try:
            entry = state.index.get(key)
            self._check_read_budget(started, read_timeout_ms)
            if entry is None:
                ensure_key_found(key)
            record = self._read_entry(entry, key)
            now_us = self._now_us()
            if record.deleted:
                ensure_key_found(key)
            if record.expires_at_us is not None and record.expires_at_us <= now_us:
                ensure_key_found(key, expired=True)
            self._check_read_budget(started, read_timeout_ms)
            return self._record_view(record, read_elapsed_ms=round((time.monotonic() - started) * 1000.0, 3))
        finally:
            self._release_state(state)

    def _record_view(self, record: Record, **extra: Any) -> Dict[str, Any]:
        result: Dict[str, Any] = {
            "key": record.key,
            "value": record.value,
            "version": record.version,
            "written_at_us": record.written_at_us,
            "expires_at_us": record.expires_at_us,
            "expires_at_ms": None if record.expires_at_us is None else record.expires_at_us // 1000,
        }
        if record.extras:
            result["extras"] = record.extras
        result.update(extra)
        return result

    def write(
        self,
        key: str,
        value: Any,
        *,
        version: Optional[int] = None,
        written_at_us: Optional[int] = None,
        expires_at_us: Optional[int] = None,
        ttl_ms: Optional[int] = None,
        extras: Optional[Dict[str, Any]] = None,
        crash_after_intent: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> Dict[str, Any]:
        self._begin_write()
        try:
            state = self._state
            now = self._now_us()
            written = now if written_at_us is None else written_at_us
            if isinstance(written, bool) or not isinstance(written, int):
                raise InvalidArgument("written_at_us must be an integer", {"field": "written_at_us"})
            if expires_at_us is not None and ttl_ms is not None:
                raise InvalidArgument("expires_at_us and ttl_ms are mutually exclusive")
            if ttl_ms is not None:
                if isinstance(ttl_ms, bool) or not isinstance(ttl_ms, int) or ttl_ms < 0:
                    raise InvalidArgument("ttl_ms must be a non-negative integer", {"field": "ttl_ms"})
                expires_at_us = written + ttl_ms * 1000
            previous = state.index.get(key)
            if version is None:
                version = 1 if previous is None else previous.version + 1
            elif isinstance(version, bool) or not isinstance(version, int) or version < 0:
                raise InvalidArgument("version must be a non-negative integer", {"field": "version"})
            if previous is not None and version <= previous.version:
                raise VersionConflict(
                    "write version must be greater than the current key version",
                    {"key": key, "current_version": previous.version, "requested_version": version},
                )
            record = Record(
                key=key,
                value=value,
                version=version,
                written_at_us=written,
                expires_at_us=expires_at_us,
                deleted=False,
                extras=extras or {},
            )
            txn_seq = self._append_transaction(
                [record],
                txn_type="write",
                crash_after_intent=crash_after_intent,
            )
            return {"key": key, "version": version, "transaction_seq": txn_seq, "record": self._record_view(record)}
        finally:
            self._end_write()

    put = write

    def delete(self, key: str) -> Dict[str, Any]:
        self._begin_write()
        try:
            state = self._state
            previous = state.index.get(key)
            if previous is None or previous.deleted:
                ensure_key_found(key)
            now = self._now_us()
            tombstone = Record(
                key=key,
                value=None,
                version=previous.version + 1,
                written_at_us=now,
                expires_at_us=None,
                deleted=True,
            )
            txn_seq = self._append_transaction([tombstone], txn_type="delete")
            return {"key": key, "deleted": True, "version": tombstone.version, "transaction_seq": txn_seq}
        finally:
            self._end_write()

    def expire_due(self, *, limit: int = 1000) -> Dict[str, Any]:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise InvalidArgument("limit must be a positive integer", {"field": "limit"})
        self._begin_write()
        try:
            state = self._state
            now = self._now_us()
            heap = list(state.expiry_heap)
            tombstones: List[Record] = []
            expired_keys: List[str] = []
            while heap and heap[0][0] <= now and len(tombstones) < limit:
                expires_at_us, key, version = heapq.heappop(heap)
                entry = state.index.get(key)
                if (
                    entry is None
                    or entry.deleted
                    or entry.version != version
                    or entry.expires_at_us != expires_at_us
                ):
                    continue
                expired_keys.append(key)
                tombstones.append(
                    Record(
                        key=key,
                        value=None,
                        version=entry.version + 1,
                        written_at_us=now,
                        expires_at_us=None,
                        deleted=True,
                    )
                )
            if not tombstones:
                return {"expired_count": 0, "expired_keys": [], "transaction_seq": None}
            txn_seq = self._append_transaction(tombstones, txn_type="expire")
            return {"expired_count": len(tombstones), "expired_keys": expired_keys, "transaction_seq": txn_seq}
        finally:
            self._end_write()

    def _next_txn(self) -> Tuple[int, str, int]:
        self._txn_seq += 1
        now = self._now_us()
        return self._txn_seq, f"txn-{self._txn_seq:012d}", now

    def _intent_path(self, txn_id: str) -> Path:
        return self.intents_dir / f"{txn_id}.intent"

    def _commit_path(self, txn_id: str) -> Path:
        return self.intents_dir / f"{txn_id}.commit"

    def _abort_path(self, txn_id: str) -> Path:
        return self.intents_dir / f"{txn_id}.abort"

    def _write_abort(self, txn_seq: int, txn_id: str, reason: str, detail: str = "") -> None:
        try:
            atomic_write_json(
                self._abort_path(txn_id),
                {
                    "abort_version": 1,
                    "txn_seq": txn_seq,
                    "txn_id": txn_id,
                    "reason": reason,
                    "detail": detail,
                    "aborted_at_us": self._now_us(),
                },
            )
        except Exception:
            # The missing commit marker is still sufficient for startup recovery.
            pass

    def _append_transaction(
        self,
        records: Sequence[Record],
        *,
        txn_type: str,
        batch_id: Optional[str] = None,
        crash_after_intent: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> int:
        if not records:
            raise InvalidArgument("a write transaction must contain at least one record")
        state = self._state
        segment_path = state.segment_path
        header = read_header(segment_path)
        pre_size = segment_path.stat().st_size
        expected_post_size = pre_size + sum(record_frame_size(record) for record in records)
        txn_seq, txn_id, created_at_us = self._next_txn()
        intent: Dict[str, Any] = {
            "intent_version": INTENT_VERSION,
            "txn_seq": txn_seq,
            "txn_id": txn_id,
            "type": "append",
            "operation": txn_type,
            "created_at_us": created_at_us,
            "active_segment": self._rel(segment_path),
            "pre_size": pre_size,
            "expected_post_size": expected_post_size,
            "segment_id": header.segment_id,
            "snapshot_seq": state.snapshot_seq,
            "snapshot_id": state.snapshot_id,
            "pre_txn_seq": state.last_txn_seq,
            "record_count": len(records),
            "keys": [record.key for record in records],
            "versions": [record.version for record in records],
            "batch_id": batch_id,
        }
        atomic_write_json(self._intent_path(txn_id), intent)

        committed = False
        try:
            if crash_after_intent is not None:
                crash_after_intent(intent)
            start_offset, appended = append_records(segment_path, records)
            if start_offset != pre_size:
                raise StorageError(
                    "segment changed while holding the single-writer lock",
                    {"expected_size": pre_size, "actual_offset": start_offset},
                )
            actual_post_size = segment_path.stat().st_size
            if actual_post_size != expected_post_size:
                raise StorageError(
                    "segment size after append does not match the intent",
                    {"expected_size": expected_post_size, "actual_size": actual_post_size},
                )
            commit = {
                "commit_version": COMMIT_VERSION,
                "txn_seq": txn_seq,
                "txn_id": txn_id,
                "type": "append",
                "operation": txn_type,
                "post_size": actual_post_size,
                "post_active_segment": self._rel(segment_path),
                "segment_id": header.segment_id,
                "snapshot_seq": state.snapshot_seq,
                "snapshot_id": state.snapshot_id,
                "committed_at_us": self._now_us(),
                "batch_id": batch_id,
            }
            atomic_write_json(self._commit_path(txn_id), commit)
            committed = True
        except Exception as exc:
            if not committed and not self._commit_path(txn_id).exists():
                truncate_to(segment_path, pre_size)
                self._write_abort(txn_seq, txn_id, "append_failed", str(exc))
            raise

        new_state = self._state_with_appended(state, appended, txn_seq=txn_seq)
        self._publish_state(new_state)
        self._write_meta_best_effort(new_state)
        return txn_seq

    def _state_with_appended(
        self,
        state: StoreState,
        appended: Iterable[Any],
        *,
        txn_seq: int,
    ) -> StoreState:
        index = dict(state.index)
        heap = list(state.expiry_heap)
        for item in appended:
            record: Record = item.record
            index[record.key] = IndexEntry(
                key=record.key,
                version=record.version,
                written_at_us=record.written_at_us,
                expires_at_us=record.expires_at_us,
                deleted=record.deleted,
                segment_path=str(state.segment_path),
                offset=item.offset,
                frame_size=item.frame_size,
            )
            if not record.deleted and record.expires_at_us is not None:
                heapq.heappush(heap, (record.expires_at_us, record.key, record.version))
        return StoreState(
            index=index,
            expiry_heap=self._compact_heap(index, heap),
            segment_path=state.segment_path,
            segment_id=state.segment_id,
            snapshot_seq=state.snapshot_seq,
            snapshot_id=state.snapshot_id,
            last_txn_seq=txn_seq,
            generated_at_us=state.generated_at_us,
            live_count=self._live_count(index, self._now_us()),
        )

    def _write_meta_best_effort(self, state: StoreState) -> None:
        meta = {
            "meta_version": META_VERSION,
            "last_txn_seq": state.last_txn_seq,
            "active_segment": self._rel(state.segment_path),
            "segment_id": state.segment_id,
            "snapshot_seq": state.snapshot_seq,
            "snapshot_id": state.snapshot_id,
            "updated_at_us": self._now_us(),
        }
        try:
            atomic_write_json(self.meta_path, meta)
            self._last_meta_error = None
        except Exception as exc:  # the commit marker remains the durability source of truth
            self._last_meta_error = str(exc)

    def _write_meta_strict(self, state: StoreState) -> None:
        meta = {
            "meta_version": META_VERSION,
            "last_txn_seq": state.last_txn_seq,
            "active_segment": self._rel(state.segment_path),
            "segment_id": state.segment_id,
            "snapshot_seq": state.snapshot_seq,
            "snapshot_id": state.snapshot_id,
            "updated_at_us": self._now_us(),
        }
        atomic_write_json(self.meta_path, meta)

    # ------------------------------------------------------------------
    # Recovery
    # ------------------------------------------------------------------

    def _recover(self) -> Dict[str, Any]:
        started_at = self._now_us()
        for orphan in self.snapshots_dir.glob(".*.tmp"):
            self._safe_unlink(orphan)
        meta = self._read_meta()
        intents = self._read_intents()
        last_seq = max([int(meta.get("last_txn_seq", 0))] + [int(item.get("txn_seq", 0)) for item in intents])
        committed: List[Dict[str, Any]] = []
        rolled_back: List[Dict[str, Any]] = []
        aborted: List[Dict[str, Any]] = []
        latest_commit: Optional[Dict[str, Any]] = None

        for intent in intents:
            txn_id = str(intent["txn_id"])
            commit_path = self._commit_path(txn_id)
            commit: Optional[Dict[str, Any]] = None
            if commit_path.exists():
                try:
                    commit = read_json(commit_path)
                    self._validate_commit(intent, commit)
                except Exception as exc:
                    rolled_back.append(
                        {
                            "txn_seq": int(intent.get("txn_seq", 0)),
                            "txn_id": txn_id,
                            "type": intent.get("type"),
                            "reason": "invalid_commit_marker",
                            "detail": str(exc),
                        }
                    )
                    self._rollback_uncommitted_intent(intent)
                    continue
            if commit is None:
                abort_path = self._abort_path(txn_id)
                if abort_path.exists():
                    try:
                        abort = read_json(abort_path)
                    except Exception:
                        abort = {}
                    aborted.append(
                        {
                            "txn_seq": int(intent.get("txn_seq", 0)),
                            "txn_id": txn_id,
                            "type": intent.get("type"),
                            "reason": abort.get("reason", "explicit_abort_marker"),
                        }
                    )
                    continue
                self._rollback_uncommitted_intent(intent)
                rolled_back.append(
                    {
                        "txn_seq": int(intent.get("txn_seq", 0)),
                        "txn_id": txn_id,
                        "type": intent.get("type"),
                        "reason": "missing_commit_marker",
                        "segment_truncated_to": int(intent.get("pre_size", 0))
                        if intent.get("type") == "append"
                        else None,
                    }
                )
                continue
            committed.append(
                {
                    "txn_seq": int(commit["txn_seq"]),
                    "txn_id": txn_id,
                    "type": commit.get("type"),
                    "operation": commit.get("operation"),
                    "post_active_segment": commit.get("post_active_segment"),
                    "post_size": commit.get("post_size"),
                }
            )
            if latest_commit is None or int(commit["txn_seq"]) > int(latest_commit["txn_seq"]):
                latest_commit = commit

        active_value = (
            latest_commit.get("post_active_segment")
            if latest_commit is not None
            else meta.get("active_segment")
        )
        if not active_value:
            active_value = self._create_initial_segment().relative_to(self.data_dir).as_posix()
        active_path = self._abs(str(active_value))
        if not active_path.exists():
            # If a stale meta pointer survived a crash, prefer the most recent readable segment.
            candidates = sorted(self.segments_dir.glob("active-*.kvs")) + sorted(self.snapshots_dir.glob("snapshot-*.kvs"))
            readable = [path for path in candidates if self._can_read_segment(path)]
            if not readable:
                raise StorageError("no readable active segment exists", {"active_segment": str(active_path)})
            active_path = max(readable, key=lambda path: path.stat().st_mtime_ns)
            active_value = self._rel(active_path)

        header = read_header(active_path)
        snapshot_seq = max(
            int(meta.get("snapshot_seq", 0)),
            int(latest_commit.get("snapshot_seq", 0)) if latest_commit else 0,
            header.snapshot_seq,
        )
        snapshot_id = (
            latest_commit.get("snapshot_id")
            if latest_commit is not None and latest_commit.get("snapshot_id") is not None
            else meta.get("snapshot_id")
        )
        completed_at = self._now_us()
        report: Dict[str, Any] = {
            "report_version": 1,
            "source": "persistent_intent_and_segment_header",
            "recovery_started_at_us": started_at,
            "recovery_completed_at_us": completed_at,
            "last_txn_seq": last_seq,
            "last_applied_txn_seq": int(latest_commit["txn_seq"]) if latest_commit else int(meta.get("last_txn_seq", 0)),
            "active_segment": active_value,
            "segment_id": header.segment_id,
            "snapshot_seq": snapshot_seq,
            "snapshot_id": snapshot_id,
            "applied_transactions": committed,
            "rolled_back_transactions": rolled_back,
            "aborted_transactions": aborted,
            "counts": {
                "scanned_intents": len(intents),
                "applied": len(committed),
                "rolled_back": len(rolled_back),
                "aborted": len(aborted),
            },
        }
        meta_state = StoreState(
            index={},
            expiry_heap=[],
            segment_path=active_path,
            segment_id=header.segment_id,
            snapshot_seq=snapshot_seq,
            snapshot_id=snapshot_id,
            last_txn_seq=last_seq,
            generated_at_us=header.generated_at_us,
            live_count=0,
        )
        self._write_meta_strict(meta_state)
        atomic_write_json(self.recovery_dir / "last_report.json", report)
        return report

    def _read_meta(self) -> Dict[str, Any]:
        if not self.meta_path.exists():
            return {}
        try:
            value = read_json(self.meta_path)
            return value if isinstance(value, dict) else {}
        except Exception:
            return {}

    def _read_intents(self) -> List[Dict[str, Any]]:
        intents: List[Dict[str, Any]] = []
        for path in sorted(self.intents_dir.glob("*.intent")):
            try:
                value = read_json(path)
            except Exception:
                continue
            if isinstance(value, dict) and "txn_seq" in value and "txn_id" in value:
                intents.append(value)
        intents.sort(key=lambda item: int(item["txn_seq"]))
        return intents

    def _validate_commit(self, intent: Mapping[str, Any], commit: Mapping[str, Any]) -> None:
        if commit.get("commit_version") != COMMIT_VERSION:
            raise FormatInvalid("unsupported commit marker version")
        if int(commit.get("txn_seq", -1)) != int(intent.get("txn_seq", -2)):
            raise FormatInvalid("commit sequence does not match its intent")
        if str(commit.get("txn_id")) != str(intent.get("txn_id")):
            raise FormatInvalid("commit transaction id does not match its intent")
        target = self._abs(str(commit.get("post_active_segment")))
        if not target.exists():
            raise StorageError("committed active segment does not exist", {"path": str(target)})
        header = read_header(target)
        if header.segment_id != commit.get("segment_id"):
            raise FormatInvalid("committed segment id does not match the segment header")
        if target.stat().st_size < int(commit.get("post_size", 0)):
            raise FormatInvalid("committed segment is shorter than its committed size")

    def _rollback_uncommitted_intent(self, intent: Mapping[str, Any]) -> None:
        intent_type = intent.get("type")
        if intent_type == "append":
            target = self._abs(str(intent.get("active_segment")))
            pre_size = int(intent.get("pre_size", 0))
            if target.exists():
                if pre_size >= 12:
                    header = read_header(target)
                    if pre_size < header.header_end:
                        raise StorageError(
                            "intent pre-size is smaller than the segment header",
                            {"path": str(target), "pre_size": pre_size},
                        )
                truncate_to(target, pre_size)
            return
        if intent_type == "snapshot":
            temp_segment = intent.get("temp_segment")
            temp_manifest = intent.get("temp_manifest")
            target = self._abs(str(intent.get("target_segment")))
            manifest = intent.get("target_manifest")
            for value in (temp_segment, temp_manifest):
                if value:
                    self._safe_unlink(self._abs(str(value)))
            if intent.get("created_target"):
                self._safe_unlink(target)
                if manifest:
                    self._safe_unlink(self._abs(str(manifest)))

    @staticmethod
    def _safe_unlink(path: Path) -> None:
        try:
            path.unlink()
        except FileNotFoundError:
            pass

    def _can_read_segment(self, path: Path) -> bool:
        try:
            read_header(path)
            return True
        except Exception:
            return False

    def _create_initial_segment(self) -> Path:
        existing = sorted(self.segments_dir.glob("active-*.kvs"))
        if existing:
            return existing[0]
        path = self.segments_dir / "active-00000001.kvs"
        create_segment(
            path,
            segment_id="segment-00000001",
            kind="segment",
            snapshot_seq=0,
            generated_at_us=self._now_us(),
        )
        return path

    # ------------------------------------------------------------------
    # Import and conflict resolution
    # ------------------------------------------------------------------

    def negotiate_import(
        self,
        data: bytes,
        *,
        expected_format: Optional[str] = None,
        source_name: str = "input",
    ) -> Dict[str, Any]:
        decoded = decode_external(
            data,
            expected_format=expected_format,
            default_written_at_us=self._now_us(),
            source_name=source_name,
        )
        return {
            "negotiation": decoded.negotiation,
            "warnings": list(decoded.warnings),
            "records": [
                {
                    "record_number": record.source_index + 1,
                    "summary": record.summary(),
                    "filled_fields": list(record.filled_fields),
                    "transformations": list(record.transformations),
                    "unknown_fields": list(record.unknown_fields),
                }
                for record in decoded.records
            ],
            "dry_run": True,
        }

    def import_data(
        self,
        data: bytes,
        *,
        expected_format: Optional[str] = None,
        batch_id: Optional[str] = None,
        source_name: str = "input",
    ) -> Dict[str, Any]:
        if batch_id is not None and (not isinstance(batch_id, str) or not batch_id.strip()):
            raise InvalidArgument("batch_id must be a non-empty string when supplied")
        normalized_batch = batch_id.strip() if batch_id else None
        if normalized_batch:
            with self._batch_lock:
                if normalized_batch in self._inflight_batches or normalized_batch in self._completed_batches:
                    raise ConcurrentWrite(
                        "this import batch is already running or has already been committed",
                        {"batch_id": normalized_batch, "policy": "single-success-per-batch"},
                    )
                self._inflight_batches.add(normalized_batch)

        succeeded = False
        try:
            decoded = decode_external(
                data,
                expected_format=expected_format,
                default_written_at_us=self._now_us(),
                source_name=source_name,
            )
            self._begin_write()
            try:
                result = self._import_decoded_locked(decoded, normalized_batch)
                succeeded = True
                return result
            finally:
                self._end_write()
        finally:
            if normalized_batch:
                with self._batch_lock:
                    self._inflight_batches.discard(normalized_batch)
                    if succeeded:
                        self._completed_batches.add(normalized_batch)

    def _import_decoded_locked(self, decoded: DecodedBatch, batch_id: Optional[str]) -> Dict[str, Any]:
        state = self._state
        records = list(decoded.records)
        reports: List[Dict[str, Any]] = []
        for record in records:
            reports.append(
                {
                    "record_number": record.source_index + 1,
                    "key": record.key,
                    "incoming": record.summary(),
                    "filled_fields": list(record.filled_fields),
                    "transformations": list(record.transformations),
                    "unknown_fields": list(record.unknown_fields),
                    "outcome": None,
                    "reason": None,
                    "existing": None,
                }
            )

        groups: "OrderedDict[str, List[int]]" = OrderedDict()
        for index, record in enumerate(records):
            groups.setdefault(record.key, []).append(index)

        accepted_records: List[Tuple[ImportedRecord, str]] = []
        rejected_keys: List[str] = []
        skipped_keys: List[str] = []

        for key, indices in groups.items():
            max_version = max(records[index].version for index in indices)
            top_version_indices = [index for index in indices if records[index].version == max_version]
            max_written = max(records[index].written_at_us for index in top_version_indices)
            tied_indices = [
                index for index in top_version_indices if records[index].written_at_us == max_written
            ]
            tied_values = {canonical_json(records[index].value) for index in tied_indices}

            if len(tied_values) > 1:
                for index in indices:
                    if index in tied_indices:
                        self._set_outcome(
                            reports[index],
                            "reject",
                            "unresolved_same_version_and_write_time",
                        )
                    else:
                        self._set_outcome(reports[index], "skip", "duplicate_key_lower_priority")
                rejected_keys.append(key)
                continue

            winner_index = tied_indices[-1]
            for index in indices:
                if index == winner_index:
                    continue
                self._set_outcome(reports[index], "skip", "duplicate_key_lower_priority")
            winner = records[winner_index]
            existing_entry = state.index.get(key)
            if existing_entry is None:
                self._set_outcome(reports[winner_index], "overwrite", "new_key")
                accepted_records.append((winner, "new_key"))
                continue

            existing_record = self._read_entry(existing_entry, key)
            reports[winner_index]["existing"] = {
                "version": existing_record.version,
                "written_at_us": existing_record.written_at_us,
                "expires_at_us": existing_record.expires_at_us,
                "value": existing_record.value,
                "deleted": existing_record.deleted,
            }
            outcome, reason = self._resolve_against_existing(winner, existing_record)
            self._set_outcome(reports[winner_index], outcome, reason)
            if outcome == "overwrite":
                accepted_records.append((winner, reason))
            elif outcome == "reject":
                rejected_keys.append(key)
            else:
                skipped_keys.append(key)

        # Duplicate losers for keys whose winner was skipped or rejected are still
        # explicitly represented, but should not trigger a second key-level count.
        overwrite_count = sum(1 for item in reports if item["outcome"] == "overwrite")
        skip_count = sum(1 for item in reports if item["outcome"] == "skip")
        reject_count = sum(1 for item in reports if item["outcome"] == "reject")
        accepted_records.sort(key=lambda pair: pair[0].source_index)
        txn_seq: Optional[int] = None
        if accepted_records:
            txn_seq = self._append_transaction(
                [record.to_record() for record, _ in accepted_records],
                txn_type="import",
                batch_id=batch_id,
            )

        result: Dict[str, Any] = {
            "negotiation": decoded.negotiation,
            "warnings": list(decoded.warnings),
            "records": reports,
            "conflicts": [
                {
                    "record_number": item["record_number"],
                    "key": item["key"],
                    "outcome": item["outcome"],
                    "reason": item["reason"],
                }
                for item in reports
                if item["outcome"] != "overwrite" or item["reason"] != "new_key"
            ],
            "summary": {
                "input_records": len(records),
                "overwrite": overwrite_count,
                "skip": skip_count,
                "reject": reject_count,
                "committed_records": len(accepted_records),
                "rejected_keys": sorted(set(rejected_keys)),
                "skipped_keys": sorted(set(skipped_keys)),
                "batch_id": batch_id,
                "transaction_seq": txn_seq,
            },
        }
        return result

    @staticmethod
    def _set_outcome(report: Dict[str, Any], outcome: str, reason: str) -> None:
        report["outcome"] = outcome
        report["reason"] = reason

    def _resolve_against_existing(
        self, incoming: ImportedRecord, existing: Record
    ) -> Tuple[str, str]:
        if incoming.version > existing.version:
            return "overwrite", "higher_version"
        if incoming.version < existing.version:
            return "skip", "lower_version"
        if incoming.written_at_us < existing.written_at_us:
            return "skip", "older_write_time"
        if incoming.written_at_us > existing.written_at_us:
            return "overwrite", "newer_write_time"
        same_value = canonical_json(incoming.value) == canonical_json(existing.value)
        if not existing.deleted and same_value and incoming.expires_at_us == existing.expires_at_us:
            return "skip", "duplicate_record"
        return "reject", "unresolved_same_version_and_write_time"

    # ------------------------------------------------------------------
    # Export and introspection
    # ------------------------------------------------------------------

    def export_records(self) -> List[Record]:
        state = self._acquire_state()
        try:
            now = self._now_us()
            records: List[Record] = []
            for key in sorted(state.index):
                entry = state.index[key]
                if entry.deleted or (entry.expires_at_us is not None and entry.expires_at_us <= now):
                    continue
                record = self._read_entry(entry, key)
                if not record.deleted and (record.expires_at_us is None or record.expires_at_us > now):
                    records.append(record)
            return records
        finally:
            self._release_state(state)

    def export_binary_with_count(self, *, endian: str = "little") -> Tuple[bytes, int]:
        records = self.export_records()
        return encode_binary(records, endian=endian), len(records)

    def export_binary(self, *, endian: str = "little") -> bytes:
        data, _ = self.export_binary_with_count(endian=endian)
        return data

    def export_jsonl_with_count(self) -> Tuple[bytes, int]:
        records = self.export_records()
        return encode_jsonl(records), len(records)

    def export_jsonl(self) -> bytes:
        data, _ = self.export_jsonl_with_count()
        return data

    def index_summary(self, *, limit: int = 1000) -> Dict[str, Any]:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise InvalidArgument("limit must be a positive integer", {"field": "limit"})
        state = self._acquire_state()
        try:
            now = self._now_us()
            live_entries = [
                entry
                for entry in state.index.values()
                if not entry.deleted and (entry.expires_at_us is None or entry.expires_at_us > now)
            ]
            live_entries.sort(key=lambda entry: entry.key)
            keys = [
                {
                    "key": entry.key,
                    "version": entry.version,
                    "written_at_us": entry.written_at_us,
                    "expires_at_us": entry.expires_at_us,
                }
                for entry in live_entries[:limit]
            ]
            return {
                "segment": self._rel(state.segment_path),
                "segment_id": state.segment_id,
                "snapshot_seq": state.snapshot_seq,
                "snapshot_id": state.snapshot_id,
                "last_txn_seq": state.last_txn_seq,
                "indexed_keys": len(state.index),
                "live_keys": len(live_entries),
                "expiry_heap_entries": len(state.expiry_heap),
                "keys": keys,
                "truncated": len(live_entries) > limit,
            }
        finally:
            self._release_state(state)

    # ------------------------------------------------------------------
    # Snapshots
    # ------------------------------------------------------------------

    def switch_snapshot(self, path: Optional[str | os.PathLike[str]] = None) -> Dict[str, Any]:
        self._begin_write()
        try:
            if path is None:
                return self._create_snapshot_locked()
            return self._load_snapshot_locked(Path(path))
        finally:
            self._end_write()

    def _snapshot_records(self, state: StoreState) -> List[Record]:
        records: List[Record] = []
        for key in sorted(state.index):
            entry = state.index[key]
            records.append(self._read_entry(entry, key))
        return records

    def _create_snapshot_locked(self) -> Dict[str, Any]:
        state = self._state
        snapshot_seq = state.snapshot_seq + 1
        snapshot_id = f"snapshot-{snapshot_seq:012d}"
        generated_at_us = self._now_us()
        records = self._snapshot_records(state)
        target = self.snapshots_dir / f"{snapshot_id}.kvs"
        manifest_target = self.snapshots_dir / f"{snapshot_id}.manifest.json"
        temp_segment = self.snapshots_dir / f".{snapshot_id}.segment.tmp"
        temp_manifest = self.snapshots_dir / f".{snapshot_id}.manifest.tmp"
        txn_seq, txn_id, created_at_us = self._next_txn()
        self._safe_unlink(temp_segment)
        self._safe_unlink(temp_manifest)
        try:
            size = create_segment(
                temp_segment,
                segment_id=snapshot_id,
                kind="snapshot",
                snapshot_seq=snapshot_seq,
                generated_at_us=generated_at_us,
                records=records,
            )
            header, candidate_index, _ = rebuild_index(temp_segment)
            if header.segment_id != snapshot_id or header.snapshot_seq != snapshot_seq:
                raise SnapshotInvalid("new snapshot header does not match its generated identity")
            candidate_index = {
                key: replace(entry, segment_path=str(target)) for key, entry in candidate_index.items()
            }
            candidate = self._candidate_snapshot_state(
                target=target,
                header=header,
                index=candidate_index,
                txn_seq=txn_seq,
                snapshot_seq=snapshot_seq,
                snapshot_id=snapshot_id,
            )
            manifest = {
                "manifest_version": 1,
                "format_version": header.format_version,
                "snapshot_seq": snapshot_seq,
                "snapshot_id": snapshot_id,
                "generated_at_us": generated_at_us,
                "segment_file": target.name,
                "segment_size": size,
                "sha256": sha256_file(temp_segment),
                "record_count": len(records),
                "live_record_count": sum(1 for record in records if not record.deleted),
            }
            atomic_write_json(temp_manifest, manifest)
            intent: Dict[str, Any] = {
                "intent_version": INTENT_VERSION,
                "txn_seq": txn_seq,
                "txn_id": txn_id,
                "type": "snapshot",
                "operation": "snapshot_create_and_switch",
                "created_at_us": created_at_us,
                "active_segment": self._rel(target),
                "pre_active_segment": self._rel(state.segment_path),
                "target_segment": self._rel(target),
                "target_manifest": self._rel(manifest_target),
                "temp_segment": self._rel(temp_segment),
                "temp_manifest": self._rel(temp_manifest),
                "created_target": True,
                "pre_size": 0,
                "expected_post_size": size,
                "snapshot_seq": snapshot_seq,
                "snapshot_id": snapshot_id,
                "pre_snapshot_seq": state.snapshot_seq,
                "record_count": len(records),
                "segment_id": snapshot_id,
                "pre_txn_seq": state.last_txn_seq,
            }
            atomic_write_json(self._intent_path(txn_id), intent)
            os.replace(temp_segment, target)
            os.replace(temp_manifest, manifest_target)
            fsync_directory(self.snapshots_dir)
            final_size = target.stat().st_size
            if final_size != size:
                raise SnapshotInvalid("snapshot size changed during atomic rename")
            commit = {
                "commit_version": COMMIT_VERSION,
                "txn_seq": txn_seq,
                "txn_id": txn_id,
                "type": "snapshot",
                "operation": "snapshot_create_and_switch",
                "post_size": final_size,
                "post_active_segment": self._rel(target),
                "segment_id": snapshot_id,
                "snapshot_seq": snapshot_seq,
                "snapshot_id": snapshot_id,
                "committed_at_us": self._now_us(),
            }
            atomic_write_json(self._commit_path(txn_id), commit)
            self._publish_state(candidate)
            self._write_meta_best_effort(candidate)
            return {
                "snapshot_id": snapshot_id,
                "snapshot_seq": snapshot_seq,
                "generated_at_us": generated_at_us,
                "segment": self._rel(target),
                "manifest": self._rel(manifest_target),
                "record_count": len(records),
                "live_record_count": candidate.live_count,
                "transaction_seq": txn_seq,
                "atomic_switch": True,
            }
        except Exception as exc:
            # No commit marker exists until every validation and rename succeeds.
            commit_exists = self._commit_path(txn_id).exists()
            self._safe_unlink(temp_segment)
            self._safe_unlink(temp_manifest)
            if self._intent_path(txn_id).exists() and not commit_exists:
                self._safe_unlink(target)
                self._safe_unlink(manifest_target)
                self._write_abort(txn_seq, txn_id, "snapshot_creation_failed", str(exc))
            if isinstance(exc, SnapshotInvalid):
                raise
            if isinstance(exc, (FormatInvalid, StorageError, InvalidArgument)):
                raise SnapshotInvalid(
                    "snapshot creation failed validation",
                    {"detail": str(exc)},
                ) from exc
            raise

    def _candidate_snapshot_state(
        self,
        *,
        target: Path,
        header: SegmentHeader,
        index: Dict[str, IndexEntry],
        txn_seq: int,
        snapshot_seq: int,
        snapshot_id: str,
    ) -> StoreState:
        return StoreState(
            index=index,
            expiry_heap=self._heap_from_index(index),
            segment_path=target,
            segment_id=header.segment_id,
            snapshot_seq=snapshot_seq,
            snapshot_id=snapshot_id,
            last_txn_seq=txn_seq,
            generated_at_us=header.generated_at_us,
            live_count=self._live_count(index, self._now_us()),
        )

    def _load_snapshot_locked(self, supplied_path: Path) -> Dict[str, Any]:
        state = self._state
        target = supplied_path.expanduser()
        if not target.is_absolute():
            target = (self.data_dir / target).resolve()
        else:
            target = target.resolve()
        try:
            if not target.exists() or not target.is_file():
                raise SnapshotInvalid("snapshot file does not exist", {"path": str(target)})
            header = read_header(target)
            if header.kind != "snapshot":
                raise SnapshotInvalid("segment is not marked as a snapshot", {"path": str(target)})
            if header.snapshot_seq <= state.snapshot_seq:
                raise SnapshotInvalid(
                    "snapshot sequence is not strictly increasing",
                    {"path": str(target), "current_snapshot_seq": state.snapshot_seq, "candidate_snapshot_seq": header.snapshot_seq},
                )
            manifest_path = target.with_suffix(".manifest.json")
            manifest: Optional[Dict[str, Any]] = None
            if manifest_path.exists():
                manifest = read_json(manifest_path)
                if int(manifest.get("snapshot_seq", -1)) != header.snapshot_seq:
                    raise SnapshotInvalid("snapshot manifest sequence does not match the segment header")
                if manifest.get("format_version") not in (None, header.format_version):
                    raise SnapshotInvalid("snapshot manifest format version does not match the segment header")
                for identity_field in ("segment_id", "snapshot_id"):
                    identity_value = manifest.get(identity_field)
                    if identity_value is not None and identity_value != header.segment_id:
                        raise SnapshotInvalid("snapshot manifest identity does not match the segment header")
                if manifest.get("segment_size") is not None and int(manifest["segment_size"]) != target.stat().st_size:
                    raise SnapshotInvalid("snapshot manifest size does not match the segment file")
                if manifest.get("sha256") and manifest["sha256"] != sha256_file(target):
                    raise SnapshotInvalid("snapshot manifest SHA-256 does not match the segment file")
            _, candidate_index, _ = rebuild_index(target)
            candidate = self._candidate_snapshot_state(
                target=target,
                header=header,
                index=candidate_index,
                txn_seq=state.last_txn_seq,
                snapshot_seq=header.snapshot_seq,
                snapshot_id=target.stem,
            )
        except SnapshotInvalid:
            raise
        except Exception as exc:
            raise SnapshotInvalid(
                "snapshot validation failed; old snapshot remains active",
                {"path": str(target), "detail": str(exc)},
            ) from exc

        txn_seq, txn_id, created_at_us = self._next_txn()
        candidate.last_txn_seq = txn_seq
        size = target.stat().st_size
        intent: Dict[str, Any] = {
            "intent_version": INTENT_VERSION,
            "txn_seq": txn_seq,
            "txn_id": txn_id,
            "type": "snapshot",
            "operation": "snapshot_load_and_switch",
            "created_at_us": created_at_us,
            "active_segment": self._rel(target),
            "pre_active_segment": self._rel(state.segment_path),
            "target_segment": self._rel(target),
            "created_target": False,
            "pre_size": 0,
            "expected_post_size": size,
            "snapshot_seq": header.snapshot_seq,
            "snapshot_id": target.stem,
            "pre_snapshot_seq": state.snapshot_seq,
            "record_count": len(candidate.index),
            "segment_id": header.segment_id,
            "pre_txn_seq": state.last_txn_seq,
        }
        atomic_write_json(self._intent_path(txn_id), intent)
        try:
            commit = {
                "commit_version": COMMIT_VERSION,
                "txn_seq": txn_seq,
                "txn_id": txn_id,
                "type": "snapshot",
                "operation": "snapshot_load_and_switch",
                "post_size": size,
                "post_active_segment": self._rel(target),
                "segment_id": header.segment_id,
                "snapshot_seq": header.snapshot_seq,
                "snapshot_id": target.stem,
                "committed_at_us": self._now_us(),
            }
            atomic_write_json(self._commit_path(txn_id), commit)
        except Exception as exc:
            self._write_abort(txn_seq, txn_id, "snapshot_commit_failed", str(exc))
            raise SnapshotInvalid("failed to persist snapshot switch commit; old snapshot remains active", {"detail": str(exc)}) from exc
        self._publish_state(candidate)
        self._write_meta_best_effort(candidate)
        return {
            "snapshot_id": target.stem,
            "snapshot_seq": header.snapshot_seq,
            "generated_at_us": header.generated_at_us,
            "segment": self._rel(target),
            "format_version": header.format_version,
            "record_count": len(candidate.index),
            "live_record_count": candidate.live_count,
            "transaction_seq": txn_seq,
            "atomic_switch": True,
        }

    def current_recovery_report(self) -> Dict[str, Any]:
        report_path = self.recovery_dir / "last_report.json"
        if report_path.exists():
            try:
                return read_json(report_path)
            except Exception:
                pass
        return self._recovery_report
