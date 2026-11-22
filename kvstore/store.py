from __future__ import annotations

import base64
import binascii
import hashlib
import json
import os
import signal
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable

from .db import IntentDB
from .errors import StoreError
from .formats import (
    decode_import,
    encode_binary,
    encode_jsonl,
    encode_jsonl_shards,
)
from .heap import ExpiryIndex
from .model import (
    DecodedRecord,
    KeyRef,
    ParsedRecord,
    Record,
    SegmentInfo,
    StoreState,
    Tombstone,
)
from .segments import SegmentManager
from .snapshot import (
    SnapshotData,
    build_snapshot_document,
    parse_snapshot,
    snapshot_bytes,
)


@dataclass(frozen=True, slots=True)
class CommitResult:
    tx_id: str
    segment: SegmentInfo
    state_version: int
    applied_records: int


def _now_default() -> int:
    return int(time.time() * 1000)


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _value_public(value: bytes) -> tuple[str, str]:
    try:
        return value.decode("utf-8"), "utf8"
    except UnicodeDecodeError:
        return base64.b64encode(value).decode("ascii"), "base64"


def result_batch_suffix() -> str:
    return uuid.uuid4().hex


class ReadView:
    """A stable read-only view of one published StoreState.

    A writer may publish any number of later states while this object is held;
    this view continues to resolve segment offsets against its original state.
    """

    def __init__(self, store: "Store", state: StoreState, captured_at_ms: int) -> None:
        self._store = store
        self.state = state
        self.captured_at_ms = captured_at_ms

    def ref(self, key: str) -> KeyRef | None:
        return self.state.index.get(key)

    def record(self, key: str, *, now_ms: int | None = None) -> Record:
        now = self._store._now_ms() if now_ms is None else now_ms
        ref = self.ref(key)
        if ref is None:
            tombstone = self.state.tombstones.get(key)
            raise StoreError(
                "E_KEY_NOT_FOUND",
                "key does not exist in this read snapshot",
                {
                    "key": key,
                    "state_version": self.state.state_version,
                    "tombstone": None
                    if tombstone is None
                    else {
                        "version": tombstone.version,
                        "write_timestamp_ms": tombstone.write_ts_ms,
                        "reason": tombstone.reason,
                    },
                },
            )
        if ref.expires_at_ms is not None and ref.expires_at_ms <= now:
            raise StoreError(
                "E_KEY_EXPIRED",
                "key is expired in this read snapshot",
                {
                    "key": key,
                    "expires_at_ms": ref.expires_at_ms,
                    "now_ms": now,
                    "state_version": self.state.state_version,
                },
            )
        info = self.state.segments.get(ref.segment_id)
        if info is None:
            raise StoreError(
                "E_SEGMENT_INVALID",
                "read snapshot no longer has the segment referenced by its key index",
                {"key": key, "segment_id": ref.segment_id},
            )
        return self._store.segments.read_record(
            info,
            offset=ref.offset,
            length=ref.length,
            expected_version=ref.version,
            key=key,
        )

    def public_record(self, key: str, *, now_ms: int | None = None) -> dict[str, Any]:
        record = self.record(key, now_ms=now_ms)
        ref = self.ref(key)
        assert ref is not None
        value, encoding = _value_public(record.value)
        return {
            "key": record.key,
            "value": value,
            "value_encoding": encoding,
            "version": record.version,
            "write_timestamp_ms": record.write_ts_ms,
            "write_timestamp_us": record.write_ts_ms * 1000,
            "expires_at_ms": record.expires_at_ms,
            "expires_at_us": None
            if record.expires_at_ms is None
            else record.expires_at_ms * 1000,
            "segment": {
                "id": ref.segment_id,
                "offset": ref.offset,
                "length": ref.length,
            },
            "token": ref.token,
            "state_version": self.state.state_version,
        }

    def records(
        self, *, now_ms: int | None = None, include_expired: bool = False
    ) -> list[Record]:
        now = self._store._now_ms() if now_ms is None else now_ms
        result: list[Record] = []
        for key, ref in sorted(self.state.index.items()):
            if (
                not include_expired
                and ref.expires_at_ms is not None
                and ref.expires_at_ms <= now
            ):
                continue
            info = self.state.segments.get(ref.segment_id)
            if info is None:
                raise StoreError(
                    "E_SEGMENT_INVALID",
                    "read snapshot references a missing segment",
                    {"key": key, "segment_id": ref.segment_id},
                )
            result.append(
                self._store.segments.read_record(
                    info,
                    offset=ref.offset,
                    length=ref.length,
                    expected_version=ref.version,
                    key=key,
                )
            )
        return result


class Store:
    def __init__(
        self,
        data_dir: str | Path,
        *,
        clock: Callable[[], int] | None = None,
        auto_recover: bool = True,
    ) -> None:
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.segments = SegmentManager(self.data_dir / "segments")
        self.db = IntentDB(self.data_dir / "intents.sqlite3")
        self._clock = clock or _now_default
        self._writer_lock = threading.RLock()
        self._publish_lock = threading.Lock()
        self._closed = False
        empty_time = self._now_ms()
        self._state = StoreState(
            state_version=0,
            index={},
            tombstones={},
            expiry=ExpiryIndex(),
            segments={},
            max_segment_seq=0,
            last_snapshot_seq=0,
            created_at_ms=empty_time,
        )
        self._last_recovery_report: dict[str, Any] | None = None
        self._last_maintenance_report: dict[str, Any] | None = None
        if auto_recover:
            self._startup_recovery()

    # ------------------------------------------------------------------
    # Clock, state publication and recovery
    # ------------------------------------------------------------------
    def _now_ms(self) -> int:
        return int(self._clock())

    def _crash_point(self, name: str) -> None:
        if os.environ.get("KVSTORE_CRASH_POINT") == name:
            try:
                os.kill(os.getpid(), signal.SIGKILL)
            finally:  # pragma: no cover - only reached if SIGKILL is unavailable
                os._exit(137)

    def _snapshot_state(self) -> StoreState:
        with self._publish_lock:
            return self._state

    def _publish(self, state: StoreState) -> None:
        with self._publish_lock:
            self._state = state

    def read_view(self) -> ReadView:
        state = self._snapshot_state()
        return ReadView(self, state, self._now_ms())

    def stats(self) -> dict[str, Any]:
        state = self._snapshot_state()
        return {
            "state": state.stats(),
            "data_dir": str(self.data_dir),
            "intent_db": str(self.db.path),
        }

    def _build_state_from_infos(
        self, infos: list[SegmentInfo]
    ) -> tuple[StoreState, list[dict[str, Any]], dict[str, tuple[SegmentInfo, list[DecodedRecord]]]]:
        issues: list[dict[str, Any]] = []
        loaded: dict[str, tuple[SegmentInfo, list[DecodedRecord]]] = {}
        for info in infos:
            try:
                decoded = self.segments.read_segment(info, verify_digest=True)
            except StoreError as exc:
                issues.append(
                    {
                        "kind": "segment_load_failed",
                        "segment_id": info.segment_id,
                        "path": info.path,
                        "code": exc.code,
                        "error": exc.message,
                        "context": exc.context,
                    }
                )
                continue
            loaded[info.segment_id] = (info, decoded)
        valid = [item[0] for item in loaded.values()]
        valid.sort(key=lambda item: item.seq)
        snapshots = [
            info
            for info in valid
            if info.kind == "snapshot" and info.snapshot_seq is not None
        ]
        base: SegmentInfo | None = None
        if snapshots:
            base = max(snapshots, key=lambda item: (int(item.snapshot_seq or 0), item.seq))
        if base is None:
            replay = valid
        else:
            replay = [base] + [info for info in valid if info.seq > base.seq]
        state = StoreState(
            state_version=0,
            index={},
            tombstones={},
            expiry=ExpiryIndex(),
            segments={},
            max_segment_seq=max((info.seq for info in valid), default=0),
            last_snapshot_seq=int(base.snapshot_seq or 0) if base else 0,
            baseline_snapshot_segment_id=base.segment_id if base else None,
            created_at_ms=self._now_ms(),
        )
        for info in replay:
            _, decoded = loaded[info.segment_id]
            self._apply_decoded(state, decoded, info)
            state.segments[info.segment_id] = info
            state.state_version = info.seq
            if info.kind == "snapshot" and info.snapshot_seq is not None:
                state.last_snapshot_seq = max(state.last_snapshot_seq, info.snapshot_seq)
        # Old, superseded segments are deliberately not in the read-state
        # catalog, but their maximum sequence is still respected to avoid reuse.
        state.max_segment_seq = max(
            state.max_segment_seq,
            int(self.db.get_meta("max_segment_seq", 0) or 0),
        )
        return state, issues, loaded

    def _startup_recovery(self) -> None:
        infos, scan_issues = self.segments.scan(recover_missing_meta=True)
        state, load_issues, loaded = self._build_state_from_infos(infos)
        self._publish(state)
        intents = self.db.list_intents()
        intent_report: list[dict[str, Any]] = []
        recovered = 0
        rolled_back = 0
        verified = 0
        committed_missing: list[dict[str, Any]] = []
        for intent in intents:
            segment_id = intent.get("segment_id")
            segment_exists = segment_id in loaded
            status = intent["status"]
            if status == "committed":
                if segment_exists:
                    verified += 1
                else:
                    committed_missing.append(
                        {
                            "tx_id": intent["tx_id"],
                            "batch_id": intent["batch_id"],
                            "segment_id": segment_id,
                            "reason": "committed intent has no valid durable segment",
                        }
                    )
                continue
            if segment_exists:
                action = "recovered_from_complete_segment"
                recovered += 1
                self.db.update_intent(
                    intent["tx_id"],
                    "recovered",
                    detail={"action": action, "segment_id": segment_id},
                    now_ms=self._now_ms(),
                )
            else:
                action = "rolled_back_no_complete_segment"
                rolled_back += 1
                self.db.update_intent(
                    intent["tx_id"],
                    "rolled_back",
                    detail={"action": action, "segment_id": segment_id},
                    now_ms=self._now_ms(),
                )
            intent_report.append(
                {
                    "tx_id": intent["tx_id"],
                    "batch_id": intent["batch_id"],
                    "kind": intent["kind"],
                    "previous_status": status,
                    "action": action,
                    "segment_id": segment_id,
                }
            )
        report = {
            "report_version": 1,
            "source": "startup_scan_of_sqlite_intents_and_durable_segments",
            "generated_at_ms": self._now_ms(),
            "facts_only": True,
            "process_exit_report_available": False,
            "note": (
                "The recovery report is reconstructed only from durable segment "
                "files, their summaries and SQLite intent rows. A SIGKILL cannot "
                "produce an orderly process-exit report."
            ),
            "scan_issues": scan_issues,
            "segment_load_issues": load_issues,
            "segments": {
                "valid_on_disk": len(infos),
                "loaded_into_state": len(loaded),
                "active_in_state": len(state.segments),
            },
            "intents": intent_report,
            "recovered_count": recovered,
            "rolled_back_count": rolled_back,
            "verified_committed_count": verified,
            "committed_missing_segments": committed_missing,
            "state": state.stats(),
        }
        self._last_recovery_report = report
        self.db.save_recovery_report(report, self._now_ms())
        self.db.set_meta("max_segment_seq", state.max_segment_seq)
        self.db.set_meta("state_version", state.state_version)
        self.db.set_meta("last_snapshot_seq", state.last_snapshot_seq)

    # ------------------------------------------------------------------
    # State helpers
    # ------------------------------------------------------------------
    def _apply_decoded(
        self, state: StoreState, decoded: Iterable[DecodedRecord], info: SegmentInfo
    ) -> None:
        for item in decoded:
            record = item.record
            token = f"{info.segment_id}:{item.record_index}"
            if record.deleted:
                state.index.pop(record.key, None)
                state.expiry.remove(record.key)
                state.tombstones[record.key] = Tombstone(
                    key=record.key,
                    version=record.version,
                    write_ts_ms=record.write_ts_ms,
                    segment_id=info.segment_id,
                    reason="deleted",
                )
                continue
            ref = KeyRef(
                key=record.key,
                version=record.version,
                segment_id=info.segment_id,
                offset=item.offset,
                length=item.length,
                write_ts_ms=record.write_ts_ms,
                expires_at_ms=record.expires_at_ms,
                token=token,
            )
            state.index[record.key] = ref
            state.expiry.upsert(
                record.key,
                record.expires_at_ms,
                record.version,
                token,
            )

    def _next_seq(self, state: StoreState) -> int:
        db_max = int(self.db.get_meta("max_segment_seq", 0) or 0)
        return max(state.max_segment_seq, db_max) + 1

    def _commit_records_locked(
        self,
        records: list[Record],
        *,
        kind: str,
        batch_id: str,
        replace_state_mode: str | None = None,
        snapshot_seq: int | None = None,
        expected_base_version: int | None = None,
        intent_payload_extra: dict[str, Any] | None = None,
    ) -> CommitResult:
        base = self._snapshot_state()
        if expected_base_version is not None and base.state_version != expected_base_version:
            raise StoreError(
                "E_STALE_BASE",
                "the operation was based on a stale state version",
                {
                    "expected_base_state_version": expected_base_version,
                    "current_state_version": base.state_version,
                },
            )
        seq = self._next_seq(base)
        tx_id = uuid.uuid4().hex
        segment_id, path, _ = self.segments.allocate(
            seq=seq, kind=kind, txn_id=tx_id, snapshot_seq=snapshot_seq
        )
        payload = {
            "kind": kind,
            "batch_id": batch_id,
            "record_count": len(records),
            "keys": sorted({record.key for record in records}),
            "snapshot_seq": snapshot_seq,
            "replace_state_mode": replace_state_mode,
        }
        if intent_payload_extra:
            payload.update(intent_payload_extra)
        now = self._now_ms()
        self.db.begin_intent(
            tx_id=tx_id,
            batch_id=batch_id,
            kind=kind,
            segment_id=segment_id,
            snapshot_seq=snapshot_seq,
            payload=payload,
            now_ms=now,
        )
        self._crash_point("after_intent")
        try:
            info, decoded = self.segments.write_segment(
                segment_id=segment_id,
                path=path,
                seq=seq,
                kind=kind,
                records=records,
                created_at_ms=now,
                batch_id=batch_id,
                snapshot_seq=snapshot_seq,
            )
        except Exception:
            self.db.update_intent(
                tx_id,
                "aborted",
                detail={"stage": "segment_write"},
                now_ms=self._now_ms(),
            )
            raise
        self.db.update_intent(
            tx_id,
            "segment_durable",
            detail={"segment_id": segment_id, "sha256": info.sha256},
            now_ms=self._now_ms(),
        )
        self._crash_point("after_segment")
        try:
            if replace_state_mode is None:
                new_state = base.clone()
                self._apply_decoded(new_state, decoded, info)
                new_state.segments[info.segment_id] = info
                new_state.max_segment_seq = max(base.max_segment_seq, seq)
                new_state.state_version = seq
            else:
                new_state = StoreState(
                    state_version=seq,
                    index={},
                    tombstones={},
                    expiry=ExpiryIndex(),
                    segments={},
                    max_segment_seq=seq,
                    last_snapshot_seq=base.last_snapshot_seq,
                    baseline_snapshot_segment_id=base.baseline_snapshot_segment_id,
                    created_at_ms=base.created_at_ms,
                )
                self._apply_decoded(new_state, decoded, info)
                new_state.segments[info.segment_id] = info
                if replace_state_mode == "snapshot":
                    if snapshot_seq is None:
                        raise StoreError(
                            "E_INTERNAL",
                            "snapshot replacement requires snapshot_seq",
                        )
                    new_state.last_snapshot_seq = snapshot_seq
                    new_state.baseline_snapshot_segment_id = info.segment_id
            self._publish(new_state)
            self._crash_point("after_publish")
            self.db.update_intent(
                tx_id,
                "committed",
                detail={
                    "segment_id": segment_id,
                    "sha256": info.sha256,
                    "state_version": seq,
                },
                now_ms=self._now_ms(),
            )
            self.db.set_meta("max_segment_seq", seq)
            self.db.set_meta("state_version", seq)
            self.db.set_meta("last_snapshot_seq", new_state.last_snapshot_seq)
            return CommitResult(
                tx_id=tx_id,
                segment=info,
                state_version=seq,
                applied_records=len(decoded),
            )
        except Exception:
            # If publication has not happened, the durable segment must not be
            # mistaken for a completed transaction on the next startup.
            published = self._snapshot_state().state_version == seq
            if not published:
                Path(info.path).unlink(missing_ok=True)
                Path(info.meta_path).unlink(missing_ok=True)
                self.db.update_intent(
                    tx_id,
                    "aborted",
                    detail={"stage": "state_publish"},
                    now_ms=self._now_ms(),
                )
            raise

    # ------------------------------------------------------------------
    # Point reads and writes
    # ------------------------------------------------------------------
    @staticmethod
    def _validate_key(key: str) -> None:
        if not isinstance(key, str) or not key:
            raise StoreError(
                "E_BAD_REQUEST", "key must be a non-empty string", {"key": key}
            )
        if len(key.encode("utf-8")) > 1_048_576:
            raise StoreError(
                "E_BAD_REQUEST", "key is too large", {"key_bytes": len(key.encode("utf-8"))}
            )

    def get(self, key: str, *, now_ms: int | None = None) -> dict[str, Any]:
        self._validate_key(key)
        return self.read_view().public_record(key, now_ms=now_ms)

    def get_record(self, key: str, *, now_ms: int | None = None) -> Record:
        self._validate_key(key)
        return self.read_view().record(key, now_ms=now_ms)

    def put(
        self,
        key: str,
        value: bytes | bytearray | memoryview | str,
        *,
        version: int | None = None,
        write_ts_ms: int | None = None,
        expires_at_ms: int | None = None,
        expected_version: int | None = None,
        base_state_version: int | None = None,
        batch_id: str | None = None,
    ) -> dict[str, Any]:
        self._validate_key(key)
        if isinstance(value, str):
            value_bytes = value.encode("utf-8")
        else:
            value_bytes = bytes(value)
        if expires_at_ms is not None and (
            not isinstance(expires_at_ms, int) or isinstance(expires_at_ms, bool)
        ):
            raise StoreError(
                "E_BAD_REQUEST",
                "expires_at_ms must be null or an integer",
                {"expires_at_ms": expires_at_ms},
            )
        with self._writer_lock:
            base = self._snapshot_state()
            if base_state_version is not None and base.state_version != base_state_version:
                raise StoreError(
                    "E_STALE_BASE",
                    "write was based on a stale state version",
                    {
                        "expected_base_state_version": base_state_version,
                        "current_state_version": base.state_version,
                    },
                )
            current = base.index.get(key)
            tombstone = base.tombstones.get(key)
            baseline_version = (
                current.version
                if current is not None
                else (tombstone.version if tombstone is not None else None)
            )
            if expected_version is not None and expected_version != baseline_version:
                raise StoreError(
                    "E_STALE_BASE",
                    "expected key version does not match the current version",
                    {
                        "key": key,
                        "expected_version": expected_version,
                        "current_version": baseline_version,
                    },
                )
            if version is None:
                version = (baseline_version or 0) + 1
            if not isinstance(version, int) or isinstance(version, bool) or version < 1:
                raise StoreError(
                    "E_BAD_REQUEST",
                    "version must be a positive integer",
                    {"version": version},
                )
            if baseline_version is not None and version <= baseline_version:
                raise StoreError(
                    "E_VERSION_CONFLICT",
                    "new version must be greater than the previous key version",
                    {
                        "key": key,
                        "incoming_version": version,
                        "previous_version": baseline_version,
                    },
                )
            write_ms = self._now_ms() if write_ts_ms is None else int(write_ts_ms)
            record = Record(
                key=key,
                value=value_bytes,
                version=version,
                write_ts_ms=write_ms,
                expires_at_ms=expires_at_ms,
                deleted=False,
            )
            result = self._commit_records_locked(
                [record],
                kind="txn",
                batch_id=batch_id or f"write-{result_batch_suffix()}",
            )
        state = self._snapshot_state()
        ref = state.index[key]
        value_text, encoding = _value_public(record.value)
        public = {
            "key": key,
            "value": value_text,
            "value_encoding": encoding,
            "version": record.version,
            "write_timestamp_ms": record.write_ts_ms,
            "write_timestamp_us": record.write_ts_ms * 1000,
            "expires_at_ms": record.expires_at_ms,
            "expires_at_us": None
            if record.expires_at_ms is None
            else record.expires_at_ms * 1000,
            "segment": {
                "id": ref.segment_id,
                "offset": ref.offset,
                "length": ref.length,
            },
            "token": ref.token,
            "state_version": result.state_version,
        }
        public["transaction"] = {
            "tx_id": result.tx_id,
            "segment_id": result.segment.segment_id,
            "state_version": result.state_version,
        }
        return public

    def delete(
        self,
        key: str,
        *,
        version: int | None = None,
        write_ts_ms: int | None = None,
        expected_version: int | None = None,
        base_state_version: int | None = None,
        batch_id: str | None = None,
    ) -> dict[str, Any]:
        self._validate_key(key)
        with self._writer_lock:
            base = self._snapshot_state()
            if base_state_version is not None and base.state_version != base_state_version:
                raise StoreError(
                    "E_STALE_BASE",
                    "delete was based on a stale state version",
                    {
                        "expected_base_state_version": base_state_version,
                        "current_state_version": base.state_version,
                    },
                )
            current = base.index.get(key)
            if current is None:
                raise StoreError(
                    "E_KEY_NOT_FOUND", "cannot delete a key that does not exist", {"key": key}
                )
            if expected_version is not None and expected_version != current.version:
                raise StoreError(
                    "E_STALE_BASE",
                    "expected key version does not match the current version",
                    {
                        "key": key,
                        "expected_version": expected_version,
                        "current_version": current.version,
                    },
                )
            if version is None:
                version = current.version + 1
            if not isinstance(version, int) or isinstance(version, bool) or version <= current.version:
                raise StoreError(
                    "E_VERSION_CONFLICT",
                    "delete version must be greater than the current version",
                    {
                        "key": key,
                        "incoming_version": version,
                        "current_version": current.version,
                    },
                )
            record = Record(
                key=key,
                value=b"",
                version=version,
                write_ts_ms=self._now_ms() if write_ts_ms is None else int(write_ts_ms),
                expires_at_ms=None,
                deleted=True,
            )
            result = self._commit_records_locked(
                [record],
                kind="txn",
                batch_id=batch_id or f"delete-{result_batch_suffix()}",
            )
            return {
                "ok": True,
                "key": key,
                "deleted_version": version,
                "tombstone": {
                    "key": key,
                    "version": version,
                    "write_timestamp_ms": record.write_ts_ms,
                    "reason": "deleted",
                },
                "transaction": {
                    "tx_id": result.tx_id,
                    "segment_id": result.segment.segment_id,
                    "state_version": result.state_version,
                },
            }

    # ------------------------------------------------------------------
    # Exports
    # ------------------------------------------------------------------
    def _records_from_view(self, view: ReadView) -> list[Record]:
        return view.records(now_ms=self._now_ms())

    def export_binary(self, *, endian: str = "little") -> bytes:
        return encode_binary(self._records_from_view(self.read_view()), endian=endian)

    def export_jsonl(self, *, shard_count: int = 1) -> bytes:
        if shard_count < 1:
            raise StoreError(
                "E_BAD_REQUEST", "shard_count must be at least 1", {"shard_count": shard_count}
            )
        return encode_jsonl(self._records_from_view(self.read_view()), shard_count=shard_count)

    def export_jsonl_shards(self, *, shard_count: int) -> dict[str, Any]:
        if shard_count < 1:
            raise StoreError(
                "E_BAD_REQUEST", "shard_count must be at least 1", {"shard_count": shard_count}
            )
        return encode_jsonl_shards(
            self._records_from_view(self.read_view()), shard_count=shard_count
        )

    # ------------------------------------------------------------------
    # Import conflict resolution and batch atomicity
    # ------------------------------------------------------------------
    @staticmethod
    def _compare_import_records(a: Record, b: Record) -> str:
        """Return a, b, same, or ambiguous for two records of the same key."""

        if a.version != b.version:
            return "a" if a.version > b.version else "b"
        if a.write_ts_ms != b.write_ts_ms:
            return "a" if a.write_ts_ms > b.write_ts_ms else "b"
        if a.content_key() == b.content_key():
            return "same"
        return "ambiguous"

    def _current_record(self, state: StoreState, key: str) -> Record | None:
        ref = state.index.get(key)
        if ref is None:
            return None
        info = state.segments.get(ref.segment_id)
        if info is None:
            raise StoreError(
                "E_SEGMENT_INVALID",
                "current key index references a segment outside the read state",
                {"key": key, "segment_id": ref.segment_id},
            )
        return self.segments.read_record(
            info,
            offset=ref.offset,
            length=ref.length,
            expected_version=ref.version,
            key=key,
        )

    @staticmethod
    def _record_report_fields(record: Record) -> dict[str, Any]:
        return {
            "key": record.key,
            "version": record.version,
            "write_timestamp_ms": record.write_ts_ms,
            "expires_at_ms": record.expires_at_ms,
            "deleted": record.deleted,
            "value_sha256": _sha(record.value),
            "value_bytes": len(record.value),
        }

    def _plan_import(
        self, parsed: list[ParsedRecord], base: StoreState
    ) -> tuple[list[dict[str, Any]], list[Record], dict[str, int], list[tuple[int, str]]]:
        decisions: list[dict[str, Any] | None] = [None] * len(parsed)
        winners: list[Record] = []
        groups: dict[str, list[int]] = {}
        for index, item in enumerate(parsed):
            groups.setdefault(item.record.key, []).append(index)
        current_cache: dict[str, Record | None] = {}
        summary = {"overwrite": 0, "skip": 0, "reject": 0}

        for key, indexes in groups.items():
            winner_index = indexes[0]
            group_rejected = False
            for index in indexes[1:]:
                relation = self._compare_import_records(
                    parsed[winner_index].record, parsed[index].record
                )
                if relation == "a":
                    decisions[index] = {
                        "outcome": "skip",
                        "reason": "file_duplicate_lower_priority",
                    }
                elif relation == "b":
                    decisions[winner_index] = {
                        "outcome": "skip",
                        "reason": "file_duplicate_lower_priority",
                    }
                    winner_index = index
                elif relation == "same":
                    decisions[index] = {
                        "outcome": "skip",
                        "reason": "file_duplicate_identical",
                    }
                else:
                    for remaining in indexes:
                        if decisions[remaining] is None:
                            decisions[remaining] = {
                                "outcome": "reject",
                                "reason": "ambiguous_file_duplicate_same_version_and_timestamp",
                            }
                    group_rejected = True
                    break
            if group_rejected:
                continue

            incoming = parsed[winner_index].record
            current_ref = base.index.get(key)
            tombstone = base.tombstones.get(key)
            if current_ref is not None:
                if key not in current_cache:
                    current_cache[key] = self._current_record(base, key)
                current_record = current_cache[key]
                assert current_record is not None
                if incoming.version > current_record.version:
                    decisions[winner_index] = {
                        "outcome": "overwrite",
                        "reason": "higher_version",
                    }
                elif incoming.version < current_record.version:
                    decisions[winner_index] = {
                        "outcome": "skip",
                        "reason": "lower_version",
                    }
                elif incoming.content_key() == current_record.content_key():
                    decisions[winner_index] = {
                        "outcome": "skip",
                        "reason": "identical_record",
                    }
                elif incoming.write_ts_ms > current_record.write_ts_ms:
                    decisions[winner_index] = {
                        "outcome": "overwrite",
                        "reason": "same_version_newer_write_timestamp",
                    }
                elif incoming.write_ts_ms < current_record.write_ts_ms:
                    decisions[winner_index] = {
                        "outcome": "skip",
                        "reason": "same_version_older_write_timestamp",
                    }
                else:
                    decisions[winner_index] = {
                        "outcome": "reject",
                        "reason": "equal_version_and_write_timestamp_with_different_content",
                    }
            elif tombstone is not None:
                if incoming.version > tombstone.version:
                    decisions[winner_index] = {
                        "outcome": "overwrite",
                        "reason": "resurrect_after_delete_higher_version",
                    }
                elif incoming.version < tombstone.version:
                    decisions[winner_index] = {
                        "outcome": "skip",
                        "reason": "lower_than_delete_tombstone_version",
                    }
                elif incoming.write_ts_ms > tombstone.write_ts_ms:
                    decisions[winner_index] = {
                        "outcome": "overwrite",
                        "reason": "resurrect_after_delete_newer_write_timestamp",
                    }
                else:
                    decisions[winner_index] = {
                        "outcome": "skip",
                        "reason": "delete_tombstone_wins_tie",
                    }
            else:
                decisions[winner_index] = {
                    "outcome": "overwrite",
                    "reason": "new_key",
                }

        reports: list[dict[str, Any]] = []
        for index, item in enumerate(parsed):
            decision = decisions[index]
            if decision is None:
                decision = {
                    "outcome": "reject",
                    "reason": "unresolved_duplicate_group",
                }
                decisions[index] = decision
            outcome = decision["outcome"]
            summary[outcome] += 1
            current_ref = base.index.get(item.record.key)
            tombstone = base.tombstones.get(item.record.key)
            current_info: dict[str, Any] | None = None
            if current_ref is not None:
                current_info = {
                    "source": "live",
                    "version": current_ref.version,
                    "segment_id": current_ref.segment_id,
                    "offset": current_ref.offset,
                    "length": current_ref.length,
                    "write_timestamp_ms": current_ref.write_ts_ms,
                    "expires_at_ms": current_ref.expires_at_ms,
                }
                if item.record.key in current_cache and current_cache[item.record.key] is not None:
                    current_info["value_sha256"] = _sha(current_cache[item.record.key].value)  # type: ignore[union-attr]
            elif tombstone is not None:
                current_info = {
                    "source": "tombstone",
                    "version": tombstone.version,
                    "segment_id": tombstone.segment_id,
                    "write_timestamp_ms": tombstone.write_ts_ms,
                }
            reports.append(
                {
                    "record_index": item.record_index,
                    "line_no": item.line_no,
                    "shard_id": item.shard_id,
                    "key": item.record.key,
                    "outcome": outcome,
                    "reason": decision["reason"],
                    "incoming": self._record_report_fields(item.record),
                    "current": current_info,
                    "defaults_used": item.defaults_used,
                }
            )
            if outcome == "overwrite":
                winners.append(item.record)
        winners.sort(key=lambda record: next(
            item.record_index for item in parsed if item.record.key == record.key
        ))
        return reports, winners, summary, [
            (item.record_index, item.record.key) for item in parsed
        ]

    def import_data(
        self,
        data: bytes | bytearray | memoryview,
        *,
        requested_format: str = "auto",
        batch_id: str | None = None,
        base_state_version: int | None = None,
    ) -> dict[str, Any]:
        batch_id = batch_id or f"import-{uuid.uuid4().hex}"
        now = self._now_ms()
        claim_key = f"import:{batch_id}"
        if not self.db.claim_batch(claim_key, uuid.uuid4().hex, now):
            raise StoreError(
                "E_CONFLICTING_BATCH",
                "this import batch is already active or completed",
                {"batch_id": batch_id},
            )
        acquired = self._writer_lock.acquire(blocking=False)
        if not acquired:
            self.db.finish_batch(claim_key, "conflicting", self._now_ms())
            raise StoreError(
                "E_CONFLICTING_BATCH",
                "another writer currently owns the import/snapshot transaction",
                {"batch_id": batch_id},
            )
        claim_finished = False
        try:
            try:
                parsed, negotiation = decode_import(
                    bytes(data),
                    requested_format=requested_format,
                    import_time_ms=self._now_ms(),
                )
            except StoreError:
                self.db.finish_batch(claim_key, "invalid", self._now_ms())
                claim_finished = True
                raise
            base = self._snapshot_state()
            if base_state_version is not None and base.state_version != base_state_version:
                self.db.finish_batch(claim_key, "stale", self._now_ms())
                claim_finished = True
                raise StoreError(
                    "E_STALE_BASE",
                    "import was based on a stale state version",
                    {
                        "batch_id": batch_id,
                        "expected_base_state_version": base_state_version,
                        "current_state_version": base.state_version,
                    },
                )
            reports, winners, summary, _ = self._plan_import(parsed, base)
            report: dict[str, Any] = {
                "report_version": 1,
                "batch_id": batch_id,
                "status": "pending",
                "format": negotiation,
                "state_version_before": base.state_version,
                "summary": {
                    **summary,
                    "input_records": len(parsed),
                    "applied_records": 0,
                },
                "records": reports,
                "rounding_report": negotiation.get("rounding_report", []),
                "cumulative_rounding_error_us": negotiation.get(
                    "cumulative_rounding_error_us", 0
                ),
            }
            now = self._now_ms()
            if summary["reject"]:
                report["status"] = "rejected"
                report["rollback"] = {
                    "performed": True,
                    "reason": "at_least_one_record_was_rejected",
                }
                self.db.save_import_report(
                    batch_id=batch_id,
                    status="rejected",
                    format_name=negotiation.get("detected_format"),
                    report=report,
                    now_ms=now,
                )
                self.db.finish_batch(claim_key, "rejected", now)
                claim_finished = True
                raise StoreError(
                    "E_IMPORT_REJECTED",
                    "import was rejected and rolled back because a record conflicted ambiguously",
                    {"batch_id": batch_id, "report": report},
                )
            if not winners:
                report["status"] = "noop"
                report["state_version_after"] = base.state_version
                self.db.save_import_report(
                    batch_id=batch_id,
                    status="noop",
                    format_name=negotiation.get("detected_format"),
                    report=report,
                    now_ms=now,
                )
                self.db.finish_batch(claim_key, "noop", now)
                claim_finished = True
                return report
            result = self._commit_records_locked(
                winners,
                kind="txn",
                batch_id=batch_id,
                expected_base_version=base_state_version,
                intent_payload_extra={"source": "import"},
            )
            report["status"] = "applied"
            report["summary"]["applied_records"] = len(winners)
            report["state_version_after"] = result.state_version
            report["transaction"] = {
                "tx_id": result.tx_id,
                "segment_id": result.segment.segment_id,
                "sha256": result.segment.sha256,
            }
            self.db.save_import_report(
                batch_id=batch_id,
                status="applied",
                format_name=negotiation.get("detected_format"),
                report=report,
                now_ms=self._now_ms(),
            )
            self.db.finish_batch(claim_key, "applied", self._now_ms())
            claim_finished = True
            return report
        finally:
            if not claim_finished:
                self.db.finish_batch(claim_key, "failed", self._now_ms())
            self._writer_lock.release()

    def get_import_report(self, batch_id: str) -> dict[str, Any]:
        report = self.db.get_import_report(batch_id)
        if report is None:
            raise StoreError(
                "E_KEY_NOT_FOUND",
                "import report does not exist",
                {"batch_id": batch_id},
            )
        return report

    # ------------------------------------------------------------------
    # Snapshots
    # ------------------------------------------------------------------
    def export_snapshot(self, *, snapshot_seq: int | None = None) -> dict[str, Any]:
        view = self.read_view()
        state = view.state
        if snapshot_seq is None:
            snapshot_seq = state.last_snapshot_seq + 1
        if snapshot_seq <= state.last_snapshot_seq:
            raise StoreError(
                "E_SNAPSHOT_STALE",
                "new snapshot sequence must be greater than the last loaded sequence",
                {
                    "requested_snapshot_seq": snapshot_seq,
                    "last_snapshot_seq": state.last_snapshot_seq,
                },
            )
        # Snapshots preserve the complete index, including records that are
        # logically expired but not yet cleaned, so their version/offset facts
        # and later import conflict baseline survive a whole-state replacement.
        records = view.records(now_ms=self._now_ms(), include_expired=True)
        doc = build_snapshot_document(
            records=records,
            tombstones=state.tombstones.values(),
            snapshot_seq=snapshot_seq,
            generated_at_ms=self._now_ms(),
            state_version=state.state_version,
            base_snapshot_seq=state.last_snapshot_seq,
        )
        return doc

    def export_snapshot_bytes(self, *, snapshot_seq: int | None = None) -> bytes:
        return snapshot_bytes(self.export_snapshot(snapshot_seq=snapshot_seq))

    def load_snapshot(
        self,
        data: bytes | str | dict[str, Any],
        *,
        expected_base_snapshot_seq: int | None = None,
        batch_id: str | None = None,
    ) -> dict[str, Any]:
        # Parsing and checksum validation happen before any lock, claim, file or
        # state mutation. Any failure therefore leaves the old state untouched.
        snapshot = parse_snapshot(data)
        acquired = self._writer_lock.acquire(blocking=False)
        if not acquired:
            raise StoreError(
                "E_CONFLICTING_BATCH",
                "another import or snapshot switch is in progress",
                {"snapshot_seq": snapshot.snapshot_seq},
            )
        claim_key = f"snapshot:{snapshot.snapshot_seq}"
        claimed = False
        claim_status = "failed"
        try:
            base = self._snapshot_state()
            if (
                expected_base_snapshot_seq is not None
                and expected_base_snapshot_seq != base.last_snapshot_seq
            ):
                raise StoreError(
                    "E_STALE_BASE",
                    "snapshot switch was based on a stale snapshot sequence",
                    {
                        "expected_base_snapshot_seq": expected_base_snapshot_seq,
                        "current_last_snapshot_seq": base.last_snapshot_seq,
                    },
                )
            if snapshot.snapshot_seq <= base.last_snapshot_seq:
                raise StoreError(
                    "E_SNAPSHOT_STALE",
                    "snapshot sequence is not monotonic",
                    {
                        "incoming_snapshot_seq": snapshot.snapshot_seq,
                        "last_snapshot_seq": base.last_snapshot_seq,
                    },
                )
            if not self.db.claim_batch(
                claim_key, batch_id or uuid.uuid4().hex, self._now_ms()
            ):
                raise StoreError(
                    "E_CONFLICTING_BATCH",
                    "this snapshot sequence is already active or completed",
                    {"snapshot_seq": snapshot.snapshot_seq},
                )
            claimed = True
            result = self._commit_records_locked(
                list(snapshot.records),
                kind="snapshot",
                batch_id=batch_id or f"snapshot-{snapshot.snapshot_seq}",
                replace_state_mode="snapshot",
                snapshot_seq=snapshot.snapshot_seq,
            )
            claim_status = "applied"
            return {
                "ok": True,
                "snapshot_seq": snapshot.snapshot_seq,
                "base_snapshot_seq": snapshot.base_snapshot_seq,
                "generated_at_ms": snapshot.generated_at_ms,
                "checksum": snapshot.checksum,
                "records_loaded": len(snapshot.records),
                "previous_last_snapshot_seq": base.last_snapshot_seq,
                "previous_state_version": base.state_version,
                "new_state_version": result.state_version,
                "transaction": {
                    "tx_id": result.tx_id,
                    "segment_id": result.segment.segment_id,
                    "sha256": result.segment.sha256,
                },
                "old_state_released_after_switch": True,
                "old_segments_retained_on_disk_for_crash_recovery": True,
            }
        finally:
            if claimed:
                self.db.finish_batch(claim_key, claim_status, self._now_ms())
            self._writer_lock.release()

    # ------------------------------------------------------------------
    # Expiry, segment verification and compaction
    # ------------------------------------------------------------------
    def expire(self, *, now_ms: int | None = None) -> dict[str, Any]:
        now = self._now_ms() if now_ms is None else int(now_ms)
        with self._writer_lock:
            base = self._snapshot_state()
            due = base.expiry.clone().pop_due(now)
            deletes: list[Record] = []
            stale_candidates: list[dict[str, Any]] = []
            cleaned_keys: list[str] = []
            for entry in due:
                ref = base.index.get(entry.key)
                if (
                    ref is None
                    or ref.token != entry.token
                    or ref.expires_at_ms is None
                    or ref.expires_at_ms > now
                ):
                    stale_candidates.append(
                        {
                            "key": entry.key,
                            "heap_token": entry.token,
                            "heap_expires_ms": entry.expires_ms,
                        }
                    )
                    continue
                deletes.append(
                    Record(
                        key=entry.key,
                        value=b"",
                        version=ref.version,
                        write_ts_ms=now,
                        expires_at_ms=None,
                        deleted=True,
                    )
                )
                cleaned_keys.append(entry.key)
            if not deletes:
                return {
                    "ok": True,
                    "now_ms": now,
                    "cleaned_count": 0,
                    "cleaned_keys": [],
                    "stale_heap_candidates": stale_candidates,
                    "tombstones_recorded": 0,
                    "state_version": base.state_version,
                }
            result = self._commit_records_locked(
                deletes,
                kind="txn",
                batch_id=f"expire-{uuid.uuid4().hex}",
                intent_payload_extra={"reason": "expiry_cleanup"},
            )
            return {
                "ok": True,
                "now_ms": now,
                "cleaned_count": len(cleaned_keys),
                "cleaned_keys": cleaned_keys,
                "stale_heap_candidates": stale_candidates,
                "tombstones_recorded": len(cleaned_keys),
                "transaction": {
                    "tx_id": result.tx_id,
                    "segment_id": result.segment.segment_id,
                    "state_version": result.state_version,
                },
            }

    def verify_segments(self) -> dict[str, Any]:
        with self._writer_lock:
            base = self._snapshot_state()
            invalid: list[dict[str, Any]] = []
            for info in sorted(base.segments.values(), key=lambda item: item.seq):
                ok, detail = self.segments.verify(info)
                if not ok:
                    invalid.append(detail)
            if not invalid:
                report = {
                    "ok": True,
                    "status": "all_segments_valid",
                    "checked_segments": len(base.segments),
                    "invalid_segments": [],
                    "invalidated_keys": [],
                    "state_rebuilt": False,
                    "state_version": base.state_version,
                }
                self._last_maintenance_report = report
                return report
            invalid_ids = {
                item.get("segment_id") for item in invalid if item.get("segment_id")
            }
            affected_keys = sorted(
                {
                    key
                    for key, ref in base.index.items()
                    if ref.segment_id in invalid_ids
                }
                | {
                    key
                    for key, tombstone in base.tombstones.items()
                    if tombstone.segment_id in invalid_ids
                }
            )
            infos, scan_issues = self.segments.scan(recover_missing_meta=True)
            new_state, load_issues, loaded = self._build_state_from_infos(infos)
            # The rebuild is computed off to the side and published in one swap.
            self._publish(new_state)
            changed_keys: list[dict[str, Any]] = []
            for key in sorted(set(base.index) | set(new_state.index)):
                old_ref = base.index.get(key)
                new_ref = new_state.index.get(key)
                old_sig = None if old_ref is None else (
                    old_ref.version,
                    old_ref.segment_id,
                    old_ref.offset,
                    old_ref.length,
                    old_ref.token,
                )
                new_sig = None if new_ref is None else (
                    new_ref.version,
                    new_ref.segment_id,
                    new_ref.offset,
                    new_ref.length,
                    new_ref.token,
                )
                if old_sig != new_sig:
                    changed_keys.append(
                        {
                            "key": key,
                            "before": None if old_ref is None else old_ref.public(),
                            "after": None if new_ref is None else new_ref.public(),
                        }
                    )
            report = {
                "ok": True,
                "status": "invalid_segments_rebuilt",
                "checked_segments": len(base.segments),
                "invalid_segments": invalid,
                "scan_issues": scan_issues,
                "load_issues": load_issues,
                "invalidated_keys": affected_keys,
                "invalidated_key_count": len(affected_keys),
                "changed_index_entries": changed_keys,
                "state_rebuilt": True,
                "state_version_before": base.state_version,
                "state_version_after": new_state.state_version,
                "valid_segments_loaded": len(loaded),
            }
            self._last_maintenance_report = report
            self.db.set_meta("last_segment_verification", report)
            return report

    def compact(self) -> dict[str, Any]:
        with self._writer_lock:
            base = self._snapshot_state()
            records: list[Record] = []
            for tombstone in sorted(base.tombstones.values(), key=lambda item: item.key):
                records.append(
                    Record(
                        key=tombstone.key,
                        value=b"",
                        version=tombstone.version,
                        write_ts_ms=tombstone.write_ts_ms,
                        expires_at_ms=None,
                        deleted=True,
                    )
                )
            for key in sorted(base.index):
                current = self._current_record(base, key)
                if current is not None:
                    records.append(current)
            if not records:
                return {
                    "ok": True,
                    "status": "empty_store_no_compaction",
                    "old_segment_count": len(base.segments),
                    "new_segment_id": None,
                    "state_version": base.state_version,
                }
            result = self._commit_records_locked(
                records,
                kind="compact",
                batch_id=f"compact-{uuid.uuid4().hex}",
                replace_state_mode="compact",
                intent_payload_extra={"reason": "segment_compaction"},
            )
            report = {
                "ok": True,
                "status": "compacted",
                "old_segment_count": len(base.segments),
                "new_segment_id": result.segment.segment_id,
                "new_segment_records": result.applied_records,
                "old_state_version": base.state_version,
                "new_state_version": result.state_version,
                "invalidated_dependency_count": len(base.index) + len(base.tombstones),
                "transaction": {
                    "tx_id": result.tx_id,
                    "segment_id": result.segment.segment_id,
                    "sha256": result.segment.sha256,
                },
            }
            self._last_maintenance_report = report
            return report

    def segment_summary(self) -> dict[str, Any]:
        state = self._snapshot_state()
        refs_by_segment: dict[str, set[str]] = {}
        for key, ref in state.index.items():
            refs_by_segment.setdefault(ref.segment_id, set()).add(key)
        for key, tombstone in state.tombstones.items():
            refs_by_segment.setdefault(tombstone.segment_id, set()).add(key)
        segments = []
        for info in sorted(state.segments.values(), key=lambda item: item.seq):
            ok, detail = self.segments.verify(info)
            segments.append(
                {
                    **info.public(),
                    "digest_ok": ok,
                    "verify_detail": detail,
                    "dependent_keys": sorted(refs_by_segment.get(info.segment_id, set())),
                }
            )
        return {"state_version": state.state_version, "segments": segments}

    def heap_check(self) -> dict[str, Any]:
        state = self._snapshot_state()
        expected = {
            key: ref.token
            for key, ref in state.index.items()
            if ref.expires_at_ms is not None
        }
        actual = {entry.key: entry.token for entry in state.expiry.entries()}
        stale = state.expiry.stale_entries(state.index)
        return {
            "ok": not stale and expected == actual,
            "heap_size": len(state.expiry),
            "expected_size": len(expected),
            "duplicate_keys": state.expiry.duplicate_keys(),
            "missing_or_wrong_entries": {
                key: {"expected_token": token, "actual_token": actual.get(key)}
                for key, token in expected.items()
                if actual.get(key) != token
            },
            "unexpected_entries": sorted(set(actual) - set(expected)),
            "stale_entries": [
                {
                    "key": entry.key,
                    "expires_ms": entry.expires_ms,
                    "version": entry.version,
                    "token": entry.token,
                }
                for entry in stale
            ],
            "entries": [entry.__dict__ if hasattr(entry, "__dict__") else {
                "expires_ms": entry.expires_ms,
                "key": entry.key,
                "version": entry.version,
                "token": entry.token,
            } for entry in state.expiry.entries()],
        }

    def recovery_report(self) -> dict[str, Any]:
        stored = self.db.latest_recovery_report()
        if stored is not None:
            return stored
        if self._last_recovery_report is not None:
            return {"generated_at_ms": self._now_ms(), "report": self._last_recovery_report}
        return {
            "generated_at_ms": self._now_ms(),
            "report": {
                "report_version": 1,
                "facts_only": True,
                "recovered_count": 0,
                "rolled_back_count": 0,
                "note": "no recovery scan has been run",
            },
        }

    def maintenance_report(self) -> dict[str, Any] | None:
        if self._last_maintenance_report is not None:
            return self._last_maintenance_report
        return self.db.get_meta("last_segment_verification", None)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self.db.close()

