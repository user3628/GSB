"""Single-writer, multi-reader storage engine tying WAL, pages and checkpoints."""
from __future__ import annotations

import os
import threading
import time
import uuid
from typing import Any, Callable, Dict, List, Optional

from .config import Config
from .errors import (
    CheckpointInvalidError,
    DuplicateRequestError,
    TruncateUnsafeError,
    WalCorruptError,
)
from .models import Checkpoint, Page, WalRecord
from .recovery import RecoveryManager
from .replay import Replayer
from .sqlite_store import SqliteStore
from .util import RWLock
from .wal import WalManager


class _RequestGate:
    """In-process idempotency: first caller runs, duplicates wait and reuse result."""

    def __init__(self):
        self._events: Dict[str, threading.Event] = {}
        self._results: Dict[str, Any] = {}
        self._guard = threading.Lock()

    def acquire(self, request_id: Optional[str]) -> Optional[str]:
        if not request_id:
            return None
        with self._guard:
            if request_id in self._events:
                return "wait"
            event = threading.Event()
            self._events[request_id] = event
            return "owner"

    def wait(self, request_id: str):
        event = self._events[request_id]
        event.wait()
        return self._results[request_id]

    def complete(self, request_id: str, result: Any):
        with self._guard:
            self._results[request_id] = result
            self._events[request_id].set()


class Engine:
    def __init__(
        self,
        root: str,
        config: Optional[Config] = None,
        crash_hook: Optional[Callable[[str], None]] = None,
    ):
        self.root = os.path.abspath(root)
        self.config = config or Config()
        self.wal_dir = os.path.join(self.root, "wal")
        self.archive_dir = os.path.join(self.root, "archive")
        os.makedirs(self.wal_dir, exist_ok=True)
        os.makedirs(self.archive_dir, exist_ok=True)

        self.store = SqliteStore(
            os.path.join(self.root, "meta.db"),
            synchronous=self.config.sqlite_synchronous,
        )
        self.wal = WalManager(
            self.wal_dir,
            segment_max_bytes=self.config.segment_max_bytes,
            fsync_wal=self.config.fsync_wal,
            fsync_dirs=self.config.fsync_dirs,
            crash_hook=crash_hook,
        )
        self.replayer = Replayer(self.wal, self.config)
        self.recovery = RecoveryManager(self.store, self.wal)
        self.lock = RWLock()
        self._gate = _RequestGate()
        self._clock = 0
        self._dirty: set = set()
        self._flusher: Optional[threading.Thread] = None
        self._flusher_stop = threading.Event()
        self.recovery_report = None

    # ------------------------------------------------------------- lifecycle
    def open(self) -> Dict[str, Any]:
        with self.lock.write():
            self.recovery_report = self.recovery.recover().to_dict()
            cp = self._load_active_checkpoint()
            if cp is not None:
                self.store.reset_pages(cp.pages)
                self._dirty = set()
            self._clock = int(self.store.get_meta("clock") or "0")
            if self.config.background_flush:
                self._start_flusher()
            return self.recovery_report

    def close(self):
        if getattr(self, "_closed", False):
            return
        self._flusher_stop.set()
        if self._flusher is not None:
            self._flusher.join(timeout=2)
            self._flusher = None
        with self.lock.write():
            self.wal.close()
            try:
                self.store.set_meta("clock", str(self._clock))
            finally:
                self.store.close()
            self._closed = True

    def _tick(self) -> int:
        self._clock += 1
        self.store.set_meta("clock", str(self._clock))
        return self._clock

    def _load_active_checkpoint(self) -> Optional[Checkpoint]:
        row = self.store.latest_checkpoint_row()
        if row is None:
            return None
        cp = self.store.row_to_checkpoint(row)
        if not cp.verify():
            raise CheckpointInvalidError(
                "active checkpoint checksum mismatch", {"seq": cp.seq}
            )
        return cp

    def active_checkpoint(self) -> Optional[Checkpoint]:
        with self.lock.read():
            return self._load_active_checkpoint()

    # ------------------------------------------------------------- idempotency
    def _enter_request(self, request_id: Optional[str]):
        if not request_id:
            return
        mode = self._gate.acquire(request_id)
        if mode == "wait":
            result = self._gate.wait(request_id)
            if isinstance(result, Exception):
                raise result
            return ("reuse", result)
        durable = self.store.ledger_get(request_id)
        if durable is not None and durable["status"] == "DONE":
            import json

            result = json.loads(durable["result"])
            self._gate.complete(request_id, result)
            return ("reuse", result)
        return ("owner", None)

    def _finish_request(self, request_id: Optional[str], result: Any):
        if not request_id:
            return
        self.store.ledger_finish(request_id, result if isinstance(result, dict) else {"value": result})
        self._gate.complete(request_id, result)

    def _fail_request(self, request_id: Optional[str], exc: Exception):
        if not request_id:
            return
        self._gate.complete(request_id, exc)

    # ------------------------------------------------------------- mutation
    def apply_modification(
        self,
        page_no: int,
        after_digest: str,
        txn_id: str,
        request_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        gate = self._enter_request(request_id)
        if gate and gate[0] == "reuse":
            return {"reused": True, "result": gate[1]}
        try:
            with self.lock.write():
                current = self.store.get_page(page_no)
                before = current.digest if current else None
                lsn = self.wal.next_lsn
                record = WalRecord(
                    lsn=lsn,
                    page_no=page_no,
                    before_digest=before,
                    after_digest=after_digest,
                    txn_id=txn_id,
                    ts=self._tick(),
                )
                intent_id = f"wal-{lsn}-{uuid.uuid4().hex[:8]}"
                self.store.conn.execute("BEGIN IMMEDIATE")
                self.store.begin_intent(
                    intent_id, "WAL_APPEND", record.to_dict(), self._clock
                )
                if request_id:
                    self.store.ledger_begin(request_id, self._clock)
                self.store.conn.execute("COMMIT")

                self.wal.append(record)

                page = Page(
                    page_no=page_no,
                    digest=after_digest,
                    version=(current.version + 1 if current else 1),
                    last_lsn=lsn,
                )
                self.store.upsert_page(page)
                self._dirty.add(page_no)
                self.store.update_intent(intent_id, "COMMITTED")
                result = {"lsn": lsn, "page": page.to_dict()}
                self._finish_request(request_id, result)
                return {"reused": False, "result": result}
        except Exception as exc:
            self._fail_request(request_id, exc)
            raise

    # ------------------------------------------------------------- checkpoint
    def checkpoint(
        self,
        incremental: bool = True,
        request_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        gate = self._enter_request(request_id)
        if gate and gate[0] == "reuse":
            return {"reused": True, "result": gate[1]}
        try:
            with self.lock.write():
                current_pages = self.store.all_pages()
                prev = self._load_active_checkpoint()
                prev_seq = prev.seq if prev else 0
                seq = prev_seq + 1
                if seq <= prev_seq:
                    raise CheckpointInvalidError(
                        "checkpoint sequence regression",
                        {"seq": seq, "previous_seq": prev_seq},
                    )

                if incremental and prev is not None:
                    changed = {p: current_pages[p] for p in self._dirty if p in current_pages}
                    new_pages = dict(prev.pages)
                    new_pages.update(changed)
                    flush_pages = changed
                    mode = "incremental"
                else:
                    new_pages = dict(current_pages)
                    flush_pages = new_pages
                    mode = "full"

                start_lsn = self.wal.next_lsn
                cp = Checkpoint(
                    seq=seq,
                    start_lsn=start_lsn,
                    pages=new_pages,
                ).seal()
                if not cp.verify():
                    raise CheckpointInvalidError("fresh checkpoint checksum failed", {"seq": seq})

                intent_id = f"cp-{seq}-{uuid.uuid4().hex[:8]}"
                self.store.conn.execute("BEGIN IMMEDIATE")
                self.store.begin_intent(
                    intent_id,
                    "CHECKPOINT",
                    {"seq": seq, "start_lsn": start_lsn, "mode": mode},
                    self._clock,
                )
                # Persist an OPEN row first so a crash leaves a detectable fact.
                self.store.insert_checkpoint(cp, "OPEN", self._clock)
                self.store.conn.execute("COMMIT")
                if self.wal.crash_hook is not None:
                    self.wal.crash_hook("checkpoint")

                # Flush only the pages this checkpoint is responsible for.
                flush_list = list(flush_pages.values())
                for i in range(0, len(flush_list), self.config.flush_group_size or 1):
                    self.store.upsert_pages_batch(
                        flush_list[i:i + self.config.flush_group_size]
                    )

                self.store.conn.execute("BEGIN IMMEDIATE")
                self.store.set_checkpoint_status(seq, "COMMITTED")
                self.store.update_intent(intent_id, "COMMITTED")
                self.store.conn.execute("COMMIT")

                self._dirty = set()
                result = {
                    "seq": seq,
                    "start_lsn": start_lsn,
                    "mode": mode,
                    "flushed_pages": len(flush_list),
                    "total_pages": len(cp.pages),
                    "checksum": cp.checksum,
                }
                self._finish_request(request_id, result)
                return {"reused": False, "result": result}
        except Exception as exc:
            self._fail_request(request_id, exc)
            raise

    # ------------------------------------------------------------- replay
    def replay(
        self,
        interrupt_after: Optional[int] = None,
        request_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        gate = self._enter_request(request_id)
        if gate and gate[0] == "reuse":
            return {"reused": True, "result": gate[1]}
        try:
            with self.lock.write():
                cp = self._load_active_checkpoint()
                if cp is None:
                    raise CheckpointInvalidError("no checkpoint to replay from")
                intent_id = f"replay-{uuid.uuid4().hex[:8]}"
                self.store.conn.execute("BEGIN IMMEDIATE")
                self.store.begin_intent(
                    intent_id,
                    "REPLAY",
                    {"start_lsn": cp.start_lsn, "interrupt_after": interrupt_after},
                    self._clock,
                )
                self.store.conn.execute("COMMIT")

                progress: List[int] = []
                result = self.replayer.replay(
                    cp,
                    interrupt_after=interrupt_after,
                    progress_cb=lambda lsn: self.store.update_intent(
                        intent_id, "OPEN", {"applied_upto": lsn}
                    ),
                )
                # Whole-state replacement: readers never see a mixed page set.
                self.store.reset_pages(result.final_pages)
                self.store.update_intent(
                    intent_id,
                    "ABORTED" if result.interrupted else "COMMITTED",
                    {"applied_upto": result.end_lsn},
                )
                out = {
                    "decision": result.decision.to_dict(),
                    "applied_records": result.applied_records,
                    "start_lsn": result.start_lsn,
                    "end_lsn": result.end_lsn,
                    "interrupted": result.interrupted,
                    "resume_from_lsn": result.resume_from_lsn,
                    "page_count": len(result.final_pages),
                }
                self._finish_request(request_id, out)
                return {"reused": False, "result": out}
        except Exception as exc:
            self._fail_request(request_id, exc)
            raise

    # ------------------------------------------------------------- truncate
    def truncate_wal(self, request_id: Optional[str] = None) -> Dict[str, Any]:
        gate = self._enter_request(request_id)
        if gate and gate[0] == "reuse":
            return {"reused": True, "result": gate[1]}
        try:
            with self.lock.write():
                cp = self._load_active_checkpoint()
                if cp is None:
                    raise CheckpointInvalidError("no checkpoint; cannot truncate WAL")
                safe_lsn = cp.start_lsn
                removable = self.wal.truncatable_segments(safe_lsn)
                unsafe = self.wal.unsafe_intervals(safe_lsn)
                if unsafe:
                    raise TruncateUnsafeError(
                        "segments still required by replay must not be truncated",
                        {"safe_lsn": safe_lsn, "uncovered": unsafe},
                    )
                intent_id = f"trunc-{uuid.uuid4().hex[:8]}"
                self.store.conn.execute("BEGIN IMMEDIATE")
                self.store.begin_intent(
                    intent_id,
                    "TRUNCATE",
                    {"safe_lsn": safe_lsn, "segments": [s.name for s in removable]},
                    self._clock,
                )
                self.store.conn.execute("COMMIT")

                archived = self.wal.truncate(safe_lsn, self.archive_dir)
                self.store.update_intent(intent_id, "COMMITTED")
                result = {
                    "archived_segments": archived,
                    "safe_lsn": safe_lsn,
                    "still_required": unsafe,
                }
                self._finish_request(request_id, result)
                return {"reused": False, "result": result}
        except Exception as exc:
            self._fail_request(request_id, exc)
            raise

    # ------------------------------------------------------------- reads
    def get_page(self, page_no: int) -> Optional[Dict[str, Any]]:
        with self.lock.read():
            page = self.store.get_page(page_no)
            return page.to_dict() if page else None

    def snapshot_pages(self) -> Dict[int, Dict[str, Any]]:
        with self.lock.read():
            return {k: v.to_dict() for k, v in self.store.all_pages().items()}

    def query_wal(self, from_lsn: int, to_lsn: int) -> List[Dict[str, Any]]:
        with self.lock.read():
            return self.wal.range_query(from_lsn, to_lsn)

    def wal_segments(self) -> List[Dict[str, Any]]:
        with self.lock.read():
            return [
                {
                    "segment": s.name,
                    "seg_id": s.seg_id,
                    "start_lsn": s.start_lsn,
                    "end_lsn": s.end_lsn,
                    "record_count": s.record_count,
                    "sealed": s.sealed,
                    "corrupt": s.corrupt,
                }
                for s in self.wal.segments()
            ]

    # ------------------------------------------------------------- flusher
    def _start_flusher(self):
        self._flusher = threading.Thread(
            target=self._flush_loop, name="wckpt-flusher", daemon=True
        )
        self._flusher.start()

    def _flush_loop(self):
        interval = max(self.config.background_flush_interval_ms, 10) / 1000.0
        while not self._flusher_stop.wait(interval):
            try:
                with self.lock.write():
                    if not self._dirty:
                        continue
                    pages = [
                        self.store.get_page(p) for p in list(self._dirty)
                    ]
                    pages = [p for p in pages if p is not None]
                    for i in range(0, len(pages), self.config.flush_group_size):
                        self.store.upsert_pages_batch(
                            pages[i:i + self.config.flush_group_size]
                        )
                    # Dirty set is cleared only once durable; checkpoint owns truncation.
                    self._dirty = set()
            except Exception:
                # Background flush must never kill the process; next flush retries.
                continue
