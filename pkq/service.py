"""业务编排：点查提交、单写者物化、多读者拉取、恢复后续跑。"""
from __future__ import annotations

import hashlib
import json
import threading
import time
import uuid

from .columnar import SnapshotStore, Snapshot
from .config import Config
from .errors import (BlockCorrupt, ConflictingQuery, PageConsumed, PageOutOfRange,
                     SessionNotFound)
from .planner import plan_query, FetchTask
from .scheduler import Scheduler, DONE, SKIPPED, NOT_EXECUTED
from .store import Store


def fingerprint_query(keys: list[int], columns: list[str], page_size: int) -> str:
    canon = json.dumps({"keys": sorted(set(keys)), "columns": sorted(columns),
                        "page_size": page_size}, sort_keys=True)
    return hashlib.sha256(canon.encode()).hexdigest()


class PkqService:
    def __init__(self, config: Config, store: Store, snapshots: SnapshotStore):
        self.config = config
        self.store = store
        self.snapshots = snapshots
        self._submit_lock = threading.Lock()
        self._write_locks: dict[str, threading.Lock] = {}
        self._write_locks_guard = threading.Lock()
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._worker: threading.Thread | None = None
        self.last_traces: dict[str, list[dict]] = {}   # session_id -> 裁决轨迹
        self.recovery_report: dict | None = None

    # ------------------------------------------------------------ 生命周期
    def start(self):
        self.recovery_report = self.store.recover()
        self._worker = threading.Thread(target=self._materialize_loop, daemon=True)
        self._worker.start()

    def shutdown(self):
        self._stop.set()
        self._wake.set()
        if self._worker:
            self._worker.join(timeout=5)

    def _session_lock(self, session_id: str) -> threading.Lock:
        with self._write_locks_guard:
            return self._write_locks.setdefault(session_id, threading.Lock())

    # ------------------------------------------------------------ 提交查询
    def submit(self, query_id: str, keys: list[int], columns: list[str] | None,
               page_size: int | None, deadline_sec: float | None = None) -> dict:
        snap = self._require_snapshot()
        keys = sorted(set(int(k) for k in keys))
        columns = columns or [c["name"] for c in snap.columns]
        eff_page = self.config.effective_page_size(page_size)
        fp = fingerprint_query(keys, columns, eff_page)

        with self._submit_lock:
            existing = self.store.get_session_by_query(query_id)
            if existing is not None:
                if existing["fingerprint"] != fp:
                    raise ConflictingQuery(
                        f"查询标识 {query_id} 已存在且内容不一致",
                        query_id=query_id)
                return self._submit_response(existing, reused=True)

            session_id = uuid.uuid4().hex[:16]
            tasks = plan_query(snap, keys, self.config.max_keys_per_query)
            waiting = max(0, len(tasks) - self.config.max_concurrent_tasks)
            if waiting > self.config.scan_queue_capacity:
                from .errors import ScanQueueFull
                raise ScanQueueFull(
                    f"等待任务 {waiting} 超过队列容量 {self.config.scan_queue_capacity}",
                    waiting=waiting, capacity=self.config.scan_queue_capacity)
            # 命中估计：位于某块主键区间内的键数量
            total_estimate = sum(
                1 for k in keys if any(b.pk_min <= k <= b.pk_max for b in snap.blocks))
            status = "MATERIALIZING" if tasks else "DONE"
            self.store.create_session(session_id, query_id, fp, keys, columns,
                                      eff_page, total_estimate, status)
            if tasks:
                self.store.insert_tasks(session_id, [t.to_dict() for t in tasks])
            session = self.store.get_session(session_id)

        if tasks:
            self._wake.set()
            # 首轮截止：等待首页物化或超时返回 NOT_READY（客户端截止与配置取小）
            budget = self.config.first_page_deadline_sec
            if deadline_sec is not None and deadline_sec > 0:
                budget = min(budget, deadline_sec)
            deadline = time.monotonic() + budget
            while time.monotonic() < deadline:
                if self.store.last_page_seq(session_id) >= 0:
                    break
                s = self.store.get_session(session_id)
                if s["status"] in ("DONE", "FAILED"):
                    break
                time.sleep(0.01)
            session = self.store.get_session(session_id)
        return self._submit_response(session, reused=False)

    def _submit_response(self, session, reused: bool) -> dict:
        sid = session["session_id"]
        resp = {
            "handle": sid,
            "query_id": session["query_id"],
            "reused": bool(reused),
            "total_estimate": session["total_estimate"],
            "page_size": session["page_size"],
            "next_page_seq": session["next_page_seq"],
        }
        first = self.store.get_page(sid, 0)
        if session["status"] == "FAILED":
            resp["status"] = "FAILED"
            resp["first_page"] = None
        elif session["status"] == "DONE" and first is None:
            resp["status"] = "EXHAUSTED"
            resp["first_page"] = None
        elif first is not None and first["state"] == "MATERIALIZED"                 and self.store.consume_page(sid, 0):
            # 首页随提交一并交付：原子登记为已消费，后续拉取从页 1 开始
            resp["status"] = "OK"
            resp["first_page"] = {"page_seq": 0, "rows": json.loads(first["rows_json"]),
                                  "row_count": first["row_count"]}
            resp["next_page_seq"] = 1
        elif reused:
            # 复用句柄：首页已被首次提交者消费，调用方从 next_page_seq 续拉
            resp["status"] = "OK"
            resp["first_page"] = None
        else:
            resp["status"] = "NOT_READY"
            resp["first_page"] = None
        return resp

    # ------------------------------------------------------------ 拉取
    def pull(self, session_id: str, page_seq: int) -> dict:
        session = self.store.get_session(session_id)
        if session is None:
            raise SessionNotFound(f"结果句柄不存在: {session_id}", handle=session_id)
        if session["status"] == "FAILED":
            from .errors import PkqError
            if session["error_code"] == "E_BLOCK_CORRUPT":
                detail = json.loads(session["error_detail"] or "{}")
                raise BlockCorrupt(detail.get("message", "块损坏"),
                                   block_id=detail.get("block_id", -1))
            raise PkqError(session["error_detail"] or "会话物化失败")
        if page_seq < 0:
            raise PageOutOfRange(f"页序号非法: {page_seq}", page_seq=page_seq)

        page = self.store.get_page(session_id, page_seq)
        if page is None:
            done = session["status"] == "DONE"
            last = self.store.last_page_seq(session_id)
            if done and page_seq == last + 1:
                if session["exhausted_reported"]:
                    raise PageOutOfRange("句柄已耗尽，继续拉取越界", page_seq=page_seq)
                self.store.mark_exhausted_reported(session_id)
                return {"handle": session_id, "page_seq": page_seq, "status": "EXHAUSTED",
                        "rows": [], "row_count": 0}
            if done and page_seq > last + 1:
                raise PageOutOfRange(f"页序号越界: {page_seq}", page_seq=page_seq)
            return {"handle": session_id, "page_seq": page_seq, "status": "NOT_READY",
                    "rows": [], "row_count": 0}
        if page["state"] == "CONSUMED":
            raise PageConsumed(f"页 {page_seq} 已被消费", page_seq=page_seq)
        if not self.store.consume_page(session_id, page_seq):
            raise PageConsumed(f"页 {page_seq} 已被并发消费", page_seq=page_seq)
        return {"handle": session_id, "page_seq": page_seq, "status": "OK",
                "rows": json.loads(page["rows_json"]), "row_count": page["row_count"]}

    def session_status(self, session_id: str) -> dict:
        session = self.store.get_session(session_id)
        if session is None:
            raise SessionNotFound(f"结果句柄不存在: {session_id}", handle=session_id)
        return {
            "handle": session_id,
            "status": session["status"],
            "next_page_seq": session["next_page_seq"],
            "materialized_pages": self.store.materialized_page_count(session_id),
            "consumed_pk_upper": session["consumed_pk_upper"],
            "tasks": self.store.task_counts(session_id),
            "total_estimate": session["total_estimate"],
        }

    # ------------------------------------------------------------ 物化（单写者）
    def _materialize_loop(self):
        while not self._stop.is_set():
            self._wake.wait(timeout=0.2)
            self._wake.clear()
            for s in self.store.list_sessions():
                if self._stop.is_set():
                    return
                if s["status"] == "MATERIALIZING":
                    self.materialize_session(s["session_id"])

    def materialize_session(self, session_id: str):
        """同一会话单写者串行：取 READY 任务 -> 调度执行 -> 逐任务提交。"""
        lock = self._session_lock(session_id)
        if not lock.acquire(blocking=False):
            return
        try:
            self._materialize_session_locked(session_id)
        except BlockCorrupt as exc:
            self.store.mark_session_failed(session_id, "E_BLOCK_CORRUPT",
                                           json.dumps({"block_id": exc.block_id,
                                                       "message": exc.message}))
        except Exception as exc:  # 物化失败须落库，不得静默
            self.store.mark_session_failed(session_id, "E_INTERNAL", str(exc))
        finally:
            lock.release()

    def _materialize_session_locked(self, session_id: str):
        snap = self._require_snapshot()
        session = self.store.get_session(session_id)
        if session is None or session["status"] != "MATERIALIZING":
            return
        ready = self.store.ready_tasks(session_id)
        if not ready:
            self.store.finalize_session(session_id, session["page_size"])
            return
        keys = json.loads(session["keys_json"])
        columns = json.loads(session["columns_json"])
        keyset = set(keys)
        page_size = session["page_size"]

        tasks = []
        for r in ready:
            p = json.loads(r["payload_json"])
            tasks.append(FetchTask(task_id=p["task_id"], blocks=[],
                                   pk_lo=p["pk_lo"], pk_hi=p["pk_hi"],
                                   row_lo=p["row_lo"], row_hi=p["row_hi"],
                                   est_bytes=p["est_bytes"], est_cost=p["est_cost"],
                                   parent_id=p["parent_id"]))
        # 恢复块索引引用
        by_id = {b.block_id: b for b in snap.blocks}
        for t, r in zip(tasks, ready):
            t.blocks = [by_id[i] for i in json.loads(r["payload_json"])["blocks"]]

        def execute(task: FetchTask) -> list[dict]:
            rows: list[dict] = []
            for blk in task.blocks:
                decoded = snap.decode_block(blk)
                lo = task.row_lo if len(task.blocks) == 1 else 0
                hi = task.row_hi if len(task.blocks) == 1 else decoded.row_count
                hi = hi if hi is not None else decoded.row_count
                for i in range(lo, hi):
                    if decoded.pks[i] in keyset:
                        row = {"pk": decoded.pks[i]}
                        for col in columns:
                            if col == "pk":
                                continue
                            row[col] = decoded.columns[col][i]
                        rows.append(row)
            rows.sort(key=lambda r: r["pk"])  # 稳定归并：主键升序
            return rows

        scheduler = Scheduler(self.config, execute)
        outcomes = scheduler.run_all(tasks, len(columns))
        self.last_traces[session_id] = [
            rec for tr in scheduler.traces for rec in tr.records]

        # 子任务（内存拆分产物）归并到父任务后逐父任务提交，保持主键升序
        parent_pk_hi = {t.task_id: t.pk_hi for t in tasks}
        grouped: dict[str, list[dict]] = {}
        skip_reasons: dict[str, str] = {}
        order: list[str] = []
        for o in outcomes:
            parent = o.task.parent_id or o.task.task_id
            if parent not in grouped:
                grouped[parent] = []
                order.append(parent)
            if o.status == SKIPPED:
                skip_reasons[parent] = o.reason
            grouped[parent].extend(o.rows)
        for parent in order:
            rows = sorted(grouped[parent], key=lambda r: r["pk"])
            reason = skip_reasons.get(parent)
            if self.config.task_commit_delay_sec > 0:
                time.sleep(self.config.task_commit_delay_sec)
            self.store.commit_task(session_id, parent, rows, parent_pk_hi[parent],
                                   page_size, finish=False, skip_reason=reason)
        self.store.finalize_session(session_id, page_size)

    def _require_snapshot(self) -> Snapshot:
        snap = self.snapshots.current()
        if snap is None:
            from .errors import SnapshotInvalid
            raise SnapshotInvalid("尚无已加载的快照")
        return snap
