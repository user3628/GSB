"""单写者日志索引引擎。

职责：
- 批次提交的解析校验、幂等与冲突检测
- APPEND_ONLY / REBUILD 两条索引路径（同一套 numpy 合并语义）
- 重打时间戳 / 批次删除触发的三类派生产物级联失效与同事务原子重建
- 意图日志 + write_lock 支撑的强制终止恢复
- 版本可见性（读连接只看到完整提交版本，支持按版本回看）

并发模型：进程内一把可重入写锁串行化所有写操作；SQLite WAL 允许
只读连接与写者并发。跨进程单写者由 ``write_lock`` 表兜底。
"""

from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Optional, Sequence, Tuple

from . import indexing
from .errors import (
    BatchNotFoundError,
    ConflictingRecordError,
    InvalidTransitionError,
    LineInvalidError,
    LockedError,
    RecordNotFoundError,
    StaleBaseError,
)
from .models import BatchResult, BatchState, LogRecord, RecoveryReport, assert_transition
from .parser import parse_lines
from .store import connect, initialize, now_ms, reset_database

CRASH_AFTER_INDEXING_INTENT = "crash_after_indexing_intent"


class LogIndexEngine:
    def __init__(self, db_path, *, holder: Optional[str] = None,
                 recovery_policy: str = "requeue"):
        if recovery_policy not in ("requeue", "abort"):
            raise ValueError("recovery_policy 只能是 requeue 或 abort")
        initialize(db_path)
        self.db_path = db_path
        import os as _os

        self.process_token = self._current_process_token()
        self.holder = holder or (
            f"pid-{_os.getpid()}-start-{self.process_token}-{uuid.uuid4().hex[:8]}"
        )
        self.recovery_policy = recovery_policy
        self._write_lock = threading.RLock()
        self._inflight: Dict[str, threading.Event] = {}
        self._inflight_result: Dict[str, object] = {}
        self._crash_points: set[str] = set()
        startup_report = self.recover()
        self.recovery_report: dict = startup_report.__dict__

    # ------------------------------------------------------------------
    # 基础工具
    # ------------------------------------------------------------------
    def _conn(self) -> sqlite3.Connection:
        return connect(self.db_path)

    def _current_version(self, conn) -> Optional[int]:
        row = conn.execute("SELECT value FROM meta WHERE key='current_version'").fetchone()
        return int(row["value"]) if row else None

    def _next_version(self, conn) -> int:
        current = self._current_version(conn)
        return 1 if current is None else current + 1

    @staticmethod
    def _current_process_token() -> str:
        import os

        try:
            stat = Path(f"/proc/{os.getpid()}/stat").read_text()
            # 第 22 个字段（索引 21）是 starttime，时钟滴答；Windows 回退到 pid
            return stat.split()[21]
        except OSError:
            return f"fallback-{os.getpid()}"

    @staticmethod
    def _holder_identity(holder: str) -> Tuple[Optional[int], Optional[str]]:
        parts = holder.split("-")
        pid = int(parts[1]) if len(parts) > 1 and parts[0] == "pid" and parts[1].isdigit() else None
        start = None
        if "start" in parts:
            index = parts.index("start")
            if index + 1 < len(parts):
                start = parts[index + 1]
        return pid, start

    @staticmethod
    def _holder_pid(holder: str) -> Optional[int]:
        parts = holder.split("-")
        if len(parts) >= 2 and parts[0] == "pid" and parts[1].isdigit():
            return int(parts[1])
        return None

    @staticmethod
    def _process_alive(pid: int) -> bool:
        import os

        if pid == os.getpid():
            return True
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        return True

    def _acquire_db_lock(self, conn) -> None:
        row = conn.execute("SELECT holder FROM write_lock WHERE id=1").fetchone()
        if row is not None and row["holder"] != self.holder:
            owner_pid, owner_start = self._holder_identity(row["holder"])
            same_process = (
                owner_pid is not None and owner_start == self.process_token
            )
            # 不同 holder 但同一进程（如恢复重开）允许接管；
            # 外部存活进程持有的锁才拦截，死进程锁视为陈旧锁
            if not same_process and (
                owner_pid is None or self._process_alive(owner_pid)
            ):
                raise LockedError(
                    "数据库已被其他写者持有", {"holder": row["holder"]}
                )
        conn.execute(
            "INSERT OR REPLACE INTO write_lock(id, holder, acquired_ms) VALUES(1, ?, ?)",
            (self.holder, now_ms()),
        )

    def _release_db_lock(self, conn) -> None:
        # 显式提交，避免 close 回滚导致锁残留
        conn.execute("DELETE FROM write_lock WHERE id=1 AND holder=?", (self.holder,))
        conn.commit()

    def _add_version_event(self, conn, version, event_type, batch_id, reason, affected):
        conn.execute(
            "INSERT INTO version_events(version, event_type, batch_id, reason, "
            "affected, created_at_ms) VALUES(?,?,?,?,?,?)",
            (version, event_type, batch_id, reason, json.dumps(sorted(affected)), now_ms()),
        )

    def _get_batch(self, conn, batch_id) -> Optional[sqlite3.Row]:
        return conn.execute(
            "SELECT * FROM batches WHERE batch_id=?", (batch_id,)
        ).fetchone()

    def _active_batch_filter(self, alias: str = "b") -> str:
        return f"{alias}.state = 'COMMITTED' AND {alias}.invalidated_version IS NULL"

    def _max_ts_for_services(self, conn, services) -> Dict[str, Optional[int]]:
        result = {s: None for s in services}
        if not services:
            return result
        placeholders = ",".join("?" for _ in services)
        rows = conn.execute(
            f"SELECT r.service AS service, MAX(r.timestamp_ms) AS mx FROM records r "
            f"JOIN batches b ON b.batch_id = r.batch_id "
            f"WHERE {self._active_batch_filter()} AND r.service IN ({placeholders}) "
            f"GROUP BY r.service",
            tuple(services),
        ).fetchall()
        for row in rows:
            result[row["service"]] = row["mx"]
        return result

    # ------------------------------------------------------------------
    # 批次提交
    # ------------------------------------------------------------------
    def submit_batch(self, batch_id: str, lines: Sequence[str],
                     base_version: Optional[int] = None) -> BatchResult:
        # 解析在任何写库动作之前完成：非法行 -> 整批不入库
        records = parse_lines(lines)

        event: Optional[threading.Event] = None
        duplicate = False
        with self._write_lock:
            event = self._inflight.get(batch_id)
            if event is None:
                event = threading.Event()
                self._inflight[batch_id] = event
            else:
                duplicate = True
        if duplicate:
            event.wait()
            result = self._inflight_result.get(batch_id)
            if isinstance(result, Exception):
                raise result
            reused = BatchResult(**result) if isinstance(result, dict) else result
            reused.reused = True
            return reused

        try:
            result = self._submit_locked(batch_id, records, base_version)
            self._inflight_result[batch_id] = result.__dict__.copy()
            return result
        except Exception as exc:  # noqa: BLE001 - 传播给等待者
            self._inflight_result[batch_id] = exc
            raise
        finally:
            event.set()
            with self._write_lock:
                self._inflight.pop(batch_id, None)

    def _submit_locked(self, batch_id, records: List[LogRecord],
                       base_version) -> BatchResult:
        with self._write_lock:
            conn = self._conn()
            try:
                self._acquire_db_lock(conn)
                current = self._current_version(conn)
                if base_version is not None and base_version != current:
                    raise StaleBaseError(
                        "基础版本不是当前版本",
                        {"base_version": base_version, "current_version": current},
                    )
                existing = self._get_batch(conn, batch_id)
                if existing is not None:
                    if existing["state"] == BatchState.COMMITTED.value:
                        # 幂等：内容必须与首次写入完全一致，否则冲突并保留首值
                        for record in records:
                            row = conn.execute(
                                "SELECT timestamp_ms, service, level, message "
                                "FROM records WHERE batch_id=? AND line_no=?",
                                (batch_id, record.line_no),
                            ).fetchone()
                            if row is None or (
                                row["timestamp_ms"], row["service"],
                                row["level"], row["message"]
                            ) != record.content_tuple():
                                raise ConflictingRecordError(
                                    "同批次同行号内容不一致，保留首次写入值",
                                    {"batch_id": batch_id, "line_no": record.line_no},
                                )
                        return BatchResult(
                            batch_id=batch_id, state=BatchState.COMMITTED.value,
                            version=existing["version"],
                            path_mode=existing["path_mode"] or "UNKNOWN",
                            record_count=conn.execute(
                                "SELECT COUNT(*) c FROM records WHERE batch_id=?",
                                (batch_id,)).fetchone()["c"],
                            reused=True,
                        )
                    if existing["state"] == BatchState.INDEXING.value:
                        raise LockedError(
                            "同批次正在处理中，请等待复用结果",
                            {"batch_id": batch_id, "state": existing["state"]},
                        )
                    if existing["state"] == BatchState.PENDING.value:
                        # 恢复器重排后的批次：复用既有行，重新驱动索引流程。
                        # 残留的未完成意图先关闭，再登记本次执行意图。
                        conn.execute(
                            "UPDATE intents SET state='SUPERSEDED', finished_at_ms=? "
                            "WHERE batch_id=? AND finished_at_ms IS NULL",
                            (now_ms(), batch_id),
                        )
                        conn.execute(
                            "INSERT INTO intents(intent_id, batch_id, op_type, state, "
                            "payload, created_at_ms) VALUES(?,?, 'SUBMIT', 'PENDING', ?, ?)",
                            (uuid.uuid4().hex, batch_id,
                             json.dumps([r.__dict__ for r in records]), now_ms()),
                        )
                        conn.commit()
                    else:
                        raise InvalidTransitionError(
                            f"批次已处于终态 {existing['state']}，不可重复提交",
                            {"batch_id": batch_id, "state": existing["state"]},
                        )
                else:
                    # 事务 1：持久化意图 + PENDING（崩溃后仍可被恢复器看到）
                    conn.execute(
                        "INSERT INTO batches(batch_id, state, base_version, created_at_ms) "
                        "VALUES(?, 'PENDING', ?, ?)",
                        (batch_id, base_version, now_ms()),
                    )
                    conn.execute(
                        "INSERT INTO intents(intent_id, batch_id, op_type, state, payload, "
                        "created_at_ms) VALUES(?,?, 'SUBMIT', 'PENDING', ?, ?)",
                        (uuid.uuid4().hex, batch_id,
                         json.dumps([r.__dict__ for r in records]), now_ms()),
                    )
                    conn.commit()

                # 事务 2：进入 INDEXING
                self._transition(conn, batch_id, BatchState.INDEXING)
                conn.execute(
                    "UPDATE intents SET state='INDEXING' WHERE batch_id=? AND finished_at_ms IS NULL",
                    (batch_id,),
                )
                conn.commit()
                if CRASH_AFTER_INDEXING_INTENT in self._crash_points:
                    self._crash_points.discard(CRASH_AFTER_INDEXING_INTENT)
                    raise SystemExit("模拟强制终止：INDEXING 意图已落盘，派生写入尚未开始")

                # 事务 3：记录 + 三类派生产物 + 提交，原子完成
                result = self._apply_submit(conn, batch_id, records, base_version)
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise
            finally:
                try:
                    self._release_db_lock(conn)
                finally:
                    conn.close()

    def _apply_submit(self, conn, batch_id, records: List[LogRecord],
                      base_version) -> BatchResult:
        current = self._current_version(conn)
        services = sorted({r.service for r in records})
        max_before = self._max_ts_for_services(conn, services)

        rebuild_services: List[str] = []
        append_services: List[str] = []
        new_points: Dict[str, List[int]] = defaultdict(list)
        for record in records:
            new_points[record.service].append(record.timestamp_ms)
        for service in services:
            mx = max_before[service]
            if mx is not None and any(ts <= mx for ts in new_points[service]):
                rebuild_services.append(service)
            else:
                append_services.append(service)

        path_mode = "REBUILD" if rebuild_services else "APPEND_ONLY"
        reason = None
        if rebuild_services:
            reason = (
                "存在时间戳不晚于该服务已有最大时间戳的记录，"
                "受影响分区改走全量重建"
            )

        # 幂等冲突检测（同 batch_id+line_no 内容必须一致）
        for record in records:
            row = conn.execute(
                "SELECT timestamp_ms, service, level, message FROM records "
                "WHERE batch_id=? AND line_no=?",
                (batch_id, record.line_no),
            ).fetchone()
            if row is not None and (
                row["timestamp_ms"], row["service"], row["level"], row["message"]
            ) != record.content_tuple():
                raise ConflictingRecordError(
                    "同批次同行号内容不一致，保留首次写入值",
                    {"batch_id": batch_id, "line_no": record.line_no},
                )

        # 写记录（主键保证重复应用不产生重复行）
        conn.executemany(
            "INSERT OR IGNORE INTO records(batch_id, line_no, timestamp_ms, service, "
            "level, message) VALUES(?,?,?,?,?,?)",
            [(batch_id, r.line_no, r.timestamp_ms, r.service, r.level, r.message)
             for r in records],
        )

        version = self._next_version(conn)

        # 分类索引与批次统计：按分区增量 upsert，重复应用幂等
        per_service_level: Dict[Tuple[str, str], int] = defaultdict(int)
        for record in records:
            per_service_level[(record.service, record.level)] += 1
        for (service, level), count in sorted(per_service_level.items()):
            conn.execute(
                "INSERT INTO classification_index(service, level, batch_id, record_count, "
                "updated_version) VALUES(?,?,?,?,?) "
                "ON CONFLICT(service, level, batch_id) DO UPDATE SET "
                "record_count=excluded.record_count, updated_version=excluded.updated_version",
                (service, level, batch_id, count, version),
            )
            conn.execute(
                "INSERT INTO batch_stats(batch_id, service, level, record_count, "
                "updated_version) VALUES(?,?,?,?,?) "
                "ON CONFLICT(batch_id, service, level) DO UPDATE SET "
                "record_count=excluded.record_count, updated_version=excluded.updated_version",
                (batch_id, service, level, count, version),
            )

        # 时间轴区间索引：仅受影响服务分区。
        # 两条路径都调用同一个 merge_intervals；区别仅在 rebuilt_count 与
        # last_reason——快路径不触发“重建”语义，只做区间增量推进。
        for service in services:
            row = conn.execute(
                "SELECT * FROM timeline_index WHERE service=? LIMIT 1", (service,)
            ).fetchone()
            # 本批此刻仍是 INDEXING，需显式纳入；其他批次只取已提交未失效的
            active_points = [r["timestamp_ms"] for r in conn.execute(
                "SELECT r.timestamp_ms FROM records r JOIN batches b "
                "ON b.batch_id=r.batch_id WHERE r.service=? AND ("
                + self._active_batch_filter() + " OR r.batch_id=?)",
                (service, batch_id),
            ).fetchall()]
            intervals = indexing.merge_intervals(active_points)
            if service in rebuild_services or row is None:
                rebuilt = 0 if row is None else row["rebuilt_count"] + 1
                timeline_reason = "INITIAL" if row is None else "REBUILD"
            else:
                rebuilt = row["rebuilt_count"]
                timeline_reason = "APPEND_ONLY"
            self._replace_timeline(conn, service, intervals, version, rebuilt,
                                   timeline_reason)
            self._update_watermark(conn, service, active_points, version)

        self._transition(conn, batch_id, BatchState.COMMITTED)
        conn.execute(
            "UPDATE batches SET version=?, path_mode=?, committed_at_ms=? WHERE batch_id=?",
            (version, path_mode, now_ms(), batch_id),
        )
        conn.execute(
            "UPDATE intents SET state='COMMITTED', finished_at_ms=? WHERE batch_id=? "
            "AND finished_at_ms IS NULL",
            (now_ms(), batch_id),
        )
        conn.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES('current_version', ?)",
            (str(version),),
        )
        self._add_version_event(conn, version, "COMMIT", batch_id, reason, services)
        return BatchResult(
            batch_id=batch_id, state=BatchState.COMMITTED.value, version=version,
            path_mode=path_mode, rebuilt_partitions=rebuild_services,
            rebuild_reason=reason, affected_services=services,
            record_count=len(records),
        )

    # ------------------------------------------------------------------
    # 状态机
    # ------------------------------------------------------------------
    def _transition(self, conn, batch_id: str, target: BatchState,
                    *, recovery: bool = False) -> sqlite3.Row:
        row = self._get_batch(conn, batch_id)
        if row is None:
            raise BatchNotFoundError("批次不存在", {"batch_id": batch_id})
        current = BatchState(row["state"])
        assert_transition(current, target, recovery=recovery)
        conn.execute(
            "UPDATE batches SET state=? WHERE batch_id=?", (target.value, batch_id)
        )
        return conn.execute(
            "SELECT * FROM batches WHERE batch_id=?", (batch_id,)
        ).fetchone()

    def mark_aborted(self, batch_id: str) -> dict:
        with self._write_lock:
            conn = self._conn()
            try:
                self._acquire_db_lock(conn)
                self._transition(conn, batch_id, BatchState.ABORTED)
                conn.execute(
                    "UPDATE intents SET state='ABORTED', finished_at_ms=? "
                    "WHERE batch_id=? AND finished_at_ms IS NULL",
                    (now_ms(), batch_id),
                )
                conn.commit()
                return {"batch_id": batch_id, "state": BatchState.ABORTED.value}
            except Exception:
                conn.rollback()
                raise
            finally:
                self._release_db_lock(conn)
                conn.close()

    # ------------------------------------------------------------------
    # 时间轴 / 分区重建
    # ------------------------------------------------------------------
    def _timeline_pairs(self, conn, service) -> List[Tuple[int, int]]:
        rows = conn.execute(
            "SELECT start_ms, end_ms FROM timeline_index WHERE service=? "
            "ORDER BY ordinal", (service,)
        ).fetchall()
        return [(r["start_ms"], r["end_ms"]) for r in rows]

    def _replace_timeline(self, conn, service, intervals, version,
                          rebuilt_count, reason) -> None:
        conn.execute("DELETE FROM timeline_index WHERE service=?", (service,))
        conn.executemany(
            "INSERT INTO timeline_index(service, start_ms, end_ms, ordinal, "
            "built_version, rebuilt_count, last_reason) VALUES(?,?,?,?,?,?,?)",
            [(service, start, end, idx, version, rebuilt_count, reason)
             for idx, (start, end) in enumerate(intervals)],
        )

    def _update_watermark(self, conn, service, points, version) -> None:
        if not points:
            conn.execute("DELETE FROM service_watermark WHERE service=?", (service,))
            return
        conn.execute(
            "INSERT INTO service_watermark(service, max_ts, updated_version) "
            "VALUES(?,?,?) ON CONFLICT(service) DO UPDATE SET max_ts=excluded.max_ts, "
            "updated_version=excluded.updated_version",
            (service, max(points), version),
        )

    def _rebuild_partition(self, conn, service, version, reason) -> None:
        """同事务内重建某服务分区的三类派生产物（级联失效核心）。"""
        rows = conn.execute(
            "SELECT r.service AS service, r.timestamp_ms AS ts, r.level AS level, "
            "r.batch_id AS batch_id FROM records r JOIN batches b ON b.batch_id=r.batch_id "
            "WHERE " + self._active_batch_filter() + " AND r.service=?",
            (service,),
        ).fetchall()
        counts, intervals = indexing.partition_summary(
            [(r["service"], r["ts"], r["level"]) for r in rows]
        )
        # 1) 分类索引：删除该服务旧派生行后按当前事实重建
        conn.execute(
            "DELETE FROM classification_index WHERE service=?", (service,)
        )
        per_batch: Dict[Tuple[str, str], int] = defaultdict(int)
        per_level: Dict[str, int] = defaultdict(int)
        for row in rows:
            per_batch[(row["batch_id"], row["level"])] += 1
            per_level[row["level"]] += 1
        for (batch_id, level), count in sorted(per_batch.items()):
            conn.execute(
                "INSERT INTO classification_index(service, level, batch_id, record_count, "
                "updated_version) VALUES(?,?,?,?,?)",
                (service, level, batch_id, count, version),
            )
        # 2) 时间轴区间索引：失效后重建
        prev = conn.execute(
            "SELECT COALESCE(MAX(rebuilt_count),0) AS c FROM timeline_index "
            "WHERE service=?", (service,)
        ).fetchone()
        self._replace_timeline(conn, service, intervals, version,
                               prev["c"] + 1, reason)
        self._update_watermark(conn, service, [r["ts"] for r in rows], version)
        # 3) 统计摘要：受影响服务相关批次全部重建
        batch_ids = sorted({r["batch_id"] for r in rows})
        if batch_ids:
            placeholders = ",".join("?" for _ in batch_ids)
            conn.execute(
                f"DELETE FROM batch_stats WHERE service=? AND batch_id IN ({placeholders})",
                tuple([service] + batch_ids),
            )
            stats: Dict[Tuple[str, str], int] = defaultdict(int)
            for row in rows:
                stats[(row["batch_id"], row["level"])] += 1
            for (batch_id, level), count in sorted(stats.items()):
                conn.execute(
                    "INSERT INTO batch_stats(batch_id, service, level, record_count, "
                    "updated_version) VALUES(?,?,?,?,?)",
                    (batch_id, service, level, count, version),
                )
        else:
            conn.execute("DELETE FROM batch_stats WHERE service=?", (service,))
        return dict(per_level), intervals

    # ------------------------------------------------------------------
    # 重打时间戳：级联失效 + 同事务原子重建
    # ------------------------------------------------------------------
    def retimestamp(self, batch_id: str, line_no: int, new_ts_ms: int) -> dict:
        with self._write_lock:
            conn = self._conn()
            try:
                self._acquire_db_lock(conn)
                batch = self._get_batch(conn, batch_id)
                if batch is None:
                    raise BatchNotFoundError("批次不存在", {"batch_id": batch_id})
                if batch["state"] != BatchState.COMMITTED.value:
                    raise InvalidTransitionError(
                        "只有 COMMITTED 批次可重打时间戳",
                        {"batch_id": batch_id, "state": batch["state"]},
                    )
                record = conn.execute(
                    "SELECT * FROM records WHERE batch_id=? AND line_no=?",
                    (batch_id, line_no),
                ).fetchone()
                if record is None:
                    raise RecordNotFoundError(
                        "记录不存在", {"batch_id": batch_id, "line_no": line_no}
                    )
                old_ts = record["timestamp_ms"]
                if old_ts == new_ts_ms:
                    return {"batch_id": batch_id, "line_no": line_no,
                            "changed": False, "version": self._current_version(conn)}

                # 事务 1：意图先行
                version = self._next_version(conn)
                payload = json.dumps(
                    {"batch_id": batch_id, "line_no": line_no,
                     "old_ts": old_ts, "new_ts": new_ts_ms}
                )
                conn.execute(
                    "INSERT INTO intents(intent_id, batch_id, op_type, state, payload, "
                    "created_at_ms) VALUES(?,?, 'RETIMESTAMP', 'PENDING', ?, ?)",
                    (uuid.uuid4().hex, batch_id, payload, now_ms()),
                )
                conn.commit()

                # 事务 2：改记录 + 三类派生产物同事务原子重建
                conn.execute(
                    "UPDATE records SET timestamp_ms=? WHERE batch_id=? AND line_no=?",
                    (new_ts_ms, batch_id, line_no),
                )
                conn.execute(
                    "INSERT INTO record_ts_history(batch_id, line_no, old_ts, new_ts, "
                    "event_version, changed_at_ms) VALUES(?,?,?,?,?,?)",
                    (batch_id, line_no, old_ts, new_ts_ms, version, now_ms()),
                )
                service = record["service"]
                per_level, intervals = self._rebuild_partition(
                    conn, service, version, "RETIMESTAMP"
                )
                conn.execute(
                    "INSERT OR REPLACE INTO meta(key, value) VALUES('current_version', ?)",
                    (str(version),),
                )
                self._add_version_event(
                    conn, version, "RETIMESTAMP", batch_id,
                    f"line {line_no}: {old_ts} -> {new_ts_ms}", [service]
                )
                conn.execute(
                    "UPDATE intents SET state='COMMITTED', finished_at_ms=? "
                    "WHERE op_type='RETIMESTAMP' AND batch_id=? AND finished_at_ms IS NULL",
                    (now_ms(), batch_id),
                )
                conn.commit()
                return {
                    "batch_id": batch_id, "line_no": line_no, "changed": True,
                    "old_ts": old_ts, "new_ts": new_ts_ms, "version": version,
                    "rebuilt_partitions": [service],
                    "rebuild_reason": "RETIMESTAMP",
                    "partition_level_counts": per_level,
                    "intervals": intervals,
                }
            except Exception:
                conn.rollback()
                raise
            finally:
                self._release_db_lock(conn)
                conn.close()

    # ------------------------------------------------------------------
    # 批次删除（失效）：同事务级联重建
    # ------------------------------------------------------------------
    def invalidate_batch(self, batch_id: str) -> dict:
        with self._write_lock:
            conn = self._conn()
            try:
                self._acquire_db_lock(conn)
                batch = self._get_batch(conn, batch_id)
                if batch is None:
                    raise BatchNotFoundError("批次不存在", {"batch_id": batch_id})
                if batch["state"] != BatchState.COMMITTED.value:
                    raise InvalidTransitionError(
                        "只有 COMMITTED 批次可被删除失效",
                        {"batch_id": batch_id, "state": batch["state"]},
                    )
                services = [r["service"] for r in conn.execute(
                    "SELECT DISTINCT service FROM records WHERE batch_id=?",
                    (batch_id,),
                ).fetchall()]
                version = self._next_version(conn)
                conn.execute(
                    "INSERT INTO intents(intent_id, batch_id, op_type, state, payload, "
                    "created_at_ms) VALUES(?,?, 'INVALIDATE', 'PENDING', ?, ?)",
                    (uuid.uuid4().hex, batch_id,
                     json.dumps({"batch_id": batch_id}), now_ms()),
                )
                conn.commit()

                # 同事务：逻辑失效批次 + 删除该批次派生行 + 受影响分区重建
                conn.execute(
                    "UPDATE batches SET state='INVALIDATED', invalidated_version=? "
                    "WHERE batch_id=?", (version, batch_id)
                )
                conn.execute(
                    "DELETE FROM classification_index WHERE batch_id=?", (batch_id,)
                )
                conn.execute("DELETE FROM batch_stats WHERE batch_id=?", (batch_id,))
                rebuilt = {}
                for service in services:
                    per_level, intervals = self._rebuild_partition(
                        conn, service, version, "INVALIDATE_BATCH"
                    )
                    rebuilt[service] = {
                        "level_counts": per_level, "intervals": intervals
                    }
                conn.execute(
                    "INSERT OR REPLACE INTO meta(key, value) VALUES('current_version', ?)",
                    (str(version),),
                )
                self._add_version_event(
                    conn, version, "INVALIDATE", batch_id,
                    f"batch {batch_id} deleted", services
                )
                conn.execute(
                    "UPDATE intents SET state='COMMITTED', finished_at_ms=? "
                    "WHERE op_type='INVALIDATE' AND batch_id=? AND finished_at_ms IS NULL",
                    (now_ms(), batch_id),
                )
                conn.commit()
                return {"batch_id": batch_id, "version": version,
                        "state": BatchState.INVALIDATED.value,
                        "rebuilt_partitions": services, "partitions": rebuilt}
            except Exception:
                conn.rollback()
                raise
            finally:
                self._release_db_lock(conn)
                conn.close()

    # ------------------------------------------------------------------
    # 只读查询（读连接可与写者并发）
    # ------------------------------------------------------------------
    def current_version(self) -> Optional[int]:
        conn = self._conn()
        try:
            return self._current_version(conn)
        finally:
            conn.close()

    def list_batches(self) -> List[dict]:
        conn = self._conn()
        try:
            return [dict(r) for r in conn.execute(
                "SELECT batch_id, state, base_version, version, path_mode, "
                "created_at_ms, committed_at_ms, invalidated_version FROM batches "
                "ORDER BY created_at_ms, batch_id"
            ).fetchall()]
        finally:
            conn.close()

    def query_records(self, service: Optional[str] = None,
                      level: Optional[str] = None,
                      start_ms: Optional[int] = None,
                      end_ms: Optional[int] = None,
                      as_of_version: Optional[int] = None) -> List[dict]:
        conn = self._conn()
        try:
            if as_of_version is None:
                sql = (
                    "SELECT r.batch_id, r.line_no, r.timestamp_ms, r.service, "
                    "r.level, r.message FROM records r JOIN batches b "
                    "ON b.batch_id=r.batch_id WHERE "
                    + self._active_batch_filter()
                )
                params: list = []
                if service is not None:
                    sql += " AND r.service=?"; params.append(service)
                if level is not None:
                    sql += " AND r.level=?"; params.append(level)
                if start_ms is not None:
                    sql += " AND r.timestamp_ms>=?"; params.append(start_ms)
                if end_ms is not None:
                    sql += " AND r.timestamp_ms<=?"; params.append(end_ms)
                sql += " ORDER BY r.timestamp_ms, r.batch_id, r.line_no"
                return [dict(r) for r in conn.execute(sql, params).fetchall()]

            # 版本回看：依据 version_events 与 record_ts_history 重放可见事实
            current = self._current_version(conn)
            if as_of_version < 1 or (current is not None and as_of_version > current):
                raise StaleBaseError(
                    "请求版本超出已提交版本范围",
                    {"as_of_version": as_of_version, "current_version": current},
                )
            sql = (
                "SELECT r.batch_id, r.line_no, r.timestamp_ms, r.service, r.level, "
                "r.message, b.created_at_ms AS created_at, b.version AS commit_version "
                "FROM records r JOIN batches b ON b.batch_id=r.batch_id "
                "WHERE b.state IN ('COMMITTED','INVALIDATED') AND b.version<=? "
                "AND (b.invalidated_version IS NULL OR b.invalidated_version>?)"
            )
            params = [as_of_version, as_of_version]
            if service is not None:
                sql += " AND r.service=?"; params.append(service)
            if level is not None:
                sql += " AND r.level=?"; params.append(level)
            rows = conn.execute(sql, params).fetchall()
            # 目标版本 V 下的时间戳 = 第一个 event_version>V 的重打事件的 old_ts；
            # 若不存在这样的事件，则沿用当前 timestamp_ms。
            history = conn.execute(
                "SELECT batch_id, line_no, old_ts, new_ts, event_version "
                "FROM record_ts_history WHERE event_version>? "
                "ORDER BY event_version", (as_of_version,)
            ).fetchall()
            ts_override = {}
            for event in history:
                key = (event["batch_id"], event["line_no"])
                if key not in ts_override:
                    ts_override[key] = event["old_ts"]
            results = []
            for row in rows:
                key = (row["batch_id"], row["line_no"])
                ts = ts_override.get(key, row["timestamp_ms"])
                if start_ms is not None and ts < start_ms:
                    continue
                if end_ms is not None and ts > end_ms:
                    continue
                item = dict(row)
                item.pop("created_at", None)
                item.pop("commit_version", None)
                item["timestamp_ms"] = ts
                results.append(item)
            results.sort(key=lambda x: (x["timestamp_ms"], x["batch_id"], x["line_no"]))
            return results
        finally:
            conn.close()

    def index_state(self, as_of_version: Optional[int] = None) -> dict:
        """当前版本直接读物化表；历史版本按当时可见事实即时重算。"""
        conn = self._conn()
        try:
            current = self._current_version(conn)
            if as_of_version is None or as_of_version == current:
                classification = [dict(r) for r in conn.execute(
                    "SELECT service, level, batch_id, record_count, updated_version "
                    "FROM classification_index ORDER BY service, level, batch_id"
                ).fetchall()]
                timeline = [dict(r) for r in conn.execute(
                    "SELECT service, start_ms, end_ms, ordinal, built_version, "
                    "rebuilt_count, last_reason FROM timeline_index "
                    "ORDER BY service, ordinal"
                ).fetchall()]
                stats = [dict(r) for r in conn.execute(
                    "SELECT batch_id, service, level, record_count, updated_version "
                    "FROM batch_stats ORDER BY batch_id, service, level"
                ).fetchall()]
                return {"version": current, "materialized": True,
                        "classification_index": classification,
                        "timeline_index": timeline, "batch_stats": stats}

            if as_of_version < 1 or (current is not None and as_of_version > current):
                raise StaleBaseError(
                    "请求版本超出已提交版本范围",
                    {"as_of_version": as_of_version, "current_version": current},
                )
            records = self.query_records(as_of_version=as_of_version)
            per_partition_level: Dict[str, Dict[str, int]] = defaultdict(
                lambda: defaultdict(int))
            per_batch: Dict[Tuple[str, str], int] = defaultdict(int)
            points: Dict[str, List[int]] = defaultdict(list)
            for record in records:
                service = record["service"]
                per_partition_level[service][record["level"]] += 1
                per_batch[(record["batch_id"], record["level"])] += 1
                points[service].append(record["timestamp_ms"])
            classification = [
                {"service": service, "level": level, "record_count": count,
                 "updated_version": as_of_version}
                for service, levels in sorted(per_partition_level.items())
                for level, count in sorted(levels.items())
            ]
            timeline = []
            for service in sorted(points):
                for ordinal, (start, end) in enumerate(
                    indexing.merge_intervals(points[service])
                ):
                    timeline.append({"service": service, "start_ms": start,
                                     "end_ms": end, "ordinal": ordinal,
                                     "built_version": as_of_version})
            stats = [
                {"batch_id": batch_id, "service": conn.execute(
                    "SELECT service FROM records WHERE batch_id=? LIMIT 1",
                    (batch_id,)).fetchone()["service"],
                 "level": level, "record_count": count,
                 "updated_version": as_of_version}
                for (batch_id, level), count in sorted(per_batch.items())
            ]
            return {"version": as_of_version, "materialized": False,
                    "classification_index": classification,
                    "timeline_index": timeline, "batch_stats": stats}
        finally:
            conn.close()

    def version_events(self) -> List[dict]:
        conn = self._conn()
        try:
            return [dict(r) for r in conn.execute(
                "SELECT * FROM version_events ORDER BY version"
            ).fetchall()]
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # 崩溃注入与恢复
    # ------------------------------------------------------------------
    def arm_crash_after_indexing(self) -> None:
        """下一次批次提交在 INDEXING 意图落盘后、派生写入前“强制终止”。"""
        self._crash_points.add(CRASH_AFTER_INDEXING_INTENT)

    def recover(self) -> RecoveryReport:
        """扫描未完成意图，按持久化事实恢复到最后完整批次边界。"""
        notes: List[str] = []
        actions: List[dict] = []
        conn = self._conn()
        try:
            lock_row = conn.execute("SELECT * FROM write_lock WHERE id=1").fetchone()
            stale_holder = lock_row["holder"] if lock_row else None
            if lock_row is not None:
                owner_pid, owner_start = self._holder_identity(lock_row["holder"])
                same_process = (
                    owner_pid is not None and owner_start == self.process_token
                )
                lock_is_stale = (not same_process) and (
                    owner_pid is None or not self._process_alive(owner_pid)
                )
                if lock_is_stale:
                    conn.execute("DELETE FROM write_lock WHERE id=1")
                    actions.append({"action": "clear_stale_write_lock",
                                    "previous_holder": stale_holder})
                else:
                    notes.append(
                        f"write_lock 由存活进程 {lock_row['holder']} 持有，保留不动"
                    )
            open_intents = [dict(r) for r in conn.execute(
                "SELECT * FROM intents WHERE finished_at_ms IS NULL ORDER BY created_at_ms"
            ).fetchall()]
            for intent in open_intents:
                batch_id = intent["batch_id"]
                batch = self._get_batch(conn, batch_id)
                if batch is None:
                    conn.execute(
                        "UPDATE intents SET state='ABORTED', finished_at_ms=? "
                        "WHERE intent_id=?", (now_ms(), intent["intent_id"]))
                    actions.append({"action": "abort_orphan_intent",
                                    "intent_id": intent["intent_id"]})
                    continue
                if batch["state"] == BatchState.INDEXING.value:
                    if self.recovery_policy == "abort":
                        self._transition(conn, batch_id, BatchState.ABORTED)
                        conn.execute(
                            "UPDATE intents SET state='ABORTED', finished_at_ms=? "
                            "WHERE intent_id=?", (now_ms(), intent["intent_id"]))
                        actions.append({"action": "abort_indexing_batch",
                                        "batch_id": batch_id})
                    else:
                        self._transition(
                            conn, batch_id, BatchState.PENDING, recovery=True
                        )
                        conn.execute(
                            "UPDATE intents SET state='PENDING' WHERE intent_id=?",
                            (intent["intent_id"],))
                        actions.append({"action": "requeue_pending_batch",
                                        "batch_id": batch_id})
                elif batch["state"] == BatchState.PENDING.value:
                    actions.append({"action": "already_pending",
                                    "batch_id": batch_id})
            last_complete = conn.execute(
                "SELECT MAX(version) AS v FROM batches WHERE state='COMMITTED'"
            ).fetchone()
            last_batch_row = conn.execute(
                "SELECT batch_id FROM batches WHERE state='COMMITTED' "
                "ORDER BY version DESC LIMIT 1"
            ).fetchone()
            last_version = last_complete["v"]
            last_batch = last_batch_row["batch_id"] if last_batch_row else None
            notes.append(
                "恢复结论仅基于 SQLite 中已提交事务；强制终止的进程不会生成"
                "完整退出报告，未完成批次回到 PENDING 等待重排或被 ABORTED。"
            )
            conn.commit()
            report = RecoveryReport(
                recovered=bool(open_intents),
                previous_write_lock_owner=stale_holder,
                open_intents=open_intents,
                actions=actions,
                last_complete_version=last_version,
                last_complete_batch=last_batch,
                notes=notes,
            )
            for action in actions:
                conn.execute(
                    "INSERT INTO recovery_events(recovered_at_ms, kind, detail) "
                    "VALUES(?,?,?)",
                    (now_ms(), action["action"], json.dumps(action)))
            conn.commit()
            return report
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def get_recovery_report(self) -> dict:
        conn = self._conn()
        try:
            events = [dict(r) for r in conn.execute(
                "SELECT * FROM recovery_events ORDER BY event_id"
            ).fetchall()]
        finally:
            conn.close()
        return {
            "startup_report": self.recovery_report,
            "persisted_recovery_events": events,
            "current_version": self.current_version(),
        }

    @staticmethod
    def reset(db_path) -> None:
        reset_database(db_path)
