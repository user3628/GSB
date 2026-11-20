"""调度服务：配额准入、批组织应用、抢占回收、状态迁移与崩溃恢复。

并发模型：
  - 每个配额组一把 threading.Lock，写者按组串行（单写者）。
  - 只读路径不加进程锁，依赖 SQLite WAL 的多版本快照。
  - 所有写操作经 Repository.run_intent 走“意图先落盘 + 单事务应用”，
    状态与预留量在同一事务更新，杜绝 BATCHED 与旧预留量同时可见。
"""

from __future__ import annotations

import threading
from typing import Any

from .batching import (
    GREEDY_FF,
    MEM_DESC_BF,
    assert_batch_latency,
    plan_placement,
)
from .database import Database
from .errors import (
    E_INVALID_TRANSITION,
    E_NOT_FOUND,
    E_PREEMPTION_NOT_POSSIBLE,
    E_QUOTA_EXCEEDED,
    BusinessError,
)
from .repository import Repository, utc_now
from .spec import (
    RequestSpec,
    WorkloadSpec,
)
from .statemachine import (
    ADMITTED,
    ALL_STATES,
    BATCHED,
    COMPLETED,
    PENDING,
    PREEMPTED,
    REASON_ADMITTED,
    REASON_BATCHED,
    REASON_BATCH_REORGANIZED,
    REASON_BATCH_STARTED,
    REASON_COMPLETED,
    REASON_PREEMPTED,
    REASON_QUOTA_REJECTED,
    REASON_READMITTED,
    REASON_SUBMITTED,
    REJECTED,
    RUNNING,
    ensure_transition,
)

# 活跃占用配额的状态：已准入、已入批、运行中都持续持有预留量
ACTIVE_STATES = (ADMITTED, BATCHED, RUNNING)
BATCH_STRATEGY_THRESHOLD = 500


class SchedulerService:
    def __init__(self, database: Database, strategy_threshold: int = BATCH_STRATEGY_THRESHOLD):
        self.db = database
        self.repo = Repository(database)
        self._group_locks: dict[str, threading.RLock] = {}
        self._locks_guard = threading.Lock()
        self.strategy_threshold = strategy_threshold

    def _group_lock(self, group_id: str) -> threading.RLock:
        with self._locks_guard:
            lock = self._group_locks.get(group_id)
            if lock is None:
                lock = threading.RLock()
                self._group_locks[group_id] = lock
            return lock

    # ------------------------------------------------------------ 规格装载
    def load_spec(self, spec: WorkloadSpec) -> dict:
        return self.repo.load_spec(spec)

    # ------------------------------------------------------------ 只读视图
    def quota_snapshot(self, group_id: str | None = None) -> list[dict]:
        groups = self.repo.list_groups()
        requests = self.repo.list_requests()
        nodes = self.repo.list_nodes()
        node_base_memory: dict[str, int] = {n["node_id"]: n["base_used_memory"] for n in nodes}
        group_nodes: dict[str, list[str]] = {}
        for node in nodes:
            group_nodes.setdefault(node["group_id"], []).append(node["node_id"])

        reserved_memory: dict[str, int] = {g["group_id"]: 0 for g in groups}
        reserved_compute: dict[str, int] = {g["group_id"]: 0 for g in groups}
        for request in requests:
            if request["state"] in ACTIVE_STATES:
                reserved_memory[request["group_id"]] += request["reserved_memory"]
                reserved_compute[request["group_id"]] += request["reserved_compute"]

        result = []
        for group in groups:
            gid = group["group_id"]
            if group_id and gid != group_id:
                continue
            base_memory = sum(node_base_memory[nid] for nid in group_nodes.get(gid, ()))
            used_memory = base_memory + reserved_memory[gid]
            result.append(
                {
                    "group_id": gid,
                    "memory_limit": group["memory_limit"],
                    "compute_limit": group["compute_limit"],
                    "node_base_used_memory": base_memory,
                    "reserved_memory": reserved_memory[gid],
                    "reserved_compute": reserved_compute[gid],
                    "used_memory": used_memory,
                    "used_compute": reserved_compute[gid],
                    "remaining_memory": group["memory_limit"] - used_memory,
                    "remaining_compute": group["compute_limit"] - reserved_compute[gid],
                    "node_ids": group_nodes.get(gid, []),
                }
            )
        return result

    def node_snapshot(self) -> list[dict]:
        nodes = self.repo.list_nodes()
        batches = self.repo.list_batches(state="PLACED")
        running = self.repo.list_batches(state="RUNNING")
        batch_memory: dict[str, int] = {}
        batch_compute: dict[str, int] = {}
        for batch in batches + running:
            batch_memory[batch["node_id"]] = (
                batch_memory.get(batch["node_id"], 0) + batch["total_memory"]
            )
            batch_compute[batch["node_id"]] = (
                batch_compute.get(batch["node_id"], 0) + batch["total_compute"]
            )
        result = []
        for node in nodes:
            allocated_memory = batch_memory.get(node["node_id"], 0)
            allocated_compute = batch_compute.get(node["node_id"], 0)
            free_memory = node["total_memory"] - node["base_used_memory"] - allocated_memory
            memory_utilization = (
                round(
                    (node["base_used_memory"] + allocated_memory) / node["total_memory"], 6
                )
                if node["total_memory"]
                else 0.0
            )
            result.append(
                {
                    "node_id": node["node_id"],
                    "group_id": node["group_id"],
                    "health": node["health"],
                    "total_memory": node["total_memory"],
                    "base_used_memory": node["base_used_memory"],
                    "allocated_batch_memory": allocated_memory,
                    "allocated_batch_compute": allocated_compute,
                    "free_memory": free_memory,
                    "total_compute": node["total_compute"],
                    "memory_utilization": memory_utilization,
                }
            )
        return result

    # ------------------------------------------------------------ 提交请求
    def submit_request(self, spec: RequestSpec) -> dict:
        existing = self.repo.get_request(spec.request_id)
        if existing is not None:
            # 幂等：同一 request_id 重复提交只生效一次，后续调用复用既有结果
            return {
                "request_id": spec.request_id,
                "deduplicated": True,
                "state": existing["state"],
                "existing": existing,
            }
        payload = {
            "request_id": spec.request_id,
            "group_id": spec.group_id,
            "spec": {
                "request_id": spec.request_id,
                "service_name": spec.service_name,
                "model_id": spec.model_id,
                "memory_need": spec.memory_need,
                "compute_need": spec.compute_need,
                "samples_per_batch": spec.samples_per_batch,
                "latency_limit_ms": spec.latency_limit_ms,
                "priority": spec.priority,
                "group_id": spec.group_id,
                "seq": spec.seq,
            },
        }

        def apply(tx, intent_id: str) -> dict:
            now = utc_now()
            tx.execute(
                "INSERT INTO requests(request_id, service_name, model_id, memory_need, "
                "compute_need, samples_per_batch, latency_limit_ms, priority, group_id, seq, "
                "state, reserved_memory, reserved_compute, created_at, updated_at) "
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', 0, 0, ?, ?)",
                (
                    spec.request_id,
                    spec.service_name,
                    spec.model_id,
                    spec.memory_need,
                    spec.compute_need,
                    spec.samples_per_batch,
                    spec.latency_limit_ms,
                    spec.priority,
                    spec.group_id,
                    spec.seq,
                    now,
                    now,
                ),
            )
            self._insert_event(
                tx, spec.request_id, None, PENDING, REASON_SUBMITTED, None, None, intent_id
            )
            return {"request_id": spec.request_id, "state": PENDING, "deduplicated": False}

        return self.repo.run_intent("SUBMIT", payload, apply)["result"]

    # ------------------------------------------------------------ 准入判定
    def _quota_usage(self, tx, group_id: str) -> dict:
        group = tx.execute(
            "SELECT * FROM quota_groups WHERE group_id=?", (group_id,)
        ).fetchone()
        if group is None:
            raise BusinessError(E_NOT_FOUND, f"配额组 {group_id} 不存在", {"group_id": group_id})
        row = tx.execute(
            "SELECT COALESCE(SUM(reserved_memory), 0) AS mem, "
            "COALESCE(SUM(reserved_compute), 0) AS comp FROM requests "
            "WHERE group_id=? AND state IN (?, ?, ?)",
            (group_id, ADMITTED, BATCHED, RUNNING),
        ).fetchone()
        base_row = tx.execute(
            "SELECT COALESCE(SUM(base_used_memory), 0) AS base FROM nodes WHERE group_id=?",
            (group_id,),
        ).fetchone()
        return {
            "group_id": group_id,
            "memory_limit": group["memory_limit"],
            "compute_limit": group["compute_limit"],
            "node_base_used_memory": base_row["base"],
            "reserved_memory": row["mem"],
            "reserved_compute": row["comp"],
            "used_memory": base_row["base"] + row["mem"],
            "used_compute": row["comp"],
            "remaining_memory": group["memory_limit"] - base_row["base"] - row["mem"],
            "remaining_compute": group["compute_limit"] - row["comp"],
        }

    @staticmethod
    def _insert_event(tx, request_id, from_state, to_state, reason, node_id, batch_id, intent_id):
        tx.execute(
            "INSERT INTO request_events(request_id, from_state, to_state, reason, node_id, "
            "batch_id, intent_id, created_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
            (
                request_id,
                from_state,
                to_state,
                reason,
                node_id,
                batch_id,
                intent_id,
                utc_now(),
            ),
        )

    def admit_request(self, request_id: str) -> dict:
        request = self.repo.get_request(request_id)
        if request is None:
            raise BusinessError(E_NOT_FOUND, f"请求 {request_id} 不存在")
        with self._group_lock(request["group_id"]):
            return self._admit_locked(request_id)

    def _admit_locked(self, request_id: str, *, reason: str = REASON_ADMITTED) -> dict:
        payload = {"request_id": request_id}

        def apply(tx, intent_id: str) -> dict:
            req = tx.execute(
                "SELECT * FROM requests WHERE request_id=?", (request_id,)
            ).fetchone()
            if req is None:
                raise BusinessError(E_NOT_FOUND, f"请求 {request_id} 不存在")
            current = req["state"]
            if current in ACTIVE_STATES:
                return {"request_id": request_id, "state": current, "already_admitted": True}
            if current not in (PENDING, PREEMPTED):
                raise BusinessError(
                    E_INVALID_TRANSITION,
                    f"请求处于 {current}，不可准入",
                    {"request_id": request_id, "state": current},
                )
            usage = self._quota_usage(tx, req["group_id"])
            need_memory = req["memory_need"]
            need_compute = req["compute_need"]
            if (
                usage["remaining_memory"] < need_memory
                or usage["remaining_compute"] < need_compute
            ):
                raise BusinessError(
                    E_QUOTA_EXCEEDED,
                    f"配额组 {req['group_id']} 资源不足，请求 {request_id} 被拒绝",
                    {
                        "group_id": req["group_id"],
                        "need_memory": need_memory,
                        "need_compute": need_compute,
                        "usage": usage,
                    },
                )
            ensure_transition(current, ADMITTED)
            now = utc_now()
            tx.execute(
                "UPDATE requests SET state='ADMITTED', reserved_memory=?, reserved_compute=?, "
                "updated_at=? WHERE request_id=?",
                (need_memory, need_compute, now, request_id),
            )
            self._insert_event(
                tx, request_id, current, ADMITTED, reason, None, None, intent_id
            )
            return {
                "request_id": request_id,
                "state": ADMITTED,
                "reserved_memory": need_memory,
                "reserved_compute": need_compute,
                "usage_after": self._quota_usage(tx, req["group_id"]),
            }

        return self.repo.run_intent("ADMIT", payload, apply)["result"]

    def reject_request(self, request_id: str, reason: str = REASON_QUOTA_REJECTED) -> dict:
        request = self.repo.get_request(request_id)
        if request is None:
            raise BusinessError(E_NOT_FOUND, f"请求 {request_id} 不存在")
        with self._group_lock(request["group_id"]):
            payload = {"request_id": request_id, "reason": reason}

            def apply(tx, intent_id: str) -> dict:
                req = tx.execute(
                    "SELECT * FROM requests WHERE request_id=?", (request_id,)
                ).fetchone()
                current = req["state"]
                ensure_transition(current, REJECTED)
                tx.execute(
                    "UPDATE requests SET state='REJECTED', reserved_memory=0, "
                    "reserved_compute=0, node_id=NULL, batch_id=NULL, updated_at=? "
                    "WHERE request_id=?",
                    (utc_now(), request_id),
                )
                self._insert_event(
                    tx, request_id, current, REJECTED, reason, None, None, intent_id
                )
                return {"request_id": request_id, "state": REJECTED}

            return self.repo.run_intent("REJECT", payload, apply)["result"]

    # ------------------------------------------------------------ 批组织
    def run_batching(
        self,
        group_id: str | None = None,
        *,
        strategy: str | None = None,
        include_ids: list[str] | None = None,
    ) -> dict:
        targets = [g["group_id"] for g in self.repo.list_groups()]
        if group_id:
            targets = [gid for gid in targets if gid == group_id]
            if not targets:
                raise BusinessError(E_NOT_FOUND, f"配额组 {group_id} 不存在")
        reports = []
        for gid in targets:
            with self._group_lock(gid):
                reports.append(self._batch_group_locked(gid, strategy, include_ids))
        fragmented = []
        oversized = []
        for report in reports:
            fragmented.extend(report["fragmented_request_ids"])
            oversized.extend(report["oversized_request_ids"])
        merged = {
            "groups": reports,
            "batches_created": sum(r["batches_created"] for r in reports),
            "fragmented": fragmented,
            "oversized": oversized,
        }
        return merged

    def _choose_strategy(self, request_count: int, override: str | None) -> str:
        if override:
            return override
        return MEM_DESC_BF if request_count > self.strategy_threshold else GREEDY_FF

    def _batch_group_locked(
        self,
        group_id: str,
        override_strategy: str | None,
        include_ids: list[str] | None,
    ) -> dict:
        all_admitted = [
            r for r in self.repo.list_requests(group_id=group_id) if r["state"] == ADMITTED
        ]
        if include_ids is not None:
            wanted = set(include_ids)
            all_admitted = [r for r in all_admitted if r["request_id"] in wanted]
        if not all_admitted:
            return {
                "group_id": group_id,
                "strategy": override_strategy or GREEDY_FF,
                "batches_created": 0,
                "fragmented_request_ids": [],
                "oversized_request_ids": [],
                "utilization": {},
                "notes": ["没有 ADMITTED 请求需要组织"],
            }

        total_in_group = len(self.repo.list_requests(group_id=group_id))
        strategy = self._choose_strategy(total_in_group, override_strategy)
        nodes = self.repo.list_nodes()
        models = {m["model_id"]: m for m in self.repo.list_models()}
        plan = plan_placement(all_admitted, nodes, models, strategy=strategy)

        # 延迟硬校验：规划器已逐批保证，这里在应用前再断言一次
        for planned_batch in plan.batches:
            assert_batch_latency(planned_batch, models[planned_batch.model_id])

        payload = {
            "group_id": group_id,
            "strategy": strategy,
            "batch_count": len(plan.batches),
            "fragmented": plan.fragmented_request_ids,
            "oversized": plan.oversized_request_ids,
        }

        def apply(tx, intent_id: str) -> dict:
            version_row = tx.execute("SELECT COUNT(*) AS n FROM batches").fetchone()
            base_version = version_row["n"]
            created_batches = []
            now = utc_now()
            for index, planned_batch in enumerate(plan.batches, start=1):
                batch_id = f"batch-{group_id}-{base_version + index:06d}"
                tx.execute(
                    "INSERT INTO batches(batch_id, model_id, node_id, group_id, total_samples, "
                    "total_memory, total_compute, estimated_latency_ms, state, reorg_count, "
                    "created_intent, version, created_at, updated_at) "
                    "VALUES(?, ?, ?, ?, ?, ?, ?, ?, 'PLACED', 0, ?, ?, ?, ?)",
                    (
                        batch_id,
                        planned_batch.model_id,
                        planned_batch.node_id,
                        planned_batch.group_id,
                        planned_batch.total_samples,
                        planned_batch.total_memory,
                        planned_batch.total_compute,
                        planned_batch.estimated_latency_ms,
                        intent_id,
                        base_version + index,
                        now,
                        now,
                    ),
                )
                for seq, request_id in enumerate(planned_batch.request_ids):
                    req = tx.execute(
                        "SELECT * FROM requests WHERE request_id=?", (request_id,)
                    ).fetchone()
                    if req is None:
                        raise BusinessError(E_NOT_FOUND, f"请求 {request_id} 不存在")
                    ensure_transition(req["state"], BATCHED)
                    tx.execute(
                        "UPDATE requests SET state='BATCHED', node_id=?, batch_id=?, "
                        "updated_at=? WHERE request_id=?",
                        (planned_batch.node_id, batch_id, now, request_id),
                    )
                    tx.execute(
                        "INSERT INTO batch_items(batch_id, request_id, seq) VALUES(?, ?, ?)",
                        (batch_id, request_id, seq),
                    )
                    self._insert_event(
                        tx,
                        request_id,
                        ADMITTED,
                        BATCHED,
                        REASON_BATCHED,
                        planned_batch.node_id,
                        batch_id,
                        intent_id,
                    )
                created_batches.append(
                    {
                        "batch_id": batch_id,
                        "node_id": planned_batch.node_id,
                        "model_id": planned_batch.model_id,
                        "request_ids": list(planned_batch.request_ids),
                        "total_samples": planned_batch.total_samples,
                        "total_memory": planned_batch.total_memory,
                        "estimated_latency_ms": planned_batch.estimated_latency_ms,
                    }
                )
            return {
                "group_id": group_id,
                "strategy": strategy,
                "batches_created": len(created_batches),
                "batches": created_batches,
                "fragmented_request_ids": plan.fragmented_request_ids,
                "oversized_request_ids": plan.oversized_request_ids,
                "utilization": plan.utilization,
                "reorganized": plan.reorganized,
                "reorg_details": plan.reorg_details,
                "notes": plan.notes,
            }

        return self.repo.run_intent("BATCH", payload, apply)["result"]

    def reorganize_batches(self, group_id: str) -> dict:
        """对存量 PLACED 批做确定性重组：拆分后重新装箱，配额扣减保持不变。"""
        with self._group_lock(group_id):
            existing = [
                b for b in self.repo.list_batches(state="PLACED") if b["group_id"] == group_id
            ]
            if not existing:
                return {"group_id": group_id, "reorganized": False, "reason": "无存量批"}
            items: dict[str, list[str]] = {}
            request_ids: list[str] = []
            for batch in existing:
                member_ids = [item["request_id"] for item in self.repo.list_batch_items(batch["batch_id"])]
                items[batch["batch_id"]] = member_ids
                request_ids.extend(member_ids)
            requests_by_id = {r["request_id"]: r for r in self.repo.list_requests(group_id=group_id)}
            members = [requests_by_id[rid] for rid in request_ids if rid in requests_by_id]
            nodes = self.repo.list_nodes()
            models = {m["model_id"]: m for m in self.repo.list_models()}
            plan = plan_placement(
                members,
                nodes,
                models,
                strategy=MEM_DESC_BF,
                existing_batches=existing,
                existing_batch_items=items,
            )
            payload = {
                "group_id": group_id,
                "old_batches": [b["batch_id"] for b in existing],
                "new_batch_count": len(plan.batches),
            }

            def apply(tx, intent_id: str) -> dict:
                now = utc_now()
                tx.execute(
                    "DELETE FROM batch_items WHERE batch_id IN "
                    f"({','.join('?' for _ in existing)})",
                    [b["batch_id"] for b in existing],
                )
                tx.execute(
                    "UPDATE batches SET state='REORGANIZED', updated_at=? WHERE batch_id IN "
                    f"({','.join('?' for _ in existing)})",
                    [now, *[b["batch_id"] for b in existing]],
                )
                tx.execute(
                    "UPDATE requests SET state='ADMITTED', node_id=NULL, batch_id=NULL, "
                    "updated_at=? WHERE request_id IN "
                    f"({','.join('?' for _ in members)})",
                    [now, *[r["request_id"] for r in members]],
                )
                for request in members:
                    self._insert_event(
                        tx,
                        request["request_id"],
                        BATCHED,
                        ADMITTED,
                        REASON_BATCH_REORGANIZED,
                        None,
                        None,
                        intent_id,
                    )
                created = []
                base_version = tx.execute("SELECT COUNT(*) AS n FROM batches").fetchone()["n"]
                for index, planned_batch in enumerate(plan.batches, start=1):
                    batch_id = f"reorg-{group_id}-{base_version + index:06d}"
                    tx.execute(
                        "INSERT INTO batches(batch_id, model_id, node_id, group_id, "
                        "total_samples, total_memory, total_compute, estimated_latency_ms, "
                        "state, reorg_count, created_intent, version, created_at, updated_at) "
                        "VALUES(?, ?, ?, ?, ?, ?, ?, ?, 'PLACED', 1, ?, ?, ?, ?)",
                        (
                            batch_id,
                            planned_batch.model_id,
                            planned_batch.node_id,
                            planned_batch.group_id,
                            planned_batch.total_samples,
                            planned_batch.total_memory,
                            planned_batch.total_compute,
                            planned_batch.estimated_latency_ms,
                            intent_id,
                            base_version + index,
                            now,
                            now,
                        ),
                    )
                    for seq, request_id in enumerate(planned_batch.request_ids):
                        req = tx.execute(
                            "SELECT * FROM requests WHERE request_id=?", (request_id,)
                        ).fetchone()
                        ensure_transition(req["state"], BATCHED)
                        tx.execute(
                            "UPDATE requests SET state='BATCHED', node_id=?, batch_id=?, "
                            "updated_at=? WHERE request_id=?",
                            (planned_batch.node_id, batch_id, now, request_id),
                        )
                        tx.execute(
                            "INSERT INTO batch_items(batch_id, request_id, seq) VALUES(?, ?, ?)",
                            (batch_id, request_id, seq),
                        )
                        self._insert_event(
                            tx,
                            request_id,
                            ADMITTED,
                            BATCHED,
                            REASON_BATCH_REORGANIZED,
                            planned_batch.node_id,
                            batch_id,
                            intent_id,
                        )
                    created.append(batch_id)
                return {
                    "group_id": group_id,
                    "reorganized": True,
                    "old_batches": [b["batch_id"] for b in existing],
                    "new_batches": created,
                    "fragmented_request_ids": plan.fragmented_request_ids,
                    "utilization": plan.utilization,
                    "reorg_details": plan.reorg_details,
                }

            return self.repo.run_intent("REORG", payload, apply)["result"]

    # ------------------------------------------------------------ 运行/完成
    def start_batch(self, batch_id: str) -> dict:
        batch = self.repo.get_batch(batch_id)
        if batch is None:
            raise BusinessError(E_NOT_FOUND, f"批 {batch_id} 不存在")
        with self._group_lock(batch["group_id"]):
            payload = {"batch_id": batch_id}

            def apply(tx, intent_id: str) -> dict:
                current = tx.execute(
                    "SELECT * FROM batches WHERE batch_id=?", (batch_id,)
                ).fetchone()
                if current["state"] == "RUNNING":
                    return {"batch_id": batch_id, "state": "RUNNING", "already_started": True}
                if current["state"] != "PLACED":
                    raise BusinessError(
                        E_INVALID_TRANSITION,
                        f"批 {batch_id} 状态 {current['state']} 不可启动",
                        {"batch_id": batch_id, "state": current["state"]},
                    )
                now = utc_now()
                tx.execute(
                    "UPDATE batches SET state='RUNNING', updated_at=? WHERE batch_id=?",
                    (now, batch_id),
                )
                member_ids = [
                    row["request_id"]
                    for row in tx.execute(
                        "SELECT request_id FROM batch_items WHERE batch_id=? ORDER BY seq",
                        (batch_id,),
                    ).fetchall()
                ]
                for request_id in member_ids:
                    req = tx.execute(
                        "SELECT * FROM requests WHERE request_id=?", (request_id,)
                    ).fetchone()
                    ensure_transition(req["state"], RUNNING)
                    tx.execute(
                        "UPDATE requests SET state='RUNNING', updated_at=? WHERE request_id=?",
                        (now, request_id),
                    )
                    self._insert_event(
                        tx,
                        request_id,
                        BATCHED,
                        RUNNING,
                        REASON_BATCH_STARTED,
                        current["node_id"],
                        batch_id,
                        intent_id,
                    )
                return {"batch_id": batch_id, "state": "RUNNING", "requests": member_ids}

            return self.repo.run_intent("START_BATCH", payload, apply)["result"]

    def complete_batch(self, batch_id: str) -> dict:
        batch = self.repo.get_batch(batch_id)
        if batch is None:
            raise BusinessError(E_NOT_FOUND, f"批 {batch_id} 不存在")
        with self._group_lock(batch["group_id"]):
            payload = {"batch_id": batch_id}

            def apply(tx, intent_id: str) -> dict:
                current = tx.execute(
                    "SELECT * FROM batches WHERE batch_id=?", (batch_id,)
                ).fetchone()
                if current["state"] == "COMPLETED":
                    return {"batch_id": batch_id, "state": "COMPLETED", "already_completed": True}
                if current["state"] != "RUNNING":
                    raise BusinessError(
                        E_INVALID_TRANSITION,
                        f"批 {batch_id} 状态 {current['state']} 不可完成",
                        {"batch_id": batch_id, "state": current["state"]},
                    )
                now = utc_now()
                tx.execute(
                    "UPDATE batches SET state='COMPLETED', updated_at=? WHERE batch_id=?",
                    (now, batch_id),
                )
                member_ids = [
                    row["request_id"]
                    for row in tx.execute(
                        "SELECT request_id FROM batch_items WHERE batch_id=? ORDER BY seq",
                        (batch_id,),
                    ).fetchall()
                ]
                for request_id in member_ids:
                    req = tx.execute(
                        "SELECT * FROM requests WHERE request_id=?", (request_id,)
                    ).fetchone()
                    ensure_transition(req["state"], COMPLETED)
                    # 完成后真实释放预留量
                    tx.execute(
                        "UPDATE requests SET state='COMPLETED', reserved_memory=0, "
                        "reserved_compute=0, updated_at=? WHERE request_id=?",
                        (now, request_id),
                    )
                    self._insert_event(
                        tx,
                        request_id,
                        RUNNING,
                        COMPLETED,
                        REASON_COMPLETED,
                        current["node_id"],
                        batch_id,
                        intent_id,
                    )
                return {"batch_id": batch_id, "state": "COMPLETED", "requests": member_ids}

            return self.repo.run_intent("COMPLETE_BATCH", payload, apply)["result"]

    # ------------------------------------------------------------ 抢占回收
    def preempt_for(self, request_id: str) -> dict:
        """为高优先级请求腾出配额：先抢占，再在同一意图事务内重新准入。"""
        request = self.repo.get_request(request_id)
        if request is None:
            raise BusinessError(E_NOT_FOUND, f"请求 {request_id} 不存在")
        with self._group_lock(request["group_id"]):
            return self._preempt_locked(request_id)

    def _preempt_locked(self, request_id: str) -> dict:
        payload = {"request_id": request_id}

        def apply(tx, intent_id: str) -> dict:
            target = tx.execute(
                "SELECT * FROM requests WHERE request_id=?", (request_id,)
            ).fetchone()
            if target is None:
                raise BusinessError(E_NOT_FOUND, f"请求 {request_id} 不存在")
            if target["state"] in ACTIVE_STATES:
                return {
                    "request_id": request_id,
                    "state": target["state"],
                    "preempted": [],
                    "note": "目标请求已持有配额，无需抢占",
                }
            if target["state"] not in (PENDING, PREEMPTED):
                raise BusinessError(
                    E_INVALID_TRANSITION,
                    f"请求处于 {target['state']}，不能通过抢占准入",
                    {"request_id": request_id, "state": target["state"]},
                )

            usage = self._quota_usage(tx, target["group_id"])
            mem_deficit = target["memory_need"] - usage["remaining_memory"]
            comp_deficit = target["compute_need"] - usage["remaining_compute"]
            mem_freed = 0
            comp_freed = 0
            victims = []

            if mem_deficit > 0 or comp_deficit > 0:
                # 仅可抢占同组、更低优先级、仍持有预留量的请求。
                # RUNNING 批在途不可抢占（在途批处置语义），只取 ADMITTED/BATCHED。
                candidates = tx.execute(
                    "SELECT * FROM requests WHERE group_id=? AND state IN (?, ?) "
                    "AND priority < ? AND request_id != ? ORDER BY priority ASC, seq ASC",
                    (target["group_id"], ADMITTED, BATCHED, target["priority"], request_id),
                ).fetchall()
                for victim in candidates:
                    if mem_freed >= mem_deficit and comp_freed >= comp_deficit:
                        break
                    victims.append(victim)
                    mem_freed += victim["reserved_memory"]
                    comp_freed += victim["reserved_compute"]

            if mem_freed < mem_deficit or comp_freed < comp_deficit:
                raise BusinessError(
                    E_PREEMPTION_NOT_POSSIBLE,
                    f"可抢占资源仍不足以接纳请求 {request_id}",
                    {
                        "request_id": request_id,
                        "memory_deficit": max(mem_deficit, 0),
                        "compute_deficit": max(comp_deficit, 0),
                        "memory_freed": mem_freed,
                        "compute_freed": comp_freed,
                        "candidate_count": len(victims),
                    },
                )

            now = utc_now()
            victim_summaries = []
            affected_batches: set[str] = set()
            for victim in victims:
                from_state = victim["state"]
                victim_batch_id = victim["batch_id"]
                victim_node_id = victim["node_id"]
                if victim_batch_id:
                    affected_batches.add(victim_batch_id)
                    tx.execute(
                        "DELETE FROM batch_items WHERE batch_id=? AND request_id=?",
                        (victim_batch_id, victim["request_id"]),
                    )
                # 状态迁移 + 真实释放预留量，同一事务完成
                ensure_transition(from_state, PREEMPTED)
                tx.execute(
                    "UPDATE requests SET state='PREEMPTED', reserved_memory=0, "
                    "reserved_compute=0, node_id=NULL, batch_id=NULL, updated_at=? "
                    "WHERE request_id=?",
                    (now, victim["request_id"]),
                )
                self._insert_event(
                    tx,
                    victim["request_id"],
                    from_state,
                    PREEMPTED,
                    REASON_PREEMPTED,
                    victim_node_id,
                    victim_batch_id,
                    intent_id,
                )
                victim_summaries.append(
                    {
                        "request_id": victim["request_id"],
                        "priority": victim["priority"],
                        "released_memory": victim["reserved_memory"],
                        "released_compute": victim["reserved_compute"],
                        "from_batch": victim_batch_id,
                    }
                )

            # 在途批处置：成员被抢占后，空批标记 ABORTED；仍有成员的批重算资源
            for batch_id in sorted(affected_batches):
                remaining = tx.execute(
                    "SELECT bi.request_id, r.memory_need, r.compute_need, r.samples_per_batch, "
                    "m.cost_per_sample FROM batch_items bi "
                    "JOIN requests r ON r.request_id=bi.request_id "
                    "JOIN models m ON m.model_id=r.model_id "
                    "WHERE bi.batch_id=? ORDER BY bi.seq",
                    (batch_id,),
                ).fetchall()
                if not remaining:
                    tx.execute(
                        "UPDATE batches SET state='ABORTED', updated_at=? WHERE batch_id=?",
                        (now, batch_id),
                    )
                    continue
                total_memory = sum(r["memory_need"] for r in remaining)
                total_compute = sum(r["compute_need"] for r in remaining)
                total_samples = sum(r["samples_per_batch"] for r in remaining)
                latency = total_samples * remaining[0]["cost_per_sample"]
                tx.execute(
                    "UPDATE batches SET total_memory=?, total_compute=?, total_samples=?, "
                    "estimated_latency_ms=?, updated_at=? WHERE batch_id=?",
                    (total_memory, total_compute, total_samples, latency, now, batch_id),
                )

            # 腾出的配额在同一事务内重新准入高优先级请求
            ensure_transition(target["state"], ADMITTED)
            tx.execute(
                "UPDATE requests SET state='ADMITTED', reserved_memory=?, reserved_compute=?, "
                "updated_at=? WHERE request_id=?",
                (target["memory_need"], target["compute_need"], now, request_id),
            )
            self._insert_event(
                tx,
                request_id,
                target["state"],
                ADMITTED,
                REASON_READMITTED,
                None,
                None,
                intent_id,
            )
            return {
                "request_id": request_id,
                "state": ADMITTED,
                "preempted": victim_summaries,
                "aborted_or_shrunk_batches": sorted(affected_batches),
                "usage_after": self._quota_usage(tx, target["group_id"]),
            }

        return self.repo.run_intent("PREEMPT", payload, apply)["result"]

    # ------------------------------------------------------------ 通用迁移
    def transition(self, request_id: str, to_state: str, reason: str) -> dict:
        if to_state not in ALL_STATES:
            raise BusinessError(E_INVALID_TRANSITION, f"未知目标状态 {to_state}")
        request = self.repo.get_request(request_id)
        if request is None:
            raise BusinessError(E_NOT_FOUND, f"请求 {request_id} 不存在")
        with self._group_lock(request["group_id"]):
            payload = {"request_id": request_id, "to_state": to_state, "reason": reason}

            def apply(tx, intent_id: str) -> dict:
                req = tx.execute(
                    "SELECT * FROM requests WHERE request_id=?", (request_id,)
                ).fetchone()
                ensure_transition(req["state"], to_state)
                now = utc_now()
                tx.execute(
                    "UPDATE requests SET state=?, updated_at=? WHERE request_id=?",
                    (to_state, now, request_id),
                )
                self._insert_event(
                    tx, request_id, req["state"], to_state, reason,
                    req["node_id"], req["batch_id"], intent_id,
                )
                return {"request_id": request_id, "from_state": req["state"], "state": to_state}

            return self.repo.run_intent("TRANSITION", payload, apply)["result"]

    # ------------------------------------------------------------ 崩溃恢复
    def recover(self) -> dict:
        """扫描 PENDING 意图，仅依据持久化事实前滚或中止。

        关键事实：PENDING 意图的应用事务从未提交，因此库内不存在其任何效果，
        前滚等价于重新执行一次幂等应用；前滚失败则显式 ABORTED。
        """
        pending = self.repo.list_pending_intents()
        before = self._recovery_snapshot()
        actions = []
        for intent in pending:
            payload = _loads(intent["payload"])
            group_id = payload.get("group_id")
            if group_id:
                with self._group_lock(group_id):
                    action = self._recover_one(intent)
            else:
                action = self._recover_one(intent)
            actions.append(action)
        after = self._recovery_snapshot()
        report = {
            "pending_intent_count": len(pending),
            "actions": actions,
            "snapshot_before": before,
            "snapshot_after": after,
            "occupancy_diff": {
                "reserved_memory": after["reserved_memory"] - before["reserved_memory"],
                "reserved_compute": after["reserved_compute"] - before["reserved_compute"],
                "batch_allocated_memory": after["batch_allocated_memory"]
                - before["batch_allocated_memory"],
            },
            "state_count_diff": {
                state: after["state_counts"].get(state, 0)
                - before["state_counts"].get(state, 0)
                for state in ALL_STATES
            },
        }
        return report

    def _recovery_snapshot(self) -> dict:
        requests = self.repo.list_requests()
        batches = self.repo.list_batches()
        reserved_memory = sum(
            r["reserved_memory"] for r in requests if r["state"] in ACTIVE_STATES
        )
        reserved_compute = sum(
            r["reserved_compute"] for r in requests if r["state"] in ACTIVE_STATES
        )
        batch_memory = sum(
            b["total_memory"] for b in batches if b["state"] in ("PLACED", "RUNNING")
        )
        state_counts: dict[str, int] = {}
        for request in requests:
            state_counts[request["state"]] = state_counts.get(request["state"], 0) + 1
        return {
            "reserved_memory": reserved_memory,
            "reserved_compute": reserved_compute,
            "batch_allocated_memory": batch_memory,
            "state_counts": state_counts,
            "last_committed_intent": self._last_committed_intent_id(),
        }

    def _last_committed_intent_id(self) -> str | None:
        conn = self.db.connect()
        row = conn.execute(
            "SELECT intent_id FROM intents WHERE status='COMMITTED' "
            "ORDER BY committed_at DESC LIMIT 1"
        ).fetchone()
        return row["intent_id"] if row else None

    def _recover_one(self, intent: dict) -> dict:
        intent_id = intent["intent_id"]
        intent_type = intent["intent_type"]
        payload = _loads(intent["payload"])
        try:
            if intent_type == "ADMIT":
                self._admit_locked(payload["request_id"], reason=REASON_READMITTED)
            elif intent_type == "BATCH":
                self._recover_batch(intent_id, payload)
            elif intent_type == "PREEMPT":
                self._preempt_locked(payload["request_id"])
            elif intent_type == "SUBMIT":
                self._recover_submit(payload)
            elif intent_type == "REJECT":
                self.reject_request(payload["request_id"], payload.get("reason", REASON_QUOTA_REJECTED))
            elif intent_type in ("START_BATCH", "COMPLETE_BATCH", "REORG", "TRANSITION", "LOAD_SPEC"):
                # 这类意图前滚需要完整计划/规格，PENDING 时效果未落库；
                # 为避免凭非持久化信息猜测，统一标记 ABORTED，由管理员重新发起。
                raise BusinessError(
                    "E_INTENT_ABORTED",
                    f"{intent_type} 意图无法仅凭持久化事实前滚，标记中止",
                )
            else:
                raise BusinessError("E_INTENT_ABORTED", f"未知意图类型 {intent_type}")
        except BusinessError as exc:
            self.repo.mark_intent_aborted(intent_id, exc.code + ":" + str(exc))
            result = {"intent_id": intent_id, "intent_type": intent_type, "action": "ABORTED", "reason": exc.code}
        else:
            # 前滚产生的是新意图；原 PENDING 意图标记 ABORTED 并记录来源
            self.repo.mark_intent_aborted(
                intent_id, f"rolled-forward via new {intent_type} intent"
            )
            self.repo.record_recovery_event(
                intent_id,
                "ROLL_FORWARD",
                {"intent_type": intent_type, "payload": payload},
            )
            result = {"intent_id": intent_id, "intent_type": intent_type, "action": "ROLL_FORWARD"}
        return result

    def _recover_submit(self, payload: dict) -> None:
        request_id = payload["request_id"]
        if self.repo.get_request(request_id) is not None:
            return
        spec_payload = payload.get("spec")
        if not spec_payload:
            raise BusinessError("E_INTENT_ABORTED", "SUBMIT 缺少完整规格，中止待重新提交")
        spec = RequestSpec(**spec_payload)
        self.submit_request(spec)

    def _recover_batch(self, intent_id: str, payload: dict) -> None:
        group_id = payload["group_id"]
        # 前滚只基于库内仍处于 ADMITTED 的持久化事实重新规划
        report = self._batch_group_locked(group_id, payload.get("strategy"), None)
        self.repo.record_recovery_event(
            intent_id,
            "ROLL_FORWARD",
            {"intent_type": "BATCH", "group_id": group_id, "batches": report["batches_created"]},
        )


def _loads(raw: str) -> dict:
    import json

    return json.loads(raw)
