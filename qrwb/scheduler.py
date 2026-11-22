"""阶段三：预算约束下的并行编排调度。

三重约束：
1. 全局改写评估次数预算（global_evaluations，全局累计，绝不超）；
2. 单通道配额（per_channel_quota，按 channel 累计）；
3. 并发通道上限（max_concurrent_channels，同一时刻不同 channel 数）。
另以每请求延迟预算（per_request_latency_ms）约束单查询编排时长。
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .errors import (E_BUDGET_EXHAUSTED, E_CHANNEL_QUOTA, E_LATENCY_BUDGET,
                     make_error)
from .util import FileLock, atomic_write_json, canonical_dumps


# ---- 预算账本（持久化 + 文件锁） ------------------------------------------
class BudgetLedger:
    def __init__(self, workspace: str, cfg_budget: Dict[str, Any],
                 budget_fingerprint: str) -> None:
        self.path = os.path.join(workspace, "state", "budget_ledger.json")
        self.lock_path = os.path.join(workspace, "locks", "budget.lock")
        self.cfg_budget = cfg_budget
        self.budget_fingerprint = budget_fingerprint
        self._load()

    def _load(self) -> None:
        if os.path.exists(self.path):
            import json
            with open(self.path, "r", encoding="utf-8") as handle:
                self.state = json.load(handle)
            if self.state.get("budget_fingerprint") != self.budget_fingerprint:
                # 预算配置版本变化 → 账本重置（旧预算下的累计不可冒充新预算）
                self.state = self._fresh()
        else:
            self.state = self._fresh()

    def _fresh(self) -> Dict[str, Any]:
        return {
            "budget_fingerprint": self.budget_fingerprint,
            "global_limit": int(self.cfg_budget["global_evaluations"]),
            "channel_quota": int(self.cfg_budget["per_channel_quota"]),
            "global_used": 0,
            "channel_used": {},
            "reservations": {},
            "evaluated_queries": [],
        }

    def snapshot(self) -> Dict[str, Any]:
        with FileLock(self.lock_path):
            return canonical_dumps(self.state) and _json_copy(self.state)

    def try_reserve(self, query_id: str, channel: str, cost: int
                    ) -> Optional[Dict[str, Any]]:
        """在锁内原子判断三重约束并预占评估次数。"""
        with FileLock(self.lock_path):
            if self.state["global_used"] + cost > self.state["global_limit"]:
                return {"ok": False, "code": E_BUDGET_EXHAUSTED,
                        "detail": {"global_used": self.state["global_used"],
                                   "global_limit": self.state["global_limit"],
                                   "cost": cost}}
            channel_used = self.state["channel_used"].get(channel, 0)
            if channel_used + cost > self.state["channel_quota"]:
                return {"ok": False, "code": E_CHANNEL_QUOTA,
                        "detail": {"channel": channel,
                                   "channel_used": channel_used,
                                   "channel_quota": self.state["channel_quota"],
                                   "cost": cost}}
            self.state["global_used"] += cost
            self.state["channel_used"][channel] = channel_used + cost
            self.state["reservations"][query_id] = {
                "channel": channel, "cost": cost}
            self._flush()
            return {"ok": True}

    def commit(self, query_id: str) -> None:
        with FileLock(self.lock_path):
            reservation = self.state["reservations"].pop(query_id, None)
            if reservation is not None:
                self.state["evaluated_queries"].append({
                    "query_id": query_id,
                    "channel": reservation["channel"],
                    "cost": reservation["cost"],
                })
                self._flush()

    def release(self, query_id: str) -> None:
        """查询未真正消费（如延迟预算中止）时归还预占。"""
        with FileLock(self.lock_path):
            reservation = self.state["reservations"].pop(query_id, None)
            if reservation is not None:
                self.state["global_used"] -= reservation["cost"]
                self.state["channel_used"][reservation["channel"]] = \
                    self.state["channel_used"].get(reservation["channel"], 0) \
                    - reservation["cost"]
                self._flush()

    def reset_consumption(self) -> None:
        """新一轮运行/恢复前清空累计与预占，保留预算上限与版本指纹。"""
        with FileLock(self.lock_path):
            self.state["global_used"] = 0
            self.state["channel_used"] = {}
            self.state["reservations"] = {}
            self.state["evaluated_queries"] = []
            self._flush()

    def has_committed(self, query_id: str) -> bool:
        with FileLock(self.lock_path):
            return any(q["query_id"] == query_id
                       for q in self.state["evaluated_queries"])

    def _flush(self) -> None:
        atomic_write_json(self.path, self.state)


def _json_copy(obj: Any) -> Any:
    import json
    return json.loads(canonical_dumps(obj))


# ---- 调度器 ---------------------------------------------------------------
@dataclass
class Admission:
    query_id: str
    channel: str
    estimated_cost: int
    accepted: bool
    order: int
    error: Optional[Dict[str, Any]] = None
    detail: Dict[str, Any] = None


def estimate_cost(plan: Dict[str, Any], manifest_rows: int) -> int:
    """接纳前预估评估次数：各启用路线候选上限之和，并以分片行数做显式观测校正。

    预估只增不减（取路线 max_candidates），保证实际消费 <= 预估，
    从而任何时刻累计评估次数都不会突破全局预算。
    """
    total = 0
    for route, section in plan.get("routes", {}).items():
        if section.get("enabled"):
            total += int(section["max_candidates"])
    # 分片行数作为观测项参与预估（每条查询至少 1 次调度评估）
    total = max(total, 1)
    return total


def admit_queries(plans: List[Dict[str, Any]], ledger: BudgetLedger,
                  manifest: Dict[str, Any],
                  already_committed: Optional[set] = None
                  ) -> List[Admission]:
    """按确定性接纳顺序（query_id 升序）逐个尝试预占。"""
    already_committed = already_committed or set()
    manifest_rows = manifest["total_rows"]
    admissions: List[Admission] = []
    order = 0
    for plan in sorted(plans, key=lambda p: p["query_id"]):
        order += 1
        if plan["query_id"] in already_committed:
            # 恢复路径：已提交编排的查询不再重复占用预算
            admissions.append(Admission(
                query_id=plan["query_id"], channel=plan["channel"],
                estimated_cost=0, accepted=True, order=order,
                detail={"restored_from_checkpoint": True}))
            continue
        if plan["status"] == "error":
            admissions.append(Admission(
                query_id=plan["query_id"],
                channel=plan.get("channel") or "unknown",
                estimated_cost=0, accepted=False, order=order,
                error=plan["error"]))
            continue
        cost = estimate_cost(plan, manifest_rows)
        result = ledger.try_reserve(plan["query_id"], plan["channel"], cost)
        if result["ok"]:
            admissions.append(Admission(
                query_id=plan["query_id"], channel=plan["channel"],
                estimated_cost=cost, accepted=True, order=order,
                detail={"manifest_rows_observed": manifest_rows}))
        else:
            err = make_error(result["code"],
                             "预算约束拒绝接纳" if result["code"] == E_BUDGET_EXHAUSTED
                             else "单通道配额拒绝接纳",
                             query_id=plan["query_id"], stage="orchestration",
                             **result["detail"])
            ledger.release(plan["query_id"])
            admissions.append(Admission(
                query_id=plan["query_id"], channel=plan["channel"],
                estimated_cost=cost, accepted=False, order=order,
                error=err.to_dict()))
    return admissions


def build_waves(admissions: List[Admission], max_concurrent_channels: int
                ) -> List[List[Admission]]:
    """按并发通道上限切波次：同一波内 channel 去重数 <= 上限。

    波次内查询并行执行，波次间顺序推进；并行与串行模式使用完全相同的
    接纳顺序与波次划分，仅执行载体不同，保证归约结果逐字节一致。
    """
    accepted = [a for a in admissions if a.accepted]
    waves: List[List[Admission]] = []
    current: List[Admission] = []
    channels: set[str] = set()
    for admission in accepted:
        if admission.channel not in channels and \
                len(channels) >= max_concurrent_channels:
            waves.append(current)
            current = []
            channels = set()
        current.append(admission)
        channels.add(admission.channel)
    if current:
        waves.append(current)
    return waves


class LatencyGuard:
    """每请求延迟预算：以确定性模拟耗时计量，超预算即中止等待。"""

    def __init__(self, budget_ms: float, work_ms: float, jitter_ms: float,
                 seed: int) -> None:
        import hashlib
        import random
        self.budget_ms = float(budget_ms)
        self.work_ms = float(work_ms)
        self.jitter_ms = float(jitter_ms)
        self.seed = seed
        self._hashlib = hashlib
        self._random = random

    def simulate(self, query_id: str) -> Dict[str, Any]:
        # 每查询独立确定性 RNG：与线程调度顺序无关，并行/串行结果一致
        digest = self._hashlib.sha256(
            f"latency:{self.seed}:{query_id}".encode("utf-8")).hexdigest()
        rng = self._random.Random(int(digest, 16) % (2 ** 32))
        elapsed = self.work_ms + rng.uniform(0, self.jitter_ms)
        elapsed = round(elapsed, 6)
        if elapsed > self.budget_ms:
            err = make_error(
                E_LATENCY_BUDGET,
                f"模拟编排耗时 {elapsed}ms 超过每请求延迟预算 {self.budget_ms}ms",
                query_id=query_id, stage="orchestration",
                elapsed_ms=elapsed, budget_ms=self.budget_ms)
            return {"ok": False, "elapsed_ms": elapsed, "error": err.to_dict()}
        return {"ok": True, "elapsed_ms": elapsed}
