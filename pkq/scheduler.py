"""三重预算调度：全局并发上限 + 单任务内存预算 + 整轮时间（代价）预算。

确定性保证：任务按主键区间升序裁决与归并，执行并发不改变输出内容；
裁决轨迹按任务编号顺序记录选择依据。
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

from .config import Config
from .encoding import estimate_decoded_bytes
from .errors import ScanQueueFull
from .planner import FetchTask

# 任务终态
DONE = "DONE"
SKIPPED = "SKIPPED"
NOT_EXECUTED = "NOT_EXECUTED"


@dataclass
class TaskOutcome:
    task: FetchTask
    status: str                 # DONE / SKIPPED / NOT_EXECUTED
    reason: str = ""
    rows: list[dict] = field(default_factory=list)  # 命中行（已投影），按主键升序


@dataclass
class RoundTrace:
    round_no: int
    records: list[dict] = field(default_factory=list)

    def add(self, task_id: str, decision: str, reason: str, **extra):
        self.records.append({"round": self.round_no, "task_id": task_id,
                             "decision": decision, "reason": reason, **extra})


class Scheduler:
    def __init__(self, config: Config, executor):
        """executor(task) -> list[dict]：解码+过滤+投影后的命中行（升序）。"""
        self.config = config
        self.executor = executor
        self.traces: list[RoundTrace] = []

    # ------------------------------------------------------------ 内存预算：拆分
    def split_for_memory(self, task: FetchTask, n_columns: int) -> list[FetchTask]:
        """单任务解压后内存估计超预算时，按块内行范围拆分为子任务。
        单行估计仍超预算时返回空列表（调用方降级跳过）。"""
        budget = self.config.task_memory_budget_bytes
        per_row = max(1, estimate_decoded_bytes(1, n_columns))
        if per_row > budget:
            return []
        sub: list[FetchTask] = []
        seq = 0
        for blk in task.blocks:
            fit = max(1, budget // per_row)
            lo = 0
            while lo < blk.row_count:
                hi = min(blk.row_count, lo + fit)
                sub.append(FetchTask(
                    task_id=f"{task.task_id}#{seq}",
                    blocks=[blk],
                    pk_lo=blk.pk_min, pk_hi=blk.pk_max,
                    row_lo=lo, row_hi=hi,
                    est_bytes=estimate_decoded_bytes(hi - lo, n_columns),
                    est_cost=max(1, blk.decode_cost * (hi - lo) // max(1, blk.row_count)),
                    parent_id=task.task_id,
                ))
                lo = hi
                seq += 1
        return sub

    # ------------------------------------------------------------ 一轮调度
    def run_round(self, tasks: list[FetchTask], n_columns: int, round_no: int) -> list[TaskOutcome]:
        trace = RoundTrace(round_no)
        self.traces.append(trace)
        cfg = self.config

        waiting = max(0, len(tasks) - cfg.max_concurrent_tasks)
        if waiting > cfg.scan_queue_capacity:
            raise ScanQueueFull(
                f"等待任务 {waiting} 超过队列容量 {cfg.scan_queue_capacity}",
                waiting=waiting, capacity=cfg.scan_queue_capacity)
        for t in tasks[cfg.max_concurrent_tasks:]:
            trace.add(t.task_id, "QUEUED", "超出全局并发上限，进入有界等待队列",
                      queue_position=tasks.index(t) - cfg.max_concurrent_tasks)

        # 内存预算裁决（提交前确定，保证轨迹有序）
        plan: list[tuple[FetchTask, str]] = []  # (task, note)
        for t in tasks:
            if t.est_bytes > cfg.task_memory_budget_bytes:
                subs = self.split_for_memory(t, n_columns)
                if not subs:
                    plan.append((t, "SKIP"))
                    trace.add(t.task_id, "DEGRADED_SKIP",
                              f"单行内存估计仍超单任务预算 {cfg.task_memory_budget_bytes}B，"
                              f"降级跳过", budget=cfg.task_memory_budget_bytes)
                    continue
                trace.add(t.task_id, "SPLIT",
                          f"内存估计 {t.est_bytes}B 超单任务预算 {cfg.task_memory_budget_bytes}B，"
                          f"按块内行范围拆分为 {len(subs)} 个子任务重试",
                          subtasks=[s.task_id for s in subs])
                for s in subs:
                    plan.append((s, "RUN"))
            else:
                plan.append((t, "RUN"))

        # 时间（代价）预算裁决：按主键区间升序纳入本轮
        runnable: list[FetchTask] = []
        outcomes: dict[str, TaskOutcome] = {}
        spent = 0
        for t, note in plan:
            if note == "SKIP":
                outcomes[t.task_id] = TaskOutcome(t, SKIPPED, "内存预算不可满足，降级跳过")
                continue
            if spent + t.est_cost > cfg.round_cost_budget:
                outcomes[t.task_id] = TaskOutcome(t, NOT_EXECUTED, "整轮时间预算耗尽，标记未执行")
                trace.add(t.task_id, "NOT_EXECUTED",
                          f"轮时间预算耗尽(已用 {spent}/{cfg.round_cost_budget})，标记未执行")
                continue
            spent += t.est_cost
            runnable.append(t)
            trace.add(t.task_id, "RUN",
                      f"并发与队列预算内，累计代价 {spent}/{cfg.round_cost_budget}，"
                      f"内存估计 {t.est_bytes}B<={cfg.task_memory_budget_bytes}B")

        # 全局并发上限内并行执行；结果按提交顺序（主键升序）归并，保证确定性
        if runnable:
            with ThreadPoolExecutor(max_workers=cfg.max_concurrent_tasks) as pool:
                results = list(pool.map(self.executor, runnable))
            for t, rows in zip(runnable, results):
                outcomes[t.task_id] = TaskOutcome(t, DONE, rows=rows)
                trace.add(t.task_id, "DONE", f"命中 {len(rows)} 行写入分页结果缓冲",
                          hit_rows=len(rows))

        return [outcomes[t.task_id] for t, _ in plan]

    def run_all(self, tasks: list[FetchTask], n_columns: int) -> list[TaskOutcome]:
        """多轮执行直到所有任务到达终态（DONE/SKIPPED）；NOT_EXECUTED 任务进入下一轮。"""
        pending = list(tasks)
        final: dict[str, TaskOutcome] = {}
        round_no = 0
        while pending:
            outs = self.run_round(pending, n_columns, round_no)
            round_no += 1
            pending = []
            for o in outs:
                if o.status == NOT_EXECUTED:
                    pending.append(o.task)
                else:
                    final[o.task.task_id] = o
        # 按主键区间升序返回（顶层任务展开其子任务）
        order = {t.task_id: i for i, t in enumerate(tasks)}
        return sorted(final.values(),
                      key=lambda o: (order.get(o.task.parent_id or o.task.task_id, 0),
                                     o.task.pk_lo, o.task.row_lo))
