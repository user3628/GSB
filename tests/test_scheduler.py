import pytest

from pkq.columnar import BlockIndexEntry
from pkq.config import Config
from pkq.errors import ScanQueueFull
from pkq.planner import FetchTask
from pkq.scheduler import Scheduler, DONE, NOT_EXECUTED, SKIPPED


def mk_task(tid, lo, hi, rows=10, cost=10, ncols=2):
    blk = BlockIndexEntry(block_id=int(tid[1:].split("#")[0]), file="x",
                          pk_min=lo, pk_max=hi, row_count=rows, decode_cost=cost)
    from pkq.encoding import estimate_decoded_bytes
    return FetchTask(task_id=tid, blocks=[blk], pk_lo=lo, pk_hi=hi,
                     est_bytes=estimate_decoded_bytes(rows, ncols), est_cost=cost)


def run_rows(task):
    return [{"pk": p} for p in range(task.pk_lo, task.pk_lo + 2)]


def test_queue_full():
    cfg = Config(max_concurrent_tasks=1, scan_queue_capacity=1)
    sch = Scheduler(cfg, run_rows)
    tasks = [mk_task(f"t{i}", i * 100, i * 100 + 9) for i in range(4)]
    with pytest.raises(ScanQueueFull):
        sch.run_all(tasks, 2)


def test_memory_split_retry():
    # 单任务预算仅容纳 ~5 行 -> 100 行任务拆分为多个子任务
    cfg = Config(task_memory_budget_bytes=5 * (8 + 2 * 17), round_cost_budget=10**9)
    sch = Scheduler(cfg, run_rows)
    t = mk_task("t0", 0, 99, rows=100, cost=10)
    outs = sch.run_all([t], 2)
    assert all(o.status == DONE for o in outs)
    assert len(outs) > 1 and all(o.task.parent_id == "t0" for o in outs)
    decisions = [r["decision"] for tr in sch.traces for r in tr.records]
    assert "SPLIT" in decisions


def test_memory_degraded_skip():
    cfg = Config(task_memory_budget_bytes=1, round_cost_budget=10**9)  # 单行都放不下
    sch = Scheduler(cfg, run_rows)
    outs = sch.run_all([mk_task("t0", 0, 9, rows=10)], 2)
    assert len(outs) == 1 and outs[0].status == SKIPPED and outs[0].reason
    decisions = [r["decision"] for tr in sch.traces for r in tr.records]
    assert "DEGRADED_SKIP" in decisions


def test_round_budget_marks_not_executed_then_next_round():
    cfg = Config(round_cost_budget=15, task_memory_budget_bytes=10**9)
    sch = Scheduler(cfg, run_rows)
    tasks = [mk_task(f"t{i}", i * 100, i * 100 + 9, cost=10) for i in range(3)]
    outs = sch.run_all(tasks, 2)
    assert all(o.status == DONE for o in outs)  # 多轮后全部完成
    first_round = sch.traces[0].records
    assert any(r["decision"] == "NOT_EXECUTED" for r in first_round)
    assert len(sch.traces) == 3  # 每轮预算仅容纳一个任务，三轮完成


def test_trace_has_task_ids_and_reasons():
    cfg = Config()
    sch = Scheduler(cfg, run_rows)
    sch.run_all([mk_task("t0", 0, 9), mk_task("t1", 100, 109)], 2)
    recs = [r for tr in sch.traces for r in tr.records]
    assert recs and all("task_id" in r and "reason" in r and "decision" in r for r in recs)
