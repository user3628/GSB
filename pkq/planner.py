"""点查取数任务切分：定位命中分块，合并相邻块为任务，区间不重叠且覆盖全部命中块。"""
from __future__ import annotations

from dataclasses import dataclass, field

from .columnar import Snapshot, BlockIndexEntry
from .errors import QueryTooLarge


@dataclass
class FetchTask:
    task_id: str
    blocks: list[BlockIndexEntry]
    pk_lo: int
    pk_hi: int
    row_lo: int = 0          # 块内行范围拆分（子任务）
    row_hi: int | None = None  # 仅对单块子任务有效
    est_bytes: int = 0
    est_cost: int = 0
    parent_id: str | None = None

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id, "pk_lo": self.pk_lo, "pk_hi": self.pk_hi,
            "blocks": [b.block_id for b in self.blocks],
            "row_lo": self.row_lo, "row_hi": self.row_hi,
            "est_bytes": self.est_bytes, "est_cost": self.est_cost,
            "parent_id": self.parent_id,
        }


def plan_query(snapshot: Snapshot, keys: list[int], max_keys: int) -> list[FetchTask]:
    """主键集合为空返回空任务列表；超过阈值抛 QueryTooLarge。"""
    uniq = sorted(set(keys))
    if not uniq:
        return []
    if len(uniq) > max_keys:
        raise QueryTooLarge(f"主键集合数量 {len(uniq)} 超过上限 {max_keys}", limit=max_keys)
    from .encoding import estimate_decoded_bytes
    hit = snapshot.locate_blocks(uniq)
    tasks: list[FetchTask] = []
    run: list[BlockIndexEntry] = []

    def flush():
        if not run:
            return
        idx = len(tasks)
        rows = sum(b.row_count for b in run)
        tasks.append(FetchTask(
            task_id=f"t{idx:04d}",
            blocks=list(run),
            pk_lo=run[0].pk_min,
            pk_hi=run[-1].pk_max,
            est_bytes=estimate_decoded_bytes(rows, len(snapshot.columns)),
            est_cost=sum(b.decode_cost for b in run),
        ))
        run.clear()

    pos = {b.block_id: i for i, b in enumerate(snapshot.blocks)}
    for blk in hit:
        if run and pos[blk.block_id] != pos[run[-1].block_id] + 1:
            flush()  # 索引不连续（中间隔着未命中块），切断任务
        run.append(blk)
    flush()
    # 任务区间不重叠且按主键升序
    for i in range(len(tasks) - 1):
        assert tasks[i].pk_hi < tasks[i + 1].pk_lo
    return tasks
