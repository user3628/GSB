"""推理批组织规划器：纯函数、确定性、整数运算。

规划器只产出“计划”（Plan），不落库；由调度服务在单个意图事务内应用，
因此重组前后的准入结论与配额扣减天然保持一致（本阶段不触碰预留量）。

约束：
  - 同模型、同节点的请求才能同批；按提交顺序（seq）相邻优先。
  - 批合计显存 <= 节点剩余显存（含已放置批）。
  - 批样本数 <= 模型 max_batch_samples；批延迟 <= 请求期望延迟上限（取批内最严）。
  - 装箱策略：GREEDY_FF（提交顺序首适应）与 MEM_DESC_BF（显存降序最佳适应）。
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .errors import E_BATCH_TOO_LARGE, BusinessError
from .statemachine import ADMITTED

GREEDY_FF = "GREEDY_FF"
MEM_DESC_BF = "MEM_DESC_BF"


@dataclass
class PlannedBatch:
    model_id: str
    node_id: str
    group_id: str
    request_ids: list[str] = field(default_factory=list)
    total_samples: int = 0
    total_memory: int = 0
    total_compute: int = 0
    estimated_latency_ms: int = 0
    min_latency_limit: int = 0


@dataclass
class PlacementPlan:
    strategy: str
    batches: list[PlannedBatch]
    fragmented_request_ids: list[str]
    oversized_request_ids: list[str]
    per_node_used_memory: dict[str, int]
    per_node_used_compute: dict[str, int]
    utilization: dict[str, float]
    reorganized: bool = False
    reorg_details: list[dict] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


def estimate_latency(total_samples: int, cost_per_sample: int) -> int:
    return total_samples * cost_per_sample


def _node_caps(nodes: list[dict], base_used_memory_key: str) -> dict[str, dict]:
    caps = {}
    for node in nodes:
        caps[node["node_id"]] = {
            "group_id": node["group_id"],
            "health": node["health"],
            "free_memory": node["total_memory"] - node[base_used_memory_key],
            "free_compute": node["total_compute"],
        }
    return caps


def _sort_requests(requests: list[dict], strategy: str) -> list[dict]:
    if strategy == MEM_DESC_BF:
        # 显存降序；同显存回退到提交顺序，保证确定性
        return sorted(requests, key=lambda r: (-r["memory_need"], r["seq"], r["request_id"]))
    return sorted(requests, key=lambda r: (r["seq"], r["request_id"]))


def _pick_node(
    request: dict,
    caps: dict[str, dict],
    used_memory: dict[str, int],
    used_compute: dict[str, int],
    strategy: str,
) -> str | None:
    candidates = []
    for node_id in sorted(caps):
        cap = caps[node_id]
        if cap["health"] != "HEALTHY":
            continue
        if cap["group_id"] != request["group_id"]:
            continue
        if cap["free_memory"] - used_memory.get(node_id, 0) < request["memory_need"]:
            continue
        if cap["free_compute"] - used_compute.get(node_id, 0) < request["compute_need"]:
            continue
        candidates.append(node_id)
    if not candidates:
        return None
    if strategy == MEM_DESC_BF:
        # 最佳适应：装入后剩余显存最小，降低大块显存被浪费的概率
        return min(
            candidates,
            key=lambda n: (
                caps[n]["free_memory"] - used_memory.get(n, 0) - request["memory_need"],
                n,
            ),
        )
    return candidates[0]  # 首适应：node_id 字典序即确定顺序


def plan_placement(
    requests: list[dict],
    nodes: list[dict],
    models: dict[str, dict],
    *,
    strategy: str = GREEDY_FF,
    existing_batches: list[dict] | None = None,
    existing_batch_items: dict[str, list[str]] | None = None,
    allow_reorg: bool = True,
    base_used_memory_key: str = "base_used_memory",
) -> PlacementPlan:
    """对给定 ADMITTED 请求做确定性批组织规划。

    existing_batches 非空时表示对存量批做重组：把存量批中的请求与新请求
    合并后重新规划，配额预留量保持不变。
    """
    if strategy not in (GREEDY_FF, MEM_DESC_BF):
        raise BusinessError("E_SPEC_INVALID", f"未知装箱策略 {strategy}")

    caps = _node_caps(nodes, base_used_memory_key)
    used_memory: dict[str, int] = {node_id: 0 for node_id in caps}
    used_compute: dict[str, int] = {node_id: 0 for node_id in caps}
    open_batches: dict[tuple, PlannedBatch] = {}
    planned: list[PlannedBatch] = []
    fragmented: list[str] = []
    oversized: list[str] = []

    reorganized = bool(existing_batches)
    reorg_details: list[dict] = []
    if reorganized:
        reorg_details.append(
            {
                "existing_batches": [b["batch_id"] for b in existing_batches],
                "rebuilt_request_count": sum(
                    len(ids) for ids in (existing_batch_items or {}).values()
                ),
            }
        )

    # 单请求自身就违反延迟/样本上限：任何批都无法容纳，直接判定 oversized
    eligible = []
    for request in requests:
        if request["state"] not in (ADMITTED, "BATCHED"):
            continue
        model = models[request["model_id"]]
        solo_latency = estimate_latency(request["samples_per_batch"], model["cost_per_sample"])
        if (
            request["samples_per_batch"] > model["max_batch_samples"]
            or solo_latency > request["latency_limit_ms"]
        ):
            oversized.append(request["request_id"])
        else:
            eligible.append(request)

    ordered = _sort_requests(eligible, strategy)

    for request in ordered:
        model = models[request["model_id"]]
        node_id = _pick_node(request, caps, used_memory, used_compute, strategy)
        if node_id is None:
            fragmented.append(request["request_id"])
            continue
        key = (request["model_id"], node_id)
        batch = open_batches.get(key)

        fits = False
        if batch is not None:
            new_samples = batch.total_samples + request["samples_per_batch"]
            new_memory = batch.total_memory + request["memory_need"]
            new_compute = batch.total_compute + request["compute_need"]
            new_latency = estimate_latency(new_samples, model["cost_per_sample"])
            strictest_limit = min(batch.min_latency_limit, request["latency_limit_ms"])
            node_free_memory = caps[node_id]["free_memory"] - used_memory.get(node_id, 0)
            node_free_compute = caps[node_id]["free_compute"] - used_compute.get(node_id, 0)
            fits = (
                new_samples <= model["max_batch_samples"]
                and new_memory <= node_free_memory
                and new_compute <= node_free_compute
                and new_latency <= strictest_limit
            )
        if batch is None or not fits:
            batch = PlannedBatch(
                model_id=request["model_id"],
                node_id=node_id,
                group_id=request["group_id"],
                min_latency_limit=request["latency_limit_ms"],
            )
            open_batches[key] = batch
            planned.append(batch)

        batch.request_ids.append(request["request_id"])
        batch.total_samples += request["samples_per_batch"]
        batch.total_memory += request["memory_need"]
        batch.total_compute += request["compute_need"]
        batch.min_latency_limit = min(batch.min_latency_limit, request["latency_limit_ms"])
        batch.estimated_latency_ms = estimate_latency(
            batch.total_samples, model["cost_per_sample"]
        )
        used_memory[node_id] = used_memory.get(node_id, 0) + request["memory_need"]
        used_compute[node_id] = used_compute.get(node_id, 0) + request["compute_need"]

    # 二次确定性碎片重组：首轮碎片化的请求按 MEM_DESC_BF 再尝试一次，
    # 仅在结果更优时采用，保证重组可复现且不改变任何配额扣减。
    if allow_reorg and fragmented and not reorganized:
        retry = plan_placement(
            eligible,
            nodes,
            models,
            strategy=MEM_DESC_BF,
            existing_batches=None,
            allow_reorg=False,
            base_used_memory_key=base_used_memory_key,
        )
        if len(retry.fragmented_request_ids) < len(fragmented):
            retry.reorganized = True
            retry.reorg_details = [
                {
                    "trigger": "fragmentation",
                    "before_fragmented": fragmented,
                    "after_fragmented": retry.fragmented_request_ids,
                }
            ]
            return retry

    utilization = {}
    for node_id, cap in caps.items():
        if cap["free_memory"] <= 0:
            utilization[node_id] = 0.0
        else:
            utilization[node_id] = round(
                used_memory.get(node_id, 0) / cap["free_memory"], 6
            )

    notes = []
    if oversized:
        notes.append(
            f"{len(oversized)} 个请求单请求即超延迟/样本上限，返回 E_BATCH_TOO_LARGE"
        )
    return PlacementPlan(
        strategy=strategy,
        batches=planned,
        fragmented_request_ids=fragmented,
        oversized_request_ids=oversized,
        per_node_used_memory=used_memory,
        per_node_used_compute=used_compute,
        utilization=utilization,
        reorganized=reorganized,
        reorg_details=reorg_details,
        notes=notes,
    )


def assert_batch_latency(batch: PlannedBatch, model: dict) -> None:
    latency = estimate_latency(batch.total_samples, model["cost_per_sample"])
    if latency > batch.min_latency_limit:
        raise BusinessError(
            E_BATCH_TOO_LARGE,
            f"批延迟 {latency}ms 超过批内最严上限 {batch.min_latency_limit}ms",
            {
                "batch_requests": batch.request_ids,
                "estimated_latency_ms": latency,
                "latency_limit_ms": batch.min_latency_limit,
            },
        )
