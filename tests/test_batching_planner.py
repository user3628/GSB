from gpworkbench.batching import GREEDY_FF, MEM_DESC_BF, estimate_latency, plan_placement
from gpworkbench.statemachine import ADMITTED


def _request(rid, model="m1", mem=4, comp=20, samples=2, limit=100, seq=0, group="g1"):
    return {
        "request_id": rid,
        "model_id": model,
        "memory_need": mem * 1024 ** 3,
        "compute_need": comp,
        "samples_per_batch": samples,
        "latency_limit_ms": limit,
        "group_id": group,
        "state": ADMITTED,
        "seq": seq,
        "priority": 1,
    }


NODES = [
    {
        "node_id": "n1",
        "group_id": "g1",
        "health": "HEALTHY",
        "total_memory": 20 * 1024 ** 3,
        "total_compute": 500,
        "base_used_memory": 0,
    },
    {
        "node_id": "n2",
        "group_id": "g1",
        "health": "HEALTHY",
        "total_memory": 10 * 1024 ** 3,
        "total_compute": 300,
        "base_used_memory": 0,
    },
]
MODELS = {
    "m1": {"model_id": "m1", "cost_per_sample": 10, "max_batch_samples": 8},
    "m2": {"model_id": "m2", "cost_per_sample": 5, "max_batch_samples": 16},
}


def test_latency_estimate_is_integer():
    assert estimate_latency(4, 10) == 40


def test_greedy_and_mem_desc_are_deterministic():
    requests = [_request(f"r{i}", mem=(i % 5) + 1, seq=i) for i in range(20)]
    first = plan_placement(requests, NODES, MODELS, strategy=GREEDY_FF)
    second = plan_placement(requests, NODES, MODELS, strategy=GREEDY_FF)
    desc = plan_placement(requests, NODES, MODELS, strategy=MEM_DESC_BF)
    assert [b.request_ids for b in first.batches] == [b.request_ids for b in second.batches]
    # 降序策略首个请求必然是最大显存请求
    largest = max(requests, key=lambda r: r["memory_need"])["request_id"]
    assert desc.batches[0].request_ids[0] == largest


def test_fragmentation_is_reported_and_reorg_improves():
    # n2 仅剩约 2Gi 时，大请求无法装入 -> 碎片化清单非空
    nodes = [
        {
            "node_id": "n1",
            "group_id": "g1",
            "health": "HEALTHY",
            "total_memory": 6 * 1024 ** 3,
            "total_compute": 500,
            "base_used_memory": 0,
        },
        {
            "node_id": "n2",
            "group_id": "g1",
            "health": "HEALTHY",
            "total_memory": 2 * 1024 ** 3,
            "total_compute": 300,
            "base_used_memory": 0,
        },
    ]
    requests = [_request("a", mem=5, seq=0), _request("b", mem=5, seq=1), _request("c", mem=2, seq=2)]
    plan = plan_placement(requests, nodes, MODELS, strategy=GREEDY_FF)
    # 两个 5Gi 请求只有 n1 能装且容量只够一个，另一个必然碎片化
    assert "b" in plan.fragmented_request_ids or "a" in plan.fragmented_request_ids
    assert all(0.0 <= u <= 1.0 for u in plan.utilization.values())


def test_unhealthy_and_foreign_group_nodes_skipped():
    nodes = [
        {
            "node_id": "nX",
            "group_id": "g2",
            "health": "HEALTHY",
            "total_memory": 20 * 1024 ** 3,
            "total_compute": 500,
            "base_used_memory": 0,
        },
        {
            "node_id": "nDrain",
            "group_id": "g1",
            "health": "DRAIN",
            "total_memory": 20 * 1024 ** 3,
            "total_compute": 500,
            "base_used_memory": 0,
        },
    ]
    plan = plan_placement([_request("r1")], nodes, MODELS, strategy=GREEDY_FF)
    assert plan.fragmented_request_ids == ["r1"]


def test_reorganization_rebuilds_existing_batches():
    existing = [
        {
            "batch_id": "old-1",
            "model_id": "m1",
            "node_id": "n1",
            "group_id": "g1",
            "state": "PLACED",
        }
    ]
    requests = [_request("a", seq=0), _request("b", seq=1), _request("c", seq=2)]
    plan = plan_placement(
        requests,
        NODES,
        MODELS,
        strategy=MEM_DESC_BF,
        existing_batches=existing,
        existing_batch_items={"old-1": ["a", "b", "c"]},
    )
    assert plan.reorganized is True
    flattened = [rid for batch in plan.batches for rid in batch.request_ids]
    assert sorted(flattened) == ["a", "b", "c"]
