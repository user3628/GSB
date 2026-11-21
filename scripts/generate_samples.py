"""Generate the committed deterministic sample corpus.

Seed: 20261121.  Running this script rewrites samples/*.json with byte-stable
content on the same Python/NumPy integer RNG semantics.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np


ROOT = Path(__file__).resolve().parent.parent
SAMPLES = ROOT / "samples"
SEED = 20261121


def policy_v1() -> dict:
    return {
        "version": 1,
        "generated_at": "2026-11-21T09:00:00+08:00",
        "runtime": {
            "max_parallel_scales": 3,
            "round_time_budget_ms": 25,
            "placement_cost_ms": 25,
            "wait_queue_limit": 4,
        },
        "services": [
            {
                "id": "chat",
                "model": "llama-3.1-8b",
                "current_replicas": 1,
                "min_replicas": 1,
                "max_replicas": 6,
                "replica_memory": "2Gi",
                "replica_compute": "800m",
                "target_concurrency": 8,
                "latency_budget_ms": 300,
                "priority": 30,
                "max_replicas_per_fault_domain": 1,
            },
            {
                "id": "rerank",
                "model": "bge-reranker-v2",
                "current_replicas": 1,
                "min_replicas": 1,
                "max_replicas": 4,
                "replica_memory": "1Gi",
                "replica_compute": "500m",
                "target_concurrency": 4,
                "latency_budget_ms": 120,
                "priority": 20,
                "max_replicas_per_fault_domain": 1,
            },
            {
                "id": "embed",
                "model": "bge-m3",
                "current_replicas": 1,
                "min_replicas": 1,
                "max_replicas": 5,
                "replica_memory": "512Mi",
                "replica_compute": "250m",
                "target_concurrency": 16,
                "latency_budget_ms": 80,
                "priority": 10,
                "max_replicas_per_fault_domain": 1,
            },
        ],
        "nodes": [
            {
                "id": "gpu-a1",
                "fault_domain": "az-a",
                "memory": "8Gi",
                "compute_share": "4",
                "healthy": True,
                "reserved_memory": "512Mi",
                "reserved_compute": "200m",
            },
            {
                "id": "gpu-a2",
                "fault_domain": "az-a",
                "memory": "8Gi",
                "compute_share": "4",
                "healthy": True,
                "reserved_memory": "256Mi",
                "reserved_compute": "100m",
            },
            {
                "id": "gpu-b1",
                "fault_domain": "az-b",
                "memory": "6Gi",
                "compute_share": "3",
                "healthy": True,
                "reserved_memory": "256Mi",
                "reserved_compute": "100m",
            },
            {
                "id": "gpu-c1",
                "fault_domain": "az-c",
                "memory": "6Gi",
                "compute_share": "3",
                "healthy": True,
                "reserved_memory": "0Mi",
                "reserved_compute": "0m",
            },
        ],
        "initial_placements": [
            {"service_id": "chat", "node_id": "gpu-a1", "count": 1},
            {"service_id": "rerank", "node_id": "gpu-a2", "count": 1},
            {"service_id": "embed", "node_id": "gpu-a1", "count": 1},
        ],
    }


def observation_batch(batch_id: str, sampled_at: str, values: dict, node_health=None) -> dict:
    payload = {
        "batch_id": batch_id,
        "sampled_at": sampled_at,
        "window_ms": 60000,
        "services": [
            {
                "service_id": service_id,
                "sampled_at": sampled_at,
                "arrivals": values[service_id][0],
                "queue_duration_ms": values[service_id][1],
                "p99_latency_ms": values[service_id][2],
            }
            for service_id in ("chat", "rerank", "embed")
        ],
    }
    if node_health:
        payload["node_health"] = node_health
    return payload


def main() -> None:
    SAMPLES.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(SEED)

    def between(low: int, high: int) -> int:
        return int(rng.integers(low, high + 1))

    batches = {
        "observations_01.json": observation_batch(
            "obs-001",
            "2026-11-21T09:05:00+08:00",
            {
                "chat": (between(540, 580), between(35, 45), between(390, 410)),
                "rerank": (between(280, 320), between(45, 55), between(150, 165)),
                "embed": (between(1700, 1900), between(15, 25), between(95, 105)),
            },
        ),
        "observations_02.json": observation_batch(
            "obs-002",
            "2026-11-21T09:10:00+08:00",
            {
                "chat": (between(540, 580), between(35, 45), between(390, 410)),
                "rerank": (between(280, 320), between(45, 55), between(150, 165)),
                "embed": (between(1700, 1900), between(15, 25), between(95, 105)),
            },
        ),
        "observations_03.json": observation_batch(
            "obs-003",
            "2026-11-21T09:15:00+08:00",
            {
                "chat": (between(2300, 2500), between(40, 52), between(410, 450)),
                "rerank": (between(80, 110), between(12, 20), between(90, 110)),
                "embed": (between(1750, 1850), between(15, 24), between(90, 100)),
            },
        ),
        "observations_04_failover.json": observation_batch(
            "obs-004",
            "2026-11-21T09:20:00+08:00",
            {
                "chat": (between(2300, 2500), between(40, 52), between(410, 450)),
                "rerank": (between(80, 110), between(12, 20), between(90, 110)),
                "embed": (between(1750, 1850), between(15, 24), between(90, 100)),
            },
            node_health={"gpu-b1": False},
        ),
        "observations_05_queue_full.json": observation_batch(
            "obs-005",
            "2026-11-21T09:25:00+08:00",
            {
                "chat": (2500, 48, 440),
                "rerank": (400, 70, 180),
                "embed": (2600, 40, 130),
            },
        ),
    }

    invalid_policy = policy_v1()
    invalid_policy["version"] = 999
    invalid_policy["services"][0]["replica_memory"] = "-2Gi"
    invalid_policy["generated_at"] = "2026-11-21T09:30:00+08:00"

    policy_v2 = policy_v1()
    policy_v2["version"] = 2
    policy_v2["generated_at"] = "2026-11-21T09:40:00+08:00"
    policy_v2["runtime"]["max_parallel_scales"] = 2
    policy_v2["runtime"]["round_time_budget_ms"] = 25
    policy_v2["services"][0]["current_replicas"] = 3
    policy_v2["services"][1]["current_replicas"] = 3
    policy_v2["services"][2]["current_replicas"] = 3
    policy_v2.pop("initial_placements", None)

    files = {
        "policy_v1.json": policy_v1(),
        "policy_v2_budget.json": policy_v2,
        "policy_invalid_unit.json": invalid_policy,
        "round_request_01.json": {
            "round_id": "round-001",
            "policy_version": 1,
            "observations": batches["observations_01.json"],
        },
        **batches,
    }
    for name, content in files.items():
        text = json.dumps(content, ensure_ascii=False, indent=2) + "\n"
        (SAMPLES / name).write_text(text, encoding="utf-8", newline="\n")
    print(f"wrote {len(files)} files with seed {SEED}")


if __name__ == "__main__":
    main()
