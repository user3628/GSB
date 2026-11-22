from __future__ import annotations

import json
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parent.parent
SAMPLES = ROOT / "samples"
SEED = 20261123


def main() -> None:
    SAMPLES.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(SEED)
    functions = [
        {
            "function_id": "fn-orders",
            "runtime": "python3.11",
            "init_us": 120_000,
            "memory_bytes": 134_217_728,
            "per_instance_concurrency": 2,
            "function_concurrency_limit": 6,
            "reserved_quota": 2,
            "init_concurrency": 2,
            "prewarm_instances": 1,
            "queue_capacity": 4,
        },
        {
            "function_id": "fn-image",
            "runtime": "python3.11",
            "init_us": 200_000,
            "memory_bytes": 268_435_456,
            "per_instance_concurrency": 1,
            "function_concurrency_limit": 3,
            "reserved_quota": 0,
            "init_concurrency": 1,
            "prewarm_instances": 0,
            "queue_capacity": 2,
        },
        {
            "function_id": "fn-report",
            "runtime": "python3.12",
            "init_us": 60_000,
            "memory_bytes": 67_108_864,
            "per_instance_concurrency": 3,
            "function_concurrency_limit": 9,
            "reserved_quota": 3,
            "init_concurrency": 2,
            "prewarm_instances": 0,
            "queue_capacity": 6,
        },
        {
            "function_id": "fn-audit",
            "runtime": "python3.11",
            "init_us": 15_000,
            "memory_bytes": 33_554_432,
            "per_instance_concurrency": 1,
            "function_concurrency_limit": 2,
            "reserved_quota": 0,
            "init_concurrency": 1,
            "prewarm_instances": 0,
            "queue_capacity": 1,
        },
    ]
    ids = [item["function_id"] for item in functions]
    weights = np.array([0.35, 0.2, 0.3, 0.15])
    arrival = 0
    calls = []
    for index in range(24):
        arrival += int(rng.integers(5, 55))
        function_id = str(rng.choice(ids, p=weights))
        calls.append(
            {
                "sequence": index,
                "call_id": f"trace-{SEED}-{index:03d}",
                "arrival_ms": arrival,
                "function_id": function_id,
                "execution_us": int(rng.integers(8_000, 75_000)),
                "priority": int(rng.integers(0, 5)),
                "retry": bool(rng.random() < 0.15),
            }
        )
    calls.sort(key=lambda item: (item["arrival_ms"], item["sequence"]))
    for index, call in enumerate(calls):
        call["sequence"] = index
    function_doc = {"seed": SEED, "functions": functions}
    trace_doc = {"seed": SEED, "batch_id": f"sample-trace-{SEED}", "calls": calls}
    injections = {
        "seed": SEED,
        "scenarios": [
            {
                "kind": "INSTANCE_CREATE_TIMEOUT",
                "function_id": "fn-image",
                "params": {"execution_us": 0},
            },
            {
                "kind": "EXECUTION_INTERRUPT",
                "function_id": "fn-audit",
                "params": {"execution_us": 500_000},
            },
            {
                "kind": "DUPLICATE_CALL_ID",
                "function_id": "fn-audit",
                "params": {"execution_us": 10_000},
            },
            {
                "kind": "QUOTA_BURST",
                "function_id": "fn-report",
                "params": {"execution_us": 350_000},
            },
            {
                "kind": "TRACE_OUT_OF_ORDER",
                "function_id": "fn-audit",
                "params": {"arrival_ms": 500},
            },
        ],
    }
    (SAMPLES / "functions.json").write_text(
        json.dumps(function_doc, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    (SAMPLES / "trajectory.json").write_text(
        json.dumps(trace_doc, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    (SAMPLES / "injections.json").write_text(
        json.dumps(injections, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    main()

