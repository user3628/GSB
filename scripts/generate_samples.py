from __future__ import annotations

import json
from pathlib import Path

import numpy as np


SEED = 20261122
FUTURE_DUE_MS = 4_102_444_800_000


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    out_dir = root / "samples"
    out_dir.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(SEED)
    modes = ["success", "fail", "flaky", "slow", "success", "fail"]
    tasks = []
    for index in range(12):
        mode = modes[index % len(modes)]
        task = {
            "task_id": f"det-{index + 1:02d}-{mode}",
            "due_at_ms": FUTURE_DUE_MS if index == 11 else 0,
            "priority": int(rng.integers(0, 10)),
            "remaining_retries": int(rng.integers(0, 3)),
            "resource_requirement": int(rng.integers(1, 7)),
            "payload": {"mode": mode, "sample_index": index},
        }
        if mode == "slow":
            task["payload"]["sleep_seconds"] = 0.08
        tasks.append(task)

    (out_dir / "tasks_deterministic.json").write_text(
        json.dumps({"queue": "demo", "tasks": tasks}, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    invalid_tasks = {
        "queue": "demo",
        "tasks": [
            {
                "task_id": "invalid-due",
                "due_at_ms": -1,
                "priority": 3,
                "remaining_retries": 1,
                "resource_requirement": 1,
            },
            {
                "task_id": "invalid-priority",
                "due_at_ms": 0,
                "priority": 10,
                "remaining_retries": 1,
                "resource_requirement": 1,
            },
            {
                "task_id": "invalid-retries",
                "due_at_ms": 0,
                "priority": 2,
                "remaining_retries": -1,
                "resource_requirement": 1,
            },
            {
                "task_id": "invalid-resource",
                "due_at_ms": 0,
                "priority": 2,
                "remaining_retries": 1,
                "resource_requirement": 0,
            },
        ],
    }
    (out_dir / "tasks_invalid.json").write_text(
        json.dumps(invalid_tasks, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    (out_dir / "tasks_timeout.json").write_text(
        json.dumps(
            {
                "queue": "timeout-demo",
                "tasks": [
                    {
                        "task_id": "timeout-retry-once",
                        "due_at_ms": 0,
                        "priority": 0,
                        "remaining_retries": 1,
                        "resource_requirement": 1,
                        "payload": {"mode": "hang", "hang_seconds": 10},
                    }
                ],
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    scenario = {
        "seed": SEED,
        "queue": "demo",
        "rounds": [
            {
                "round_id": "demo-r1",
                "max_concurrency": 3,
                "time_budget_ms": 700,
                "max_task_resource": 6,
                "waiting_queue_capacity": 20,
                "dispatch_timeout_ms": 120,
            },
            {
                "round_id": "demo-r2-budget-zero",
                "max_concurrency": 3,
                "time_budget_ms": 0,
                "max_task_resource": 6,
                "waiting_queue_capacity": 20,
                "dispatch_timeout_ms": 120,
            },
        ],
    }
    (out_dir / "scenario.json").write_text(
        json.dumps(scenario, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(out_dir / "tasks_deterministic.json")
    print(out_dir / "tasks_invalid.json")
    print(out_dir / "tasks_timeout.json")
    print(out_dir / "scenario.json")


if __name__ == "__main__":
    main()

