from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

from tests.conftest import spec


RUNNING = {"ADMITTED", "CREATING", "EXECUTING"}


def _snapshot_violations(snapshot):
    violations = []
    instances = {item["instance_id"]: item for item in snapshot["instances"]}
    active = [call for call in snapshot["calls"] if call["status"] in RUNNING]
    occupied_by_instance = {}
    for call in active:
        instance = instances.get(call["instance_id"])
        if instance is None or instance["state"] == "TERMINATED":
            violations.append(("active_call_without_instance", call["call_id"]))
        else:
            occupied_by_instance[instance["instance_id"]] = (
                occupied_by_instance.get(instance["instance_id"], 0) + 1
            )
    for instance in snapshot["instances"]:
        if instance["state"] == "TERMINATED":
            if instance["occupied_slots"] != 0:
                violations.append(("terminated_occupied", instance["instance_id"]))
            continue
        expected = occupied_by_instance.get(instance["instance_id"], 0)
        if instance["occupied_slots"] != expected:
            violations.append(
                (
                    "slot_mismatch",
                    instance["instance_id"],
                    instance["occupied_slots"],
                    expected,
                )
            )
    quota = {item["function_id"]: item for item in snapshot["quota"]}
    expected_on_demand = {}
    expected_reserved = {}
    for call in active:
        if call["quota_bucket"] == "ON_DEMAND":
            expected_on_demand[call["function_id"]] = (
                expected_on_demand.get(call["function_id"], 0) + 1
            )
        elif call["quota_bucket"] == "RESERVED":
            expected_reserved[call["function_id"]] = (
                expected_reserved.get(call["function_id"], 0) + 1
            )
    for function_id, row in quota.items():
        if row["on_demand_used"] != expected_on_demand.get(function_id, 0):
            violations.append(("quota_on_demand_mismatch", function_id))
        if row["reserved_used"] != expected_reserved.get(function_id, 0):
            violations.append(("quota_reserved_mismatch", function_id))
    return violations


def test_concurrent_writers_and_readers_see_only_complete_commits(engine_factory):
    engine = engine_factory()
    engine.load_specs(
        {
            "functions": [
                spec(
                    "f",
                    function_concurrency_limit=4,
                    per_instance_concurrency=2,
                    queue_capacity=20,
                    init_us=8_000,
                )
            ]
        }
    )
    stop = threading.Event()
    reader_errors: list[object] = []

    def reader() -> None:
        while not stop.is_set():
            try:
                snapshot = engine.snapshot("f")
                violations = _snapshot_violations(snapshot)
                if violations:
                    reader_errors.append(violations)
            except Exception as exc:
                reader_errors.append(exc)
            time.sleep(0.001)

    readers = [threading.Thread(target=reader, name=f"reader-{i}") for i in range(4)]
    for thread in readers:
        thread.start()
    try:
        with ThreadPoolExecutor(max_workers=12) as pool:
            futures = [
                pool.submit(
                    engine.invoke,
                    {
                        "call_id": f"parallel-{index}",
                        "function_id": "f",
                        "execution_us": 40_000,
                        "priority": index % 3,
                    },
                )
                for index in range(16)
            ]
            results = [future.result() for future in futures]
    finally:
        stop.set()
    for thread in readers:
        thread.join(timeout=3)
    assert all(result["ok"] for result in results)
    assert reader_errors == []
    assert engine.audit_invariants()["ok"] is True
    quota = engine.quota("f")[0]
    assert quota["on_demand_used"] == 0
    assert quota["reserved_used"] == 0
    assert all(instance["occupied_slots"] == 0 for instance in engine.list_instances("f"))

