from __future__ import annotations

from schedsim.engine import SchedulingEngine
from schedsim.errors import E_SCHED_QUEUE_FULL, SchedError
from tests.support import make_node, make_payload, make_pod, run_to_completion


def test_node_selector_taint_and_every_node_rejection_are_recorded(tmp_path) -> None:
    engine = SchedulingEngine(tmp_path / "sched.db")
    payload = make_payload(
        [
            make_node(
                "tainted-ssd",
                labels={"disktype": "ssd"},
                taints=[{"key": "maintenance", "value": "true", "effect": "NoSchedule"}],
            ),
            make_node("plain-hdd", labels={"disktype": "hdd"}),
        ],
        [make_pod("p1", node_selector={"disktype": "ssd"})],
    )
    engine.submit_batch(payload)
    round_result = engine.run_round("batch-1")
    assert round_result["placements"] == []
    assert round_result["batch_status"] == "COMPLETED"
    rejections = engine.get_rejections("batch-1")["rejections"]
    by_node = {(row["node_name"], row["constraint_type"]) for row in rejections}
    assert ("tainted-ssd", "taint_no_schedule") in by_node
    assert ("plain-hdd", "node_selector") in by_node
    assert all(row["constraint_text"] for row in rejections)


def test_taint_toleration_allows_placement(tmp_path) -> None:
    engine = SchedulingEngine(tmp_path / "sched.db")
    payload = make_payload(
        [
            make_node(
                "n1",
                taints=[{"key": "dedicated", "value": "gpu", "effect": "NoSchedule"}],
            )
        ],
        [
            make_pod(
                "p1",
                tolerations=[
                    {"key": "dedicated", "operator": "Equal", "value": "gpu", "effect": "NoSchedule"}
                ],
            )
        ],
    )
    engine.submit_batch(payload)
    result = engine.run_round("batch-1")
    assert [(item["pod_name"], item["node_name"]) for item in result["placements"]] == [("p1", "n1")]


def test_resource_decrement_is_visible_per_placement_and_has_snapshot(tmp_path) -> None:
    engine = SchedulingEngine(tmp_path / "sched.db")
    payload = make_payload(
        [make_node("n1", cpu="1000m", memory="1Gi")],
        [
            make_pod("first", cpu="400m", memory="512Mi", priority=2, arrival_seq=1),
            make_pod("second", cpu="700m", memory="256Mi", priority=1, arrival_seq=2),
        ],
    )
    engine.submit_batch(payload)
    engine.run_round("batch-1")
    placements = engine.get_placements("batch-1")["placements"]
    assert len(placements) == 1
    assert placements[0]["pod_name"] == "first"
    assert placements[0]["remaining_before"] == {
        "cpu_millicores": 1000,
        "memory_bytes": 1024**3,
    }
    assert placements[0]["remaining_after"] == {
        "cpu_millicores": 600,
        "memory_bytes": 512 * 1024**2,
    }
    rejections = engine.get_rejections("batch-1")["rejections"]
    cpu_rejection = [
        row
        for row in rejections
        if row["pod_name"] == "second" and row["constraint_type"] == "insufficient_cpu"
    ][0]
    assert cpu_rejection["remaining_snapshot"] == {
        "cpu_millicores": 600,
        "memory_bytes": 512 * 1024**2,
    }


def test_hard_pod_anti_affinity_and_required_pod_affinity(tmp_path) -> None:
    engine = SchedulingEngine(tmp_path / "sched.db")
    anti_affinity = {
        "podAntiAffinity": {
            "requiredDuringSchedulingIgnoredDuringExecution": [
                {
                    "labelSelector": {"matchLabels": {"app": "web"}},
                    "topologyKey": "kubernetes.io/hostname",
                }
            ]
        }
    }
    required_affinity = {
        "podAffinity": {
            "requiredDuringSchedulingIgnoredDuringExecution": [
                {
                    "labelSelector": {"matchLabels": {"app": "cache"}},
                    "topologyKey": "topology.kubernetes.io/zone",
                }
            ]
        }
    }
    payload = make_payload(
        [make_node("n1", zone="z1"), make_node("n2", zone="z1")],
        [
            make_pod("web", labels={"app": "web"}, priority=5, arrival_seq=1),
            make_pod("cache", labels={"app": "cache"}, priority=4, arrival_seq=2),
            make_pod(
                "web-copy",
                labels={"app": "web"},
                affinity=anti_affinity,
                priority=3,
                arrival_seq=3,
            ),
            make_pod(
                "client",
                labels={"app": "client"},
                affinity=required_affinity,
                priority=2,
                arrival_seq=4,
            ),
        ],
        budget={"per_node_round_limit": 1},
    )
    engine.submit_batch(payload)
    run_to_completion(engine, "batch-1")
    placements = {item["pod_name"]: item["node_name"] for item in engine.get_placements("batch-1")["placements"]}
    assert placements["web"] == "n1"
    assert placements["web-copy"] == "n2"
    assert placements["client"] in {"n1", "n2"}
    client_placement = [
        item
        for item in engine.get_placements("batch-1")["placements"]
        if item["pod_name"] == "client"
    ][0]
    satisfaction = client_placement["score_breakdown"]["affinity_satisfaction"]
    assert satisfaction["required_pod_affinity"]["satisfied"] is True
    assert satisfaction["required_pod_affinity"]["terms"][0]["matched_pods"] == ["cache"]
    rejection_types = {
        row["constraint_type"] for row in engine.get_rejections("batch-1")["rejections"]
    }
    assert "pod_anti_affinity_required" in rejection_types


def test_required_pod_affinity_retries_after_lower_priority_dependency_arrives(tmp_path) -> None:
    engine = SchedulingEngine(tmp_path / "sched.db")
    required_affinity = {
        "podAffinity": {
            "requiredDuringSchedulingIgnoredDuringExecution": [
                {
                    "labelSelector": {"matchLabels": {"app": "cache"}},
                    "topologyKey": "kubernetes.io/hostname",
                }
            ]
        }
    }
    payload = make_payload(
        [make_node("n1")],
        [
            make_pod(
                "client",
                labels={"app": "client"},
                affinity=required_affinity,
                priority=10,
                arrival_seq=1,
            ),
            make_pod("cache", labels={"app": "cache"}, priority=1, arrival_seq=2),
        ],
        budget={"concurrency_limit": 2},
    )
    engine.submit_batch(payload)
    first = engine.run_round("batch-1")
    assert [(item["pod_name"], item["node_name"]) for item in first["placements"]] == [
        ("cache", "n1")
    ]
    assert first["unschedulable_pods"][0]["reason"] == "POD_AFFINITY_PENDING"
    second = engine.run_round("batch-1")
    assert [(item["pod_name"], item["node_name"]) for item in second["placements"]] == [
        ("client", "n1")
    ]
    assert engine.get_batch_summary("batch-1")["status"] == "COMPLETED"


def test_deterministic_tolerance_and_node_name_tie_break(tmp_path) -> None:
    engine = SchedulingEngine(tmp_path / "sched.db")
    for index in range(2):
        payload = make_payload(
            [make_node("zz-node"), make_node("aa-node")],
            [make_pod("p1")],
            batch_id=f"det-{index}",
            budget={"score_tolerance": 10},
        )
        engine.submit_batch(payload)
        first = engine.run_round(f"det-{index}")["placements"][0]
        assert first["node_name"] == "aa-node"
        assert first["score_breakdown"]["selection"]["within_tolerance"] == ["aa-node", "zz-node"]


def test_concurrency_time_and_node_round_budgets(tmp_path) -> None:
    engine = SchedulingEngine(tmp_path / "sched.db")
    payload = make_payload(
        [make_node("n1", cpu="4000m", memory="4Gi")],
        [
            make_pod("p1", priority=3, arrival_seq=1),
            make_pod("p2", priority=2, arrival_seq=2),
            make_pod("p3", priority=1, arrival_seq=3),
        ],
        budget={"concurrency_limit": 1, "time_budget_ms": 100, "per_node_round_limit": 2},
    )
    engine.submit_batch(payload)
    first = engine.run_round("batch-1")
    assert len(first["placements"]) == 1
    assert first["budget_usage"]["concurrency_used"] == 1
    assert {row["reason"] for row in first["skipped_pods"]} == {"CONCURRENCY_LIMIT"}
    second = engine.run_round("batch-1")
    assert len(second["placements"]) == 1
    third = engine.run_round("batch-1")
    assert len(third["placements"]) == 1
    assert engine.get_batch_summary("batch-1")["status"] == "COMPLETED"


def test_time_budget_zero_skips_without_violating_budget(tmp_path) -> None:
    engine = SchedulingEngine(tmp_path / "sched.db")
    payload = make_payload(
        [make_node("n1")],
        [make_pod("p1"), make_pod("p2")],
        budget={"time_budget_ms": 0, "concurrency_limit": 10},
    )
    engine.submit_batch(payload)
    result = engine.run_round("batch-1")
    assert result["placements"] == []
    assert result["budget_usage"]["spent_ms"] == 0
    assert result["budget_usage"]["time_budget_exhausted"] is True
    assert {row["reason"] for row in result["skipped_pods"]} == {
        "ROUND_TIME_BUDGET_EXHAUSTED"
    }


def test_per_node_limit_defers_to_next_round(tmp_path) -> None:
    engine = SchedulingEngine(tmp_path / "sched.db")
    payload = make_payload(
        [make_node("n1")],
        [
            make_pod("p1", priority=2, arrival_seq=1),
            make_pod("p2", priority=1, arrival_seq=2),
        ],
        budget={"per_node_round_limit": 1},
    )
    engine.submit_batch(payload)
    first = engine.run_round("batch-1")
    assert len(first["placements"]) == 1
    assert first["unschedulable_pods"][0]["reason"] == "NODE_PLACEMENT_CAP_RETRY"
    assert engine.get_batch_summary("batch-1")["status"] == "INCOMPLETE"
    second = engine.run_round("batch-1")
    assert len(second["placements"]) == 1
    assert engine.get_batch_summary("batch-1")["status"] == "COMPLETED"


def test_bounded_queue_returns_business_error_without_persisting(tmp_path) -> None:
    engine = SchedulingEngine(tmp_path / "sched.db")
    payload = make_payload(
        [make_node("n1")],
        [make_pod("p1"), make_pod("p2")],
        budget={"concurrency_limit": 1, "queue_capacity": 0},
    )
    try:
        engine.submit_batch(payload)
        raise AssertionError("expected E_SCHED_QUEUE_FULL")
    except SchedError as exc:
        assert exc.code == E_SCHED_QUEUE_FULL
        assert exc.context["queued_count"] == 1
    assert not engine.storage.batch_exists("batch-1")

