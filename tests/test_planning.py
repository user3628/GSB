from __future__ import annotations

from pathlib import Path

from gitops_convergence.engine import Engine, EngineConfig

from conftest import app, increment, snapshot


def batch_signature(plan: dict) -> list[tuple[int, tuple[str, ...]]]:
    return [
        (batch["batch_index"], tuple(batch["app_ids"])) for batch in plan["batches"]
    ]


def test_full_and_incremental_paths_make_identical_decisions(tmp_path: Path) -> None:
    engine = Engine(
        tmp_path / "plan.db",
        EngineConfig(switch_threshold=1, max_batch_replicas=3),
    )
    engine.submit_snapshot(
        snapshot(
            [
                app("a", desired="v2", effective="v1", replicas=1),
                app("b", desired="v2", effective="v1", replicas=1, dependencies=["a"]),
                app("c", desired="v2", effective="v1", replicas=1, dependencies=["a"]),
                app("d", desired="v2", effective="v1", replicas=1, dependencies=["b", "c"]),
                app("e", desired="v2", effective="v1", replicas=1),
            ],
            clusters=[{"cluster_id": "c1", "replica_quota": 100}],
        )
    )
    exact = engine.preview_plan(1, {"mode": "full"})
    local = engine.preview_plan(1, {"mode": "incremental"})
    assert exact["plan_hash"] == local["plan_hash"]
    assert batch_signature(exact) == batch_signature(local)
    assert exact["recompute"]["path"] == "EXACT_FULL"
    assert local["recompute"]["path"] == "INCREMENTAL"

    batch_of = {
        app_id: batch["batch_index"]
        for batch in exact["batches"]
        for app_id in batch["app_ids"]
    }
    assert batch_of["a"] < batch_of["b"] < batch_of["d"]
    assert batch_of["a"] < batch_of["c"] < batch_of["d"]
    assert engine.trigger_convergence(1, {"mode": "full"})["final_state_counts"] == {
        "CONVERGED": 5
    }


def test_quota_splits_batches_without_dropping_apps(tmp_path: Path) -> None:
    engine = Engine(
        tmp_path / "quota.db",
        EngineConfig(switch_threshold=100, max_batch_replicas=2),
    )
    apps = [
        app(f"app-{index:02d}", desired="v2", effective="v1", replicas=1)
        for index in range(11)
    ]
    apps.append(app("oversized", desired="v2", effective="v1", replicas=9))
    engine.submit_snapshot(
        snapshot(
            apps,
            clusters=[{"cluster_id": "c1", "replica_quota": 3}],
        )
    )
    plan = engine.preview_plan(1, {"mode": "full"})
    planned = [app_id for batch in plan["batches"] for app_id in batch["app_ids"]]
    assert sorted(planned) == sorted(item["app_id"] for item in apps)
    assert len(plan["batches"]) > 6
    assert all(
        batch["quota_exceeded"] or batch["cluster_replica_usage"]["c1"] <= 3
        for batch in plan["batches"]
    )
    oversized = next(
        batch for batch in plan["batches"] if "oversized" in batch["app_ids"]
    )
    assert oversized["quota_exceeded"] is True


def test_auto_switches_to_incremental_above_threshold(tmp_path: Path) -> None:
    engine = Engine(
        tmp_path / "threshold.db",
        EngineConfig(switch_threshold=2, max_batch_replicas=100),
    )
    engine.submit_snapshot(
        snapshot([app("a"), app("b"), app("c", dependencies=["b"])])
    )
    engine.submit_increment(
        1,
        increment(
            version=2,
            base_version=1,
            changes=[{"app_id": "c", "desired_version": "v2"}],
        ),
    )
    auto = engine.preview_plan(2, {"mode": "auto"})
    full = engine.preview_plan(2, {"mode": "full"})
    local = engine.preview_plan(2, {"mode": "incremental"})
    assert auto["recompute"]["path"] == "INCREMENTAL"
    assert auto["recompute"]["affected_app_count"] == 1
    assert auto["recompute"]["switch_threshold"] == 2
    assert "threshold 2" in auto["recompute"]["reason"]
    assert auto["plan_hash"] == full["plan_hash"] == local["plan_hash"]
