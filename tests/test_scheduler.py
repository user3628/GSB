from __future__ import annotations


import pytest

from qoplib.errors import (
    E_COMPUTE_BUDGET,
    E_MEMORY_BUDGET,
    E_WORKER_QUOTA,
    ComputeBudgetError,
    MemoryBudgetError,
    WorkerQuotaError,
)
from qoplib.manifest import load_manifest
from qoplib.scheduler import enforce_runtime_admission, estimate_shard, plan_schedule


def test_demo_schedule_admits_all_four_shards(demo_data):
    manifest = load_manifest(demo_data / "manifest.yaml")
    decision = plan_schedule(manifest)
    assert len(decision.accepted) == 4
    assert decision.rejected == []
    assert decision.worker_count >= 1
    assert decision.total_accepted_compute <= \
        manifest.budget.total_element_budget


def test_cumulative_compute_never_exceeds_budget(demo_data):
    manifest = load_manifest(demo_data / "manifest_tight.yaml")
    decision = plan_schedule(manifest)
    cumulative = 0
    for accepted in decision.accepted:
        cumulative += accepted.compute_elements
    assert cumulative <= manifest.budget.total_element_budget
    codes = {item.reason_code for item in decision.rejected}
    assert E_COMPUTE_BUDGET in codes or E_WORKER_QUOTA in codes
    detail = decision.rejected[0].budget_detail
    assert "total_element_budget" in detail
    assert "worker_element_quota" in detail


def test_worker_quota_rejection(demo_data):
    manifest = load_manifest(demo_data / "manifest.yaml")
    object.__setattr__(manifest.budget, "worker_element_quota", 10)
    decision = plan_schedule(manifest)
    assert decision.accepted == []
    assert {item.reason_code for item in decision.rejected} == {E_WORKER_QUOTA}


def test_memory_budget_rejection(demo_data):
    manifest = load_manifest(demo_data / "manifest_tiny_memory.yaml")
    decision = plan_schedule(manifest)
    assert {item.reason_code for item in decision.rejected} == {
        E_MEMORY_BUDGET}
    detail = decision.rejected[0].budget_detail
    assert detail["working_set_bytes"] > detail["peak_memory_bytes"]


def test_runtime_admission_is_second_line_of_defence(demo_data):
    manifest = load_manifest(demo_data / "manifest.yaml")
    shard = manifest.shards[0]
    estimate = estimate_shard(shard)
    with pytest.raises(ComputeBudgetError) as exc:
        enforce_runtime_admission(
            estimate, manifest.budget,
            manifest.budget.total_element_budget)
    assert exc.value.code == E_COMPUTE_BUDGET

    object.__setattr__(manifest.budget, "worker_element_quota", 1)
    with pytest.raises(WorkerQuotaError) as exc:
        enforce_runtime_admission(estimate, manifest.budget, 0)
    assert exc.value.code == E_WORKER_QUOTA

    object.__setattr__(manifest.budget, "worker_element_quota", 10_000)
    object.__setattr__(manifest.budget, "peak_memory_bytes", 1)
    with pytest.raises(MemoryBudgetError) as exc:
        enforce_runtime_admission(estimate, manifest.budget, 0)
    assert exc.value.code == E_MEMORY_BUDGET


def test_lane_assignment_covers_every_accepted_shard(demo_data):
    manifest = load_manifest(demo_data / "manifest.yaml")
    decision = plan_schedule(manifest, max_workers=2)
    assert decision.worker_count <= 2
    assigned = {item.shard_id for item in decision.lane_assignments}
    assert assigned == {item.shard_id for item in decision.accepted}


def test_worker_quota_path_with_manifest_override(demo_data):
    manifest = load_manifest(demo_data / "manifest_tight.yaml")
    # Force a quota below every shard's compute cost.
    object.__setattr__(manifest.budget, "worker_element_quota", 100)
    decision = plan_schedule(manifest)
    assert decision.accepted == []
    assert all(item.reason_code == E_WORKER_QUOTA
               for item in decision.rejected)
    assert all(item.budget_detail["compute_elements"] >
               item.budget_detail["worker_element_quota"]
               for item in decision.rejected)
