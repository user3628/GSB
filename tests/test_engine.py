from __future__ import annotations

import json
import re
from typing import Any

from app.engine import find_node, find_pod_trace, schedule_batch
from app.matcher import evaluate_requirement
from app.models import BatchSpecInput, SelectorRequirement, normalize_spec
from app.scoring import format_fraction
from tests.conftest import load_raw, run_sample


def _checks(step: dict[str, Any], node: str) -> dict[str, dict[str, Any]]:
    trace = next(trace for trace in step["filterTrace"] if trace["node"] == node)
    return {check["plugin"]: check for check in trace["checks"]}


def test_original_sample_placement_and_rejections(original_result: dict[str, Any]) -> None:
    assert original_result["status"] == "completed"
    assert original_result["summary"] == {
        "nodes": 3,
        "pendingPods": 8,
        "placed": 4,
        "rejected": 4,
        "preempted": 0,
    }
    assert {(item["pod"], item["node"]) for item in original_result["placed"]} == {
        ("web-a", "node-a"),
        ("cache-a", "node-b"),
        ("data-a", "node-a"),
        ("maintenance-a", "node-c"),
    }
    rejection_codes = {item["pod"]: item["reason"]["code"] for item in original_result["rejected"]}
    assert rejection_codes == {
        "web-b": "E_POD_ANTI_AFFINITY_VIOLATION",
        "accelerator-job": "E_UNSATISFIED_AFFINITY",
        "maintenance-b": "E_TAINT_NOT_TOLERATED",
        "batch-a": "E_INSUFFICIENT_RESOURCES",
    }
    web_a = find_pod_trace(original_result, "web-a")
    assert web_a is not None
    assert web_a["podResources"] == {
        "requests": {"cpuMillicores": 1000, "memoryBytes": 1024**3},
        "limits": {"cpuMillicores": 2000, "memoryBytes": 2 * 1024**3},
    }


def test_modified_label_changes_only_accelerator_placement(
    original_result: dict[str, Any], modified_result: dict[str, Any]
) -> None:
    original_placed = {item["pod"]: item["node"] for item in original_result["placed"]}
    modified_placed = {item["pod"]: item["node"] for item in modified_result["placed"]}
    assert "accelerator-job" not in original_placed
    assert modified_placed["accelerator-job"] == "node-b"
    changed = {
        pod
        for pod in set(original_placed) | set(modified_placed)
        if original_placed.get(pod) != modified_placed.get(pod)
    }
    assert changed == {"accelerator-job"}
    assert modified_result["nodes"][1]["remaining"]["cpuMillicores"] == 1000
    assert original_result["nodes"][1]["remaining"]["cpuMillicores"] == 1800


def test_node_selector_and_node_affinity_are_both_required(original_result: dict[str, Any]) -> None:
    step = find_pod_trace(original_result, "accelerator-job")
    assert step is not None
    node_b = next(trace for trace in step["filterTrace"] if trace["node"] == "node-b")
    selector = next(check for check in node_b["checks"] if check["plugin"] == "NodeSelector")
    assert selector["passed"] is False
    assert selector["code"] == "E_UNSATISFIED_AFFINITY"
    violation = selector["details"]["firstViolationExpression"]
    assert violation == {
        "source": "nodeSelector",
        "key": "accelerator",
        "operator": "Equal",
        "expectedValue": "nvidia",
        "labelValue": "none",
    }


def test_hard_pod_anti_affinity_is_recomputed_after_first_placement(original_result: dict[str, Any]) -> None:
    step = find_pod_trace(original_result, "web-b")
    assert step is not None
    assert step["decision"] == "rejected"
    node_a = next(trace for trace in step["filterTrace"] if trace["node"] == "node-a")
    anti = next(check for check in node_a["checks"] if check["plugin"] == "PodAntiAffinity")
    assert anti["code"] == "E_POD_ANTI_AFFINITY_VIOLATION"
    assert anti["details"]["firstViolation"]["conflicts"] == [
        {"name": "web-a", "node": "node-a", "topologyValue": "node-a"}
    ]
    assert step["constraintDelta"]["changed"] is False


def test_constraint_snapshot_contains_each_sequential_delta(original_result: dict[str, Any]) -> None:
    placed_steps = [step for step in original_result["steps"] if step["decision"] == "placed"]
    for step in placed_steps:
        assert step["constraintDelta"]["addedPods"] == [step["pod"]]
        assert step["constraintDelta"]["changed"] is True
        assert step["pod"] in {
            pod["name"] for pod in step["constraintAfter"]["placedPods"]
        }
    assert original_result["constraints"]["final"]["placedPods"][-1]["name"] == "maintenance-a"


def test_integer_resource_ledgers_and_invariants(original_result: dict[str, Any]) -> None:
    assert original_result["resourceConsistency"]["allNodesConsistent"] is True
    node_a = find_node(original_result, "node-a")
    node_b = find_node(original_result, "node-b")
    node_c = find_node(original_result, "node-c")
    assert node_a is not None and node_b is not None and node_c is not None
    assert node_a["used"] == {"cpuMillicores": 3000, "memoryBytes": 512 * 1024**2 + 3 * 1024**3}
    assert node_a["remaining"] == {"cpuMillicores": 1000, "memoryBytes": 5 * 1024**3 - 512 * 1024**2}
    assert node_b["used"] == {"cpuMillicores": 1200, "memoryBytes": 512 * 1024**2 + 1024**3}
    assert node_c["used"] == {"cpuMillicores": 100, "memoryBytes": 128 * 1024**2}
    for node in (node_a, node_b, node_c):
        assert node["invariant"]["cpu"]["consistent"] is True
        assert node["invariant"]["memory"]["consistent"] is True
        assert node["invariant"]["signed64Safe"] is True
    assert node_a["initialLedger"]
    assert node_b["initialLedger"]
    assert node_c["initialLedger"] == []


def test_insufficient_resource_has_remaining_snapshot_and_no_mutation(original_result: dict[str, Any]) -> None:
    step = find_pod_trace(original_result, "batch-a")
    assert step is not None
    assert step["decision"] == "rejected"
    assert step["rejection"]["code"] == "E_INSUFFICIENT_RESOURCES"
    node_b_trace = next(trace for trace in step["filterTrace"] if trace["node"] == "node-b")
    resource = next(check for check in node_b_trace["checks"] if check["plugin"] == "NodeResourcesFit")
    assert resource["details"]["remainingBefore"] == {"cpuMillicores": 1800, "memoryBytes": 4831838208}
    assert resource["details"]["request"] == {"cpuMillicores": 3500, "memoryBytes": 1024**3}
    assert resource["details"]["shortfall"]["cpuMillicores"] == 1700
    assert resource["details"]["fallbackReason"]
    assert step["resourceAttempts"]["node-b"]["remainingBefore"] == resource["details"]["remainingBefore"]
    node_b = find_node(original_result, "node-b")
    assert node_b is not None
    assert node_b["remaining"] == {"cpuMillicores": 1800, "memoryBytes": 4831838208}


def test_noexecute_window_allows_and_rejects_by_elapsed_time(original_result: dict[str, Any]) -> None:
    allowed = find_pod_trace(original_result, "maintenance-a")
    rejected = find_pod_trace(original_result, "maintenance-b")
    assert allowed is not None and rejected is not None
    allowed_taint = _checks(allowed, "node-c")["Taints"]
    assert allowed_taint["passed"] is True
    window = allowed_taint["details"]["taintEvaluations"][0]["windowAttempts"][0]
    assert window == {
        "tolerationSeconds": 30,
        "taintTimeAddedSeconds": 100,
        "nowSeconds": 120,
        "elapsedSeconds": 20,
        "remainingSeconds": 10,
        "valid": True,
        "expiresAtSeconds": 130,
    }
    assert rejected["rejection"]["primaryNode"] == "node-c"
    assert rejected["rejection"]["code"] == "E_TAINT_NOT_TOLERATED"
    expired = rejected["rejection"]["details"]["firstViolation"]
    assert expired["reason"] == "NoExecute toleration time window expired"
    assert expired["windowAttempts"][0]["remainingSeconds"] == -10


def test_prefer_no_schedule_reduces_score_without_hard_rejection(original_result: dict[str, Any]) -> None:
    step = find_pod_trace(original_result, "cache-a")
    assert step is not None
    assert step["decision"] == "placed"
    assert step["selectedNode"] == "node-b"
    taint_check = _checks(step, "node-b")["Taints"]
    assert taint_check["passed"] is True
    assert taint_check["details"]["preferNoSchedulePenalties"] == [
        {"key": "spot", "value": "", "effect": "PreferNoSchedule"}
    ]
    components = {item["component"]: item for item in step["selectedScore"]["components"]}
    assert components["preferNoSchedulePenalty"]["points"] == "-5.000000"
    assert components["preferNoSchedulePenalty"]["taints"][0]["key"] == "spot"


def test_score_is_fixed_point_and_bit_for_bit_deterministic() -> None:
    first = run_sample("original", "determinism-a")
    second = run_sample("original", "determinism-b")
    first.pop("batchId")
    second.pop("batchId")
    assert json.dumps(first, sort_keys=True, ensure_ascii=False) == json.dumps(second, sort_keys=True, ensure_ascii=False)
    for step in first["steps"]:
        if step["decision"] != "placed":
            continue
        score = step["selectedScore"]
        assert re.fullmatch(r"\d+\.\d{6}", score["score"])
        assert score["scoreDecimals"] == 6
        assert sum(component["pointsUnits"] for component in score["components"] if component["component"] != "preferNoSchedulePenalty") >= 0
        assert "roundingErrorPoints" in score["components"][0]
        assert score["rounding"]["method"]
        assert score["rounding"]["componentResidualTotalPoints"]
    audit = first["scoreContract"]["roundingAudit"]
    assert audit["evaluationCount"] == len(audit["evaluations"]) > 0
    assert "/" in audit["cumulativeResidualExact"]
    assert re.fullmatch(r"-?\d+\.\d{12}", audit["cumulativeResidualPoints"])


def test_gt_lt_notin_and_exists_operators() -> None:
    def check(operator: str, values: tuple[str, ...], labels: dict[str, str]) -> bool:
        passed, _ = evaluate_requirement(
            labels,
            SelectorRequirement("version", operator, values),
        )
        return passed

    assert check("Gt", ("3",), {"version": "4"}) is True
    assert check("Gt", ("3",), {"version": "3"}) is False
    assert check("Lt", ("5",), {"version": "4"}) is True
    assert check("Lt", ("5",), {"version": "5"}) is False
    assert check("NotIn", ("gold",), {"tier": "silver"}) is True
    assert check("NotIn", ("gold",), {}) is True
    assert check("Exists", (), {"version": "4"}) is True
    assert check("DoesNotExist", (), {}) is True
    assert check("Lt", ("5",), {"version": "not-an-int"}) is False


def test_gt_and_lt_operators_participate_in_real_node_filtering() -> None:
    raw = {
        "name": "numeric-affinity",
        "nodes": [
            {
                "name": "too-old",
                "allocatable": {"cpu": "1", "memory": "1Gi"},
                "labels": {"env": "prod", "version": "3"},
            },
            {
                "name": "target",
                "allocatable": {"cpu": "1", "memory": "1Gi"},
                "labels": {"env": "prod", "version": "6"},
            },
            {
                "name": "too-new",
                "allocatable": {"cpu": "1", "memory": "1Gi"},
                "labels": {"env": "prod", "version": "9"},
            },
        ],
        "pods": [
            {
                "name": "numeric-pod",
                "labels": {"app": "numeric"},
                "resources": {"requests": {"cpu": "100m", "memory": "128Mi"}},
                "nodeSelector": {"env": "prod"},
                "affinity": {
                    "nodeAffinity": {
                        "requiredDuringSchedulingIgnoredDuringExecution": {
                            "nodeSelectorTerms": [
                                {
                                    "matchExpressions": [
                                        {"key": "version", "operator": "Gt", "values": ["4"]},
                                        {"key": "version", "operator": "Lt", "values": ["8"]},
                                    ]
                                }
                            ]
                        }
                    }
                },
            }
        ],
    }
    result = schedule_batch(normalize_spec(BatchSpecInput.model_validate(raw)), batch_id="numeric")
    assert result["placed"][0]["node"] == "target"
    step = result["steps"][0]
    too_old = next(trace for trace in step["filterTrace"] if trace["node"] == "too-old")
    too_new = next(trace for trace in step["filterTrace"] if trace["node"] == "too-new")
    assert too_old["firstRejection"]["code"] == "E_UNSATISFIED_AFFINITY"
    assert too_old["firstRejection"]["details"]["firstViolationExpression"]["operator"] == "Gt"
    assert too_new["firstRejection"]["details"]["firstViolationExpression"]["operator"] == "Lt"


def test_negative_rounding_residual_formatting_is_symmetric() -> None:
    from fractions import Fraction

    assert format_fraction(Fraction(-1, 10**13), 9) == "0.000000000"
    assert format_fraction(Fraction(-1, 2 * 10**9), 9) == "-0.000000001"


def test_preemption_recalculates_margin_and_respects_non_preemptable_pods(preemption_result: dict[str, Any]) -> None:
    assert preemption_result["summary"]["placed"] == 1
    assert preemption_result["summary"]["preempted"] == 1
    assert preemption_result["preempted"][0]["pod"] == "low-priority-job"
    assert all(item["pod"] != "protected-job" for item in preemption_result["preempted"])
    event = preemption_result["preemption"]["events"][0]
    assert event["victims"][0]["remainingBefore"] == {"cpuMillicores": 800, "memoryBytes": 1879048192}
    assert event["victims"][0]["remainingAfter"] == {"cpuMillicores": 2800, "memoryBytes": 4026531840}
    node = preemption_result["nodes"][0]
    assert node["remaining"] == {"cpuMillicores": 300, "memoryBytes": 805306368}
    assert node["assignedPods"] == [
        {"name": "low-priority-job", "status": "preempted"},
        {"name": "protected-job", "status": "assigned"},
    ]
    assert node["invariant"]["cpu"] == {
        "allocatable": 3000,
        "initialAccepted": 2200,
        "scheduled": 2500,
        "preemptionReclaimed": 2000,
        "used": 2700,
        "remaining": 300,
        "equation": "allocatable - initialAccepted - scheduled + preemptionReclaimed = remaining",
        "consistent": True,
    }


def test_assigned_pod_overflow_is_recorded_without_negative_remaining() -> None:
    raw = {
        "name": "assigned-overflow",
        "nodes": [
            {
                "name": "small",
                "allocatable": {"cpu": "1", "memory": "1Gi"},
                "labels": {"kubernetes.io/hostname": "small"},
                "assignedPods": [
                    {
                        "name": "too-large",
                        "labels": {"app": "old"},
                        "resources": {"requests": {"cpu": "2", "memory": "512Mi"}},
                    }
                ],
            }
        ],
        "pods": [
            {
                "name": "new",
                "labels": {"app": "new"},
                "resources": {"requests": {"cpu": "100m", "memory": "128Mi"}},
            }
        ],
    }
    result = schedule_batch(normalize_spec(BatchSpecInput.model_validate(raw)), batch_id="overflow")
    node = result["nodes"][0]
    assert node["accountingOverflow"] is True
    assert result["resourceConsistency"]["allNodesConsistent"] is False
    assert node["remaining"] == {"cpuMillicores": 1000, "memoryBytes": 1024**3}
    assert node["initialLedger"][0]["accepted"] is False
    assert node["initialLedger"][0]["rejection"]["remainingSnapshot"] == {
        "cpuMillicores": 1000,
        "memoryBytes": 1024**3,
    }
    assert node["invariant"]["cpu"]["remaining"] == 1000
    assert result["rejected"][0]["reason"]["code"] == "E_NODE_ACCOUNTING_OVERFLOW"
