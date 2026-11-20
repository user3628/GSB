from __future__ import annotations

import copy
from typing import Any

from app.engine import simulate
from app.models import PolicySnapshot, TraceSample


def make_snapshot(target_rule: dict[str, Any], *, consecutive: int | None = None) -> PolicySnapshot:
    breaker = target_rule.get("circuit_breaker", {"consecutive_failures": 10, "window_ms": 10_000})
    if consecutive is not None:
        breaker = {**breaker, "consecutive_failures": consecutive}
    return PolicySnapshot.model_validate(
        {
            "version": 99,
            "generated_at": "2026-11-20T00:00:00Z",
            "virtual_service": {
                "service": "test-gateway",
                "routes": [
                    {
                        "rule_id": "r-test",
                        "route_id": "test",
                        "path_prefix": "/test",
                        "headers": {},
                        "priority": 100,
                    }
                ],
                "default_route": "default",
            },
            "target_rules": {
                "test": {**target_rule, "circuit_breaker": breaker},
                "default": {
                    "rate_limit": {"threshold": 100, "window_ms": 1000},
                    "timeout_ms": 500,
                    "circuit_breaker": {"consecutive_failures": 10, "window_ms": 10000},
                },
            },
            "checksum": "",
        }
    )


def make_trace(requests: list[dict[str, Any]]) -> TraceSample:
    normalized = []
    for index, request in enumerate(requests, start=1):
        normalized.append(
            {
                "request_id": request.get("request_id", f"r-{index}"),
                "timestamp_ms": request["timestamp_ms"],
                "method": request.get("method", "GET"),
                "path": request.get("path", "/test/items"),
                "headers": request.get("headers", {}),
                "upstream": {
                    "status_code": request.get("status_code", 200),
                    "latency_ms": request.get("latency_ms", 10),
                    "connection_error": request.get("connection_error", False),
                },
            }
        )
    return TraceSample.model_validate({"trace_id": "unit-trace", "requests": normalized})


def test_sample_routing_default_ambiguity_and_repeatability(policy_v1, policy_v2, trace):
    first = simulate(policy_v1, trace)
    second = simulate(policy_v1, trace)
    assert first == second

    by_id = {item["request_id"]: item for item in first["decisions"]}
    assert by_id["req-011"]["route_id"] == "default"
    assert by_id["req-011"]["is_default_route"] is True
    assert by_id["req-011"]["match_count"] == 0

    ambiguous = simulate(policy_v2, trace)["decisions"]
    request_012 = next(item for item in ambiguous if item["request_id"] == "req-012")
    assert request_012["route_id"] == "checkout_campaign"
    assert request_012["match_count"] == 3
    assert request_012["matched_rule_ids"] == [
        "r-checkout-campaign",
        "r-checkout-retry",
        "r-checkout",
    ]
    assert request_012["route_resolution"]["winning_rule_id"] == "r-checkout-campaign"
    assert request_012["route_resolution"]["tied_at_top_precedence"] is True
    assert request_012["route_resolution"]["strategy"].startswith("priority_desc")


def test_fixed_window_is_epoch_aligned_and_end_exclusive():
    snapshot = make_snapshot(
        {
            "rate_limit": {"threshold": 1, "window_ms": 1000},
            "timeout_ms": 500,
        }
    )
    trace = make_trace(
        [
            {"request_id": "a", "timestamp_ms": 999},
            {"request_id": "b", "timestamp_ms": 1000},
            {"request_id": "c", "timestamp_ms": 1999},
            {"request_id": "d", "timestamp_ms": 2000},
        ]
    )
    decisions = simulate(snapshot, trace)["decisions"]
    assert [item["decision_type"] for item in decisions] == [
        "SUCCESS",
        "SUCCESS",
        "RATE_LIMITED",
        "SUCCESS",
    ]
    assert decisions[2]["rate_limit"]["window_start_ms"] == 1000
    assert decisions[2]["rate_limit"]["window_end_ms"] == 2000
    assert decisions[2]["response_status"] == 429
    assert decisions[2]["rate_limit"]["observed_count"] == 2


def test_breaker_consecutive_failures_window_and_half_open_probe():
    snapshot = make_snapshot(
        {
            "rate_limit": {"threshold": 100, "window_ms": 1000},
            "timeout_ms": 500,
            "circuit_breaker": {"window_ms": 1000},
        },
        consecutive=2,
    )
    trace = make_trace(
        [
            {"request_id": "f1", "timestamp_ms": 0, "status_code": 503},
            {"request_id": "f2", "timestamp_ms": 10, "status_code": 503},
            {"request_id": "rejected", "timestamp_ms": 1009, "status_code": 200},
            {"request_id": "probe", "timestamp_ms": 1010, "status_code": 200},
            {"request_id": "f3", "timestamp_ms": 1011, "status_code": 503},
            {"request_id": "f4", "timestamp_ms": 1012, "status_code": 503},
        ]
    )
    result = simulate(snapshot, trace)
    decisions = {item["request_id"]: item for item in result["decisions"]}
    assert decisions["f1"]["breaker"]["state_after"] == "CLOSED"
    assert decisions["f2"]["breaker"]["state_after"] == "OPEN"
    assert decisions["f2"]["breaker"]["open_until_ms"] == 1010
    assert decisions["rejected"]["decision_type"] == "CIRCUIT_OPEN"
    assert decisions["rejected"]["upstream_attempted"] is False
    assert decisions["probe"]["breaker"]["half_open_probe"] is True
    assert decisions["probe"]["breaker"]["state_after"] == "CLOSED"
    assert decisions["f4"]["breaker"]["state_after"] == "OPEN"
    assert decisions["f4"]["breaker"]["open_until_ms"] == 2012
    assert result["stats"]["breaker_transitions"]["test"] == 4


def test_timeout_boundary_is_strict_and_integer_only():
    snapshot = make_snapshot(
        {
            "rate_limit": {"threshold": 100, "window_ms": 1000},
            "timeout_ms": 500,
        },
        consecutive=10,
    )
    trace = make_trace(
        [
            {"request_id": "equal", "timestamp_ms": 0, "latency_ms": 500},
            {"request_id": "over", "timestamp_ms": 1, "latency_ms": 501},
        ]
    )
    decisions = simulate(snapshot, trace)["decisions"]
    assert decisions[0]["decision_type"] == "SUCCESS"
    assert decisions[1]["decision_type"] == "TIMEOUT"
    assert decisions[1]["response_latency_ms"] == 500


def test_conflict_markers_do_not_change_decisions(policy_v2, trace):
    with_markers = simulate(policy_v2, trace, detect_conflicts=True)
    without_markers = simulate(policy_v2, trace, detect_conflicts=False)
    stripped = copy.deepcopy(with_markers["decisions"])
    for decision in stripped:
        decision["parameter_conflicts"] = []
    assert stripped == without_markers["decisions"]
    assert with_markers["stats"]["conflict_count"] == 3
    assert with_markers["conflicts"][0]["timeout_ms"] == 6000
    assert with_markers["conflicts"][0]["breaker_window_ms"] == 3000

    checkout_decisions = [
        decision for decision in with_markers["decisions"] if decision["route_id"] == "checkout"
    ]
    assert checkout_decisions
    assert all(decision["parameter_conflicts"] for decision in checkout_decisions)
    assert all(
        decision["parameter_conflicts"][0]["condition"] == "timeout_ms > circuit_breaker.window_ms"
        for decision in checkout_decisions
    )


def test_orders_are_deduplicated_sorted_and_quantified(policy_v2, trace):
    first = simulate(policy_v2, trace)
    second = simulate(policy_v2, trace)
    assert first["orders"] == second["orders"]
    orders = first["orders"]
    keys = [(item["route_id"], item["difference_type"]) for item in orders]
    assert len(keys) == len(set(keys))
    assert [item["rank"] for item in orders] == list(range(1, len(orders) + 1))

    expected_sort = sorted(
        orders,
        key=lambda item: (
            -item["priority_score"],
            -item["type_weight"],
            -item["affected_requests"],
            item["route_id"],
            item["difference_type"],
        ),
    )
    assert [(item["route_id"], item["difference_type"]) for item in orders] == [
        (item["route_id"], item["difference_type"]) for item in expected_sort
    ]
    for order in orders:
        assert order["priority_score"] == order["affected_ratio_bp"] * order["type_weight"]
        assert order["affected_ratio_bp"] == (
            order["affected_requests"] * 10_000 // order["total_requests"]
        )

    keyed = {(item["route_id"], item["difference_type"]): item for item in orders}
    conflict_order = keyed[("checkout", "TIMEOUT_BREAKER_CONFLICT")]
    assert conflict_order["priority"] == "P1"
    assert conflict_order["metrics"]["conflicts"][0]["timeout_ms"] == 6000
    assert conflict_order["metrics"]["conflicts"][0]["breaker_window_ms"] == 3000
    assert conflict_order["metrics"]["affected_request_ids"] == [
        "req-001",
        "req-002",
        "req-003",
        "req-004",
        "req-005",
        "req-006",
        "req-013",
        "req-014",
        "req-015",
        "req-016",
        "req-017",
        "req-018",
    ]
    assert conflict_order["metrics"]["decision_refs"][0]["request_id"] == "req-001"
    assert ("catalog", "RATE_LIMIT_HIGH_REJECTION") in keyed
    assert keyed[("catalog", "RATE_LIMIT_HIGH_REJECTION")]["metrics"]["affected_request_ids"] == [
        "req-009",
        "req-010",
    ]
    assert ("checkout_campaign", "ROUTE_MATCH_AMBIGUITY") in keyed
