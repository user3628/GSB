from __future__ import annotations

import json
from fractions import Fraction

from app.benchmark import compensation_benchmark
from app.compiler import compile_snapshot, compute_snapshot_digest
from app.fixedpoint import derive_budget, format_q6, round_half_up
from app.models import HealthStatus, Route, SnapshotRequest


def test_round_half_up_contract() -> None:
    assert round_half_up(Fraction(3, 2)) == 2
    assert round_half_up(Fraction(1, 2)) == 1
    assert round_half_up(Fraction(1, 3)) == 0
    assert round_half_up(Fraction(5, 2)) == 3
    assert format_q6(1_234_567) == "1.234567"


def test_budget_derivation_is_bitwise_deterministic_and_tracks_error() -> None:
    first = derive_budget(600, 2, HealthStatus.degraded)
    for _ in range(100):
        repeated = derive_budget(600, 2, HealthStatus.degraded)
        assert repeated == first
        assert json.dumps(repeated.payload(), sort_keys=True) == json.dumps(
            first.payload(), sort_keys=True
        )
    assert first.cumulative_error != 0
    assert len(first.steps) == 4
    assert all(step.rounding_mode for step in first.steps)
    assert first.steps[-1].rounded_q6 == first.health_penalty_q6


def test_same_routes_in_different_input_order_have_identical_compilation() -> None:
    payload = json.loads(
        (SnapshotRequest.model_validate(
            json.load(open("samples/routes_snapshot_v1.json", encoding="utf-8"))
        )).model_dump_json()
    )
    routes = [Route.model_validate(item) for item in payload["routes"]]
    first = compile_snapshot(1, routes)
    second = compile_snapshot(1, list(reversed(routes)))
    assert first.digest == second.digest == compute_snapshot_digest(1, routes)
    assert [(p.route.route_id, p.budget) for p in first.routes] == [
        (p.route.route_id, p.budget) for p in second.routes
    ]
    assert [a.payload() for a in first.upstream_aggregates] == [
        a.payload() for a in second.upstream_aggregates
    ]


def test_fixed_mixed_magnitude_benchmark_has_observable_naive_error() -> None:
    result = compensation_benchmark()
    assert result["naive_float64"]["error_q6"] != 0
    assert abs(result["kahan_float64"]["error_q6"]) < abs(
        result["naive_float64"]["error_q6"]
    )
    assert result["error_bounds"]["naive_within_bound"] is True
    assert result["error_bounds"]["kahan_within_bound"] is True
