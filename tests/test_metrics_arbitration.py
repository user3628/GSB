from __future__ import annotations

from decimal import Decimal

from leasefair.clock import FixedClock
from leasefair.samples import build_demo_report, seed_fixed_scenario
from leasefair.service import LeaseService
from tests.conftest import ORIGIN_MS, SAMPLE_PATH


def test_fixed_sample_metrics_are_repeatable_and_rebalance_contract(tmp_path) -> None:
    service, clock, sample = seed_fixed_scenario(tmp_path / "seed.sqlite", SAMPLE_PATH)
    first = service.metrics(now_ms=ORIGIN_MS)
    second = service.metrics(now_ms=ORIGIN_MS)
    assert first == second
    assert first["cluster"]["occupied_keys"] == 4
    assert first["cluster"]["registered_keys"] == 5
    assert first["cluster"]["occupancy"]["decimal"] == "0.800000000"
    assert first["rebalance"]["suggestion_count"] == 3
    shares = [holder["actual_lease_share"]["q"] for holder in first["holders"]]
    assert sum(shares) == 1_000_000_000
    assert first["weighted_fairness"]["decimal"] == "0.872758620"

    tolerant = LeaseService(
        tmp_path / "tolerant.sqlite",
        clock=FixedClock(ORIGIN_MS),
        rebalance_relative_tolerance=Decimal("1.0"),
    )
    # Build the same imbalance in a second database.
    for key in sample["registered_keys"]:
        tolerant.register_key(key)
    tolerant.acquire("alpha", request_id="t1", holder_id="holder-a", ttl_ms=7200000, weight="0.45")
    tolerant.acquire("delta", request_id="t2", holder_id="holder-a", ttl_ms=300000, weight="0.45")
    tolerant.acquire("beta", request_id="t3", holder_id="holder-b", ttl_ms=30000, weight="0.35")
    tolerant.acquire("gamma", request_id="t4", holder_id="holder-c", ttl_ms=300, weight="0.2")
    high_tolerance_metrics = tolerant.metrics(now_ms=ORIGIN_MS)
    assert high_tolerance_metrics["rebalance"]["suggestion_count"] == 0


def test_arbitration_is_order_independent_and_ties_are_stable(tmp_path) -> None:
    service, clock, sample = seed_fixed_scenario(tmp_path / "seed.sqlite", SAMPLE_PATH)
    forward = service.arbitrate(
        sample["free_key_for_arbitration"], candidates=sample["candidates"], now_ms=ORIGIN_MS
    )["arbitration"]
    reverse = service.arbitrate(
        sample["free_key_for_arbitration"],
        candidates=list(reversed(sample["candidates"])),
        now_ms=ORIGIN_MS,
    )["arbitration"]
    assert forward == reverse
    assert forward["winner"]["holder_id"] == "holder-b"
    assert forward["tolerance"]["q"] == 1000
    assert all("left_projected_fairness" in item for item in forward["comparisons"])
    assert all("right_projected_fairness" in item for item in forward["comparisons"])

    tied_candidates = [
        {"holder_id": holder, "ttl_ms": 300, "weight": "0.5"}
        for holder in ("holder-z", "holder-y", "holder-x")
    ]
    tied = service.arbitrate(
        "epsilon", candidates=tied_candidates, now_ms=ORIGIN_MS
    )["arbitration"]
    assert tied["winner"]["holder_id"] == "holder-x"
    for comparison in tied["comparisons"]:
        assert comparison["within_tolerance"] is True
        assert "holder_id lexicographic" in comparison["final_basis"]


def test_demo_report_invariants(tmp_path) -> None:
    service, clock, sample = seed_fixed_scenario(tmp_path / "demo.sqlite", SAMPLE_PATH)
    report = build_demo_report(service, clock, sample)
    assert report["invariants"]["repeat_metrics_bit_identical"] is True
    assert report["invariants"]["fixed_sample_matches_persisted_rows"] is True
    assert report["invariants"]["arbitration_order_independent"] is True
    assert report["invariants"]["granted_holder"] == report["invariants"]["arbitration_winner"]
    assert report["invariants"]["occupancy"] == "0.800000000"
    assert report["time_contract"]["rows"][0]["expires_at_ms"] == ORIGIN_MS + 300
    assert report["numerical_error_report"]["mixed_magnitude_stress"]["mixed_magnitude_terms"]["naive_error_q_estimate"] > 0

