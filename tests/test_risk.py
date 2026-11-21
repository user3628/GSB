from __future__ import annotations

from decimal import Decimal

from app.risk import BaselineStats, Observation, calculate_risk, compute_stats


def test_nearest_rank_p90_and_fixed_point_rounding_ledger(policy_v1):
    observations = [
        Observation(
            sequence=index + 1,
            timestamp_ms=index * 1_000,
            latency_micros=(index + 1) * 10_000,
            success=index != 0,
        )
        for index in range(10)
    ]
    stats = compute_stats(observations)
    assert stats.sample_count == 10
    assert stats.p90_rank == 9
    assert stats.p90_micros == 90_000
    assert stats.p90_method == "nearest_rank_ceil_0.90"

    policy = __import__("app.policy", fromlist=["PolicySnapshot"]).PolicySnapshot.from_mapping(
        policy_v1
    )
    baseline = BaselineStats(
        sample_count=40,
        success_count=38,
        error_rate=Decimal("0.05"),
        success_rate=Decimal("0.95"),
        p90_micros=180_000,
        p90_ms=Decimal("180"),
    )
    risk = calculate_risk(observations, baseline, policy)
    payload = risk.as_dict()
    assert payload["rounding"]["mode"] == "ROUND_HALF_UP"
    assert payload["rounding"]["fixed_point_scale"] == 1_000_000
    assert payload["rounding"]["steps"]
    assert payload["rounding"]["steps"][-1]["id"] == "risk.total"
    assert payload["rounding"]["cumulative_rounding_error_micros"] >= 0
    assert Decimal(payload["risk_score"]) == risk.risk_score
    assert payload["p90_contract"]["sample_size"] == 10


def test_fast_and_full_share_policy_window_epoch_but_fast_requires_review(seeded_service):
    compare = seeded_service.compare_modes("B-COMPARE")
    assert all(compare["assertions"].values()) is True
    assert compare["full"]["candidate_stats"]["sample_count"] == 40
    assert compare["fast"]["candidate_stats"]["sample_count"] == 12
    assert compare["fast"]["sampling"]["sample_rate"] == "0.3"
    assert compare["fast"]["decision"]["requires_full_review"] is True
    assert compare["fast"]["decision"]["can_auto_rollback"] is False
    assert Decimal(compare["fast"]["confidence"]["lower"]) < Decimal(
        compare["fast"]["risk_threshold"]
    )
    assert compare["fast"]["decision"]["confidence_bound_exceeded"] is True
    assert compare["differences"]


def test_policy_version_switch_changes_decision_on_same_window(seeded_service):
    v1 = seeded_service.get_risk("B-COMPARE", policy_version=1)
    v2 = seeded_service.get_risk("B-COMPARE", policy_version=2)
    assert v1["window_versions"]["window_version"] == v2["window_versions"]["window_version"]
    assert v1["policy_version"] == 1
    assert v2["policy_version"] == 2
    assert v1["policy_digest"] != v2["policy_digest"]
    assert v1["decision"]["action"] == "RELEASE"
    assert v2["decision"]["action"] == "AUTO_ROLLBACK"
    assert Decimal(v2["risk_score"]) > Decimal(v2["risk_threshold"])


def test_fast_decision_is_read_only_even_with_policy_switch(seeded_service):
    before = seeded_service.get_batch("B-COMPARE")
    result = seeded_service.apply_decision(
        "B-COMPARE",
        {
            "mode": "FAST",
            "policy_version": 2,
            "decision_time_ms": 1_763_920_070_000,
        },
    )
    after = seeded_service.get_batch("B-COMPARE")
    assert result["applied"] is False
    assert result["action"] == "REQUIRES_FULL_REVIEW"
    assert after["status"] == before["status"] == "OBSERVING"
    assert after["policy_version"] == before["policy_version"] == 1
    assert after["window_epoch"] == before["window_epoch"]
