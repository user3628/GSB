from __future__ import annotations

import hashlib

import pytest

from obs_sampler.errors import BusinessError
from obs_sampler.models import BatchRequest

from .conftest import make_log, make_span, policy_document


def _head_sample_digest(decisions) -> tuple[int, str]:
    sampled_uids = sorted(
        decision["record_uid"]
        for decision in decisions
        if decision["head_sampled"] and not decision["duplicate"]
    )
    digest = hashlib.sha256()
    for record_uid in sampled_uids:
        digest.update(record_uid.encode("utf-8"))
        digest.update(b"\n")
    return len(sampled_uids), digest.hexdigest()


def test_v1_reports_match_naive_population_and_are_within_bound(
    seeded_service, naive_expected
):
    expected = naive_expected["versions"]["1"]
    state = seeded_service.get_policy_state(1)
    assert state["commit"]["leader_count"] == expected["leader_count"]
    assert state["commit"]["duplicate_count"] == expected["duplicate_count"]
    assert sum(row["population_count"] for row in state["counts"]) == expected["leader_count"]

    for key, naive in expected["strata"].items():
        service, level = key.split("/", 1)
        report = seeded_service.get_bias_report(1, service, level)["report"]
        assert report["population_count"] == naive["population_count"]
        assert report["population_weight"] == naive["population_weight"]
        assert report["emitted_count"] >= report["min_samples"]
        assert report["weighted_total"] is not None
        assert report["numeric_error_bound"] < 1e-9
        assert report["within_error_bound"] is True
        assert abs(report["relative_bias"]) <= report["configured_error_bound"]
        assert report["relative_error_bound"] <= report["configured_error_bound"]

    decisions = seeded_service.get_batch_result("fixed-sample-batch-1")["decisions"]
    sampled_count, sampled_digest = _head_sample_digest(decisions)
    baseline = expected["head_only_sample"]
    assert sampled_count == baseline["sampled_leader_count"]
    assert sampled_digest == baseline["sampled_uids_sha256"]


def test_policy_upgrade_rebuilds_v2_while_preserving_v1_snapshot(
    seeded_service, sample_policy_v2, naive_expected
):
    old_state = seeded_service.get_policy_state(1)
    seeded_service.submit_policy(sample_policy_v2)
    new_state = seeded_service.get_policy_state(2)
    preserved_v1 = seeded_service.get_policy_state(1)

    assert old_state["commit"]["record_count"] == 1416
    assert new_state["commit"]["record_count"] == 1416
    assert new_state["commit"]["leader_count"] == naive_expected["versions"]["2"]["leader_count"]
    assert new_state["commit"]["decision_digest"] != old_state["commit"]["decision_digest"]
    assert preserved_v1["commit"]["decision_digest"] == old_state["commit"]["decision_digest"]
    assert seeded_service.get_policy()["policy_version"] == 2

    for key, naive in naive_expected["versions"]["2"]["strata"].items():
        service, level = key.split("/", 1)
        report = seeded_service.get_bias_report(2, service, level)["report"]
        assert report["population_count"] == naive["population_count"]
        assert report["population_weight"] == naive["population_weight"]

    v2_decisions = seeded_service.get_batch_result("fixed-sample-batch-1")["decisions"]
    sampled_count, sampled_digest = _head_sample_digest(v2_decisions)
    v2_baseline = naive_expected["versions"]["2"]["head_only_sample"]
    assert sampled_count == v2_baseline["sampled_leader_count"]
    assert sampled_digest == v2_baseline["sampled_uids_sha256"]


def test_unsampled_stratum_returns_business_error_not_zero_estimate(service):
    policy = policy_document(
        rates={"checkout": {"INFO": 0.0}},
        min_samples=2,
        tail_base_rate=0.0,
    )
    service.submit_policy(policy)
    service.submit_batch(
        BatchRequest(
            batch_id="unsampled",
            logs=[make_log(index, trace_id=f"u-{index}") for index in range(3)],
        )
    )
    state = service.get_policy_state(1)
    row = state["bias_report"][0]
    assert row["status"] == "INSUFFICIENT"
    assert row["insufficient_reason"] == "UNSAMPLED_STRATUM"
    assert row["weighted_total"] is None
    assert row["population_count"] == 3

    with pytest.raises(BusinessError) as raised:
        service.get_bias_report(1, "checkout", "INFO")
    assert raised.value.code == "E_INSUFFICIENT_SAMPLES"
    assert raised.value.details["reason"] == "UNSAMPLED_STRATUM"


def test_too_few_emitted_samples_returns_insufficient(service):
    policy = policy_document(
        rates={"checkout": {"INFO": 1.0}},
        min_samples=100,
        tail_base_rate=0.0,
    )
    service.submit_policy(policy)
    service.submit_batch(
        BatchRequest(
            batch_id="small",
            logs=[make_log(index, trace_id=f"small-{index}") for index in range(3)],
        )
    )
    with pytest.raises(BusinessError) as raised:
        service.get_bias_report(1, "checkout", "INFO")
    assert raised.value.code == "E_INSUFFICIENT_SAMPLES"
    assert raised.value.details["reason"] == "SAMPLE_SIZE_BELOW_MINIMUM"


def test_tail_can_make_zero_head_rate_stratum_estimable(service):
    policy = policy_document(
        rates={"checkout": {"INFO": 0.0}},
        min_samples=1,
        tail_threshold_us=1,
        tail_base_rate=1.0,
        error_multiplier=1.0,
    )
    service.submit_policy(policy)
    service.submit_batch(
        BatchRequest(
            batch_id="tail-only",
            spans=[
                make_span(
                    index,
                    trace_id=f"tail-{index}",
                    latency_us=100_000,
                    sample_weight=2.0,
                )
                for index in range(3)
            ],
        )
    )
    report = service.get_bias_report(1, "checkout", "INFO")["report"]
    assert report["emitted_count"] == 3
    assert report["population_weight"] == 6.0
    assert report["weighted_total"] == 6.0


def test_report_contains_quantified_kahan_and_sampling_error_bounds(seeded_service):
    report = seeded_service.get_bias_report(1, "payments", "INFO")["report"]
    assert report["weighted_total_compensation"] is not None
    assert report["numeric_error_bound"] >= 0.0
    assert report["variance_sum"] >= 0.0
    assert report["standard_error"] >= 0.0
    assert report["confidence_95_bound"] >= 0.0
    assert report["relative_error_bound"] >= 0.0
    assert report["configured_error_bound"] == 0.4
