from __future__ import annotations

import json

from alert_sieve.domain import HOP_BASE, SCORE_SCALE
from alert_sieve.scoring import (
    FixedPointContractError,
    combine_components,
    hop_component,
    rank_with_tolerance,
    score_pair,
    scores_within_tolerance,
    time_component,
)

from conftest import SAMPLES, load_baseline


def test_reduction_suppression_and_boundary_are_deterministic(store):
    load_baseline(store)
    reduction = store.query_reduction(include_alerts=True)
    assert reduction["totals"] == {
        "raw_count": 10,
        "retained_count": 6,
        "deduped_count": 4,
        "fingerprint_count": 6,
    }
    checkout = next(
        alert for alert in reduction["alerts"] if alert["alert_uid"] == "seed-alerts-v1:6"
    )
    canonical = next(
        alert for alert in reduction["alerts"] if alert["alert_uid"] == "seed-alerts-v1:5"
    )
    assert checkout["deduped"] is True
    assert checkout["canonical_alert_uid"] == "seed-alerts-v1:5"
    assert checkout["window_ms"] == 30_000
    assert canonical["is_canonical"] is True

    index = store.query_suppression_index("checkout-5xx")
    assert index["rows"][0]["effective_window_ms"] == 30_000
    assert index["rows"][0]["rule_ids"] == ["r-checkout-5xx"]
    assert index["rows"][0]["alert_count"] == 2
    assert index["rows"][0]["canonical_count"] == 1
    assert index["rows"][0]["deduped_count"] == 1

    first = store.query_reduction(include_alerts=True)
    second = store.query_reduction(include_alerts=True)
    assert json.dumps(first, sort_keys=True) == json.dumps(second, sort_keys=True)


def test_root_cause_scores_use_fixed_point_and_stable_order(store):
    load_baseline(store)
    result = store.query_root_causes(service="edge-gateway")
    assert len(result["alerts"]) == 1
    candidates = result["alerts"][0]["candidates"]
    assert candidates[0]["candidate_alert_uid"] == "seed-alerts-v1:5"
    assert candidates[0]["hops"] == 1
    assert candidates[0]["delta_ms"] == 5_000
    assert candidates[0]["hop_component"] == 300_000
    assert candidates[0]["time_component"] == 983_333
    assert candidates[0]["score_fixed"] == 295_000
    assert all(candidate["score_fixed"] <= SCORE_SCALE for candidate in candidates)
    again = store.query_root_causes(service="edge-gateway")
    assert json.dumps(result, sort_keys=True) == json.dumps(again, sort_keys=True)


def test_topology_increment_rebuilds_scope_and_trace_atomically(store):
    load_baseline(store)
    before = store.query_root_causes(service="edge-gateway")
    before_services = {c["service"] for c in before["alerts"][0]["candidates"]}
    assert "payment" in before_services

    payload = json.loads((SAMPLES / "topology_increment_v2.json").read_text(encoding="utf-8"))
    result = store.submit_topology_increment(payload)
    assert result["affected_services"] == ["checkout", "edge-gateway"]
    assert result["version"] == 2
    event = store.query_invalidation(result["products"]["invalidation_event_id"])
    assert event["event"]["commit_seq"] == result["commit_seq"]
    target_keys = {(t["artifact"], t["object_kind"], t["object_key"]) for t in event["targets"]}
    for artifact in ("aggregate", "suppression", "rootcause"):
        assert (artifact, "service", "checkout") in target_keys
        assert (artifact, "service", "edge-gateway") in target_keys
    assert all(t["generation"] == result["commit_seq"] for t in event["targets"])

    after = store.query_root_causes(service="edge-gateway")
    after_services = {c["service"] for c in after["alerts"][0]["candidates"]}
    assert "payment" not in after_services
    assert "checkout" in after_services
    assert all(c["generation"] == result["commit_seq"] for c in after["alerts"][0]["candidates"])


def test_rule_change_updates_dedup_and_cascade_trace(store):
    load_baseline(store)
    assert store.query_reduction()["totals"]["retained_count"] == 6
    payload = json.loads((SAMPLES / "rules_increment_v2.json").read_text(encoding="utf-8"))
    result = store.submit_rules_increment(payload)
    assert result["affected_services"] == ["checkout", "edge-gateway"]
    reduction = store.query_reduction(include_alerts=True)
    assert reduction["totals"]["raw_count"] == 10
    assert reduction["totals"]["retained_count"] == 7
    second_checkout = next(
        alert for alert in reduction["alerts"] if alert["alert_uid"] == "seed-alerts-v1:6"
    )
    assert second_checkout["is_canonical"] is True
    event = store.query_invalidation(result["products"]["invalidation_event_id"])
    assert event["event"]["cause"] == "rules_increment"
    assert event["event"]["scope"]["services"] == ["checkout", "edge-gateway"]


def test_artifact_refs_exist_for_services_and_fingerprints(store):
    load_baseline(store)
    with store._read_transaction() as conn:
        rows = conn.execute(
            "SELECT artifact, ref_kind, ref_key, product_key FROM artifact_refs"
        ).fetchall()
    assert rows
    service_refs = {(row["artifact"], row["ref_key"]) for row in rows if row["ref_kind"] == "service"}
    fp_refs = {(row["artifact"], row["ref_key"]) for row in rows if row["ref_kind"] == "fingerprint"}
    for artifact in ("aggregate", "suppression", "rootcause"):
        assert (artifact, "checkout") in service_refs
        assert (artifact, "checkout-5xx") in fp_refs


def test_scoring_contract_and_tolerance_ranking():
    assert hop_component(1) == HOP_BASE // 2
    assert time_component(0, 1000) == SCORE_SCALE
    assert time_component(1000, 1000) == 0
    assert combine_components(SCORE_SCALE, SCORE_SCALE) == SCORE_SCALE
    result = score_pair(
        hops=2,
        source_timestamp_ms=5000,
        candidate_timestamp_ms=1000,
        time_window_ms=5000,
    )
    assert 0 <= result.score_fixed <= SCORE_SCALE
    assert result.delta_ms == 4000
    assert scores_within_tolerance(1000, 1999, 1000)
    assert not scores_within_tolerance(1000, 2001, 1000)

    rows = [
        {"candidate_alert_uid": "b", "candidate_service": "svc", "candidate_fingerprint": "fp", "score_fixed": 1000},
        {"candidate_alert_uid": "a", "candidate_service": "svc", "candidate_fingerprint": "fp", "score_fixed": 500},
        {"candidate_alert_uid": "c", "candidate_service": "svc", "candidate_fingerprint": "fp", "score_fixed": 4000},
    ]
    ranked = rank_with_tolerance(rows, tolerance=1000)
    assert [row["candidate_alert_uid"] for row in ranked] == ["c", "a", "b"]
    assert [row["rank"] for row in ranked] == [1, 2, 3]
    assert all("score_fixed" in row for row in ranked)

    for bad in (
        lambda: hop_component(0),
        lambda: time_component(-1, 1000),
        lambda: time_component(1001, 1000),
        lambda: combine_components(-1, 0),
        lambda: score_pair(
            hops=1,
            source_timestamp_ms=1000,
            candidate_timestamp_ms=2000,
            time_window_ms=5000,
        ),
    ):
        try:
            bad()
        except FixedPointContractError:
            pass
        else:
            raise AssertionError("expected FixedPointContractError")
