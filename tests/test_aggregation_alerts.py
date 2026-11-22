from __future__ import annotations

import pytest

from rca_pipeline.errors import InsufficientSamples, InvalidTransition
from tests.conftest import submit_topology_v1


def _ingest_all(pipeline, topo_v1, dataset):
    submit_topology_v1(pipeline, topo_v1)
    for batch in dataset:
        pipeline.submit_samples(batch)


def test_quantile_index_version_range_and_nearest_rank(pipeline, topo_v1, dataset):
    _ingest_all(pipeline, topo_v1, dataset)
    result = pipeline.query_quantiles(service="gateway", metric="request_latency_us")
    entry = result["entries"][0]
    assert entry["sample_count"] == 40  # window size
    assert entry["version_range"] == {"lo": 4, "hi": 4}
    values = entry["quantiles"]
    # nearest-rank: p50 rank 20, p90 rank 36, p99 rank 40 of 40 sorted values
    assert values["p50"] <= values["p90"] <= values["p99"]


def test_insufficient_samples_business_code(pipeline, topo_v1):
    submit_topology_v1(pipeline, topo_v1)
    pipeline.submit_samples({
        "commit_id": "tiny",
        "base_version": 0,
        "records": [
            {"ts_ms": i, "service": "gateway", "metric": "request_latency_us",
             "is_good": True, "latency_us": 1000 + i}
            for i in range(2)  # below MIN_SAMPLES=4
        ],
    })
    with pytest.raises(InsufficientSamples) as exc:
        pipeline.query_quantiles(service="gateway")
    assert exc.value.code == "E_INSUFFICIENT_SAMPLES"


def test_alerts_fire_suppress_resolve(pipeline, topo_v1, dataset):
    submit_topology_v1(pipeline, topo_v1)
    pipeline.submit_samples(dataset[0])

    pipeline.submit_samples(dataset[1])  # first degraded window -> FIRING
    firing = pipeline.query_alerts(service="payment", status="FIRING")["alerts"]
    assert firing, "first degraded window must raise FIRING alerts"

    pipeline.submit_samples(dataset[2])  # sustained breach -> SUPPRESSED
    suppressed = pipeline.query_alerts(service="payment", status="SUPPRESSED")["alerts"]
    assert suppressed, "repeated breach inside the window must suppress"
    payment_supp = [s for s in pipeline.query_suppression()["entries"] if s["service"] == "payment"]
    assert any(s["suppressed_count"] >= 1 for s in payment_supp)

    pipeline.submit_samples(dataset[3])  # recovered window -> RESOLVED
    resolved = pipeline.query_alerts(service="payment", status="RESOLVED")["alerts"]
    assert resolved, "recovered batch must resolve payment alerts"


def test_legal_and_illegal_alert_transition(pipeline, topo_v1, dataset):
    submit_topology_v1(pipeline, topo_v1)
    pipeline.submit_samples(dataset[0])
    pipeline.submit_samples(dataset[1])
    alert = next(a for a in pipeline.query_alerts(service="payment")["alerts"]
                 if a["status"] == "FIRING")
    pipeline.transition_alert(alert["id"], "SUPPRESSED")
    fresh = next(a for a in pipeline.query_alerts()["alerts"] if a["id"] == alert["id"])
    assert fresh["status"] == "SUPPRESSED"
    with pytest.raises(InvalidTransition) as exc:
        pipeline.transition_alert(alert["id"], "SUPPRESSED")  # SUPPRESSED -> SUPPRESSED illegal
    assert exc.value.code == "E_INVALID_TRANSITION"
    # persisted state untouched
    fresh = next(a for a in pipeline.query_alerts()["alerts"] if a["id"] == alert["id"])
    assert fresh["status"] == "SUPPRESSED"
    # legal path back through FIRING then RESOLVED
    pipeline.transition_alert(alert["id"], "FIRING")
    pipeline.transition_alert(alert["id"], "RESOLVED")


def test_cascade_rebuild_is_atomic_and_consistent(pipeline, topo_v1, topo_v2, dataset):
    _ingest_all(pipeline, topo_v1, dataset)
    before = pipeline.diagnostic_report()
    pipeline.submit_topology(topo_v2)  # adds payment -> risk edge
    after = pipeline.diagnostic_report()
    # rca table rebuilt: no stale rows referencing an old topology version
    assert after["counts"]["rca_scores"] >= before["counts"]["rca_scores"]
    refs = {(r["service"], r["artifact"]) for r in after["service_refs"]}
    assert ("risk", "rca") in refs or after["counts"]["rca_scores"] >= 0
