from __future__ import annotations

import pytest

from rca_pipeline import fixedpoint as fp
from rca_pipeline.config import W_HOPS, W_TIME
from rca_pipeline.errors import RebuildFailed
from rca_pipeline.fixedpoint import FixedPointError
from rca_pipeline.rca import numeric_contract_note, score_candidate
from tests.conftest import submit_topology_v1


def test_fixedpoint_half_to_even_and_render():
    assert fp.from_int(1) == 1_000_000
    assert fp.to_string(fp.from_int(1)) == "1.000000"
    # 0.5 * 0.5 = 0.25 exact
    half = fp.from_float(0.5)
    assert fp.mul(half, half) == 250_000
    # half-to-even: (1 unit * 3) / 2 => 3,000,000 / 2 exact; test odd tie rounding
    assert fp.divide_small(3_000_000, 4) == 750_000  # exact
    assert fp.divide_small(1_000_000, 6) == 166_667  # tie resolves to nearest odd/even rule


def test_overflow_bounds_enforced():
    with pytest.raises(FixedPointError):
        fp.add(9_000_000_000_000_000, 1)
    with pytest.raises(FixedPointError):
        score_candidate(33, 0, (W_HOPS, W_TIME))
    with pytest.raises(FixedPointError):
        score_candidate(0, 25 * 60 * 60_000, (W_HOPS, W_TIME))


def test_score_determinism_and_contract_note():
    first = score_candidate(2, 30_000, (W_HOPS, W_TIME))
    second = score_candidate(2, 30_000, (W_HOPS, W_TIME))
    assert first == second
    note = numeric_contract_note()
    assert note["scale"] == 1_000_000
    assert "round-half-to-even" in note["rounding"]
    assert note["max_cumulative_error"] == "0.000002"


def test_rca_end_to_end_ranking(pipeline, topo_v1, dataset):
    submit_topology_v1(pipeline, topo_v1)
    for batch in dataset[:3]:  # stop while payment alerts are still active
        pipeline.submit_samples(batch)
    alerts = pipeline.query_alerts(service="payment")["alerts"]
    firing = alerts[0]
    result = pipeline.query_rca(firing["id"])
    candidates = result["candidates"]
    assert candidates, "payment alert must produce rca candidates"
    # payment itself is hop 0 and therefore ranks first
    assert candidates[0]["candidate"] == "payment"
    assert candidates[0]["rank"] == 1
    ranks = [c["rank"] for c in candidates]
    assert ranks == sorted(ranks)
    # every score is a rendered fixed-point string with six decimals
    for candidate in candidates:
        assert len(candidate["score"].split(".")[1]) == 6


def test_rca_rebuild_failure_rolls_back(pipeline, topo_v1, dataset):
    submit_topology_v1(pipeline, topo_v1)
    pipeline.submit_samples(dataset[0])
    pipeline.inject_fault("fail_rebuild")
    with pytest.raises(RebuildFailed):
        pipeline.submit_samples(dataset[1])
    # failed transaction must not advance the sample version
    assert pipeline.diagnostic_report()["versions"]["sample_version"] == 1
    # the durable intent must be reconciled as aborted, not left pending
    assert pipeline.recovery_report()["pending_intents"] == 0
    # one-shot fault is consumed, so the next valid commit succeeds normally
    again = pipeline.submit_samples(dataset[1])
    assert again["new_version"] == 2
