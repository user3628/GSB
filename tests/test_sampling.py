from __future__ import annotations

from math import isclose

from obs_sampler.sampling import (
    DEDUP_WINDOW,
    EMITTED,
    HEAD,
    TAIL_PROMOTED,
    TAIL_RETAINED,
    Decision,
    KahanAccumulator,
    Record,
    decide_records,
    decision_digest,
)

from .conftest import make_log, make_span, sample_policy_object


def _record(row: dict, *, index: int, kind: str = "log") -> Record:
    return Record(
        record_uid=f"batch:{kind}:{index:08d}",
        batch_id="batch",
        kind=kind,
        source_index=index,
        timestamp_ms=row["timestamp_ms"],
        service=row["service"],
        level=row["level"],
        trace_id=row["trace_id"],
        span_id=row["span_id"],
        parent_span_id=row["parent_span_id"],
        message_fingerprint=row.get("message_fingerprint"),
        latency_us=row.get("latency_us", 0),
        error=bool(row.get("error", False)),
        sample_weight=row.get("sample_weight", 1.0),
    )


def _decide(records: list[Record], policy) -> list[Decision]:
    decisions, _ = decide_records(records, policy)
    return decisions


def test_decisions_are_repeatable_for_same_version_seed_and_sequence():
    policy = sample_policy_object(rates={"checkout": {"INFO": 0.5}}, seed=991)
    records = [
        _record(make_log(index, trace_id=f"trace-{index}"), index=index)
        for index in range(200)
    ]
    first = _decide(records, policy)
    second = _decide(records, policy)
    assert decision_digest(first) == decision_digest(second)
    assert [(d.state, d.path, d.effective_weight) for d in first] == [
        (d.state, d.path, d.effective_weight) for d in second
    ]


def test_seed_and_version_change_deterministic_decisions():
    records = [
        _record(make_log(index, trace_id=f"trace-{index}"), index=index)
        for index in range(200)
    ]
    baseline = decision_digest(
        _decide(records, sample_policy_object(version=1, seed=1, rates={"checkout": {"INFO": 0.5}}))
    )
    other_seed = decision_digest(
        _decide(records, sample_policy_object(version=1, seed=2, rates={"checkout": {"INFO": 0.5}}))
    )
    other_version = decision_digest(
        _decide(records, sample_policy_object(version=2, seed=1, rates={"checkout": {"INFO": 0.5}}))
    )
    assert baseline != other_seed
    assert baseline != other_version


def test_dedup_window_boundaries_are_inclusive_and_millisecond_based():
    policy = sample_policy_object(dedup_window_ms=500, tail_base_rate=0.0)
    records = [
        _record(make_log(0, timestamp_ms=1000, trace_id="same", span_id="s0"), index=0),
        _record(make_log(1, timestamp_ms=1500, trace_id="same", span_id="s1"), index=1),
        _record(make_log(2, timestamp_ms=1501, trace_id="same", span_id="s2"), index=2),
    ]
    decisions = _decide(records, policy)
    by_uid = {decision.record_uid: decision for decision in decisions}
    assert by_uid["batch:log:00000000"].duplicate is False
    assert by_uid["batch:log:00000001"].duplicate is True
    assert by_uid["batch:log:00000001"].path == DEDUP_WINDOW
    assert by_uid["batch:log:00000002"].duplicate is False


def test_logs_and_spans_are_deduplicated_per_signal_with_shared_policy_version():
    policy = sample_policy_object(dedup_window_ms=500, tail_base_rate=0.0)
    same_trace = "shared-trace"
    records = [
        _record(make_log(0, timestamp_ms=1000, trace_id=same_trace), index=0, kind="log"),
        _record(make_span(0, timestamp_ms=1000, trace_id=same_trace), index=0, kind="span"),
        _record(make_log(1, timestamp_ms=1100, trace_id=same_trace), index=1, kind="log"),
        _record(make_span(1, timestamp_ms=1100, trace_id=same_trace), index=1, kind="span"),
    ]
    decisions = _decide(records, policy)
    assert sum(1 for decision in decisions if not decision.duplicate) == 2
    assert sum(1 for decision in decisions if decision.duplicate) == 2
    assert {decision.policy_version for decision in decisions} == {policy.version}


def test_optional_global_trace_dedup_counts_one_record_across_signals():
    policy = sample_policy_object(
        dedup_window_ms=500,
        dedup_scope="trace_global",
        tail_base_rate=0.0,
    )
    same_trace = "global-trace"
    records = [
        _record(make_log(0, timestamp_ms=1000, trace_id=same_trace), index=0, kind="log"),
        _record(make_span(0, timestamp_ms=1000, trace_id=same_trace), index=0, kind="span"),
    ]
    decisions = _decide(records, policy)
    assert sum(1 for decision in decisions if not decision.duplicate) == 1
    assert sum(1 for decision in decisions if decision.duplicate) == 1


def _find_promoted(policy) -> Decision:
    for index in range(2_000):
        record = _record(
            make_span(
                index,
                latency_us=200_000,
                trace_id=f"promote-{index}",
                span_id=f"span-{index}",
                sample_weight=2.0,
            ),
            index=index,
            kind="span",
        )
        decision = _decide([record], policy)[0]
        if decision.path == TAIL_PROMOTED:
            return decision
    raise AssertionError("deterministic search did not find a tail promotion")


def test_tail_promotion_adjusts_weight_away_from_head_only_weight():
    policy = sample_policy_object(
        rates={"checkout": {"INFO": 0.5}},
        tail_threshold_us=100_000,
        tail_base_rate=0.5,
        error_multiplier=2.0,
    )
    decision = _find_promoted(policy)
    assert decision.state == EMITTED
    assert decision.head_sampled is False
    assert decision.tail_triggered is True
    assert decision.tail_sampled is True
    assert isclose(decision.inclusion_probability, 0.75)
    assert isclose(decision.effective_weight, 2.0 / 0.75)
    assert not isclose(decision.effective_weight, 2.0 / 0.5)
    assert decision.state_history == ["PENDING", "DROPPED", "EMITTED"]


def test_tail_never_demotes_a_head_sampled_record_and_retains_with_tail_priority():
    policy = sample_policy_object(
        rates={"checkout": {"INFO": 1.0}},
        tail_threshold_us=100_000,
        tail_base_rate=1.0,
        error_multiplier=1.0,
    )
    record = _record(
        make_span(1, latency_us=999_000, error=False, trace_id="retain"), index=1, kind="span"
    )
    decision = _decide([record], policy)[0]
    assert decision.head_sampled is True
    assert decision.tail_sampled is True
    assert decision.state == EMITTED
    assert decision.path == TAIL_RETAINED

    no_tail = sample_policy_object(
        rates={"checkout": {"INFO": 1.0}},
        tail_threshold_us=100_000,
        tail_base_rate=0.0,
        error_multiplier=1.0,
    )
    decision = _decide([record], no_tail)[0]
    assert decision.state == EMITTED
    assert decision.path == HEAD


def test_error_multiplier_increases_tail_inclusion_probability():
    policy = sample_policy_object(
        rates={"checkout": {"ERROR": 0.0}},
        tail_threshold_us=10_000_000,
        tail_base_rate=0.2,
        error_multiplier=5.0,
    )
    error_record = _record(
        make_span(10, latency_us=1, error=True, trace_id="error"), index=10, kind="span"
    )
    decision = _decide([error_record], policy)[0]
    assert decision.tail_triggered is True
    assert decision.tail_sampled is True
    assert isclose(decision.inclusion_probability, 1.0)
    assert decision.path == TAIL_PROMOTED


def test_head_state_history_contains_pending_sampled_emitted():
    policy = sample_policy_object(rates={"checkout": {"INFO": 1.0}}, tail_base_rate=0.0)
    record = _record(make_log(0), index=0)
    decision = _decide([record], policy)[0]
    assert decision.state_history == ["PENDING", "SAMPLED", "EMITTED"]
    assert decision.path == HEAD


def test_kahan_accumulator_preserves_low_order_addition():
    kahan = KahanAccumulator()
    kahan.add(1e16)
    kahan.add(1.0)
    kahan.add(1.0)
    naive = 1e16 + 1.0 + 1.0
    assert kahan.total == 1e16 + 2.0
    assert naive == 1e16
    assert kahan.numeric_error_bound > 0.0
