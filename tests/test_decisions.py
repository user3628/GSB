from __future__ import annotations

import sqlite3
from dataclasses import replace

import pytest

from gateway_middleware.config import Settings
from gateway_middleware.errors import E_RATE_LIMITED
from gateway_middleware.fixedpoint import FixedPointError, apply_scale, checked_mul_div, damp_toward
from gateway_middleware.models import BatchInput
from gateway_middleware.sample_data import generate_small, seed_dataset
from gateway_middleware.service import GatewayService
from gateway_middleware.windows import EXACT_METHOD

from .conftest import event_for_token, seeded_tiny_service


def test_rate_limit_triggers_both_windows_and_keeps_auth_reason(tmp_path):
    service, token, seeded_version = seeded_tiny_service(tmp_path)
    base = 1_700_000_000_000
    events = [event_for_token(token, seq=index, timestamp_ms=base + 100) for index in range(30)]

    result = service.submit_batch(BatchInput(batch_id="burst", events=events))
    rejected = [decision for decision in result.decisions if decision.decision_code == E_RATE_LIMITED]

    assert rejected
    assert result.allowed_count >= 1
    assert result.rate_limited_count == len(rejected)
    for decision in rejected:
        assert decision.auth_ok is True
        assert decision.auth_code == "AUTH_OK"
        assert decision.auth_reason == "TOKEN_VALID"
        assert "short" in (decision.trigger_window or "")
        assert decision.decision_reasons[:2] == ("AUTH_OK", E_RATE_LIMITED)
        assert decision.short is not None and decision.long is not None
        assert decision.short.estimated_weight > decision.short_quota
        # Dynamic quota is fixed-point and remains inside the configured damping bounds.
        assert 2 <= decision.short_quota <= 7
        assert 10 <= decision.long_quota <= 30
        assert 500_000 <= decision.quota_scale_ppm <= 1_500_000

    first = result.decisions[0]
    assert first.read_version == seeded_version
    assert first.window_version == seeded_version
    assert first.token_version == seeded_version
    assert first.commit_version == seeded_version + 1
    assert all(
        decision.read_version == decision.window_version == decision.token_version
        for decision in result.decisions
    )

    with sqlite3.connect(service.store.db_path) as connection:
        rejected_total = connection.execute(
            "SELECT COUNT(*) FROM window_events WHERE admitted = 0"
        ).fetchone()[0]
    assert rejected_total == len(rejected)

    windows = service.query_windows(
        route_key="/api/test",
        client_id="c1",
        at_ms=base + 100,
    )
    assert windows["short"]["event_count"] == 30
    assert windows["short"]["method"] != EXACT_METHOD
    assert windows["quota"]["scale_ppm"] >= 500_000
    assert windows["window_boundary"].startswith("left_open_right_closed")


def test_exact_path_replay_is_byte_stable_per_decision(small_service):
    service, dataset = small_service
    first = service.submit_batch(dataset.batch("exact-stable"))
    replay = service.submit_batch(dataset.batch("exact-stable"))
    assert replay.replayed is True
    assert replay.commit_version == first.commit_version
    assert replay.summary_dict()["event_count"] == first.summary_dict()["event_count"]
    for left, right in zip(first.decisions, replay.decisions, strict=True):
        assert left.decision_code == right.decision_code
        assert left.allowed == right.allowed
        assert left.short == right.short
        assert left.long == right.long
        assert left.short_quota == right.short_quota
        assert left.long_quota == right.long_quota
        assert left.read_version == right.read_version
        assert left.window_version == right.window_version
        assert left.token_version == right.token_version


def test_input_event_order_does_not_change_exact_decisions(tmp_path):
    def run(order: str, db_name: str):
        settings = replace(
            Settings(),
            db_path=tmp_path / db_name,
            short_exact_limit=10_000,
            long_exact_limit=10_000,
        )
        service = GatewayService(settings)
        dataset = generate_small(settings)
        seed_dataset(service, dataset)
        events = list(dataset.events)
        if order == "reverse":
            events.reverse()
        result = service.submit_batch(BatchInput(batch_id="ordered", events=events))
        return {
            decision.request_id: (
                decision.decision_code,
                decision.short.estimated_weight if decision.short else None,
                decision.long.estimated_weight if decision.long else None,
                decision.short_quota,
                decision.long_quota,
            )
            for decision in result.decisions
        }

    assert run("forward", "forward.sqlite3") == run("reverse", "reverse.sqlite3")


def test_fixed_point_guards_and_scale_bounds():
    with pytest.raises(FixedPointError):
        checked_mul_div((1 << 63) - 1, 2, 1)
    with pytest.raises(FixedPointError):
        checked_mul_div(1, 1, 0)
    assert damp_toward(1_000_000, 500_000, 200_000) == 900_000
    assert damp_toward(1_000_000, 1_500_000, 200_000) == 1_100_000
    assert apply_scale(100, 2_000_000, min_scale=500_000, max_scale=1_500_000) == 150
    with pytest.raises(FixedPointError):
        checked_mul_div(100, -1, 1)


def test_historical_window_version_reads_historical_buckets_and_quota(tmp_path):
    service, token, seeded_version = seeded_tiny_service(
        tmp_path, short_limit=100, long_limit=1_000
    )
    base = 1_700_000_000_000
    first_events = [
        event_for_token(token, seq=index, timestamp_ms=base + 10 + index)
        for index in range(3)
    ]
    second_events = [
        event_for_token(token, seq=index, timestamp_ms=base + 2_000 + index).model_copy(
            update={"request_id": f"second-{index}"}
        )
        for index in range(4)
    ]
    first = service.submit_batch(BatchInput(batch_id="history-1", events=first_events))
    second = service.submit_batch(BatchInput(batch_id="history-2", events=second_events))

    historical = service.query_windows(
        route_key="/api/test",
        client_id="c1",
        version=first.commit_version,
        at_ms=base + 20,
    )
    current = service.query_windows(
        route_key="/api/test",
        client_id="c1",
        version=second.commit_version,
        at_ms=base + 2_010,
    )

    assert historical["snapshot_version"] == first.commit_version
    assert historical["short"]["event_count"] == 3
    assert sum(bucket["event_count"] for bucket in historical["buckets"] if bucket["window_name"] == "short") == 3
    assert current["short"]["event_count"] == 4
    assert historical["quota"]["scale_ppm"] != 0


def test_token_registry_is_empty_before_its_seed_snapshot(small_service):
    service, _ = small_service
    assert service.store.load_auth_registry(0).tokens == {}
    assert service.snapshot_info(0)["kind"] == "genesis"
    assert service.snapshot_info(1)["kind"] == "token_seed"
