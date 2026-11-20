from __future__ import annotations

from dataclasses import replace

from gateway_middleware.config import Settings
from gateway_middleware.windows import EXACT_METHOD, RESERVOIR_METHOD, WindowSeries

from .conftest import event


def _series(new_events, *, settings: Settings | None = None, existing=None) -> WindowSeries:
    settings = settings or replace(Settings(), db_path=__import__("pathlib").Path("unused.sqlite3"))
    return WindowSeries(
        route_key="/api/test",
        client_id="c1",
        existing_rows=existing or [],
        new_events=new_events,
        settings=settings,
        secret=settings.local_secret,
    )


def test_left_open_right_closed_boundary_and_late_event(settings):
    existing = [
        {
            "global_seq": 1,
            "request_id": "old",
            "event_seq": 99,
            "timestamp_ms": 1_000,
            "weight": 1,
            "admitted": True,
        }
    ]
    events = [
        event(seq=0, request_id="at-1500", timestamp_ms=1_500),
        event(seq=1, request_id="at-1999", timestamp_ms=1_999),
        event(seq=2, request_id="at-2000", timestamp_ms=2_000),
    ]
    series = _series(events, settings=settings, existing=existing)

    estimate_1500 = series.estimate("at-1500", "short")
    estimate_1999 = series.estimate("at-1999", "short")
    estimate_2000 = series.estimate("at-2000", "short")

    # (500, 1500]: old@1000 and self. Future events are not visible at this decision point.
    assert estimate_1500.event_count == 2
    # (999, 1999]: old@1000, 1500 and self.
    assert estimate_1999.event_count == 3
    # (1000, 2000]: old@1000 is excluded by the left-open boundary.
    assert estimate_2000.event_count == 3
    assert estimate_2000.exact_weight == 3


def test_bucket_assignment_is_half_open_and_non_overlapping(settings):
    series = _series(
        [
            event(seq=0, request_id="r0", timestamp_ms=1_900),
            event(seq=1, request_id="r1", timestamp_ms=1_999),
            event(seq=2, request_id="r2", timestamp_ms=2_000),
        ],
        settings=settings,
    )
    assert series.bucket_start(1_900, "short") == 1_900
    assert series.bucket_start(1_999, "short") == 1_900
    assert series.bucket_start(2_000, "short") == 2_000
    assert series.bucket_start(1_999, "long") == 1_000
    assert series.bucket_start(2_000, "long") == 2_000


def test_exact_path_is_order_independent_and_integer_only(settings):
    events = [
        event(seq=index, request_id=f"r{index}", timestamp_ms=5_000 + index, weight=(index % 3) + 1)
        for index in range(50)
    ]
    forward = _series(events, settings=settings)
    reverse = _series(list(reversed(events)), settings=settings)

    forward_estimate = forward.estimate("r49", "short")
    reverse_estimate = reverse.estimate("r49", "short")

    assert forward_estimate.method == EXACT_METHOD
    assert forward_estimate.sample_rate == 1.0
    assert forward_estimate.estimated_weight == forward_estimate.exact_weight
    assert forward_estimate.to_dict() == reverse_estimate.to_dict()


def test_sampling_trigger_rate_estimate_and_interval(settings):
    sampling_settings = replace(settings, short_exact_limit=16, long_exact_limit=32)
    events = [
        event(seq=index, request_id=f"r{index}", timestamp_ms=10_000, weight=1 + index % 3)
        for index in range(200)
    ]
    series = _series(events, settings=sampling_settings)

    early = series.estimate("r10", "short")
    sampled = series.estimate("r199", "short")

    assert early.method == EXACT_METHOD
    assert early.event_count == 11
    assert sampled.method == RESERVOIR_METHOD
    assert sampled.event_count == 200
    assert sampled.sample_rate_den > 1
    assert sampled.sample_rate == 1 / sampled.sample_rate_den
    assert sampled.confidence_low <= sampled.exact_weight <= sampled.confidence_high
    assert sampled.confidence_high > sampled.confidence_low
    assert sampled.variance > 0
