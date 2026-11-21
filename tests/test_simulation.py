from __future__ import annotations

from copy import deepcopy

import pytest

from qos_workbench.diagnostics import build_diagnostics
from qos_workbench.errors import BusinessError
from qos_workbench.models import Policy, PolicySnapshot
from qos_workbench.simulation import simulate_snapshot


def traffic():
    return [
        {"timestamp_ms": 0, "queue_id": "q", "packet_bytes": 1000, "color": "green"},
        {"timestamp_ms": 0, "queue_id": "q", "packet_bytes": 1000, "color": "green"},
        {"timestamp_ms": 0, "queue_id": "q", "packet_bytes": 1000, "color": "green"},
        {"timestamp_ms": 1000, "queue_id": "q", "packet_bytes": 1000, "color": "green"},
        {"timestamp_ms": 1000, "queue_id": "q", "packet_bytes": 1000, "color": "green"},
        {"timestamp_ms": 1000, "queue_id": "q", "packet_bytes": 1000, "color": "green"},
    ]


def test_dual_token_bucket_packet_decisions(small_snapshot):
    result = simulate_snapshot(small_snapshot, traffic())
    packets = result["packets"]
    assert [packet["egress_color"] for packet in packets] == [
        "green",
        "yellow",
        "red",
        "green",
        "yellow",
        "red",
    ]
    assert [packet["action"] for packet in packets] == [
        "send",
        "send",
        "drop",
        "send",
        "send",
        "drop",
    ]
    assert packets[0]["committed_tokens_after_units"] == 0
    assert packets[0]["peak_tokens_after_units"] == 8_000_000_000
    assert packets[1]["committed_tokens_after_units"] == 0
    assert packets[1]["peak_tokens_after_units"] == 0
    stats = result["per_queue"]["q"]
    assert stats["sent_bytes"] == 4_000
    assert stats["dropped_bytes"] == 2_000
    assert stats["green_bytes"] == 2_000
    assert stats["yellow_bytes"] == 2_000
    assert stats["red_bytes"] == 2_000
    assert stats["marked_bytes"] == 4_000
    assert result["token_model"]["rounding"] == "no token rounding; integer arithmetic only"


def test_repeated_simulation_is_packet_for_packet_identical(small_snapshot):
    first = simulate_snapshot(small_snapshot, deepcopy(traffic()))
    second = simulate_snapshot(small_snapshot, deepcopy(traffic()))
    assert first["packets"] == second["packets"]
    assert first["per_queue"] == second["per_queue"]
    assert first["sample_sha256"] == second["sample_sha256"]


def test_arrival_yellow_and_red_never_become_green(small_snapshot):
    result = simulate_snapshot(
        small_snapshot,
        [
            {"timestamp_ms": 0, "queue_id": "q", "packet_bytes": 1000, "color": "yellow"},
            {"timestamp_ms": 1, "queue_id": "q", "packet_bytes": 1000, "color": "red"},
        ],
    )
    assert result["packets"][0]["egress_color"] == "yellow"
    assert result["packets"][1]["egress_color"] == "red"
    assert result["packets"][1]["reason"] == "arrival_red"


def test_out_of_order_queue_timestamps_are_rejected(small_snapshot):
    with pytest.raises(BusinessError) as caught:
        simulate_snapshot(
            small_snapshot,
            [
                {"timestamp_ms": 5, "queue_id": "q", "packet_bytes": 10, "color": "green"},
                {"timestamp_ms": 4, "queue_id": "q", "packet_bytes": 10, "color": "green"},
            ],
        )
    assert caught.value.code == "E_SAMPLE_INVALID"


def test_unknown_sample_queue_is_rejected(small_snapshot):
    with pytest.raises(BusinessError) as caught:
        simulate_snapshot(
            small_snapshot,
            [{"timestamp_ms": 0, "queue_id": "missing", "packet_bytes": 10, "color": "green"}],
        )
    assert caught.value.code == "E_SAMPLE_INVALID"
    assert caught.value.details["unknown_queue_ids"] == ["missing"]


def test_diagnostics_uses_decimal_tolerance_and_fixed_precision(small_snapshot):
    result = simulate_snapshot(small_snapshot, traffic())
    report = build_diagnostics(small_snapshot, result, relative_tolerance="0.10")
    entry = report["queues"][0]
    assert entry["classification"] == "over"
    assert entry["actual_sent_rate_bps"] == "32000.000000"
    assert entry["deviation_bps"] == "24000.000000"
    assert entry["deviation_ratio"] == "3.000000"
    assert report["over_shaped_queue_ids"] == ["q"]
    assert report["precision_contract"]["decimal_places"] == 6
    assert report["precision_contract"]["rounding_mode"] == "ROUND_HALF_UP"

    tolerant = build_diagnostics(small_snapshot, result, relative_tolerance="4.0")
    assert tolerant["queues"][0]["classification"] == "stable"
    assert tolerant["over_shaped_queue_ids"] == []


def test_diagnostics_marks_dormant_queues_as_no_traffic():
    snapshot = PolicySnapshot(
        3,
        1,
        1,
        (
            Policy("active", 8_000, 16_000, 1_000, 2_000, 1, "all", None, 1),
            Policy("idle", 1_000, 2_000, 1_000, 2_000, 2, "all", None, 1),
        ),
    )
    simulation = simulate_snapshot(
        snapshot,
        [{"timestamp_ms": 0, "queue_id": "active", "packet_bytes": 100, "color": "green"}],
    )
    report = build_diagnostics(snapshot, simulation)
    assert report["no_traffic_queue_ids"] == ["idle"]
    assert "idle" not in report["under_shaped_queue_ids"]
    assert [entry["queue_id"] for entry in report["queues"]] == ["active", "idle"]

