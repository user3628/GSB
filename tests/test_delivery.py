from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from eventbus.errors import BusinessError


def _topic(bus, *, partitions=1):
    return bus.create_topic("delivery", partitions=partitions, group_id="workers")


def _events(count: int, key: str = "partition-a"):
    return [
        {
            "event_id": f"d-{index}",
            "partition_key": key,
            "event_time": f"2026-02-01T00:00:{index:02d}Z",
            "key": f"group-{index % 2}",
            "measure": float(index + 0.5),
        }
        for index in range(count)
    ]


def test_sequence_delivery_ack_idempotence_and_lease_contention(bus):
    _topic(bus)
    bus.publish("delivery", "batch", _events(3))

    first = bus.poll("delivery", "workers", 0, "consumer-a", lease_seconds=30)
    assert first["event"]["production_seq"] == 1
    assert first["event"]["state"] == "DISPATCHED"
    with pytest.raises(BusinessError) as exc:
        bus.poll("delivery", "workers", 0, "consumer-b", lease_seconds=30)
    assert exc.value.code == "E_LEASE_HELD"

    ack1 = bus.ack(
        "delivery",
        "workers",
        0,
        "consumer-a",
        first["event"]["event_id"],
        first["lease"]["lease_token"],
    )
    assert ack1["idempotent"] is False
    assert ack1["offset"] == 2
    ack2 = bus.ack(
        "delivery",
        "workers",
        0,
        "consumer-a",
        first["event"]["event_id"],
        first["lease"]["lease_token"],
    )
    assert ack2["idempotent"] is True
    assert ack2["offset"] == 2
    assert bus.get_aggregates("delivery", "workers")["groups"] == [
        {"key": "group-0", "count": 1, "sum": 0.5, "compensation": 0.0}
    ]

    second = bus.poll("delivery", "workers", 0, "consumer-b", lease_seconds=30)
    assert second["event"]["production_seq"] == 2
    third = {
        "event_id": "never-dispatched",
        "partition_key": "partition-a",
        "event_time": "2026-02-01T00:00:10Z",
        "key": "group-x",
        "measure": 1,
    }
    bus.publish("delivery", "batch-2", [third])
    with pytest.raises(BusinessError) as exc:
        bus.ack(
            "delivery",
            "workers",
            0,
            "consumer-b",
            "never-dispatched",
            "not-a-lease",
        )
    assert exc.value.code == "E_NOT_DISPATCHED"


def test_lease_timeout_redelivers_same_event_without_skipping(bus):
    _topic(bus)
    bus.publish("delivery", "batch", _events(2))
    first = bus.poll("delivery", "workers", 0, "slow", lease_seconds=0.1)
    assert first["event"]["production_seq"] == 1
    time.sleep(0.2)
    redelivered = bus.poll("delivery", "workers", 0, "fast", lease_seconds=30)
    assert redelivered["event"]["event_id"] == first["event"]["event_id"]
    assert redelivered["event"]["production_seq"] == 1
    assert redelivered["lease"]["attempt"] == 2
    trace = bus.get_event(first["event"]["event_id"])["state_trace"]
    assert [item["to_state"] for item in trace] == [
        "ACCEPTED",
        "DISPATCHED",
        "REDELIVERED",
        "DISPATCHED",
    ]
    assert trace[2]["reason"] == "lease_timeout"


def test_concurrent_poll_has_one_winner(bus):
    _topic(bus)
    bus.publish("delivery", "batch", _events(3))

    def poll(consumer: str):
        try:
            return ("ok", bus.poll("delivery", "workers", 0, consumer, lease_seconds=30))
        except BusinessError as exc:
            return (exc.code, None)

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(poll, [f"c-{index}" for index in range(8)]))
    winners = [value for code, value in results if code == "ok" and value["event"]]
    errors = [code for code, _ in results if code != "ok"]
    assert len(winners) == 1
    assert errors == ["E_LEASE_HELD"] * 7
    active = [
        event
        for event in bus.list_events(topic_id="delivery")
        if event["state"] == "DISPATCHED"
    ]
    assert len(active) == 1


def test_concurrent_duplicate_ack_is_applied_once(bus):
    _topic(bus)
    bus.publish("delivery", "batch", _events(1))
    polled = bus.poll("delivery", "workers", 0, "consumer", lease_seconds=30)

    def ack(_: int):
        return bus.ack(
            "delivery",
            "workers",
            0,
            "consumer",
            polled["event"]["event_id"],
            polled["lease"]["lease_token"],
        )

    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(ack, range(6)))
    assert sum(1 for result in results if not result["idempotent"]) == 1
    assert sum(1 for result in results if result["idempotent"]) == 5
    aggregate = bus.get_aggregates("delivery", "workers")["groups"]
    assert aggregate[0]["count"] == 1
    assert aggregate[0]["sum"] == pytest.approx(0.5)


def test_invalid_transition_does_not_mutate_acked_event(bus):
    _topic(bus)
    bus.publish("delivery", "batch", _events(1))
    polled = bus.poll("delivery", "workers", 0, "consumer", lease_seconds=30)
    bus.ack(
        "delivery",
        "workers",
        0,
        "consumer",
        polled["event"]["event_id"],
        polled["lease"]["lease_token"],
    )
    before = bus.get_event("d-0")
    with pytest.raises(BusinessError) as exc:
        bus.dead_letter("delivery", "workers", 0, "d-0")
    assert exc.value.code == "E_INVALID_TRANSITION"
    after = bus.get_event("d-0")
    assert after["state"] == before["state"] == "ACKED"
    assert after["state_trace"] == before["state_trace"]
