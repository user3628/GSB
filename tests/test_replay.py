from __future__ import annotations

import pytest

from eventbus.errors import BusinessError
from eventbus.hashing import partition_for
from eventbus.aggregation import NeumaierSum
from eventbus.verifier import StateVerifier
from tests.conftest import consume_all, create_sample_topic, publish_sample


def expected_aggregates_for_events(
    sample: dict, event_ids: set[str], extra_events: list[dict] | None = None
) -> list[dict]:
    routed = {
        event_id: partition
        for partition, event_ids_in_partition in sample["expected"][
            "partition_assignments"
        ].items()
        for event_id in event_ids_in_partition
        for partition in [int(partition)]
    }
    selected = [event for event in sample["events"] if event["event_id"] in event_ids]
    selected.extend(extra_events or [])

    def measure(event):
        if "measure" in event:
            return event["measure"]
        return event.get("body", {}).get("measure")

    selected.sort(
        key=lambda event: (
            event.get("key") or event["body"]["key"],
            routed.get(event["event_id"], 0),
            sample["expected"]["sequence_by_event_id"].get(event["event_id"], 10**9),
        )
    )
    grouped: dict[str, tuple[int, NeumaierSum]] = {}
    for event in selected:
        key = event.get("key") or event["body"]["key"]
        if key not in grouped:
            grouped[key] = (0, NeumaierSum())
        count, acc = grouped[key]
        acc.add(measure(event))
        grouped[key] = (count + 1, acc)
    return [
        {
            "key": key,
            "count": grouped[key][0],
            "sum": grouped[key][1].value,
            "compensation": grouped[key][1].compensation,
        }
        for key in sorted(grouped)
    ]


def test_rewind_rolls_back_aggregate_and_replays_in_order(bus, sample):
    create_sample_topic(bus, sample)
    publish_sample(bus, sample)
    consume_all(bus, sample)
    full = bus.get_aggregates("orders", "analytics")["groups"]
    assert full == list(sample["expected"]["aggregate_by_key"].values())
    assert StateVerifier(bus.db.path).run()["ok"] is True

    partition_zero_ids = set(sample["expected"]["partition_assignments"]["0"])
    result = bus.rewind_offset("orders", "analytics", 0, 1)
    assert result["old_offset"] == 9
    assert result["offset"] == 1
    assert result["replaying"] is True
    assert result["replay_end"] == 9
    assert result["rolled_back_confirmations"] == 8
    rolled_back = bus.get_aggregates("orders", "analytics")["groups"]
    remaining_ids = {
        event["event_id"]
        for event in sample["events"]
        if event["event_id"] not in partition_zero_ids
    }
    assert rolled_back == expected_aggregates_for_events(sample, remaining_ids)
    assert StateVerifier(bus.db.path).run()["ok"] is True

    new_key = next(
        f"during-replay-{index}"
        for index in range(10000)
        if partition_for("orders", f"during-replay-{index}", 3) == 0
    )
    bus.publish(
        "orders",
        "during-replay-batch",
        [
            {
                "event_id": "during-replay",
                "partition_key": new_key,
                "event_time": "2026-01-15T09:00:00Z",
                "key": "alpha",
                "measure": 7.0,
            }
        ],
    )
    delivered_sequences = []
    for _ in range(8):
        polled = bus.poll("orders", "analytics", 0, "replayer", lease_seconds=30)
        assert polled["event"] is not None
        assert polled["event"]["event_id"] != "during-replay"
        delivered_sequences.append(polled["event"]["production_seq"])
        bus.ack(
            "orders",
            "analytics",
            0,
            "replayer",
            polled["event"]["event_id"],
            polled["lease"]["lease_token"],
        )
    assert delivered_sequences == list(range(1, 9))
    after_replay_boundary = bus.poll("orders", "analytics", 0, "replayer", lease_seconds=30)
    assert after_replay_boundary["event"]["event_id"] == "during-replay"
    assert after_replay_boundary["event"]["production_seq"] == 9
    assert after_replay_boundary["replaying"] is False
    bus.ack(
        "orders",
        "analytics",
        0,
        "replayer",
        "during-replay",
        after_replay_boundary["lease"]["lease_token"],
    )
    final = bus.get_aggregates("orders", "analytics")["groups"]
    expected_final = expected_aggregates_for_events(
        sample,
        {event["event_id"] for event in sample["events"]},
        extra_events=[
            {
                "event_id": "during-replay",
                "partition_key": new_key,
                "key": "alpha",
                "measure": 7.0,
            }
        ],
    )
    assert final == expected_final
    assert StateVerifier(bus.db.path).run()["ok"] is True


def test_rewind_gap_and_not_found_codes_do_not_change_offset(bus, sample):
    create_sample_topic(bus, sample)
    publish_sample(bus, sample)
    before = bus.get_offset("orders", "analytics", 0)
    with pytest.raises(BusinessError) as expired:
        bus.rewind_offset("orders", "analytics", 0, 0)
    assert expired.value.code == "E_OFFSET_EXPIRED"
    with pytest.raises(BusinessError) as missing:
        bus.rewind_offset("orders", "analytics", 0, 9999)
    assert missing.value.code == "E_OFFSET_NOT_FOUND"
    after = bus.get_offset("orders", "analytics", 0)
    assert after["offset"] == before["offset"]
    assert after["version"] == before["version"]
