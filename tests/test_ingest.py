from __future__ import annotations

import copy

import pytest

from eventbus.errors import BusinessError
from eventbus.hashing import partition_for
from tests.conftest import create_sample_topic, publish_sample


def test_topic_and_partition_routing_are_deterministic(bus, sample):
    topic = create_sample_topic(bus, sample)
    assert topic["created"] is True
    assert [item["next_seq"] for item in topic["partition_state"]] == [1, 1, 1]
    assert create_sample_topic(bus, sample)["created"] is False

    result = publish_sample(bus, sample)
    assert result["accepted"] == 24
    assert result["duplicates"] == 0
    for event, response in zip(sample["events"], result["events"], strict=True):
        expected_partition = partition_for(
            sample["topic"]["topic"], event["partition_key"], 3
        )
        assert response["partition"] == expected_partition
        assert response["production_seq"] == sample["expected"]["sequence_by_event_id"][
            event["event_id"]
        ]

    counts = sample["expected"]["partition_counts"]
    fetched = bus.get_topic(sample["topic"]["topic"])
    assert [item["event_count"] for item in fetched["partition_state"]] == [
        counts["0"],
        counts["1"],
        counts["2"],
    ]


def test_exact_republish_is_idempotent_and_conflict_preserves_first(bus, sample):
    create_sample_topic(bus, sample)
    first = publish_sample(bus, sample)
    seq_before = [
        part["next_seq"] for part in bus.get_topic("orders")["partition_state"]
    ]
    duplicate = publish_sample(bus, sample, batch_id="retry-in-another-batch")
    assert duplicate["accepted"] == 0
    assert duplicate["duplicates"] == 24
    assert duplicate["events"][0]["body_digest"] == first["events"][0]["body_digest"]
    assert [
        part["next_seq"] for part in bus.get_topic("orders")["partition_state"]
    ] == seq_before

    conflicting_event = copy.deepcopy(sample["events"][0])
    conflicting_event["measure"] = 999999.0
    with pytest.raises(BusinessError) as exc:
        bus.publish("orders", "conflict-batch", [conflicting_event])
    assert exc.value.code == "E_CONFLICTING_EVENT"
    assert exc.value.context["event_id"] == "evt-001"
    original = bus.get_event("evt-001")
    assert original["measure"] == sample["events"][0]["measure"]
    assert [
        part["next_seq"] for part in bus.get_topic("orders")["partition_state"]
    ] == seq_before


def test_invalid_batch_is_atomic_and_not_persisted(bus, sample):
    create_sample_topic(bus, sample)
    seq_before = [1, 1, 1]
    invalid = copy.deepcopy(sample["events"][0])
    invalid["event_id"] = "atomic-valid"
    broken = copy.deepcopy(sample["events"][1])
    broken["event_id"] = "atomic-invalid"
    broken["measure"] = "not-a-number"
    with pytest.raises(BusinessError) as exc:
        bus.publish("orders", "bad-batch", [invalid, broken])
    assert exc.value.code == "E_EVENT_INVALID"
    assert [part["next_seq"] for part in bus.get_topic("orders")["partition_state"]] == seq_before
    assert bus.list_events(topic_id="orders") == []


def test_body_digest_mismatch_is_event_invalid(bus, sample):
    create_sample_topic(bus, sample)
    event = copy.deepcopy(sample["events"][0])
    event["body_digest"] = "0" * 64
    with pytest.raises(BusinessError) as exc:
        bus.publish("orders", "bad-digest", [event])
    assert exc.value.code == "E_EVENT_INVALID"
    assert bus.list_events(topic_id="orders") == []


def test_same_event_id_twice_in_one_batch_is_counted_once(bus, sample):
    create_sample_topic(bus, sample)
    event = sample["events"][0]
    result = bus.publish("orders", "one-batch", [event, event])
    assert result["accepted"] == 1
    assert result["duplicates"] == 1
    assert len(bus.list_events(topic_id="orders")) == 1
