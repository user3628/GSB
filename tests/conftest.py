from __future__ import annotations

import json
from pathlib import Path

import pytest

from eventbus.service import EventBus


ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture()
def sample() -> dict:
    return json.loads((ROOT / "data" / "sample_events.json").read_text(encoding="utf-8"))


@pytest.fixture()
def bus(tmp_path) -> EventBus:
    return EventBus(tmp_path / "eventbus.db")


def create_sample_topic(bus: EventBus, sample: dict) -> dict:
    topic = sample["topic"]
    return bus.create_topic(
        topic["topic"],
        partitions=topic["partitions"],
        group_id=topic["group_id"],
        min_retained_seq=topic.get("min_retained_seq", 1),
    )


def publish_sample(bus: EventBus, sample: dict, *, batch_id: str | None = None) -> dict:
    return bus.publish(
        sample["topic"]["topic"],
        batch_id,
        sample["events"],
    )


def consume_all(bus: EventBus, sample: dict, *, consumer_id: str = "consumer-all") -> list[dict]:
    topic = sample["topic"]["topic"]
    group = sample["topic"]["group_id"]
    acknowledged = []
    for partition in range(sample["topic"]["partitions"]):
        while True:
            polled = bus.poll(topic, group, partition, consumer_id, lease_seconds=30)
            if polled["event"] is None:
                break
            result = bus.ack(
                topic,
                group,
                partition,
                consumer_id,
                polled["event"]["event_id"],
                polled["lease"]["lease_token"],
            )
            acknowledged.append(
                {
                    "partition": partition,
                    "production_seq": polled["event"]["production_seq"],
                    "event_id": polled["event"]["event_id"],
                    "ack": result,
                }
            )
    return acknowledged
