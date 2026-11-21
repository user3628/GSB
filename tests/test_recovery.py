from __future__ import annotations

import sqlite3

import pytest

from eventbus.service import EventBus
from eventbus.verifier import StateVerifier
from tests.conftest import consume_all, create_sample_topic, publish_sample


class SimulatedCrash(RuntimeError):
    pass


def test_crash_after_ack_intent_recovers_to_same_aggregate(tmp_path, sample):
    db_path = tmp_path / "crash.db"
    bus = EventBus(db_path)
    create_sample_topic(bus, sample)
    publish_sample(bus, sample)
    partition = 0
    first = bus.poll("orders", "analytics", partition, "consumer", lease_seconds=30)
    first_ack = bus.ack(
        "orders",
        "analytics",
        partition,
        "consumer",
        first["event"]["event_id"],
        first["lease"]["lease_token"],
    )
    second = bus.poll("orders", "analytics", partition, "consumer", lease_seconds=30)
    aggregate_before_crash = bus.get_aggregates("orders", "analytics")["groups"]
    offset_before_crash = bus.get_offset("orders", "analytics", partition)["offset"]
    bus.test_hooks = True
    bus.crash_hook = lambda: (_ for _ in ()).throw(SimulatedCrash("forced stop"))
    with pytest.raises(SimulatedCrash):
        bus.ack(
            "orders",
            "analytics",
            partition,
            "consumer",
            second["event"]["event_id"],
            second["lease"]["lease_token"],
            crash_after_intent=True,
        )

    with sqlite3.connect(db_path) as connection:
        pending = connection.execute(
            "SELECT COUNT(*) FROM intents WHERE status = 'PENDING'"
        ).fetchone()[0]
        state = connection.execute(
            "SELECT state FROM events WHERE event_id = ?", (second["event"]["event_id"],)
        ).fetchone()[0]
    assert pending == 1
    assert state == "DISPATCHED"
    assert bus.get_offset("orders", "analytics", partition)["offset"] == offset_before_crash

    recovered = EventBus(db_path)
    report = recovered.get_latest_recovery_report()
    assert report["pending_intents_found"] == 1
    assert report["pending_intents_aborted"][0]["kind"] == "ACK"
    assert report["released_leases"][0]["event_id"] == second["event"]["event_id"]
    assert recovered.get_aggregates("orders", "analytics")["groups"] == aggregate_before_crash
    assert recovered.get_offset("orders", "analytics", partition)["offset"] == offset_before_crash
    assert recovered.get_event(second["event"]["event_id"])["state"] == "REDELIVERED"

    resumed = recovered.poll(
        "orders", "analytics", partition, "resumed-consumer", lease_seconds=30
    )
    assert resumed["event"]["event_id"] == second["event"]["event_id"]
    assert resumed["event"]["production_seq"] == second["event"]["production_seq"]
    recovered.ack(
        "orders",
        "analytics",
        partition,
        "resumed-consumer",
        resumed["event"]["event_id"],
        resumed["lease"]["lease_token"],
    )
    consume_all(recovered, sample, consumer_id="resumed-consumer")

    reference = EventBus(tmp_path / "reference.db")
    create_sample_topic(reference, sample)
    publish_sample(reference, sample)
    consume_all(reference, sample, consumer_id="reference-consumer")
    assert recovered.get_aggregates("orders", "analytics")["groups"] == reference.get_aggregates(
        "orders", "analytics"
    )["groups"]
    assert StateVerifier(db_path).run()["ok"] is True
    assert StateVerifier(reference.db.path).run()["ok"] is True
    assert first_ack["offset"] == 2


def test_restart_repairs_corrupt_aggregate_from_confirmations(tmp_path, sample):
    db_path = tmp_path / "repair.db"
    bus = EventBus(db_path)
    create_sample_topic(bus, sample)
    publish_sample(bus, sample)
    consume_all(bus, sample)
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            "UPDATE aggregate_state SET event_count = 999, measure_sum = -12345"
        )
        connection.commit()
    assert StateVerifier(db_path).run()["ok"] is False
    recovered = EventBus(db_path)
    report = recovered.get_latest_recovery_report()
    assert report["aggregate_keys_rebuilt"] == 4
    assert recovered.get_aggregates("orders", "analytics")["groups"] == list(
        sample["expected"]["aggregate_by_key"].values()
    )
    assert StateVerifier(db_path).run()["ok"] is True
