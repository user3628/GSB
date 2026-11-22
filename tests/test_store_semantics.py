from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from mqmw.errors import MWError
from mqmw.store import SQLiteStore


@pytest.fixture
def store(tmp_path):
    return SQLiteStore(str(tmp_path / "store.db"))


def setup_queue(
    store: SQLiteStore,
    queue: str = "q",
    consumer: str = "c",
    group: str = "g",
    endpoint: str = "http://127.0.0.1:9/consume",
    priority: int = 10,
):
    store.register_queue(queue, priority)
    store.register_consumer(consumer, group, endpoint, [queue])


def enqueue_one(
    store: SQLiteStore,
    queue: str = "q",
    message_id: str = "m1",
    *,
    visibility_timeout_ms: int = 10_000,
    max_retries: int = 3,
    body=None,
):
    return store.enqueue(
        queue,
        [
            {
                "message_id": message_id,
                "body": body or {"message_id": message_id},
                "visibility_timeout_ms": visibility_timeout_ms,
                "max_retries": max_retries,
            }
        ],
    )


def test_sequence_is_monotonic_gapless_and_enqueue_is_idempotent(store):
    setup_queue(store)
    first = enqueue_one(store, "q", "m1")
    duplicate = enqueue_one(store, "q", "m1", body={"ignored": "same id wins"})
    second = enqueue_one(store, "q", "m2")
    third = enqueue_one(store, "q", "m3")
    assert first["messages"][0]["seq"] == 1
    assert duplicate["messages"][0]["seq"] == 1
    assert duplicate["messages"][0]["duplicate"] is True
    assert second["messages"][0]["seq"] == 2
    assert third["messages"][0]["seq"] == 3
    snap = store.snapshot("q")
    assert [m["seq"] for m in snap["messages"]] == [1, 2, 3]
    assert snap["queues"][0]["next_seq"] == 4
    assert len(snap["deliveries"]) == 0

    d1 = store.claim("q", "c", "at_least_once")
    d2 = store.claim("q", "c", "at_least_once")
    assert d1 and d1["message_id"] == "m1"
    assert d2 and d2["message_id"] == "m2"
    # The duplicate enqueue changed neither sequence nor delivery history.
    assert len(store.get_message("q", "m1")["deliveries"]) == 1


def test_invalid_transition_has_no_persisted_state_change(store):
    setup_queue(store)
    enqueue_one(store)
    before = store.get_message("q", "m1")
    revision = before["revision"]
    with pytest.raises(MWError) as raised:
        store.transition_state("q", "m1", "ACKED")
    assert raised.value.code == "E_INVALID_TRANSITION"
    after = store.get_message("q", "m1")
    assert after["status"] == "PENDING"
    assert after["deliveries"] == []
    assert after["acks"] == []
    assert after["revision"] == revision


def test_visibility_timeout_redelivers_in_sequence_then_dead_letters(store):
    setup_queue(store)
    enqueue_one(store, "q", "m1", visibility_timeout_ms=5, max_retries=1)
    enqueue_one(store, "q", "m2", visibility_timeout_ms=10_000, max_retries=1)

    first = store.claim("q", "c", "at_least_once")
    assert first and first["message_id"] == "m1"
    time.sleep(0.02)
    reap = store.reap_expired()
    assert reap["redelivered"] == 1
    msg = store.get_message("q", "m1")
    assert msg["status"] == "REDELIVERED"
    assert msg["redelivery_count"] == 1
    assert msg["deliveries"][0]["state"] == "EXPIRED"

    # A redelivery is the next sequential eligible message; it does not skip
    # the later m2 simply because m1 already has a failed delivery.
    second = store.claim("q", "c", "at_least_once")
    assert second and second["message_id"] == "m1"
    assert second["attempt"] == 2
    time.sleep(0.02)
    store.reap_expired()
    assert store.get_message("q", "m1")["status"] == "DEAD_LETTERED"
    assert store.get_message("q", "m1")["deliveries"][-1]["state"] == "EXPIRED"


def test_exactly_once_duplicate_ack_does_not_double_decrement(store):
    setup_queue(store)
    enqueue_one(store, "q", "m1")
    enqueue_one(store, "q", "m2")
    delivery = store.claim("q", "c", "exactly_once")
    assert delivery is not None
    assert store.get_queue_summary("q")["active_deliveries"] == 1
    ack = store.ack(
        delivery["delivery_id"],
        delivery["lease_token"],
        "c",
        "exactly_once",
        fault_point=None,
    )
    assert ack["message_state"] == "ACKED"
    assert store.get_queue_summary("q")["active_deliveries"] == 0
    with pytest.raises(MWError) as raised:
        store.ack(
            delivery["delivery_id"],
            delivery["lease_token"],
            "c",
            "exactly_once",
            fault_point=None,
        )
    assert raised.value.code == "E_DUPLICATE_ACK"
    assert store.get_queue_summary("q")["active_deliveries"] == 0
    assert store.get_message("q", "m1")["status"] == "ACKED"


def test_group_offset_advances_only_over_contiguous_acks(store):
    setup_queue(store)
    for mid in ("m1", "m2", "m3"):
        enqueue_one(store, "q", mid)
    d1 = store.claim("q", "c", "at_least_once")
    d2 = store.claim("q", "c", "at_least_once")
    assert d1 and d2
    store.ack(d2["delivery_id"], d2["lease_token"], "c", "at_least_once", fault_point=None)
    summary = store.get_queue_summary("q")
    assert summary["group_offsets"][0]["next_seq"] == 1
    store.ack(d1["delivery_id"], d1["lease_token"], "c", "at_least_once", fault_point=None)
    summary = store.get_queue_summary("q")
    offset = next(o for o in summary["group_offsets"] if o["group_name"] == "g")
    assert offset["next_seq"] == 3


def test_concurrent_claims_have_exactly_one_winner(store):
    setup_queue(store)
    enqueue_one(store)
    barrier = threading.Barrier(8)

    def claim():
        barrier.wait(timeout=5)
        return store.claim("q", "c", "at_least_once")

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: claim(), range(8)))
    winners = [r for r in results if r is not None]
    assert len(winners) == 1
    assert store.get_message("q", "m1")["status"] == "DISPATCHED"
    assert len(store.get_message("q", "m1")["deliveries"]) == 1


def test_request_id_is_executed_once_and_result_reused(store):
    calls = 0
    lock = threading.Lock()
    barrier = threading.Barrier(2)

    def operation():
        nonlocal calls
        with lock:
            calls += 1
        time.sleep(0.05)
        return {"value": 42}

    def invoke():
        barrier.wait(timeout=5)
        return store.execute_request("same-request", "demo", operation)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: invoke(), range(2)))
    assert calls == 1
    assert [r[0]["value"] for r in results] == [42, 42]
    assert sum(1 for r in results if r[1]) == 1
    with pytest.raises(MWError) as raised:
        store.execute_request("same-request", "other-op", lambda: None)
    assert raised.value.code == "E_REQUEST_CONFLICT"


def test_snapshot_reads_are_consistent_while_writes_commit(store):
    setup_queue(store)
    stop = threading.Event()
    errors: list[BaseException] = []

    def writer():
        try:
            for i in range(30):
                enqueue_one(store, "q", f"w-{i}")
                time.sleep(0.002)
        except BaseException as exc:  # pragma: no cover
            errors.append(exc)
        finally:
            stop.set()

    def reader():
        try:
            while not stop.is_set():
                snap = store.snapshot("q")
                next_seq = next(q["next_seq"] for q in snap["queues"] if q["queue_name"] == "q")
                assert len(snap["messages"]) == next_seq - 1
                assert [m["seq"] for m in snap["messages"]] == list(
                    range(1, next_seq)
                )
        except BaseException as exc:  # pragma: no cover
            errors.append(exc)

    writer_thread = threading.Thread(target=writer)
    readers = [threading.Thread(target=reader) for _ in range(4)]
    writer_thread.start()
    for thread in readers:
        thread.start()
    writer_thread.join(timeout=10)
    for thread in readers:
        thread.join(timeout=10)
    assert not errors
    assert store.get_queue_summary("q")["next_seq"] == 31


def test_missing_queue_is_rejected_before_creating_an_intent(store):
    with pytest.raises(MWError) as enqueue_error:
        enqueue_one(store, "missing-q", "m1")
    assert enqueue_error.value.code == "E_NOT_FOUND"
    with pytest.raises(MWError) as consumer_error:
        store.register_consumer(
            "c-missing", "g-missing", "http://127.0.0.1:9/consume", ["missing-q"]
        )
    assert consumer_error.value.code == "E_NOT_FOUND"
    assert store.list_intents() == []
