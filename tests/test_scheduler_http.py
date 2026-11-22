from __future__ import annotations

import json
import time

import requests

from mqmw.constants import ACKED, DEAD_LETTERED, DISPATCHED, PENDING, REDELIVERED


def setup(store, queue, consumer, group, endpoint, priority=10):
    store.register_queue(queue, priority)
    store.register_consumer(consumer, group, endpoint, [queue])


def enqueue(store, queue, prefix, count, *, visibility_timeout_ms=5_000, max_retries=2):
    return store.enqueue(
        queue,
        [
            {
                "message_id": f"{prefix}-{i:03d}",
                "body": {"id": i},
                "visibility_timeout_ms": visibility_timeout_ms,
                "max_retries": max_retries,
            }
            for i in range(1, count + 1)
        ],
    )


def run_round(base, request_id, mode, config, consumer_id=None):
    body = {"request_id": request_id, "mode": mode, "config": config}
    if consumer_id:
        body["consumer_id"] = consumer_id
    return requests.post(base + "/v1/rounds", json=body, timeout=30)


def test_exactly_once_auto_ack_trace_and_duplicate_ack(mw_server, consumer_server):
    store = mw_server["store"]
    endpoint = consumer_server["base"] + "/consume?behavior=success&auto_ack=true"
    setup(store, "pay", "pay-c", "pay-g", endpoint)
    enqueue(store, "pay", "p", 2)
    config = {
        "global_concurrency": 2,
        "per_queue_inflight": 2,
        "time_budget_ms": 700,
        "dispatch_timeout_ms": 300,
        "wait_queue_capacity": 4,
    }
    response = run_round(
        mw_server["base"], "exact-round-1", "exactly_once", config, consumer_id="pay-c"
    )
    assert response.status_code == 200
    result = response.json()
    assert result["status"] == "OK"
    assert result["claimed"] == 2
    assert result["http_accepted"] == 2
    assert result["acks_observed"] == 2
    assert result["per_queue"]["pay"]["acked"] == 2

    trace_events = [t["event"] for t in result["traces"]]
    assert "CLAIMED" in trace_events
    assert "HTTP_ACCEPTED" in trace_events
    assert "ACKED" in trace_events
    assert trace_events[-1] == "ROUND_END"
    stored_round = requests.get(
        mw_server["base"] + "/v1/rounds/" + result["round_id"], timeout=5
    ).json()["round"]
    assert [t["event"] for t in stored_round["traces"]][-1] == "ROUND_END"

    summary = store.get_queue_summary("pay")
    assert summary["active_deliveries"] == 0
    assert summary["status_counts"][ACKED] == 2

    snap = requests.get(
        mw_server["base"] + "/v1/snapshot", params={"queue": "pay"}, timeout=5
    ).json()
    delivery = snap["deliveries"][0]
    duplicate = requests.post(
        mw_server["base"] + "/v1/acks",
        json={
            "request_id": "duplicate-ack-1",
            "delivery_id": delivery["delivery_id"],
            "lease_token": delivery["lease_token"],
            "consumer_id": "pay-c",
            "mode": "exactly_once",
        },
        timeout=5,
    )
    assert duplicate.status_code == 409
    assert duplicate.json()["error"]["code"] == "E_DUPLICATE_ACK"
    assert store.get_queue_summary("pay")["active_deliveries"] == 0

    replay = run_round(
        mw_server["base"], "exact-round-1", "exactly_once", config, consumer_id="pay-c"
    ).json()
    assert replay["replayed"] is True
    assert replay["round_id"] == result["round_id"]
    assert len(store.snapshot("pay")["deliveries"]) == 2


def test_refusal_transitions_to_dead_letter(mw_server, consumer_server):
    store = mw_server["store"]
    endpoint = consumer_server["base"] + "/consume?behavior=refuse&refuse_status=409"
    setup(store, "retry-q", "retry-c", "retry-g", endpoint)
    enqueue(store, "retry-q", "r", 1, max_retries=0)
    result = run_round(
        mw_server["base"],
        "refuse-round-1",
        "at_least_once",
        {
            "global_concurrency": 1,
            "per_queue_inflight": 1,
            "time_budget_ms": 500,
            "dispatch_timeout_ms": 200,
            "wait_queue_capacity": 1,
        },
        consumer_id="retry-c",
    ).json()
    assert result["dispatch_refusals"] == 1
    assert result["dead_lettered"] == 1
    assert "E_DISPATCH_REFUSED" in result["error_codes"]
    message = store.get_message("retry-q", "r-001")
    assert message["status"] == DEAD_LETTERED
    assert message["deliveries"][0]["state"] == "REFUSED"


def test_hanging_endpoint_times_out_without_blocking_other_queue(mw_server, consumer_server):
    store = mw_server["store"]
    hang_endpoint = consumer_server["base"] + "/consume?behavior=hang&sleep_ms=2000"
    ok_endpoint = (
        consumer_server["base"]
        + "/consume?behavior=success&delay_ms=80&auto_ack=true"
    )
    setup(store, "hang-q", "hang-c", "hang-g", hang_endpoint, priority=1)
    setup(store, "ok-q", "ok-c", "ok-g", ok_endpoint, priority=2)
    enqueue(store, "hang-q", "h", 1)
    enqueue(store, "ok-q", "o", 1)
    started = time.monotonic()
    result = run_round(
        mw_server["base"],
        "hang-round-1",
        "at_least_once",
        {
            "global_concurrency": 2,
            "per_queue_inflight": 1,
            "time_budget_ms": 1000,
            "dispatch_timeout_ms": 400,
            "wait_queue_capacity": 2,
        },
    ).json()
    elapsed = time.monotonic() - started
    assert elapsed < 1.4, elapsed
    assert result["dispatch_timeouts"] == 1
    assert result["http_accepted"] == 1
    timeout_outcome = next(o for o in result["outcomes"] if o["outcome"] == "TIMED_OUT")
    assert timeout_outcome["connection_closed"] is True
    assert timeout_outcome["error_code"] == "E_DISPATCH_TIMEOUT"
    assert store.get_message("hang-q", "h-001")["status"] == REDELIVERED
    assert store.get_message("ok-q", "o-001")["status"] == ACKED
    consumer_state = requests.get(consumer_server["base"] + "/state", timeout=5).json()
    assert consumer_state["max_active_requests"] >= 2
    assert "E_DISPATCH_TIMEOUT" in result["error_codes"]


def test_global_and_per_queue_concurrency_limits(mw_server, consumer_server):
    store = mw_server["store"]
    endpoint = consumer_server["base"] + "/consume?behavior=success&delay_ms=120"
    setup(store, "a-q", "a-c", "a-g", endpoint, priority=1)
    setup(store, "b-q", "b-c", "b-g", endpoint, priority=1)
    enqueue(store, "a-q", "a", 3)
    enqueue(store, "b-q", "b", 3)
    result = run_round(
        mw_server["base"],
        "limits-round-1",
        "at_least_once",
        {
            "global_concurrency": 2,
            "per_queue_inflight": 1,
            "time_budget_ms": 1200,
            "dispatch_timeout_ms": 1000,
            "wait_queue_capacity": 6,
        },
    ).json()
    assert result["claimed"] == 2
    assert result["http_accepted"] == 2
    assert result["per_queue"]["a-q"]["inflight_at_end"] == 1
    assert result["per_queue"]["b-q"]["inflight_at_end"] == 1
    assert sum(1 for s in result["skipped"] if s["reason"] == "E_PER_QUEUE_INFLIGHT_LIMIT") == 4
    consumer_state = requests.get(consumer_server["base"] + "/state", timeout=5).json()
    assert consumer_state["max_active_requests"] == 2
    statuses = [
        store.get_message("a-q", f"a-{i:03d}")["status"] for i in range(1, 4)
    ] + [store.get_message("b-q", f"b-{i:03d}")["status"] for i in range(1, 4)]
    assert statuses.count(DISPATCHED) == 2
    assert statuses.count(PENDING) == 4


def test_priority_and_bounded_admission_queue_full(mw_server, consumer_server):
    store = mw_server["store"]
    endpoint = consumer_server["base"] + "/consume?behavior=success&auto_ack=true"
    setup(store, "high-q", "high-c", "high-g", endpoint, priority=1)
    setup(store, "low-q", "low-c", "low-g", endpoint, priority=2)
    enqueue(store, "high-q", "high", 1)
    enqueue(store, "low-q", "low", 1)
    result = run_round(
        mw_server["base"],
        "priority-round-1",
        "at_least_once",
        {
            "global_concurrency": 1,
            "per_queue_inflight": 1,
            "time_budget_ms": 500,
            "dispatch_timeout_ms": 200,
            "wait_queue_capacity": 0,
        },
    ).json()
    assert result["claimed"] == 1
    assert result["queue_order"][0]["priority"] == 1
    assert result["queue_order"][0]["rotation_order"][0]["queue_name"] == "high-q"
    assert result["skipped"][0]["queue_name"] == "low-q"
    assert result["skipped"][0]["reason"] == "E_QUEUE_FULL"
    assert result["skipped"][0]["priority"] == 2
    assert store.get_message("high-q", "high-001")["status"] == ACKED
    assert store.get_message("low-q", "low-001")["status"] == PENDING


def test_strict_queue_full_returns_429_without_dispatch(mw_server, consumer_server):
    store = mw_server["store"]
    endpoint = consumer_server["base"] + "/consume?behavior=success&auto_ack=true"
    setup(store, "full-q", "full-c", "full-g", endpoint)
    enqueue(store, "full-q", "f", 5)
    response = run_round(
        mw_server["base"],
        "strict-full-round-1",
        "at_least_once",
        {
            "global_concurrency": 1,
            "per_queue_inflight": 1,
            "time_budget_ms": 500,
            "dispatch_timeout_ms": 200,
            "wait_queue_capacity": 1,
            "strict_wait_queue": True,
        },
        consumer_id="full-c",
    )
    assert response.status_code == 429
    result = response.json()
    assert result["error"]["code"] == "E_QUEUE_FULL"
    assert result["dispatch_attempts"] == 0
    snapshot = store.snapshot("full-q")
    assert len(snapshot["deliveries"]) == 0
    assert all(m["status"] == PENDING for m in snapshot["messages"])


def test_time_budget_exhaustion_is_deterministic_and_keeps_messages_pending(
    mw_server, consumer_server
):
    store = mw_server["store"]
    endpoint = consumer_server["base"] + "/consume?behavior=success&auto_ack=true"
    setup(store, "budget-q", "budget-c", "budget-g", endpoint)
    enqueue(store, "budget-q", "budget", 4)
    result = run_round(
        mw_server["base"],
        "budget-round-1",
        "at_least_once",
        {
            "global_concurrency": 1,
            "per_queue_inflight": 1,
            "time_budget_ms": 1,
            "dispatch_timeout_ms": 200,
            "wait_queue_capacity": 8,
        },
        consumer_id="budget-c",
    ).json()
    assert result["claimed"] == 0
    assert result["budget_exhausted"] is True
    assert "E_TIME_BUDGET_EXHAUSTED" in result["error_codes"]
    assert result["degradation_order"]
    assert all(
        s["reason"] in {"E_TIME_BUDGET_EXHAUSTED", "E_QUEUE_FULL"}
        for s in result["degradation_order"]
    )
    assert [s["seq"] for s in result["degradation_order"]] == sorted(
        s["seq"] for s in result["degradation_order"]
    )
    assert all(m["status"] == PENDING for m in store.snapshot("budget-q")["messages"])


def test_round_wall_time_is_bounded_by_remaining_budget(mw_server, consumer_server):
    store = mw_server["store"]
    endpoint = consumer_server["base"] + "/consume?behavior=hang&sleep_ms=2000"
    setup(store, "deadline-q", "deadline-c", "deadline-g", endpoint)
    enqueue(store, "deadline-q", "d", 1)
    started = time.monotonic()
    result = run_round(
        mw_server["base"],
        "deadline-round-1",
        "at_least_once",
        {
            "global_concurrency": 1,
            "per_queue_inflight": 1,
            "time_budget_ms": 120,
            "dispatch_timeout_ms": 1000,
            "wait_queue_capacity": 1,
        },
        consumer_id="deadline-c",
    ).json()
    elapsed = time.monotonic() - started
    assert result["claimed"] == 1
    assert result["dispatch_timeouts"] == 1
    assert result["budget_limited_dispatches"] == 1
    assert elapsed < 0.8, elapsed
    timeout_outcome = next(o for o in result["outcomes"] if o["outcome"] == "TIMED_OUT")
    assert timeout_outcome["effective_timeout_ms"] < 1000
    assert timeout_outcome["connection_closed"] is True


def test_per_queue_blocked_candidate_is_reconsidered_after_lease_release(
    mw_server, consumer_server
):
    store = mw_server["store"]
    endpoint = consumer_server["base"] + "/consume?behavior=success&delay_ms=40&auto_ack=true"
    setup(store, "hot-q", "hot-c", "hot-g", endpoint, priority=1)
    setup(store, "tail-q", "tail-c", "tail-g", endpoint, priority=2)
    enqueue(store, "hot-q", "hot", 2)
    enqueue(store, "tail-q", "tail", 1)
    result = run_round(
        mw_server["base"],
        "requeue-round-1",
        "at_least_once",
        {
            "global_concurrency": 2,
            "per_queue_inflight": 1,
            "time_budget_ms": 1000,
            "dispatch_timeout_ms": 600,
            "wait_queue_capacity": 4,
        },
    ).json()
    assert result["claimed"] == 3
    assert result["http_accepted"] == 3
    assert result["acks_observed"] == 3
    assert store.get_message("hot-q", "hot-001")["status"] == ACKED
    assert store.get_message("hot-q", "hot-002")["status"] == ACKED
    assert store.get_message("tail-q", "tail-001")["status"] == ACKED


def test_equal_priority_queue_rotation_is_persisted(mw_server, consumer_server):
    store = mw_server["store"]
    endpoint = consumer_server["base"] + "/consume?behavior=success&auto_ack=true"
    setup(store, "rotation-a", "rot-a", "rot-g", endpoint, priority=7)
    setup(store, "rotation-b", "rot-b", "rot-g", endpoint, priority=7)
    enqueue(store, "rotation-a", "ra", 1)
    enqueue(store, "rotation-b", "rb", 1)
    config = {
        "global_concurrency": 1,
        "per_queue_inflight": 1,
        "time_budget_ms": 1,
        "dispatch_timeout_ms": 100,
        "wait_queue_capacity": 2,
    }
    first = run_round(
        mw_server["base"], "rotation-round-1", "at_least_once", config
    ).json()
    second = run_round(
        mw_server["base"], "rotation-round-2", "at_least_once", config
    ).json()
    first_order = [
        q["queue_name"]
        for group in first["queue_order"]
        if group["priority"] == 7
        for q in group["rotation_order"]
    ]
    second_order = [
        q["queue_name"]
        for group in second["queue_order"]
        if group["priority"] == 7
        for q in group["rotation_order"]
    ]
    assert first["claimed"] == 0
    assert second["claimed"] == 0
    assert first_order == ["rotation-a", "rotation-b"]
    assert second_order == ["rotation-b", "rotation-a"]
