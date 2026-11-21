from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor


def create_orders(client, partitions=2, mode="explicit"):
    response = client.post(
        "/topics",
        json={
            "name": "orders",
            "partition_count": partitions,
            "routing_mode": mode,
            "routing_salt": "fixed-salt",
        },
    )
    assert response.status_code == 200, response.text
    return response.json()


def publish(client, seq, partition=0, producer="p", body=None, txn_id=None, key=None):
    payload = {
        "producer_id": producer,
        "client_sequence": seq,
        "body": body if body is not None else {"seq": seq},
    }
    if partition is not None:
        payload["partition"] = partition
    if txn_id:
        payload["txn_id"] = txn_id
    if key is not None:
        payload["partition_key"] = key
    return client.post("/topics/orders/messages", json=payload)


def claim(client, consumer="c1", timeout_ms=5_000, partition=0):
    return client.post(
        f"/topics/orders/partitions/{partition}/claim",
        json={"consumer_id": consumer, "visibility_timeout_ms": timeout_ms},
    )


def test_routing_sequence_and_idempotency(component):
    client, _, _ = component
    create_orders(client, partitions=2, mode="explicit")

    first = publish(client, "s1")
    assert first.status_code == 200, first.text
    assert first.json()["data"]["sequence"] == 1
    duplicate = publish(client, "s1")
    assert duplicate.status_code == 200
    assert duplicate.json()["data"]["duplicate"] is True
    assert duplicate.json()["data"]["sequence"] == 1
    second = publish(client, "s2")
    assert second.json()["data"]["sequence"] == 2

    invalid = publish(client, "bad", partition=9)
    assert invalid.status_code == 400
    assert invalid.json()["code"] == "E_PARTITION_INVALID"

    bad_type = publish(client, "bad2", partition="zero")
    assert bad_type.status_code == 400
    assert bad_type.json()["code"] == "E_PARTITION_INVALID"

    hash_topic = client.post(
        "/topics",
        json={
            "name": "events",
            "partition_count": 4,
            "routing_mode": "hash",
            "routing_salt": "events-salt",
        },
    )
    assert hash_topic.status_code == 200
    routed = []
    for i in range(4):
        response = client.post(
            "/topics/events/messages",
            json={
                "producer_id": "pe",
                "client_sequence": f"e{i}",
                "partition_key": "same-key",
                "body": {"i": i},
            },
        )
        assert response.status_code == 200
        routed.append(response.json()["data"]["partition"])
    assert len(set(routed)) == 1
    assert routed == [routed[0]] * 4

    missing_key = client.post(
        "/topics/events/messages",
        json={"producer_id": "pe", "client_sequence": "no-key", "body": {}},
    )
    assert missing_key.status_code == 400
    assert missing_key.json()["code"] == "E_PARTITION_INVALID"


def test_strict_lease_order_expiry_and_duplicate_ack(component):
    client, _, _ = component
    create_orders(client)
    for seq in ("1", "2", "3"):
        assert publish(client, seq).status_code == 200

    first = claim(client, consumer="a", timeout_ms=100)
    assert first.status_code == 200
    message = first.json()["data"]["message"]
    assert message["sequence"] == 1
    token = first.json()["data"]["lease_token"]

    held = claim(client, consumer="b", timeout_ms=1_000)
    assert held.status_code == 409
    assert held.json()["code"] == "E_LEASE_HELD"

    time.sleep(0.13)
    retried = claim(client, consumer="b", timeout_ms=1_000)
    assert retried.status_code == 200
    assert retried.json()["data"]["message"]["sequence"] == 1
    assert retried.json()["data"]["attempt"] == 2

    stale_ack = client.post("/acks", json={"lease_token": token})
    assert stale_ack.status_code == 409
    assert stale_ack.json()["code"] in {"E_LEASE_EXPIRED", "E_LEASE_INVALID"}
    state = client.get("/topics/orders/partitions/0").json()["data"]
    assert state["active_lease"]["consumer_id"] == "b"
    assert state["head_sequence"] == 1
    assert state["delivery_sequence"] == 1

    fresh_token = retried.json()["data"]["lease_token"]
    acked = client.post("/acks", json={"lease_token": fresh_token, "sequence": 1})
    assert acked.status_code == 200
    duplicate = client.post("/acks", json={"lease_token": fresh_token, "sequence": 1})
    assert duplicate.status_code == 409
    assert duplicate.json()["code"] == "E_DUPLICATE_ACK"

    next_claim = claim(client, consumer="b", timeout_ms=100)
    assert next_claim.json()["data"]["message"]["sequence"] == 2
    time.sleep(0.13)
    late_ack = client.post(
        "/acks", json={"lease_token": next_claim.json()["data"]["lease_token"], "sequence": 2}
    )
    assert late_ack.status_code == 409
    assert late_ack.json()["code"] == "E_LEASE_EXPIRED"
    state = client.get("/topics/orders/partitions/0").json()["data"]
    assert state["active_lease"] is None
    reclaim = claim(client, consumer="b")
    assert reclaim.json()["data"]["message"]["sequence"] == 2
    assert reclaim.json()["data"]["attempt"] == 2


def test_concurrent_claim_has_single_winner(component):
    client, _, _ = component
    create_orders(client)
    assert publish(client, "only").status_code == 200

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(claim, client, f"c{i}") for i in range(2)]
        responses = [future.result() for future in futures]
    codes = [response.json()["code"] for response in responses]
    assert codes.count("OK") == 1
    assert codes.count("E_LEASE_HELD") == 1
    winner = next(r for r in responses if r.json()["code"] == "OK")
    assert winner.json()["data"]["message"]["sequence"] == 1


def test_rollback_placeholder_is_skipped(component):
    client, _, _ = component
    create_orders(client)
    assert publish(client, "1", txn_id="txn-rollback").status_code == 200
    assert publish(client, "2").status_code == 200

    blocked = claim(client)
    assert blocked.json()["data"]["reason"] == "HEAD_PENDING_TRANSACTION"
    resolved = client.post("/transactions/txn-rollback/resolve", json={"injection": "rollback"})
    assert resolved.status_code == 200
    assert resolved.json()["data"]["state"] == "ROLLED_BACK"

    delivered = claim(client)
    assert delivered.status_code == 200
    assert delivered.json()["data"]["message"]["sequence"] == 2
    messages = client.get("/topics/orders/partitions/0/messages").json()["data"]
    assert [m["state"] for m in messages] == ["ROLLED_BACK", "COMMITTED"]

