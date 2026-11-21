from __future__ import annotations

import time


def create_orders(client):
    assert client.post(
        "/topics",
        json={"name": "orders", "partition_count": 2, "routing_mode": "explicit"},
    ).status_code == 200


def produce(client, seq, partition=0, txn_id=None, producer="p"):
    payload = {
        "producer_id": producer,
        "client_sequence": seq,
        "partition": partition,
        "body": {"seq": seq},
    }
    if txn_id:
        payload["txn_id"] = txn_id
    response = client.post("/topics/orders/messages", json=payload)
    assert response.status_code == 200, response.text
    return response.json()["data"]


def claim(client, partition=0, consumer="c"):
    return client.post(
        f"/topics/orders/partitions/{partition}/claim",
        json={"consumer_id": consumer, "visibility_timeout_ms": 5_000},
    )


def test_pending_commit_and_cross_partition_atomicity(component):
    client, _, _ = component
    create_orders(client)
    produce(client, "1")
    produce(client, "2", partition=0, txn_id="txn-commit")
    produce(client, "3", partition=0, txn_id="txn-commit")
    produce(client, "4")
    produce(client, "b1", partition=1, txn_id="txn-commit", producer="pb")

    before = client.get("/transactions/txn-commit").json()["data"]
    assert before["state"] == "PENDING"
    assert before["pending_count"] == 3
    assert all(m["state"] == "PENDING" for m in before["messages"])
    first = claim(client)
    assert first.json()["data"]["message"]["sequence"] == 1
    token = first.json()["data"]["lease_token"]
    assert client.post("/acks", json={"lease_token": token}).status_code == 200
    blocked = claim(client)
    assert blocked.json()["data"]["reason"] == "HEAD_PENDING_TRANSACTION"
    assert blocked.json()["data"]["blocked_sequence"] == 2

    resolved = client.post("/transactions/txn-commit/resolve")
    assert resolved.status_code == 200, resolved.text
    body = resolved.json()["data"]
    assert body["state"] == "COMMITTED"
    assert body["pending_count"] == 0
    assert all(m["state"] == "COMMITTED" for m in body["messages"])
    assert body["decision_source"].startswith("coordinator:")

    p0 = client.get("/topics/orders/partitions/0").json()["data"]
    p1 = client.get("/topics/orders/partitions/1").json()["data"]
    assert p0["visibility_sequence"] == 4
    assert p1["visibility_sequence"] == 1
    assert p0["delivery_ready"] is True
    assert p0["head_sequence"] == 2
    assert p0["delivery_sequence"] == 2

    for expected in (2, 3, 4):
        response = claim(client)
        assert response.json()["data"]["message"]["sequence"] == expected
        token = response.json()["data"]["lease_token"]
        assert client.post("/acks", json={"lease_token": token}).status_code == 200


def test_timeout_keeps_pending_and_retry_commits(component):
    client, _, coordinator = component
    create_orders(client)
    produce(client, "1", txn_id="txn-hang")

    timed_out = client.post(
        "/transactions/txn-hang/resolve",
        json={"injection": "hang", "timeout_ms": 100},
    )
    assert timed_out.status_code == 504
    assert timed_out.json()["code"] == "E_TXN_TIMEOUT"
    pending = client.get("/transactions/txn-hang").json()["data"]
    assert pending["state"] == "PENDING"
    assert pending["pending_count"] == 1

    time.sleep(0.15)
    observations = client.get("/health")  # keep the client active; no-op
    assert observations.status_code == 200
    import requests

    obs = requests.get(coordinator + "/admin/observations", timeout=2).json()["data"]
    assert any(item.get("client_disconnected") for item in obs), obs

    retried = client.post(
        "/transactions/txn-hang/resolve", json={"injection": "commit"}
    )
    assert retried.status_code == 200, retried.text
    assert retried.json()["data"]["state"] == "COMMITTED"
    assert retried.json()["data"]["outcome"] == "COMMIT"


def test_unknown_unreachable_and_conflict_codes(component):
    client, _, _ = component
    create_orders(client)
    produce(client, "1", txn_id="txn-unknown")
    unknown = client.post("/transactions/txn-unknown/resolve", json={"injection": "unknown"})
    assert unknown.status_code == 409
    assert unknown.json()["code"] == "E_TXN_UNKNOWN"
    assert client.get("/transactions/txn-unknown").json()["data"]["state"] == "PENDING"

    committed_after_unknown = client.post(
        "/transactions/txn-unknown/resolve", json={"injection": "commit", "force": True}
    )
    assert committed_after_unknown.status_code == 200, committed_after_unknown.text
    assert committed_after_unknown.json()["data"]["state"] == "COMMITTED"

    produce(client, "2", partition=1, txn_id="txn-refused", producer="pr")
    refused = client.post("/transactions/txn-refused/resolve", json={"injection": "refuse"})
    assert refused.status_code == 502
    assert refused.json()["code"] == "E_TXN_UNREACHABLE"
    assert client.get("/transactions/txn-refused").json()["data"]["state"] == "PENDING"
    recovered = client.post(
        "/transactions/txn-refused/resolve", json={"injection": "commit", "force": True}
    )
    assert recovered.status_code == 200

    produce(client, "3", txn_id="txn-conflict")
    first = client.post("/transactions/txn-conflict/resolve", json={"injection": "commit"})
    assert first.status_code == 200
    conflict = client.post(
        "/transactions/txn-conflict/resolve",
        json={"injection": "duplicate_conflict", "force": True},
    )
    assert conflict.status_code == 409
    assert conflict.json()["code"] == "E_TXN_CONFLICT"
    detail = client.get("/transactions/txn-conflict").json()["data"]
    assert detail["state"] == "COMMITTED"
    assert len(detail["conflicts"]) == 1
    assert detail["conflicts"][0]["existing_outcome"] == "COMMIT"
    assert detail["conflicts"][0]["incoming_outcome"] == "ROLLBACK"

    duplicate = client.post(
        "/transactions/txn-conflict/resolve",
        json={"injection": "duplicate_same", "force": True},
    )
    assert duplicate.status_code == 409
    assert duplicate.json()["code"] == "E_DUPLICATE_ACK"
    assert client.get("/transactions/txn-conflict").json()["data"]["state"] == "COMMITTED"

    faults = client.get("/admin/fault-events").json()["data"]
    codes = {event["observed_code"] for event in faults}
    assert {"E_TXN_UNKNOWN", "E_TXN_UNREACHABLE", "E_TXN_CONFLICT", "E_DUPLICATE_ACK"}.issubset(codes)

