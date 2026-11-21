from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
from pathlib import Path

from fastapi.testclient import TestClient

from seqtxn.api import create_app


def create_orders(client):
    assert client.post(
        "/topics",
        json={"name": "orders", "partition_count": 1, "routing_mode": "explicit"},
    ).status_code == 200


def produce(client, seq, txn_id=None):
    payload = {
        "producer_id": "p",
        "client_sequence": seq,
        "partition": 0,
        "body": {"seq": seq},
    }
    if txn_id:
        payload["txn_id"] = txn_id
    response = client.post("/topics/orders/messages", json=payload)
    assert response.status_code == 200, response.text


def run_crashing_child(db_path: Path, coordinator_url: str, code: str, crash_at: str):
    env = os.environ.copy()
    env["SEQ_TXN_CRASH_AT"] = crash_at
    repo = Path(__file__).resolve().parents[1]
    env["PYTHONPATH"] = str(repo)
    completed = subprocess.run(
        [sys.executable, "-c", code, str(db_path), coordinator_url],
        env=env,
        cwd=repo,
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert completed.returncode == 97, (completed.returncode, completed.stdout, completed.stderr)


def test_lease_crash_releases_unfinished_lease(tmp_path, coordinator, closed_port):
    db_path = tmp_path / "crash-lease.db"
    app = create_app(
        db_path=db_path,
        coordinator_url=coordinator,
        unreachable_url=f"http://127.0.0.1:{closed_port}",
        query_timeout_seconds=0.2,
    )
    with TestClient(app) as client:
        create_orders(client)
        produce(client, "one")

    child = """
import sys
from seqtxn.controller import MessageController
c = MessageController(sys.argv[1], sys.argv[2])
c.initialize()
c.claim('orders', 0, 'crash-consumer', 5000)
"""
    run_crashing_child(db_path, coordinator, child, "lease_after_effect")

    conn = sqlite3.connect(db_path)
    active = conn.execute("SELECT * FROM leases WHERE state='ACTIVE'").fetchone()
    started = conn.execute("SELECT * FROM intents WHERE state='STARTED'").fetchone()
    conn.close()
    assert active is not None
    assert started is not None

    app2 = create_app(
        db_path=db_path,
        coordinator_url=coordinator,
        unreachable_url=f"http://127.0.0.1:{closed_port}",
        query_timeout_seconds=0.2,
    )
    with TestClient(app2) as client:
        detail = client.get("/topics/orders/partitions/0").json()["data"]
        assert detail["active_lease"] is None
        report = client.get("/admin/recovery-report").json()["data"]
        assert report["facts"]["leases_released"] >= 1
        assert any(e["action"] == "RELEASE_UNCOMMITTED_LEASE" for e in report["events"])
        assert report["complete_exit_report_possible_after_kill"] is False
        claim = client.post(
            "/topics/orders/partitions/0/claim",
            json={"consumer_id": "after-restart", "visibility_timeout_ms": 5000},
        )
        assert claim.status_code == 200
        assert claim.json()["data"]["message"]["sequence"] == 1
        assert claim.json()["data"]["attempt"] == 2


def test_transaction_crash_is_renegotiated(tmp_path, coordinator, closed_port):
    db_path = tmp_path / "crash-txn.db"
    app = create_app(
        db_path=db_path,
        coordinator_url=coordinator,
        unreachable_url=f"http://127.0.0.1:{closed_port}",
        query_timeout_seconds=0.2,
    )
    with TestClient(app) as client:
        create_orders(client)
        produce(client, "one", txn_id="txn-commit")

    child = """
import sys
from seqtxn.controller import MessageController
c = MessageController(sys.argv[1], sys.argv[2])
c.initialize()
c.resolve_transaction('txn-commit')
"""
    run_crashing_child(db_path, coordinator, child, "txn_after_remote")
    conn = sqlite3.connect(db_path)
    state = conn.execute("SELECT state FROM transactions WHERE txn_id='txn-commit'").fetchone()[0]
    pending_count = conn.execute("SELECT COUNT(*) FROM pending_messages").fetchone()[0]
    started = conn.execute(
        "SELECT COUNT(*) FROM intents WHERE state='STARTED' AND kind='TXN_RESOLVE'"
    ).fetchone()[0]
    conn.close()
    assert state == "PENDING"
    assert pending_count == 1
    assert started == 1

    app2 = create_app(
        db_path=db_path,
        coordinator_url=coordinator,
        unreachable_url=f"http://127.0.0.1:{closed_port}",
        query_timeout_seconds=0.5,
        auto_renegotiate=True,
    )
    with TestClient(app2) as client:
        txn = client.get("/transactions/txn-commit").json()["data"]
        assert txn["state"] == "COMMITTED"
        assert txn["pending_count"] == 0
        report = client.get("/admin/recovery-report").json()["data"]
        assert report["facts"]["renegotiated"] >= 1
        assert any(e["action"] == "RENEGOTIATED_FROM_PENDING" for e in report["events"])

