from __future__ import annotations

import multiprocessing as mp
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier

from leasefair.clock import FixedClock
from leasefair.service import LeaseService
from leasefair.storage import Store
from tests.conftest import ORIGIN_MS, ROOT


def _process_acquire(db_path: str, index: int, barrier, queue) -> None:
    service = LeaseService(db_path, clock=FixedClock(ORIGIN_MS))
    barrier.wait()
    try:
        response = service.acquire(
            "contended",
            request_id=f"process-{index}",
            holder_id=f"holder-{index}",
            ttl_ms=10_000,
            weight="0.5",
        )
        queue.put((index, response["code"], response.get("lease", {}).get("version")))
    except Exception as exc:  # pragma: no cover - diagnostic path
        queue.put((index, "EXCEPTION", repr(exc)))


def test_single_writer_serializes_concurrent_processes(tmp_path) -> None:
    db_path = str(tmp_path / "multi.sqlite")
    LeaseService(db_path, clock=FixedClock(ORIGIN_MS))
    context = mp.get_context("spawn")
    barrier = context.Barrier(3)
    queue = context.Queue()
    processes = [
        context.Process(target=_process_acquire, args=(db_path, index, barrier, queue))
        for index in range(3)
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=30)
        assert process.exitcode == 0
    results = [queue.get(timeout=5) for _ in processes]
    codes = [result[1] for result in results]
    assert codes.count("OK") == 1
    assert codes.count("E_LOCK_HELD") == 2


def test_same_request_concurrently_executes_once(tmp_path) -> None:
    db_path = tmp_path / "idempotent.sqlite"
    LeaseService(db_path, clock=FixedClock(ORIGIN_MS))
    barrier = Barrier(2)

    def worker(index: int):
        service = LeaseService(db_path, clock=FixedClock(ORIGIN_MS))
        barrier.wait()
        return service.acquire(
            "same",
            request_id="one-request",
            holder_id="one-holder",
            ttl_ms=5000,
            weight="0.5",
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(worker, index) for index in range(2)]
        results = [future.result(timeout=30) for future in futures]
    assert results[0] == results[1]
    assert results[0]["code"] == "OK"


def test_forced_termination_after_prepared_intent(tmp_path) -> None:
    db_path = tmp_path / "crash-grant.sqlite"
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "crash_worker.py"),
            "acquire",
            str(db_path),
            str(ORIGIN_MS),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 91
    recovered = LeaseService(db_path, clock=FixedClock(ORIGIN_MS))
    report = recovered.recovery_report()["report"]
    assert report["graceful_exit_report_guaranteed"] is False
    assert report["scanned_intents"] == 1
    assert report["summary"]["rolled_back_grants"] == 1
    assert report["actions"][0]["lock_available_after"] is True
    assert recovered.get_lock("crash-lock")["state"] == "available"


def test_recovery_compensates_durable_renewal_like_state(tmp_path) -> None:
    db_path = tmp_path / "rollback-renew.sqlite"
    service = LeaseService(db_path, clock=FixedClock(ORIGIN_MS))
    granted = service.acquire(
        "renew-lock", request_id="seed", holder_id="holder", ttl_ms=30_000, weight="0.5"
    )
    version = int(granted["lease"]["version"])
    store = Store(db_path)
    with store.write() as con:
        prior = store.snapshot_lock(con, "renew-lock")
        intent_id = store.create_prepared_intent(
            con,
            request_id="manual-renew",
            operation="renew",
            lock_key="renew-lock",
            payload_hash="manual",
            payload={"operation": "renew"},
            prior_state=prior,
            now_ms=ORIGIN_MS + 100,
        )
        con.execute("UPDATE intentions SET owner_pid=-1 WHERE id=?", (intent_id,))
        con.execute(
            "UPDATE leases SET expires_at_ms=?, ttl_ms=?, renew_count=1, last_renewed_at_ms=? "
            "WHERE lock_key=? AND version=?",
            (ORIGIN_MS + 90_100, 60_000, ORIGIN_MS + 100, "renew-lock", version),
        )
        con.execute(
            "UPDATE expiry_index SET expires_at_ms=? WHERE lock_key=?",
            (ORIGIN_MS + 90_100, "renew-lock"),
        )
        store.finish_intent(
            con,
            intent_id,
            phase="prepared",
            intended_state={
                "operation": "renew",
                "lease_id": int(prior["lease"]["id"]),
                "version": version,
                "prior_expires_at_ms": ORIGIN_MS + 30_000,
                "expires_at_ms": ORIGIN_MS + 90_100,
                "prior_ttl_ms": 30_000,
                "ttl_ms": 60_000,
                "prior_renew_count": 0,
                "renew_count": 1,
                "renewed_at_ms": ORIGIN_MS + 100,
            },
            now_ms=ORIGIN_MS + 100,
        )
    recovered = LeaseService(db_path, clock=FixedClock(ORIGIN_MS + 100))
    report = recovered.recovery_report()["report"]
    assert report["summary"]["rolled_back_renews"] == 1
    lock = recovered.get_lock("renew-lock", now_ms=ORIGIN_MS + 100)
    assert lock["lease"]["expires_at_ms"] == ORIGIN_MS + 30_000
    assert lock["lease"]["renew_count"] == 0


def test_recovery_compensates_durable_grant_like_state(tmp_path) -> None:
    db_path = tmp_path / "rollback-grant.sqlite"
    service = LeaseService(db_path, clock=FixedClock(ORIGIN_MS))
    store = Store(db_path)
    with store.write() as con:
        store.ensure_registered(con, "manual-grant", ORIGIN_MS)
        prior = store.snapshot_lock(con, "manual-grant")
        intent_id = store.create_prepared_intent(
            con,
            request_id="manual-grant-request",
            operation="acquire",
            lock_key="manual-grant",
            payload_hash="manual-grant",
            payload={"operation": "acquire"},
            prior_state=prior,
            now_ms=ORIGIN_MS,
        )
        con.execute("UPDATE intentions SET owner_pid=-1 WHERE id=?", (intent_id,))
        store.upsert_holder(con, "manual-holder", "0.5", 500_000_000, ORIGIN_MS)
        cursor = con.execute(
            "INSERT INTO leases(lock_key, holder_id, version, granted_at_ms, expires_at_ms, ttl_ms, "
            "renew_count, weight_text, weight_q, status, acquire_request_id) "
            "VALUES(?, ?, 1, ?, ?, 30000, 0, ?, ?, 'active', ?)",
            (
                "manual-grant",
                "manual-holder",
                ORIGIN_MS,
                ORIGIN_MS + 30_000,
                "0.5",
                500_000_000,
                "manual-grant-request",
            ),
        )
        lease_id = int(cursor.lastrowid)
        con.execute("UPDATE lock_state SET version=1 WHERE lock_key=?", ("manual-grant",))
        con.execute(
            "INSERT INTO expiry_index(lock_key, lease_id, expires_at_ms) VALUES(?, ?, ?)",
            ("manual-grant", lease_id, ORIGIN_MS + 30_000),
        )
        store.finish_intent(
            con,
            intent_id,
            phase="prepared",
            intended_state={
                "operation": "grant",
                "lease_id": lease_id,
                "version": 1,
                "holder_id": "manual-holder",
                "granted_at_ms": ORIGIN_MS,
                "expires_at_ms": ORIGIN_MS + 30_000,
                "ttl_ms": 30000,
                "renew_count": 0,
                "request_id": "manual-grant-request",
            },
            now_ms=ORIGIN_MS,
        )
    recovered = LeaseService(db_path, clock=FixedClock(ORIGIN_MS))
    report = recovered.recovery_report()["report"]
    assert report["summary"]["rolled_back_grants"] == 1
    lock = recovered.get_lock("manual-grant", now_ms=ORIGIN_MS)
    assert lock["state"] == "available"
    assert lock["version"] == 0
    assert "lease" not in lock

