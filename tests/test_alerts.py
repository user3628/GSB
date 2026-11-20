from __future__ import annotations

import json
import threading

import pytest

from alert_sieve.errors import BusinessError

from conftest import (
    SAMPLES,
    alert_row,
    load_baseline,
    make_alert_increment,
    make_alert_snapshot,
)


def test_alert_validation_is_atomic(store):
    bad = {
        "kind": "alert_increment",
        "batch_id": "bad-batch",
        "base_version": 0,
        "version": 1,
        "checksum": "sha256:" + "0" * 64,
        "alerts": [
            alert_row(1000, "ok", "svc"),
            {**alert_row(2000, "bad", "svc"), "severity": "NOT_A_LEVEL"},
        ],
    }
    with pytest.raises(BusinessError) as exc:
        store.submit_alert_increment(bad)
    assert exc.value.code == "E_ALERT_INVALID"
    assert store.query_state()["counts"]["alerts"] == 0
    assert store.query_state()["commit_seq"] == 0


def test_snapshot_and_increment_checksums_are_validated(store):
    body = make_alert_snapshot(1, "snap", [alert_row(1000, "fp", "svc")])
    body["checksum"] = "sha256:" + "f" * 64
    with pytest.raises(BusinessError) as exc:
        store.submit_alert_snapshot(body)
    assert exc.value.code == "E_SNAPSHOT_INVALID"
    assert store.query_state()["commit_seq"] == 0


def test_batch_idempotency_and_line_conflict(store):
    load_baseline(store)
    payload = json.loads((SAMPLES / "alerts_increment_v2.json").read_text(encoding="utf-8"))
    first = store.submit_alert_increment(payload)
    assert first["inserted_count"] == 1
    commit_seq = first["commit_seq"]
    replay = store.submit_alert_increment(payload)
    assert replay["replay"] is True
    assert replay["commit_seq"] == commit_seq
    assert store.query_state()["counts"]["alerts"] == 11

    changed_row = dict(payload["alerts"][0])
    changed_row["resource"] = "different-resource"
    changed = make_alert_increment(
        payload["batch_id"],
        payload["base_version"],
        payload["version"],
        [changed_row],
        payload["affected_fingerprints"],
    )
    with pytest.raises(BusinessError) as exc:
        store.submit_alert_increment(changed)
    assert exc.value.code == "E_CONFLICTING_ALERT"

    # Conflict classification takes precedence over a stale checksum for a
    # replayed batch id, so the caller still gets the specific business code.
    changed_again = json.loads(json.dumps(changed))
    changed_again["checksum"] = payload["checksum"]
    with pytest.raises(BusinessError) as exc:
        store.submit_alert_increment(changed_again)
    assert exc.value.code == "E_CONFLICTING_ALERT"

    alert_rows = store.query_reduction(include_alerts=True)["alerts"]
    original = next(row for row in alert_rows if row["alert_uid"] == "seed-alerts-inc-v2:1")
    assert original["resource"] == "inventory-queue"


def test_stale_base_does_not_change_state(store):
    load_baseline(store)
    payload = make_alert_increment("stale-a", 0, 1, [alert_row(1000, "new-fp", "ledger")])
    before = store.query_state()
    with pytest.raises(BusinessError) as exc:
        store.submit_alert_increment(payload)
    assert exc.value.code == "E_STALE_BASE"
    after = store.query_state()
    assert after == before


def test_full_snapshot_replaces_without_mixing(store):
    load_baseline(store)
    replacement = make_alert_snapshot(2, "replacement-v2", [alert_row(5000, "only-fp", "solo")])
    result = store.submit_alert_snapshot(replacement)
    assert result["alert_count"] == 1
    reduction = store.query_reduction(include_alerts=True)
    assert reduction["totals"]["raw_count"] == 1
    assert [row["alert_uid"] for row in reduction["alerts"]] == ["replacement-v2:1"]
    assert store.query_state()["counts"]["fingerprint_meta"] == 1


def test_two_concurrent_increments_same_base_only_one_succeeds(store):
    load_baseline(store)
    payload_a = make_alert_increment("concurrent-a", 1, 2, [alert_row(1000, "fp-a", "ledger")])
    payload_b = make_alert_increment("concurrent-b", 1, 2, [alert_row(1000, "fp-b", "ledger")])
    barrier = threading.Barrier(2)
    outcomes: list[tuple[str, str | None, int | None]] = []
    lock = threading.Lock()

    def submit(batch: str, payload: dict) -> None:
        barrier.wait()
        try:
            result = store.submit_alert_increment(payload)
            with lock:
                outcomes.append((batch, None, result["commit_seq"]))
        except BusinessError as exc:
            with lock:
                outcomes.append((batch, exc.code, None))

    threads = [
        threading.Thread(target=submit, args=("a", payload_a)),
        threading.Thread(target=submit, args=("b", payload_b)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sorted(code or "OK" for _, code, _ in outcomes) == ["E_STALE_BASE", "OK"]
    state = store.query_state()
    assert state["alert_version"] == 2
    assert state["counts"]["alerts"] == 11


def test_concurrent_duplicate_batch_waits_and_reuses_result(store):
    load_baseline(store)
    payload = make_alert_increment("same-batch", 1, 2, [alert_row(1000, "fp-same", "ledger")])
    barrier = threading.Barrier(2)
    results = []
    lock = threading.Lock()

    def submit() -> None:
        barrier.wait()
        result = store.submit_alert_increment(payload)
        with lock:
            results.append(result)

    threads = [threading.Thread(target=submit), threading.Thread(target=submit)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert len(results) == 2
    assert results[0]["commit_seq"] == results[1]["commit_seq"]
    assert sum(1 for result in results if result.get("replay")) == 1
    assert store.query_state()["counts"]["alerts"] == 11
    assert store.query_state()["commit_seq"] == 4


def test_fingerprint_label_change_requires_declaration(store):
    load_baseline(store)
    payload = make_alert_increment("label-change", 1, 2, [alert_row(1000, "ledger-db-connection", "ledger", labels={"env": "staging", "kind": "database", "region": "cn-east"})])
    with pytest.raises(BusinessError) as exc:
        store.submit_alert_increment(payload)
    assert exc.value.code == "E_ALERT_INVALID"
    assert store.query_state()["alert_version"] == 1

    declared = make_alert_increment(
        "label-change",
        1,
        2,
        [alert_row(1000, "ledger-db-connection", "ledger", labels={"env": "staging", "kind": "database", "region": "cn-east"})],
        [{"fingerprint": "ledger-db-connection", "service": "ledger", "labels": {"env": "staging", "kind": "database", "region": "cn-east"}}],
    )
    result = store.submit_alert_increment(declared)
    assert result["affected_services"] == ["checkout", "edge-gateway", "ledger", "payment"]
    event = store.query_invalidation(result["products"]["invalidation_event_id"])
    assert event["event"]["cause"] == "alert_increment"
    assert {target["artifact"] for target in event["targets"]} == {"aggregate", "suppression", "rootcause"}
