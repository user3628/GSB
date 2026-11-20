from __future__ import annotations

import json
import os
import subprocess
import sys

import pytest

from alert_sieve.errors import BusinessError
from alert_sieve.store import Store

from conftest import ROOT, SAMPLES, alert_row, load_baseline, make_alert_snapshot


def _code(callable_):
    try:
        callable_()
    except BusinessError as exc:
        return exc.code
    return None


def test_required_business_codes(store):
    load_baseline(store)

    invalid_alert = {
        "kind": "alert_increment",
        "batch_id": "invalid",
        "base_version": 1,
        "version": 2,
        "checksum": "sha256:" + "0" * 64,
        "alerts": [{**alert_row(1000, "x", "ledger"), "timestamp_ms": "not-an-int"}],
    }
    assert _code(lambda: store.submit_alert_increment(invalid_alert)) == "E_ALERT_INVALID"

    bad_topology = json.loads((SAMPLES / "topology_v1.json").read_text(encoding="utf-8"))
    bad_topology["checksum"] = "sha256:" + "1" * 64
    assert _code(lambda: store.submit_topology_snapshot(bad_topology)) == "E_SNAPSHOT_INVALID"

    stale = json.loads((SAMPLES / "topology_increment_v2.json").read_text(encoding="utf-8"))
    stale["base_version"] = 0
    # Recompute the increment checksum so the stale-base path, not checksum validation, is reached.
    from alert_sieve.domain import topology_increment_checksum

    stale["checksum"] = topology_increment_checksum(
        0, stale["version"], [("checkout", "notifications")], [("checkout", "payment")], [], []
    )
    assert _code(lambda: store.submit_topology_increment(stale)) == "E_STALE_BASE"

    malformed = make_alert_snapshot(2, "replacement", [alert_row(1000, "new", "ledger")])
    malformed["alerts"].append({"line_no": 2, **alert_row(2000, "other", "ledger")})
    # The checksum does not cover the appended alert.
    assert _code(lambda: store.submit_alert_snapshot(malformed)) == "E_SNAPSHOT_INVALID"

    assert _code(lambda: store.query_batch("missing-batch")) == "E_ORDER_NOT_FOUND"
    assert store.query_state()["topology_version"] == 1


def test_rebuild_conflict_for_same_snapshot_version(store):
    load_baseline(store)
    replacement = make_alert_snapshot(1, "different-content", [alert_row(1, "only", "solo")])
    with pytest.raises(BusinessError) as exc:
        store.submit_alert_snapshot(replacement)
    assert exc.value.code == "E_REBUILD_CONFLICT"
    assert store.query_state()["alert_version"] == 1
    assert store.query_reduction()["totals"]["raw_count"] == 10


def test_conflicting_alert_preserves_first(store):
    load_baseline(store)
    payload = make_alert_snapshot(1, "conflict-batch", [alert_row(1000, "fp", "ledger")])
    # A snapshot with the same version is a rebuild conflict, so use an increment first.
    # The batch ledger path is covered directly in test_alerts; this verifies persistence
    # after an otherwise valid increment.
    inc = json.loads((SAMPLES / "alerts_increment_v2.json").read_text(encoding="utf-8"))
    store.submit_alert_increment(inc)
    altered_row = dict(inc["alerts"][0])
    altered_row["severity"] = "CRITICAL"
    from conftest import make_alert_increment

    altered = make_alert_increment(
        inc["batch_id"],
        inc["base_version"],
        inc["version"],
        [altered_row],
        inc["affected_fingerprints"],
    )
    with pytest.raises(BusinessError) as exc:
        store.submit_alert_increment(altered)
    assert exc.value.code == "E_CONFLICTING_ALERT"
    alert_rows = store.query_reduction(include_alerts=True)["alerts"]
    first = next(row for row in alert_rows if row["alert_uid"] == "seed-alerts-inc-v2:1")
    assert first["severity"] == "WARNING"


def test_forced_termination_rolls_back_and_recovery_is_facts_only(tmp_path):
    db_path = tmp_path / "crash.db"
    store = Store(db_path)
    load_baseline(store)
    topology_v2 = json.loads((SAMPLES / "topology_increment_v2.json").read_text(encoding="utf-8"))
    store.submit_topology_increment(topology_v2)
    before_digest = store.query_product_digest()
    before_state = store.query_state()
    assert before_state["topology_version"] == 2

    payload = SAMPLES / "topology_increment_v3_crash.json"
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT / "src") + (
        os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else ""
    )
    proc = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "crash_worker.py"), str(db_path), str(payload)],
        cwd=str(ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert proc.returncode == 91, proc.stderr
    store.close() if hasattr(store, "close") else None

    recovered = Store(db_path)
    after_state = recovered.query_state()
    assert after_state["alert_version"] == before_state["alert_version"]
    assert after_state["topology_version"] == 2
    assert after_state["rules_version"] == before_state["rules_version"]
    assert after_state["commit_seq"] == before_state["commit_seq"]
    assert recovered.query_product_digest() == before_digest
    report = recovered.query_recovery_report()
    assert report["pending_intent_count"] == 0
    assert report["facts_only"] is True
    assert any(
        item["outcome"] == "aborted_after_uncommitted_rebuild"
        for item in report["recovered_intents"]
    )
    assert "graceful" in report["disclaimer"]
