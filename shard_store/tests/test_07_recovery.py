"""Requirement 7: pending-intent scan, reopen/abort, identical re-merge digest."""
import sqlite3

from tests.conftest import create_session, sample_part, upload_part


def _prepare_session(client, manifest, key="k/recover"):
    obj = manifest["objects"][0]
    sess = create_session(
        client, object_key=key, min_part_size=1,
        expected_total_size=obj["expected_total_size"],
    )
    for part_number in range(1, obj["part_count"] + 1):
        upload_part(client, sess["session_id"], part_number,
                    sample_part(manifest, 0, part_number))
    return sess, obj


def test_simulated_crash_then_recovery_and_remerge(client, manifest):
    sess, obj = _prepare_session(client, manifest)
    crash = client.post(
        f"/api/v1/sessions/{sess['session_id']}/merge",
        json={"simulate_crash_after_intent": True},
    )
    assert crash.status_code == 503
    assert crash.json()["error"]["code"] == "E_SIMULATED_CRASH"
    intent_id = crash.json()["error"]["details"]["intent_id"]

    # Durable facts immediately after the simulated SIGKILL.
    state = client.get(f"/api/v1/sessions/{sess['session_id']}").json()["data"]
    assert state["status"] == "merging"
    assert state["last_intent_id"] == intent_id
    assert client.get(f"/api/v1/objects/{obj['object_key']}").status_code == 404

    # Merge is blocked while the session is stuck in merging.
    blocked = client.post(f"/api/v1/sessions/{sess['session_id']}/merge", json={})
    assert blocked.json()["error"]["code"] == "E_SESSION_CONFLICT"

    report = client.post("/api/v1/recovery/run", json={}).json()["data"]
    assert report["scanned_pending_intents"] == 1
    assert report["reopened_for_retry"] == 1
    action = report["actions"][0]
    assert action["intent_id"] == intent_id
    assert action["resolution"] == "reopened_for_retry"
    assert action["object_visible_at_scan"] is False

    state = client.get(f"/api/v1/sessions/{sess['session_id']}").json()["data"]
    assert state["status"] == "open"

    remerge = client.post(f"/api/v1/sessions/{sess['session_id']}/merge",
                          json={"strategy": "full"})
    assert remerge.status_code == 200
    assert remerge.json()["data"]["object_sha256"] == obj["expected_object_sha256"]

    reports = client.get("/api/v1/recovery/reports").json()["data"]
    assert any(r["report_id"] == report["report_id"] for r in reports)


def test_recovery_on_clean_database_is_noop(client):
    report = client.post("/api/v1/recovery/run", json={}).json()["data"]
    assert report["scanned_pending_intents"] == 0
    assert report["reopened_for_retry"] == 0


def test_pending_intent_with_non_merging_session_is_aborted(client, manifest, settings):
    sess, obj = _prepare_session(client, manifest, key="k/aborted-intent")
    crash = client.post(
        f"/api/v1/sessions/{sess['session_id']}/merge",
        json={"simulate_crash_after_intent": True},
    )
    intent_id = crash.json()["error"]["details"]["intent_id"]
    # External divergence: session row manually moved away from merging.
    conn = sqlite3.connect(str(settings.db_path))
    conn.execute("UPDATE upload_sessions SET status='cancelled' WHERE session_id = ?",
                 (sess["session_id"],))
    conn.commit()
    conn.close()

    report = client.post("/api/v1/recovery/run", json={}).json()["data"]
    assert report["aborted"] == 1
    assert report["actions"][0]["resolution"] == "aborted"
    assert report["actions"][0]["intent_id"] == intent_id


def test_recovery_runs_automatically_on_process_restart(client, manifest):
    """A brand-new process (new app instance on the same DB) must resolve the
    pending intent during startup and the re-merge yield the same digest."""
    from fastapi.testclient import TestClient
    from app.main import create_app

    sess, obj = _prepare_session(client, manifest, key="k/restart")
    crash = client.post(
        f"/api/v1/sessions/{sess['session_id']}/merge",
        json={"simulate_crash_after_intent": True},
    )
    assert crash.status_code == 503

    # Simulate a fresh process: new application instance over the same DB file.
    restarted = create_app()
    with TestClient(restarted) as new_client:
        startup_report = restarted.state.last_recovery
        assert startup_report["scanned_pending_intents"] == 1
        assert startup_report["reopened_for_retry"] == 1

        state = new_client.get(
            f"/api/v1/sessions/{sess['session_id']}"
        ).json()["data"]
        assert state["status"] == "open"
        assert state["part_count"] == obj["part_count"]

        remerge = new_client.post(
            f"/api/v1/sessions/{sess['session_id']}/merge",
            json={"strategy": "full"},
        )
        assert remerge.status_code == 200
        assert remerge.json()["data"]["object_sha256"] == obj["expected_object_sha256"]
