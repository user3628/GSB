from __future__ import annotations

from pathlib import Path

from conftest import bootstrap
from fastapi.testclient import TestClient

from mirror_judge.config import Settings
from mirror_judge.main import create_app


def test_running_replay_is_marked_aborted_by_recovery(tmp_path: Path, records, policy):
    db_path = tmp_path / "inspect.sqlite"
    app = create_app(Settings(db_path, "http://127.0.0.1:1/_test/replay"))
    client = TestClient(app, raise_server_exceptions=False)
    state = bootstrap(client, records, policy)
    version = state["commit"]["version"]
    store = app.state.store
    snapshot = store.read_snapshot(version)
    batch_id = snapshot["artifacts"]["batches"]["batches"][0]["batch_id"]
    replay_intent = store.create_intent(
        "replay", version, {"run_id": "run-crash-test", "source_version": version, "batch_ids": [batch_id]}
    )
    store.start_replay_run(
        run_id="run-crash-test", source_version=version, batch_ids=[batch_id], intent_id=replay_intent
    )
    pending_commit = store.create_intent("policy_increment", version, {"policy_version": "p-crashed"})
    report = store.recover()
    assert report["last_complete_version"] == version
    assert replay_intent in report["aborted_intent_ids"]
    assert pending_commit in report["aborted_intent_ids"]
    assert "run-crash-test" in report["aborted_replay_run_ids"]
    assert batch_id in report["aborted_batch_ids"]
    run = store.get_replay_run("run-crash-test")
    assert run["status"] == "aborted"
    recovered_state = app.state.service.state(version)
    assert recovered_state["batch_summary"]["states"][batch_id]["state"] == "aborted"
    assert store.current_version() == version
