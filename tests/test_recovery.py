import subprocess
import sys
from pathlib import Path

from graphstore.engine import GraphEngine
from graphstore.recovery import recover

ROOT = Path(__file__).resolve().parent.parent


def test_recovery_aborts_uncommitted_intent(engine, tmp_path):
    intent_id = engine.store.create_intent("apply_delta", 2, {"version": 2})
    engine.store._writer.execute(
        "INSERT INTO nodes VALUES (2, 'z9', 'Person', '{}')"
    )

    fresh = GraphEngine(engine.store.path)
    report = recover(fresh.store)
    action = next(item for item in report["actions"] if item["intent_id"] == intent_id)
    assert action["decision"] == "aborted"
    assert report["last_complete_version"] == 1
    row = fresh.store._writer.execute(
        "SELECT status FROM intents WHERE intent_id=?", (intent_id,)
    ).fetchone()
    assert row["status"] == "ABORTED"
    fresh.close()


def test_recovery_completes_committed_intent(engine):
    intent_id = engine.store.create_intent("synthetic", 1, {"version": 1})
    fresh = GraphEngine(engine.store.path)
    report = recover(fresh.store)
    action = next(item for item in report["actions"] if item["intent_id"] == intent_id)
    assert action["decision"] == "completed"
    assert report["last_complete_version"] == 1
    fresh.close()


def test_hard_kill_and_restart_reports_abort(tmp_path):
    db_path = tmp_path / "crash.db"
    snapshot_path = ROOT / "samples" / "snapshot_v1.json"
    worker = tmp_path / "worker.py"
    worker.write_text(
        "import sys\n"
        f"sys.path.insert(0, {str(ROOT)!r})\n"
        "from graphstore.engine import GraphEngine\n"
        f"eng = GraphEngine({str(db_path)!r})\n"
        "eng.start()\n"
        f"eng.load_snapshot({str(snapshot_path)!r})\n"
        "def kill(point):\n"
        "    if point == 'after_delta_intent':\n"
        "        import os; os._exit(137)\n"
        "eng.crash_hook = kill\n"
        "try:\n"
        "    eng.apply_delta({'add_nodes': [{'id': 'p9', 'label': 'Person', "
        "'properties': {}}], 'generated_at': 't'})\n"
        "except SystemExit:\n"
        "    import os; os._exit(137)\n"
    )
    killed = subprocess.run([sys.executable, str(worker)], capture_output=True)
    assert killed.returncode == 137

    restarted = GraphEngine(str(db_path))
    report = restarted.start()
    assert report["pending_intents_found"] >= 1
    assert report["last_complete_version"] == 1
    assert report["current_version_after_recovery"] == 1
    assert any(item["decision"] == "aborted" for item in report["actions"])
    assert restarted.current_version == 1
    view = restarted.registry.acquire(1)
    try:
        assert "p9" not in view.snapshot.nodes
        assert restarted.verify_integrity()["consistent"]
    finally:
        restarted.registry.release(view)
    restarted.close()
