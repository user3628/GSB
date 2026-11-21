import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from fiw.engine import Engine
from fiw.errors import BizError
from fiw.models import STATUS_ABORTED, STATUS_RUNNING

ROOT = Path(__file__).resolve().parent.parent

CRASH_SCRIPT = """
import sys
sys.path.insert(0, {root!r})
from fiw.engine import Engine
eng = Engine({db!r})
eng.load_plan({endpoints!r}, {plan!r})
eng.transition("plan-test", "RUNNING")
eng.run_round("plan-test", 1)
eng.run_round("plan-test", 2)  # crashes right after the intent is committed
"""


def _run_crash(env, db_path, crash_token):
    script = CRASH_SCRIPT.format(root=str(ROOT), db=str(db_path),
                                 endpoints=str(env.endpoints_path),
                                 plan=str(env.plan_path))
    proc_env = dict(os.environ)
    proc_env["FIW_CRASH_AFTER_INTENT"] = crash_token
    return subprocess.run([sys.executable, "-c", script], env=proc_env,
                          capture_output=True, text=True, timeout=60)


def test_recovery_after_midround_kill(env, tmp_path):
    db_path = tmp_path / "fiw.db"
    proc = _run_crash(env, db_path, "plan-test:2")
    assert proc.returncode == 2, proc.stderr  # hard kill, no clean exit

    # restart: a fresh engine scans unfinished intents
    engine = Engine(db_path)
    try:
        pending = engine.store.pending_intents()
        assert len(pending) == 1
        assert pending[0]["kind"] == "ROUND_EXEC"
        assert json.loads(pending[0]["payload_json"])["round_no"] == 2

        report = engine.recover()
        assert report["scanned_pending_intents"] == 1
        actions = {(a["action"], a.get("interrupted_round")): a
                   for a in report["actions"]}
        rollback = actions[("round_rolled_back", 2)]
        assert rollback["last_committed_round"] == 1
        assert ("plan_aborted", None) in actions

        # plan is ABORTED, round 2 never committed, round 1 intact
        assert engine.get_plan("plan-test")["status"] == STATUS_ABORTED
        with pytest.raises(BizError):
            engine.get_round("plan-test", 2)
        assert engine.get_round("plan-test", 1)["round_no"] == 1

        # recovery is idempotent: second scan finds nothing
        followup = engine.recover()
        assert followup["scanned_pending_intents"] == 0
    finally:
        engine.close()


def test_recovery_without_pending_intents_is_noop(env, tmp_path):
    db_path = tmp_path / "fiw.db"
    engine = Engine(db_path)
    try:
        engine.load_plan(str(env.endpoints_path), str(env.plan_path))
        engine.transition("plan-test", STATUS_RUNNING)
        engine.run_round("plan-test", 1)
        report = engine.recover()
        assert report["scanned_pending_intents"] == 0
        assert report["actions"] == []
        assert engine.get_plan("plan-test")["status"] == STATUS_RUNNING
    finally:
        engine.close()
