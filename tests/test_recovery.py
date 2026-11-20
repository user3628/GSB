from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import uuid
from pathlib import Path

from checkpoint_recovery import CheckpointManager, ManagerConfig
from checkpoint_recovery.models import utc_now


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"


def _env() -> dict[str, str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(SRC_DIR) + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    return env


def test_forced_termination_restart_invalidates_writing_and_reports_difference(
    tmp_path: Path, payload_factory
) -> None:
    data_dir = tmp_path / "data"
    with CheckpointManager(ManagerConfig(data_dir=data_dir)) as manager:
        manager.submit_checkpoint(payload_factory(1), wait=True)
        manager.submit_checkpoint(payload_factory(2), wait=True)

    payload_path = tmp_path / "payload.json"
    payload_path.write_text(json.dumps(payload_factory(3)), encoding="utf-8")
    script = """
import json
import sys
import time
from pathlib import Path
from checkpoint_recovery import CheckpointManager, ManagerConfig

payload = json.loads(Path(sys.argv[1]).read_text(encoding='utf-8'))
manager = CheckpointManager(ManagerConfig(
    data_dir=Path(sys.argv[2]),
    writer_delay_seconds=2.0,
    fault_crash_after_temp_files=True,
    fault_crash_delay_seconds=0.3,
))
submission = manager.submit_checkpoint(payload, wait=False)
print(submission.checkpoint_id, flush=True)
time.sleep(20)
"""
    completed = subprocess.run(
        [sys.executable, "-c", script, str(payload_path), str(data_dir)],
        cwd=PROJECT_ROOT,
        env=_env(),
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert completed.returncode == 91, completed.stderr
    crashed_checkpoint_id = completed.stdout.strip().splitlines()[0]
    assert (data_dir / "tmp" / crashed_checkpoint_id).exists()

    with CheckpointManager(ManagerConfig(data_dir=data_dir)) as manager:
        crashed = manager.get_checkpoint(crashed_checkpoint_id)
        assert crashed["status"] == "INVALID"
        assert not (data_dir / "tmp" / crashed_checkpoint_id).exists()
        assert manager.get_write_request(crashed["request_id"])["state"] == "ERROR"
        startup = manager.startup_recovery
        assert any(
            item["checkpoint_id"] == crashed_checkpoint_id
            for item in startup["writing_checkpoints_invalidated"]
        )
        baseline = manager.get_recovery_baseline()
        assert baseline["checkpoint"]["round"] == 2
        assert baseline["fallback"]["occurred"] is False
        report = manager.get_recovery_report(target_round=4, target_scheduler_position=153)
        difference = report["difference_report"]
        assert difference["rounds_to_replay"] == 2
        assert difference["scheduler_steps_to_advance"] == 74
        assert difference["optimizer_momentum_available"] is True
        assert difference["shard_digest_conclusion"] == "PASS"
        assert report["persisted_facts_only"] is True
        assert report["exit_report_available"] is False


def test_latest_corrupt_checkpoint_falls_back_to_earlier_committed(tmp_path: Path, payload_factory) -> None:
    data_dir = tmp_path / "data"
    with CheckpointManager(ManagerConfig(data_dir=data_dir)) as manager:
        manager.submit_checkpoint(payload_factory(1), wait=True)
        manager.submit_checkpoint(payload_factory(2), wait=True)
        third_submission = manager.submit_checkpoint(payload_factory(3), wait=True)
        third = manager.get_checkpoint(third_submission.checkpoint_id)
        manifest = json.loads((data_dir / third["manifest_path"]).read_text(encoding="utf-8"))
        shard_path = data_dir / "checkpoints" / third["checkpoint_id"] / manifest["params"][0]["relative_path"]
        corrupted = bytearray(shard_path.read_bytes())
        corrupted[0] ^= 0xFF
        shard_path.write_bytes(corrupted)

    with CheckpointManager(ManagerConfig(data_dir=data_dir)) as manager:
        report = manager.get_recovery_report(target_round=5)
        assert report["recovery_baseline"]["round"] == 2
        difference = report["difference_report"]
        assert difference["fallback_occurred"] is True
        assert difference["fallback_reason"] == "latest_committed_checkpoint_failed_verification"
        assert difference["rounds_to_replay"] == 3
        assert difference["shard_digest_conclusion"] == "PASS"
        rejected = difference["rejected_checkpoints"]
        assert len(rejected) == 1
        assert rejected[0]["checkpoint_id"] == third["checkpoint_id"]
        assert rejected[0]["verification"]["valid"] is False
        assert manager.get_checkpoint(third["checkpoint_id"])["status"] == "INVALID"


def test_pending_prune_intent_is_completed_on_restart(tmp_path: Path, payload_factory) -> None:
    data_dir = tmp_path / "data"
    with CheckpointManager(ManagerConfig(data_dir=data_dir)) as manager:
        first = manager.submit_checkpoint(payload_factory(1), wait=True)
        manager.submit_checkpoint(payload_factory(2), wait=True)
        latest = manager.submit_checkpoint(payload_factory(3), wait=True)
        intent_id = f"prune-{uuid.uuid4().hex}"
        intent_payload = {
            "intent_id": intent_id,
            "operation": "PRUNE",
            "checkpoint_id": first.checkpoint_id,
            "round": 1,
            "created_at": utc_now(),
            "reason": "simulated_crash_after_intent",
        }
        manager.storage.write_intent_file(intent_id, intent_payload)
        manager.db.create_intent(intent_id, first.checkpoint_id, intent_payload)

    with CheckpointManager(ManagerConfig(data_dir=data_dir)) as manager:
        recovered = manager.get_checkpoint(first.checkpoint_id)
        assert recovered["status"] == "PRUNED"
        assert recovered["manifest_path"] is None
        assert not (data_dir / "checkpoints" / first.checkpoint_id).exists()
        assert manager.get_checkpoint(latest.checkpoint_id)["status"] == "COMMITTED"
        assert any(
            item["intent_id"] == intent_id
            for item in manager.startup_recovery["prune_intents_completed"]
        )
        with sqlite3.connect(data_dir / "checkpoints.db") as conn:
            intent_state = conn.execute(
                "SELECT state FROM intent_log WHERE intent_id = ?", (intent_id,)
            ).fetchone()[0]
        assert intent_state == "DONE"

