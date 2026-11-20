import json
import subprocess
import sys
from pathlib import Path

from logindex.engine import LogIndexEngine
from logindex.models import BatchState
from logindex.store import connect, reset_database

CRASH_SCRIPT = r"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from logindex.engine import LogIndexEngine
db, batch_id, sample = sys.argv[1], sys.argv[2], sys.argv[3]
engine = LogIndexEngine(db)
engine.arm_crash_after_indexing()
lines = Path(sample).read_text(encoding="utf-8").splitlines()
try:
    engine.submit_batch(batch_id, lines)
except SystemExit as exc:
    sys.stderr.write(str(exc))
    raise
"""


def _crash_subprocess(db_path, batch_id, sample_path):
    project = Path(__file__).resolve().parents[1]
    helper = project / "runtime" / "_crash_helper.py"
    helper.write_text(CRASH_SCRIPT, encoding="utf-8")
    return subprocess.run(
        [sys.executable, str(helper), str(db_path), batch_id, str(sample_path)],
        cwd=project, capture_output=True, text=True, timeout=60,
    )


def test_recovery_requeues_indexing_batch(db_path, sample_lines):
    engine = LogIndexEngine(db_path)
    engine.submit_batch("B1", sample_lines("batch1_full.txt"))
    sample = Path(__file__).resolve().parents[1] / "samples" / "batch_crash.txt"

    proc = _crash_subprocess(db_path, "CRASH1", sample)
    assert proc.returncode != 0
    assert "强制终止" in proc.stderr

    # 新进程打开同一数据库，触发恢复
    recovered = LogIndexEngine(db_path)
    assert recovered.current_version() == 1
    startup = recovered.recovery_report
    assert startup["last_complete_version"] == 1
    assert startup["last_complete_batch"] == "B1"
    kinds = [a["action"] for a in startup["actions"]]
    assert "requeue_pending_batch" in kinds

    batch = next(b for b in recovered.list_batches() if b["batch_id"] == "CRASH1")
    assert batch["state"] == BatchState.PENDING.value

    # 重排后提交成功，版本延续
    result = recovered.submit_batch("CRASH1", sample.read_text(encoding="utf-8").splitlines())
    assert result.state == "COMMITTED"
    assert result.version == 2
    assert len(recovered.query_records()) == 15


def test_recovery_abort_policy(db_path, sample_lines):
    engine = LogIndexEngine(db_path)
    engine.submit_batch("B1", sample_lines("batch1_full.txt"))
    sample = Path(__file__).resolve().parents[1] / "samples" / "batch_crash.txt"
    proc = _crash_subprocess(db_path, "CRASH2", sample)
    assert proc.returncode != 0

    aborted = LogIndexEngine(db_path, recovery_policy="abort")
    actions = [a["action"] for a in aborted.recovery_report["actions"]]
    assert "abort_indexing_batch" in actions
    batch = next(b for b in aborted.list_batches() if b["batch_id"] == "CRASH2")
    assert batch["state"] == BatchState.ABORTED.value


def test_recovery_report_based_only_on_persisted_facts(db_path, sample_lines):
    engine = LogIndexEngine(db_path)
    engine.submit_batch("B1", sample_lines("batch1_full.txt"))
    sample = Path(__file__).resolve().parents[1] / "samples" / "batch_crash.txt"
    _crash_subprocess(db_path, "CRASH3", sample)

    recovered = LogIndexEngine(db_path)
    report = recovered.get_recovery_report()
    persisted = report["persisted_recovery_events"]
    assert any(e["kind"] == "requeue_pending_batch" for e in persisted)
    # 未完成批次没有版本号，最后完整边界仍是 B1/v1
    conn = connect(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM batches WHERE batch_id='CRASH3'"
        ).fetchone()
        assert row["state"] == "PENDING" and row["version"] is None
    finally:
        conn.close()


def test_clean_start_has_empty_open_intents(db_path, sample_lines):
    engine = LogIndexEngine(db_path)
    engine.submit_batch("B1", sample_lines("batch1_full.txt"))
    # 模拟另一个进程正常重启：无未完成意图，最后完整边界为 B1/v1
    restarted = LogIndexEngine(db_path)
    report = restarted.recovery_report
    assert report["open_intents"] == []
    assert report["last_complete_version"] == 1
    assert report["last_complete_batch"] == "B1"
    assert report["recovered"] is False
