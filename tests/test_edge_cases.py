import threading

import pytest

from logindex.engine import LogIndexEngine
from logindex.errors import LockedError
from logindex.models import BatchState
from logindex.store import connect


def test_external_lock_blocks_second_writer_while_held(db_path, sample_lines):
    """存活外部进程持锁期间，本进程写者必须被 E_LOCKED 拦截。"""
    import os

    engine = LogIndexEngine(db_path)
    engine.submit_batch("B1", sample_lines("batch1_full.txt"))
    # 用当前进程 PID 但不同 start token 模拟“另一个存活进程”持锁
    foreign_holder = f"pid-{os.getpid()}-start-foreign-token-12345678"
    conn = connect(db_path)
    conn.execute(
        "INSERT INTO write_lock(id, holder, acquired_ms) VALUES(1, ?, 1)",
        (foreign_holder,))
    conn.commit()
    conn.close()

    with pytest.raises(LockedError) as exc:
        engine.submit_batch("B2", sample_lines("batch2_append.txt"))
    assert exc.value.code == "E_LOCKED"
    # 清理外部锁后恢复可写
    cleanup = connect(db_path)
    cleanup.execute("DELETE FROM write_lock")
    cleanup.commit()
    cleanup.close()
    result = engine.submit_batch("B2", sample_lines("batch2_append.txt"))
    assert result.version == 2


def test_stale_lock_from_dead_process_is_cleared(db_path, sample_lines):
    """持有者进程不存在时，恢复器把锁判为陈旧锁并清理。"""
    engine = LogIndexEngine(db_path)
    conn = engine._conn()
    # 伪造一个属于不存在 PID 的锁
    conn.execute(
        "INSERT INTO write_lock(id, holder, acquired_ms) VALUES(1, 'pid-999999-start-dead-zz', 1)"
    )
    conn.commit()
    conn.close()
    restarted = LogIndexEngine(db_path)
    actions = [a["action"] for a in restarted.recovery_report["actions"]]
    assert "clear_stale_write_lock" in actions
    # 清理后可以正常写入
    result = restarted.submit_batch("B1", sample_lines("batch1_full.txt"))
    assert result.version == 1


def test_manual_abort_legal_transition(engine):
    conn = connect(engine.db_path)
    conn.execute(
        "INSERT INTO batches(batch_id, state, created_at_ms) VALUES('P1','PENDING',?)",
        (0,))
    conn.commit()
    conn.close()
    outcome = engine.mark_aborted("P1")
    assert outcome["state"] == BatchState.ABORTED.value


def test_invalid_batch_leaves_no_persistent_trace(engine, sample_lines):
    with pytest.raises(Exception):
        engine.submit_batch("BAD", sample_lines("batch_invalid.txt"))
    conn = connect(engine.db_path)
    try:
        for table in ("batches", "records", "intents", "classification_index",
                      "timeline_index", "batch_stats"):
            assert conn.execute(f"SELECT COUNT(*) c FROM {table}").fetchone()["c"] == 0
    finally:
        conn.close()


def test_no_intermediate_state_visible_via_separate_connection(engine, sample_lines):
    """写事务未提交时，独立只读连接只能看到上一个完整版本。"""
    engine.submit_batch("B1", sample_lines("batch1_full.txt"))
    conn = engine._conn()
    try:
        engine._acquire_db_lock(conn)
        conn.execute(
            "INSERT INTO batches(batch_id, state, created_at_ms) VALUES('X','PENDING',?)",
            (0,))
        reader = connect(engine.db_path)
        try:
            visible = reader.execute("SELECT COUNT(*) c FROM batches").fetchone()["c"]
        finally:
            reader.close()
        assert visible == 1
        conn.rollback()
    finally:
        engine._release_db_lock(conn)
        conn.close()


def test_repeated_idempotent_apply_no_duplicate_timeline_rows(engine, sample_lines):
    engine.submit_batch("B1", sample_lines("batch1_full.txt"))
    engine.submit_batch("B1", sample_lines("batch1_full.txt"))
    conn = connect(engine.db_path)
    try:
        timeline = conn.execute("SELECT service, ordinal FROM timeline_index").fetchall()
        assert len(timeline) == len({(r["service"], r["ordinal"]) for r in timeline})
        assert conn.execute("SELECT COUNT(*) c FROM records").fetchone()["c"] == 12
    finally:
        conn.close()
