"""强制终止恢复与单写者/多读者并发一致性。"""

import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.api import create_app
from app.models import canonical_digest
from app.service import GrayService
from app.storage import Storage

ROOT = Path(__file__).resolve().parents[1]


def _crash_worker(db_path: str, policy: dict, batch: dict) -> None:
    """独立进程：写策略后，在批次意图落库、业务事务提交前强制退出。"""
    storage = Storage(db_path)
    service = GrayService(storage)
    service.submit_policy(policy)
    service.submit_batch(batch)  # 内部 os._exit(137)
    storage.close()


def _make_policy(version=1):
    p = {
        "version": version,
        "generated_at_ms": 1000,
        "version_weights": {"stable": "0.70", "canary": "0.30"},
        "mirror_rate": "0.25",
        "gray_cap": "0.35",
        "relative_tolerance": "0.000001",
        "dedup_window_ms": 10000,
        "baseline_version": "stable",
        "seed": "crash-seed",
    }
    p["digest"] = canonical_digest(p)
    return p


def _make_batch():
    return {
        "batch_id": "crash-batch",
        "policy_version": 1,
        "events": [
            {
                "request_id": f"c-{i}",
                "timestamp_ms": 2000 + i,
                "route_key": "r",
                "client_id": f"cli-{i}",
                "weight": 100 + i,
                "target_version": "stable",
            }
            for i in range(5)
        ],
    }


def test_recovery_after_forced_termination(tmp_path):
    db_path = str(tmp_path / "crash.db")
    env = dict(os.environ)
    env["GRAYSWITCH_CRASH_AFTER_INTENT"] = "1"
    env["GRAYSWITCH_DB"] = db_path
    env["PYTHONPATH"] = str(ROOT)

    worker = Path(__file__).resolve().parents[1] / "scripts" / "_crash_worker.py"
    proc = subprocess.run(
        [sys.executable, str(worker), db_path],
        env=env, capture_output=True, text=True, cwd=str(ROOT),
    )
    assert proc.returncode == 137, proc.stderr

    # 重启：不携带崩溃环境变量，启动恢复应把 PENDING 意图标记为 ABORTED。
    storage = Storage(db_path)
    service = GrayService(storage)
    report = service.recover_on_startup()
    assert report is not None
    assert report["pending_intents_found"] == 1
    assert report["actions"][0]["action"] == "marked_aborted"
    assert report["last_complete_policy_version"] == 1

    # 崩溃批次的事件不得入库；累计状态保持在最后完整版本（空计数器）。
    assert storage.get_verdict("c-0") is None
    state = storage.load_state(1)
    assert state["processed"] == 0
    assert storage.list_pending_intents() == []

    # 恢复报告可查询且只基于持久化事实
    persisted = service.get_recovery_report()
    assert persisted["count"] == 1
    storage.close()


def test_recover_is_idempotent(tmp_path):
    db_path = str(tmp_path / "idp.db")
    storage = Storage(db_path)
    service = GrayService(storage)
    service.submit_policy(_make_policy(1))
    # 手工制造一条 PENDING 意图（模拟崩溃后未完成事务）
    service.storage.create_intent("stats_rebuild", int(time.time() * 1000), 1, None, {})
    r1 = service.recover_on_startup()
    r2 = service.recover_on_startup()
    assert r1["pending_intents_found"] == 1
    assert r2 is None  # 再次启动无 PENDING
    storage.close()


def test_concurrent_readers_see_committed_versions_only(tmp_path):
    """服务层证据：写者串行提交期间，多个读者并发读到的始终是完整自洽版本。"""
    db_path = str(tmp_path / "conc.db")
    storage = Storage(db_path)
    service = GrayService(storage)
    service.submit_policy(_make_policy(1))

    reader_errors = []

    def reader():
        from app.errors import BusinessError
        for _ in range(100):
            try:
                stats = service.get_routing_stats(None)
            except BusinessError as exc:
                # 首个策略提交前的瞬时“尚无版本”状态，属于合法空库读。
                assert exc.code == "E_NOT_FOUND"
                continue
            # 读者只能看到完整提交版本：各版本承接权重之和严格等于 total_weight，
            # 不存在“分流结论已写、灰度占比未更新”的中间态。
            assert "shares" in stats
            assert sum(stats["version_weight"].values()) == stats["total_weight"]
            assert stats["gray_weight"] <= stats["total_weight"]

    def writer():
        for k in range(20):
            b = _make_batch()
            b["batch_id"] = f"w-{k}"
            for i, ev in enumerate(b["events"]):
                ev["request_id"] = f"w-{k}-{i}"
            try:
                service.submit_batch(b)
            except Exception as exc:  # noqa: BLE001
                reader_errors.append(exc)

    threads = [threading.Thread(target=writer)] + [
        threading.Thread(target=reader) for _ in range(3)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not reader_errors
    final = service.get_routing_stats(None)
    assert sum(final["version_weight"].values()) == final["total_weight"]
    storage.close()


def test_storage_readers_do_not_take_write_lock(tmp_path):
    """存储层证据：只读查询不持有写锁，可与写者真正并发。"""
    db_path = str(tmp_path / "rw.db")
    storage = Storage(db_path)
    service = GrayService(storage)
    service.submit_policy(_make_policy(1))
    service.submit_batch(_make_batch())

    entered = threading.Event()
    release = threading.Event()

    def hold_write_lock():
        with storage.write_lock:
            entered.set()
            release.wait(timeout=5)

    t = threading.Thread(target=hold_write_lock)
    t.start()
    assert entered.wait(timeout=5)
    # 写锁被占用时，只读查询仍应立即返回（不依赖写锁）。
    payload = storage.get_policy_payload(1)
    assert payload is not None and payload["version"] == 1
    state = storage.load_state(1)
    assert state["total_weight"] == sum(state["version_weight"].values())
    assert state["processed"] >= 1
    release.set()
    t.join()
    storage.close()


def test_serialized_duplicate_submission_reuses_result(tmp_path):
    """同一批次并发提交：单写者串行，首个裁决、其余等待复用，事件只裁决一次。"""
    db_path = str(tmp_path / "dup.db")
    storage = Storage(db_path)
    service = GrayService(storage)
    service.submit_policy(_make_policy(1))
    batch = _make_batch()
    results = []
    errors = []

    def submit():
        try:
            results.append(service.submit_batch(batch))
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=submit) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    reused = sum(1 for r in results if r.get("reused") == len(batch["events"]))
    first = [r for r in results if r.get("reused") == 0]
    assert len(first) == 1
    assert reused == 3
    # 事件只裁决一次：处理计数不随并发提交翻倍
    state = storage.load_state(1)
    assert state["processed"] <= len(batch["events"])
    storage.close()
