from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from kvstore.errors import KeyNotFound
from kvstore.store import KVStore
import kvstore.store as store_module


ROOT = Path(__file__).resolve().parent.parent


def test_hard_kill_after_intent_is_rolled_back_on_restart(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    store = KVStore(data_dir)
    store.write("committed:before", 1, version=1)
    store.switch_snapshot()
    store.write("committed:after-snapshot", 2, version=1)

    marker = tmp_path / "crash-marker.json"
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "kvstore.crash_worker",
            "--data-dir",
            str(data_dir),
            "--marker",
            str(marker),
        ],
        cwd=str(ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.time() + 10
        while time.time() < deadline and not marker.exists():
            if proc.poll() is not None:
                stdout, stderr = proc.communicate(timeout=1)
                raise AssertionError(f"crash worker exited early: {stdout}\n{stderr}")
            time.sleep(0.02)
        assert marker.exists(), "crash worker did not persist its intent"
        marker_data = json.loads(marker.read_text(encoding="utf-8"))
        assert marker_data["state"] == "intent_persisted_before_segment_append"
        proc.kill()
        proc.wait(timeout=10)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)

    recovered = KVStore(data_dir)
    report = recovered.current_recovery_report()
    assert report["source"] == "persistent_intent_and_segment_header"
    assert report["counts"]["rolled_back"] == 1
    assert report["rolled_back_transactions"][0]["reason"] == "missing_commit_marker"
    assert report["rolled_back_transactions"][0]["type"] == "append"
    assert {"committed:before", "committed:after-snapshot"} <= {
        item["key"] for item in recovered.index_summary()["keys"]
    }
    with pytest.raises(KeyNotFound) as missing:
        recovered.get("crash:pending")
    assert missing.value.code == "E_KEY_NOT_FOUND"
    assert recovered.index_summary()["snapshot_seq"] == 1


def test_in_process_failed_append_is_aborted_and_does_not_truncate_later_commit(
    tmp_path: Path, monkeypatch
) -> None:
    data_dir = tmp_path / "data"
    store = KVStore(data_dir)
    store.write("before", 1, version=1)
    original_append = store_module.append_records

    def broken_append(path, records):
        with path.open("r+b") as handle:
            handle.seek(0, os.SEEK_END)
            handle.write(b"partial-write-with-no-commit")
            handle.flush()
        raise RuntimeError("simulated append failure")

    monkeypatch.setattr(store_module, "append_records", broken_append)
    try:
        try:
            store.write("failed", 2, version=1)
        except RuntimeError:
            pass
        else:
            raise AssertionError("simulated append failure did not surface")
    finally:
        monkeypatch.setattr(store_module, "append_records", original_append)

    store.write("after", 3, version=1)
    assert [item["txn_id"] for item in store._recovery_report["rolled_back_transactions"]] == []
    reopened = KVStore(data_dir)
    assert reopened.get("before")["value"] == 1
    assert reopened.get("after")["value"] == 3
    with pytest.raises(KeyNotFound):
        reopened.get("failed")
    report = reopened.current_recovery_report()
    assert report["counts"]["rolled_back"] == 0
    assert report["counts"]["aborted"] == 1
    assert report["aborted_transactions"][0]["reason"] == "append_failed"
