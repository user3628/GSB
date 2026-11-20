from __future__ import annotations

import json
import threading
import time
from pathlib import Path

from kvstore.errors import ConcurrentWrite
from kvstore.store import KVStore


def test_two_threads_same_batch_only_one_succeeds(tmp_path: Path) -> None:
    store = KVStore(tmp_path / "data")
    payload = b'{"key":"k","value":1,"version":1,"written_at_us":4100000000000000}\n'
    barrier = threading.Barrier(2)
    results = []
    errors = []

    def worker() -> None:
        barrier.wait()
        try:
            results.append(store.import_data(payload, expected_format="json_lines", batch_id="parallel-batch"))
        except Exception as exc:  # assertion below checks type/code
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)
    assert len(results) == 1
    assert len(errors) == 1
    assert isinstance(errors[0], ConcurrentWrite)
    assert errors[0].code == "E_CONCURRENT_WRITE"


def test_snapshot_switch_while_writer_active_returns_concurrent(tmp_path: Path) -> None:
    store = KVStore(tmp_path / "data")
    store.write("a", 1, version=1)
    store._begin_write()
    try:
        try:
            store.switch_snapshot()
        except ConcurrentWrite as exc:
            assert exc.code == "E_CONCURRENT_WRITE"
        else:
            raise AssertionError("snapshot switch should not wait for an active writer")
    finally:
        store._end_write()


def test_reader_latency_does_not_depend_on_writer_progress(tmp_path: Path) -> None:
    store = KVStore(tmp_path / "data")
    store.write("a", 1, version=1)
    store._begin_write()
    try:
        started = time.monotonic()
        result = store.get("a", read_timeout_ms=50)
        elapsed_ms = (time.monotonic() - started) * 1000
        assert result["value"] == 1
        assert elapsed_ms < 50
    finally:
        store._end_write()
