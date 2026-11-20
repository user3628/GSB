from __future__ import annotations

import multiprocessing as mp
import queue
from pathlib import Path

import numpy as np

from gemm_lab.blocking import BlockPlan
from gemm_lab.cache import ResultCache
from gemm_lab.config_store import VersionedConfigStore


PLAN = BlockPlan(4, 8, 32, 16, 32)


def _auto_config_worker(root: str, start, result_queue, count: int) -> None:
    try:
        store = VersionedConfigStore(Path(root))
        start.wait(10)
        sequences = []
        for index in range(count):
            record = store.publish(
                fingerprint=f"fp-{mp.current_process().pid}-{index}",
                plan=PLAN,
                sum_strategy="pairwise",
                cache_budget={"l1_bytes": 32768, "l2_bytes": 1048576},
                selection_source="auto",
            )
            sequences.append(record.seq)
        result_queue.put(("ok", sequences))
    except BaseException as exc:  # pragma: no cover - reported to parent
        result_queue.put(("error", repr(exc)))


def _exact_seq_worker(root: str, start, result_queue) -> None:
    try:
        store = VersionedConfigStore(Path(root))
        start.wait(10)
        record = store.publish(
            fingerprint=f"race-{mp.current_process().pid}",
            plan=PLAN,
            sum_strategy="pairwise",
            cache_budget={"l1_bytes": 32768, "l2_bytes": 1048576},
            selection_source="auto",
            expected_seq=1,
        )
        result_queue.put(("ok", record.seq))
    except BaseException as exc:
        result_queue.put(("error", getattr(exc, "code", type(exc).__name__)))


def _cache_worker(root: str, index: int, start, result_queue) -> None:
    try:
        cache = ResultCache(Path(root))
        start.wait(10)
        cache_key = f"key-{index}"
        published = cache.publish(
            key=cache_key,
            result=np.full((2, 2), index, dtype=np.float64),
            key_components={"index": index},
            config_seq=1,
        )
        result_queue.put(("ok", published.cache_seq))
    except BaseException as exc:  # pragma: no cover
        result_queue.put(("error", repr(exc)))


def _collect(processes, result_queue, expected_messages):
    messages = [result_queue.get(timeout=20) for _ in range(expected_messages)]
    for process in processes:
        process.join(timeout=20)
        assert process.exitcode == 0
    return messages


def test_concurrent_config_publish_is_continuous(tmp_path: Path):
    context = mp.get_context("spawn")
    start = context.Event()
    result_queue = context.Queue()
    root = tmp_path / "config_store"
    processes = [
        context.Process(target=_auto_config_worker, args=(str(root), start, result_queue, 3))
        for _ in range(4)
    ]
    for process in processes:
        process.start()
    start.set()
    messages = _collect(processes, result_queue, 4)
    assert all(message[0] == "ok" for message in messages)
    sequences = sorted(sequence for _, values in messages for sequence in values)
    assert sequences == list(range(1, 13))
    state = VersionedConfigStore(root).verify_state()
    assert state.seq == 12


def test_concurrent_exact_sequence_allows_one_publisher(tmp_path: Path):
    context = mp.get_context("spawn")
    root = tmp_path / "config_store"
    store = VersionedConfigStore(root)
    store.publish(
        fingerprint="initial",
        plan=PLAN,
        sum_strategy="pairwise",
        cache_budget={"l1_bytes": 32768, "l2_bytes": 1048576},
        selection_source="auto",
    )
    start = context.Event()
    result_queue = context.Queue()
    processes = [
        context.Process(target=_exact_seq_worker, args=(str(root), start, result_queue))
        for _ in range(4)
    ]
    for process in processes:
        process.start()
    start.set()
    messages = _collect(processes, result_queue, 4)
    successes = [message for message in messages if message[0] == "ok"]
    failures = [message for message in messages if message[0] == "error"]
    assert len(successes) == 1 and successes[0][1] == 2
    assert len(failures) == 3
    assert all("E_STALE_PUBLISH" in message[1] for message in failures)


def test_concurrent_cache_publish_is_continuous(tmp_path: Path):
    context = mp.get_context("spawn")
    root = tmp_path / "cache"
    start = context.Event()
    result_queue = context.Queue()
    processes = [
        context.Process(target=_cache_worker, args=(str(root), index, start, result_queue))
        for index in range(4)
    ]
    for process in processes:
        process.start()
    start.set()
    messages = _collect(processes, result_queue, 4)
    assert all(message[0] == "ok" for message in messages)
    assert sorted(message[1] for message in messages) == [1, 2, 3, 4]
    assert ResultCache(root).status()["entry_count"] == 4

