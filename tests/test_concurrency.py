"""Requirement 6: cross-process file-lock publish protection."""
from __future__ import annotations

import multiprocessing as mp
from pathlib import Path

from depworkbench.checkpoint import CheckpointRecord, CheckpointStore
from depworkbench.errors import E_STALE_CHECKPOINT, WorkbenchError


def _publish(cp_root: str, index: int, queue: mp.Queue) -> None:
    try:
        store = CheckpointStore(Path(cp_root))
        store.publish(CheckpointRecord.create(
            index, index, f"in{index}", "model", "cfg", {"tokens": index}))
        queue.put(("ok", index))
    except WorkbenchError as exc:
        queue.put((exc.code, index))


def test_concurrent_publishers_only_contiguous_indices_win(tmp_path):
    cp_root = tmp_path / "cp"
    ctx = mp.get_context("fork")
    queue: mp.Queue = ctx.Queue()
    processes = [
        ctx.Process(target=_publish, args=(str(cp_root), index, queue))
        for index in range(4)
    ]
    for process in processes:
        process.start()
    outcomes = [queue.get() for _ in processes]
    for process in processes:
        process.join(timeout=10)

    store = CheckpointStore(cp_root)
    prefix, warnings = store.load_consistent_prefix()
    ok_indices = [r.shard_index for r in prefix]
    # Every accepted publish is contiguous from 0 with no gap or duplicate;
    # every losing publisher is reported stale rather than corrupting state.
    assert ok_indices == list(range(len(ok_indices)))
    stale = sorted(index for code, index in outcomes
                   if code == E_STALE_CHECKPOINT)
    assert len(ok_indices) + len(stale) == 4
    assert set(ok_indices).isdisjoint(stale)
    assert warnings == []


def test_ordered_cross_process_publish_succeeds(tmp_path):
    cp_root = tmp_path / "cp2"
    ctx = mp.get_context("fork")
    queue: mp.Queue = ctx.Queue()
    for index in range(3):
        process = ctx.Process(target=_publish,
                              args=(str(cp_root), index, queue))
        process.start()
        process.join(timeout=10)
        assert queue.get() == ("ok", index)
    store = CheckpointStore(cp_root)
    prefix, _ = store.load_consistent_prefix()
    assert [r.shard_index for r in prefix] == [0, 1, 2]
