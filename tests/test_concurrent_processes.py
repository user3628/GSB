"""Multi-process publish protection: real file locks across processes."""
import json
import multiprocessing
import os

from evalbench.checkpoint import CheckpointStore

PUB = dict(record_fingerprint="r", config_fingerprint="c", artifact={"v": 1})


def _publish_parse(ws, shard, queue):
    store = CheckpointStore(ws)
    try:
        store.publish(shard, "parse", **PUB)
        queue.put(("ok", shard))
    except Exception as exc:  # business error expected for losers
        queue.put((getattr(exc, "code", "E_INTERNAL"), shard))


def _publish_full_chain(ws, shard, queue):
    store = CheckpointStore(ws)
    try:
        for stage in ("parse", "calibrate", "diagnose"):
            store.publish(shard, stage, **PUB)
        queue.put(("ok", shard))
    except Exception as exc:
        queue.put((getattr(exc, "code", "E_INTERNAL"), shard))


def test_concurrent_processes_same_shard(tmp_path):
    """8 processes race to publish the same shard's first checkpoint:
    exactly one wins, the rest get E_STALE_CHECKPOINT."""
    ws = str(tmp_path / "ws")
    ctx = multiprocessing.get_context("spawn")
    queue = ctx.Queue()
    procs = [ctx.Process(target=_publish_parse, args=(ws, "shared", queue))
             for _ in range(8)]
    for p in procs:
        p.start()
    for p in procs:
        p.join(timeout=30)
    results = [queue.get(timeout=10) for _ in procs]
    wins = [r for r in results if r[0] == "ok"]
    stale = [r for r in results if r[0] == "E_STALE_CHECKPOINT"]
    assert len(wins) == 1, results
    assert len(stale) == 7, results
    assert CheckpointStore(ws).latest_seq("shared") == 1


def test_concurrent_processes_distinct_shards(tmp_path):
    """Processes publishing different shards never block each other."""
    ws = str(tmp_path / "ws")
    ctx = multiprocessing.get_context("spawn")
    queue = ctx.Queue()
    procs = [ctx.Process(target=_publish_full_chain, args=(ws, f"shard_{i}", queue))
             for i in range(4)]
    for p in procs:
        p.start()
    for p in procs:
        p.join(timeout=30)
    results = [queue.get(timeout=10) for _ in procs]
    assert all(code == "ok" for code, _ in results), results
    store = CheckpointStore(ws)
    for i in range(4):
        assert store.latest_seq(f"shard_{i}") == 3
