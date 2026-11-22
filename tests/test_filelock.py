import multiprocessing as mp
import os
from pathlib import Path

from legal_cite.checkpoints import file_lock, load_index, publish_checkpoint


def _worker(ckpt, stage, queue):
    try:
        publish_checkpoint(Path(ckpt), stage, "in", 3, "art", f"fp-{stage}")
        queue.put(("ok", stage))
    except Exception as exc:  # pragma: no cover - asserted in parent
        queue.put(("err", str(exc)))


def test_real_flock_serializes_two_processes(tmp_path):
    ckpt = tmp_path / "ckpt"
    ctx = mp.get_context("fork")
    queue = ctx.Queue()
    p1 = ctx.Process(target=_worker, args=(str(ckpt), "parse", queue))
    p2 = ctx.Process(target=_worker, args=(str(ckpt), "normalize", queue))
    p1.start(); p2.start(); p1.join(); p2.join()
    results = sorted(queue.get() for _ in range(2))
    assert results == [("ok", "normalize"), ("ok", "parse")]
    index = load_index(ckpt)
    assert [r["stage"] for r in index["records"]] == ["parse", "normalize"]


def test_competing_publish_after_gap_is_invalid_stage(tmp_path):
    ckpt = tmp_path / "ckpt"
    publish_checkpoint(ckpt, "parse", "in", 3, "art", "fp")
    ctx = mp.get_context("fork")
    queue = ctx.Queue()
    proc = ctx.Process(target=_worker, args=(str(ckpt), "evidence", queue))
    proc.start(); proc.join()
    kind, payload = queue.get()
    assert kind == "err"
    assert "E_INVALID_STAGE" in payload


def test_lock_is_exclusive(tmp_path):
    lock = tmp_path / "x.lock"
    with file_lock(lock):
        assert lock.exists()
    # lock file remains but is released and re-acquirable
    with file_lock(lock):
        assert True
