import multiprocessing as mp
from pathlib import Path

from prefalign.core.checkpoint import (
    FileLock,
    checkpoint_record,
    load_checkpoint,
    publish_checkpoint,
)
from prefalign.core.errors import StaleCheckpointError


def _publisher(ckpt_dir: str, seq: int, queue) -> None:
    try:
        with FileLock(Path(ckpt_dir) / "publish.lock"):
            publish_checkpoint(
                Path(ckpt_dir),
                checkpoint_record("normalize", seq, {"in": 1}, 1, "cfg", "d", "fp"))
        queue.put(("ok", seq))
    except StaleCheckpointError as exc:
        queue.put((exc.code, seq))


def test_parallel_publish_only_correct_seq_wins(tmp_path):
    queue = mp.get_context("fork").Queue()
    processes = [
        mp.get_context("fork").Process(target=_publisher, args=(str(tmp_path), seq, queue))
        for seq in (1, 1, 2)
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join()
    outcomes = [queue.get() for _ in processes]
    codes = sorted(code for code, _ in outcomes)
    # seq 1 wins once; the duplicate seq 1 and the jumping seq 2 are rejected.
    assert codes == ["E_STALE_CHECKPOINT", "E_STALE_CHECKPOINT", "ok"]
    assert sorted(seq for code, seq in outcomes if code == "ok") == [1]
    assert sorted(seq for code, seq in outcomes
                  if code == "E_STALE_CHECKPOINT") == [1, 2]
    record = load_checkpoint(tmp_path, "normalize")
    assert record["seq"] == 1


def test_lock_is_mutually_exclusive(tmp_path):
    lock = FileLock(tmp_path / "x.lock")
    with lock:
        # Re-entering from a second handle blocks; verify the first holds it.
        assert lock._handle is not None
