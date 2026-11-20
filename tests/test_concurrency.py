from __future__ import annotations

import json
import multiprocessing as mp
import time
from pathlib import Path

from qoplib.checkpoint import (
    CheckpointStore,
    build_record,
    deterministic_sequence,
)
from qoplib.errors import StaleCheckpointError


def _concurrent_committer(args):
    run_dir, shard_index = args
    store = CheckpointStore(run_dir, enforce_contiguous=True)
    stages = ("validate", "quantize", "linear", "verify")
    for stage in stages:
        sequence = deterministic_sequence(shard_index, stage)
        # Wait until the global contiguous cursor reaches this record, then
        # publish exactly once. This mirrors the parent-side publisher fence
        # while still exercising cross-process file locking.
        for _ in range(10000):
            try:
                if store.next_sequence() == sequence:
                    store.commit(build_record(
                        sequence, f"shard_{shard_index:02d}", shard_index,
                        stage, "in_fp", 1, f"fp-{sequence}", {}, "run_fp"))
                    break
            except StaleCheckpointError:
                pass
            time.sleep(0.001)
        else:
            raise RuntimeError(
                f"shard {shard_index} stage {stage} never reached cursor")
    return shard_index


def test_concurrent_publishers_preserve_contiguous_sequence(tmp_path):
    ctx = mp.get_context("spawn")
    run_dir = str(tmp_path / "run")
    # Interleave shard indices so process spawn order differs from sequence.
    work = [(run_dir, idx) for idx in (3, 1, 2, 0)]
    with ctx.Pool(processes=4) as pool:
        finished = pool.map(_concurrent_committer, work)
    assert sorted(finished) == [0, 1, 2, 3]

    store = CheckpointStore(run_dir)
    counter = json.loads((Path(run_dir) / "counter.json").read_text())
    sequences = [item["sequence"] for item in counter["committed"]]
    assert sequences == list(range(16))
    assert len(store.all_records()) == 16
