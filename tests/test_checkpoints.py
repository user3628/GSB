from __future__ import annotations

import json
import multiprocessing
from pathlib import Path

import numpy as np

from conv_workbench.canonical import npy_bytes
from conv_workbench.checkpoints import CheckpointStore
from conv_workbench.engine import Workbench
from conv_workbench.errors import WorkbenchError


def _publish_worker(run_dir: str, value: float, barrier, queue) -> None:
    try:
        store = CheckpointStore(run_dir, "race")
        fingerprint = "a" * 64
        template = {
            "tensor_fingerprint": fingerprint,
            "op_name": "conv2d",
            "op_fingerprint": "b" * 64,
            "input_fingerprint": fingerprint,
            "config_version": "race-v1",
            "chain_prefix": "c" * 64,
            "operator_summary": {"name": "conv2d"},
            "shape": [1, 1, 1, 1],
            "dtype": "float64",
            "expected_range": {"shape": [1, 1, 1, 1], "range": [-10.0, 10.0]},
            "execution": {},
        }
        barrier.wait(timeout=10)
        store.publish_stage(stage_index=0, artifact_bytes=npy_bytes(np.array([[[[value]]]])), template=template)
        queue.put(("ok", value))
    except WorkbenchError as exc:
        queue.put((exc.code, value))
    except BaseException as exc:
        queue.put(("E_TEST", repr(exc)))


def test_concurrent_stage_publication_rejects_stale_worker(tmp_path: Path):
    context = multiprocessing.get_context("spawn")
    barrier = context.Barrier(2)
    queue = context.Queue()
    run_dir = str(tmp_path / "race")
    processes = [
        context.Process(target=_publish_worker, args=(run_dir, value, barrier, queue))
        for value in (1.0, 2.0)
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=20)
        assert process.exitcode == 0
    results = [queue.get(timeout=5), queue.get(timeout=5)]
    assert sorted(result[0] for result in results) == ["E_STALE_CHECKPOINT", "ok"]
    records = CheckpointStore(run_dir, "race").load_records()
    assert [record["checkpoint_seq"] for record in records] == [1]


def _rewrite_journal(store: CheckpointStore, lines: list[str]) -> None:
    store.journal_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_loader_ignores_malformed_and_duplicate_records(demo_workspace: Path, tmp_path: Path):
    workbench = Workbench(demo_workspace, tmp_path / "records")
    workbench.run(shard_ids=["medium_odd_dilated"])
    store = CheckpointStore(workbench.run_dir, "medium_odd_dilated")
    lines = store.journal_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 3
    _rewrite_journal(store, [lines[0], "{not valid json", lines[1], lines[1], lines[2]])
    records = store.load_records()
    assert [record["checkpoint_seq"] for record in records] == [1, 2, 3]
    assert [record["checkpoint_sha256"] for record in records] == [
        json.loads(line)["checkpoint_sha256"] for line in (lines[0], lines[1], lines[2])
    ]


def test_loader_stops_at_last_consistent_checkpoint_on_corruption(demo_workspace: Path, tmp_path: Path):
    workbench = Workbench(demo_workspace, tmp_path / "corrupt")
    workbench.run(shard_ids=["medium_odd_dilated"])
    store = CheckpointStore(workbench.run_dir, "medium_odd_dilated")
    lines = store.journal_path.read_text(encoding="utf-8").splitlines()
    second = json.loads(lines[1])
    second["artifact_digest"] = "0" * 64
    _rewrite_journal(store, [lines[0], json.dumps(second, separators=(",", ":"), sort_keys=True), lines[2]])
    records = store.load_records()
    assert [record["checkpoint_seq"] for record in records] == [1]

