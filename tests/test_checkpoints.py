from __future__ import annotations

import multiprocessing as mp
import os
from pathlib import Path

import pytest

from posalign.canonical import atomic_write_bytes, atomic_write_json, read_json
from posalign.checkpoints import CheckpointStore
from posalign.errors import BusinessError, E_INVALID_STAGE, E_STALE_CHECKPOINT


def _head(workspace: Path) -> None:
    (workspace / "state").mkdir(parents=True, exist_ok=True)
    atomic_write_json(
        workspace / "state" / "head.json",
        {
            "schema": "posalign.checkpoint_head.v1",
            "generation": 1,
            "seq": 0,
            "generation_max_stage_rank": -1,
            "products": {},
            "active_run": None,
            "last_invalidation": None,
        },
    )


def _concurrent_publisher(workspace: str, index: int, queue) -> None:
    from posalign.errors import BusinessError
    from posalign.checkpoints import CheckpointStore

    try:
        CheckpointStore(workspace).publish(
            stage="parse",
            shard=f"s{index}",
            payload={"index": index},
            fingerprint=f"f{index}",
            generation=1,
            governance_version="g",
            governance_fingerprints={},
            shard_content_fp=f"shard{index}",
            sequence=1,
        )
        queue.put(("ok", index))
    except BusinessError as error:
        queue.put((error.code, index))


def test_checkpoint_content_and_sequence(tmp_path):
    _head(tmp_path)
    store = CheckpointStore(tmp_path)
    checkpoint = store.publish(
        stage="parse",
        shard="s1",
        payload={"ok": True},
        fingerprint="fp",
        generation=1,
        governance_version="gov",
        governance_fingerprints={"alias": "a"},
        shard_content_fp="content",
        mapping_version=0,
    )
    assert checkpoint["seq"] == 1
    record = read_json(tmp_path / "state" / "checkpoints" / "00000001.json")
    assert record["stage"] == "parse"
    assert record["shard_content_fingerprint"] == "content"
    assert record["governance_version"] == "gov"
    assert record["mapping_table_version"] == 0
    assert record["seq"] == 1


def test_missing_prerequisite_and_cross_stage_order(tmp_path):
    _head(tmp_path)
    store = CheckpointStore(tmp_path)
    with pytest.raises(BusinessError) as caught:
        store.publish(
            stage="align",
            shard="s1",
            payload={},
            fingerprint="a",
            generation=1,
            governance_version="g",
            governance_fingerprints={},
            shard_content_fp="c",
        )
    assert caught.value.code == E_INVALID_STAGE
    store.publish(
        stage="parse",
        shard="s1",
        payload={},
        fingerprint="p",
        generation=1,
        governance_version="g",
        governance_fingerprints={},
        shard_content_fp="c",
    )
    store.publish(
        stage="align",
        shard="s1",
        payload={},
        fingerprint="a",
        generation=1,
        governance_version="g",
        governance_fingerprints={},
        shard_content_fp="c",
    )
    with pytest.raises(BusinessError) as caught2:
        store.publish(
            stage="parse",
            shard="s2",
            payload={},
            fingerprint="p2",
            generation=1,
            governance_version="g",
            governance_fingerprints={},
            shard_content_fp="c2",
        )
    assert caught2.value.code == E_STALE_CHECKPOINT
    assert caught2.value.details["stage_rank"] < caught2.value.details["generation_max_stage_rank"]


def test_multi_process_sequence_is_serialized(tmp_path):
    _head(tmp_path)
    context = mp.get_context("fork" if os.name != "nt" else "spawn")
    queue = context.Queue()
    processes = [
        context.Process(target=_concurrent_publisher, args=(str(tmp_path), index, queue))
        for index in range(5)
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=15)
        assert process.exitcode == 0
    results = [queue.get(timeout=5) for _ in processes]
    successes = [result for result in results if result[0] == "ok"]
    rejects = [result for result in results if result[0] == E_STALE_CHECKPOINT]
    assert len(successes) == 1
    assert len(rejects) == 4
    head = read_json(tmp_path / "state" / "head.json")
    assert head["seq"] == 1
    assert len(head["products"]) == 1


def test_atomic_write_leaves_previous_file_on_failure(tmp_path, monkeypatch):
    target = tmp_path / "artifact.json"
    target.write_bytes(b"old\n")

    def fail_replace(source, destination):
        raise OSError("simulated interruption")

    monkeypatch.setattr("posalign.canonical.os.replace", fail_replace)
    with pytest.raises(OSError):
        atomic_write_bytes(target, b"new\n")
    assert target.read_bytes() == b"old\n"
    assert not list(tmp_path.glob(".artifact.json.tmp-*"))

