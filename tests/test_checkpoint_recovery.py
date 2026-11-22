from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

import dep_engine.canonical as canonical_module
from dep_engine import CheckpointStore, Engine, EngineError


def test_atomic_write_never_leaves_target_or_temp_on_replace_failure(
    tmp_path: Path, monkeypatch
):
    target = tmp_path / "artifact.json"

    def fail_replace(source, destination):
        raise OSError("simulated interruption before rename")

    monkeypatch.setattr(canonical_module.os, "replace", fail_replace)
    with pytest.raises(OSError):
        canonical_module.atomic_write_bytes(target, b'{"partial":true}\n')
    assert not target.exists()
    assert list(tmp_path.glob(".artifact.json.tmp-*")) == []


def test_corrupt_latest_checkpoint_is_ignored_and_rebuilt(engine: Engine):
    stopped = engine.build(stop_after_shards=1)
    assert stopped["checkpoint"]["last_sequence"] == 4
    latest = engine.checkpoints_dir / "000000000004.json"
    latest.write_bytes(latest.read_bytes()[:17])

    damaged_state = engine.store().scan()
    assert damaged_state.last_sequence == 3
    assert damaged_state.ignored
    assert damaged_state.ignored[0]["sequence"] == 4

    resumed = engine.build()
    assert resumed["status"] == "complete"
    assert resumed["checkpoint"]["last_sequence"] == 12
    assert resumed["checkpoint"]["ignored"] == []
    sequences = [
        record["sequence"] for record in engine.store().scan().records
    ]
    assert sequences == list(range(1, 13))
    verification = engine.verify_against_full()
    assert verification["full_match"] is True


def test_duplicate_misnamed_checkpoint_is_ignored(engine: Engine):
    engine.build(stop_after_shards=1)
    source = engine.checkpoints_dir / "000000000004.json"
    duplicate = engine.checkpoints_dir / "000000000005.json"
    duplicate.write_bytes(source.read_bytes())
    state = engine.store().scan()
    assert state.last_sequence == 4
    assert state.ignored[0]["sequence"] == 5
    assert "sequence" in state.ignored[0]["reason"]


def test_checkpoint_record_contains_required_fields(engine: Engine):
    engine.build(stop_after_shards=1)
    state = engine.store().scan()
    record = state.records[0]
    required = {
        "shard_index",
        "stage",
        "input_fingerprint",
        "config_fingerprint",
        "output_digest",
        "sequence",
        "shard_sequence",
    }
    assert required <= set(record)
    assert record["stage"] == "featureization"
    assert record["sequence"] == 1
    assert record["shard_sequence"] == 1


def test_concurrent_publish_allows_exactly_one_worker(workspace: Path):
    ctx = __import__("multiprocessing").get_context("fork")
    queue = ctx.Queue()
    barrier = ctx.Barrier(2)
    names = {
        0: "data/shards/shard_001.txt",
        1: "data/shards/shard_002.txt",
        2: "data/shards/shard_003.txt",
    }
    processes = [
        ctx.Process(
            target=_publish_worker,
            args=(str(workspace), names, barrier, queue, marker),
        )
        for marker in ("a", "b")
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=20)
        assert process.exitcode == 0
    outcomes = [queue.get(timeout=5) for _ in processes]
    assert sum(1 for outcome in outcomes if outcome["status"] == "ok") == 1
    stale = [outcome for outcome in outcomes if outcome["status"] == "stale"]
    assert len(stale) == 1
    assert stale[0]["code"] == "E_STALE_CHECKPOINT"
    state = CheckpointStore(
        workspace / "state" / "checkpoints", names
    ).scan()
    assert state.last_sequence == 1
    assert len(state.records) == 1


def _publish_worker(workspace, names, barrier, queue, marker):
    from dep_engine.canonical import sha256_obj

    try:
        store = CheckpointStore(Path(workspace) / "state" / "checkpoints", names)
        barrier.wait(timeout=10)
        record = store.publish(
            shard_index=0,
            shard_name=names[0],
            stage="featureization",
            input_fingerprint=sha256_obj({"input": "same"}),
            input_components={"fixture": True},
            config_fingerprint=sha256_obj({"config": 1}),
            stage_config_fingerprint=sha256_obj({"stage": "featureization"}),
            stage_config_components={"stage": "featureization"},
            upstream_output_digests={},
            output={"marker": marker},
            expected_shard_sequence=1,
        )
        queue.put({"status": "ok", "sequence": record["sequence"]})
    except EngineError as exc:
        queue.put({"status": "stale", "code": exc.code, "details": exc.details})
    except BaseException as exc:  # pragma: no cover - diagnostic path
        queue.put({"status": "error", "error": repr(exc)})


def test_stale_and_lagging_shard_sequence_is_rejected(workspace: Path):
    from dep_engine.canonical import sha256_obj

    names = {
        0: "data/shards/shard_001.txt",
        1: "data/shards/shard_002.txt",
        2: "data/shards/shard_003.txt",
    }
    store = CheckpointStore(workspace / "state" / "checkpoints", names)
    common = dict(
        shard_index=0,
        shard_name=names[0],
        stage="featureization",
        input_fingerprint=sha256_obj({"x": 1}),
        input_components={"x": 1},
        config_fingerprint=sha256_obj({"x": 2}),
        stage_config_fingerprint=sha256_obj({"x": 3}),
        stage_config_components={"x": 3},
        upstream_output_digests={},
        output={"x": 4},
    )
    store.publish(**common, expected_shard_sequence=1)
    with pytest.raises(EngineError) as caught:
        store.publish(**common, expected_shard_sequence=1)
    assert caught.value.code == "E_STALE_CHECKPOINT"
    assert caught.value.details["reason"] == "stale_or_lagging"
    assert caught.value.details["required_shard_sequence"] == 2


def test_cross_shard_misaligned_publish_is_stale(workspace: Path):
    from dep_engine.canonical import sha256_obj

    names = {
        0: "data/shards/shard_001.txt",
        1: "data/shards/shard_002.txt",
        2: "data/shards/shard_003.txt",
    }
    store = CheckpointStore(workspace / "state" / "checkpoints", names)
    with pytest.raises(EngineError) as caught:
        store.publish(
            shard_index=0,
            shard_name=names[1],
            stage="featureization",
            input_fingerprint=sha256_obj({"x": 1}),
            input_components={"x": 1},
            config_fingerprint=sha256_obj({"x": 2}),
            stage_config_fingerprint=sha256_obj({"x": 3}),
            stage_config_components={"x": 3},
            upstream_output_digests={},
            output={"x": 4},
            expected_shard_sequence=1,
        )
    assert caught.value.code == "E_STALE_CHECKPOINT"
    assert caught.value.details["reason"] == "cross_shard_misaligned"
