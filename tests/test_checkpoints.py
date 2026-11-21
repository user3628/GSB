from __future__ import annotations

import multiprocessing
from pathlib import Path

import pytest

from rope_diag.artifacts import ArtifactRef
from rope_diag.checkpoints import CheckpointStore
from rope_diag.errors import RopeError


def _ref(stage: str = "frequency_generation", digest: str = "a" * 64) -> ArtifactRef:
    return ArtifactRef(
        stage=stage,
        shard_id="s",
        artifact_digest=digest,
        path=f"artifacts/{stage}/s/{digest}",
        arrays={"angle_table": digest},
    )


def test_sequential_publish_and_stale_duplicate(tmp_path: Path) -> None:
    store = CheckpointStore(tmp_path)
    first = store.publish(
        shard_id="s",
        shard_fingerprint="fp",
        config_version="cfg",
        stage="frequency_generation",
        sequence=1,
        artifact=_ref(),
    )
    assert first.sequence == 1
    with pytest.raises(RopeError) as duplicate:
        store.publish(
            shard_id="s",
            shard_fingerprint="fp",
            config_version="cfg",
            stage="frequency_generation",
            sequence=1,
            artifact=_ref(),
        )
    assert duplicate.value.code == "E_STALE_CHECKPOINT"
    with pytest.raises(RopeError) as misplaced:
        store.publish(
            shard_id="s",
            shard_fingerprint="fp",
            config_version="cfg",
            stage="scoring_normalization",
            sequence=2,
            artifact=_ref("scoring_normalization", "b" * 64),
        )
    assert misplaced.value.code == "E_STALE_CHECKPOINT"
    second = store.publish(
        shard_id="s",
        shard_fingerprint="fp",
        config_version="cfg",
        stage="block_rotation",
        sequence=2,
        artifact=_ref("block_rotation", "b" * 64),
    )
    assert second.previous_artifact_digest == "a" * 64


def test_interrupted_record_write_leaves_no_partial_checkpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = CheckpointStore(tmp_path)
    import rope_diag.checkpoints as checkpoint_module

    original = checkpoint_module.atomic_write_json
    calls = {"count": 0}

    def fail_first(path, value, **kwargs):
        calls["count"] += 1
        if calls["count"] == 1:
            raise OSError("simulated interruption")
        return original(path, value, **kwargs)

    monkeypatch.setattr(checkpoint_module, "atomic_write_json", fail_first)
    with pytest.raises(OSError):
        store.publish(
            shard_id="s",
            shard_fingerprint="fp",
            config_version="cfg",
            stage="frequency_generation",
            sequence=1,
            artifact=_ref(),
        )
    record_dir = tmp_path / "checkpoints" / "s" / "cfg" / "records"
    assert not list(record_dir.glob("*.json")) if record_dir.exists() else True
    assert not list((tmp_path / "checkpoints").rglob("*.tmp-*"))
    monkeypatch.setattr(checkpoint_module, "atomic_write_json", original)
    record = store.publish(
        shard_id="s",
        shard_fingerprint="fp",
        config_version="cfg",
        stage="frequency_generation",
        sequence=1,
        artifact=_ref(),
    )
    assert record.sequence == 1


def _concurrent_publish(workspace: str, queue) -> None:
    try:
        store = CheckpointStore(workspace)
        record = store.publish(
            shard_id="shared",
            shard_fingerprint="fp",
            config_version="cfg",
            stage="frequency_generation",
            sequence=1,
            artifact=_ref(),
        )
        queue.put(("ok", record.sequence))
    except RopeError as exc:
        queue.put(("error", exc.code))


def test_concurrent_workers_only_one_publishes_same_sequence(tmp_path: Path) -> None:
    context = multiprocessing.get_context("spawn")
    queue = context.Queue()
    workers = [
        context.Process(target=_concurrent_publish, args=(str(tmp_path), queue))
        for _ in range(2)
    ]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=10)
    results = sorted(queue.get(timeout=2) for _ in range(2))
    assert results == [("error", "E_STALE_CHECKPOINT"), ("ok", 1)]
