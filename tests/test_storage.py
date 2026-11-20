from __future__ import annotations

import multiprocessing
from pathlib import Path

import pytest

from ree.errors import REEError
from ree.models import sha256_bytes
from ree.pipeline import Pipeline


def _concurrent_publisher(
    root: str,
    runtime: str,
    artifact_path: str,
    artifact_digest: str,
    config_version: str,
    barrier,
    queue,
) -> None:
    from ree.errors import REEError
    from ree.pipeline import Pipeline

    pipeline = Pipeline(root, runtime_dir=runtime)
    barrier.wait(timeout=10)
    try:
        record = pipeline.checkpoints.publish(
            stage="parse",
            expected_seq=1,
            input_digest=sha256_bytes(b"input"),
            config_version=config_version,
            artifact_digest=artifact_digest,
            artifact_path=artifact_path,
            input_components={"probe": 1},
        )
        queue.put(("ok", record.seq))
    except REEError as exc:
        queue.put(("error", exc.code))


def test_checkpoint_publish_is_contiguous_and_rejects_stale(workspace: Path) -> None:
    pipeline = Pipeline(workspace, runtime_dir=".ree_checkpoints")
    digest, path, _ = pipeline.artifacts.save("parse", {"probe": "parse"})
    record = pipeline.checkpoints.publish(
        stage="parse",
        expected_seq=1,
        input_digest=sha256_bytes(b"input"),
        config_version=sha256_bytes(b"config"),
        artifact_digest=digest,
        artifact_path=path,
        input_components={"probe": 1},
    )
    assert record.seq == 1
    with pytest.raises(REEError) as duplicate:
        pipeline.checkpoints.publish(
            stage="parse",
            expected_seq=1,
            input_digest=sha256_bytes(b"input"),
            config_version=sha256_bytes(b"config"),
            artifact_digest=digest,
            artifact_path=path,
            input_components={"probe": 1},
        )
    assert duplicate.value.code == "E_STALE_CHECKPOINT"
    with pytest.raises(REEError) as gap:
        pipeline.checkpoints.publish(
            stage="estimate",
            expected_seq=3,
            input_digest=sha256_bytes(b"input"),
            config_version=sha256_bytes(b"config"),
            artifact_digest=digest,
            artifact_path=path,
            input_components={"probe": 1},
        )
    assert gap.value.code == "E_STALE_CHECKPOINT"


def test_cross_stage_publish_without_prerequisite_is_rejected(workspace: Path) -> None:
    pipeline = Pipeline(workspace, runtime_dir=".ree_cross_stage")
    digest, path, _ = pipeline.artifacts.save("estimate", {"probe": "estimate"})
    with pytest.raises(REEError) as captured:
        pipeline.checkpoints.publish(
            stage="estimate",
            expected_seq=1,
            input_digest=sha256_bytes(b"input"),
            config_version=sha256_bytes(b"config"),
            artifact_digest=digest,
            artifact_path=path,
            input_components={"probe": 1},
        )
    assert captured.value.code == "E_STALE_CHECKPOINT"
    assert "prerequisite" in captured.value.message


def test_file_lock_allows_only_one_concurrent_publisher(workspace: Path) -> None:
    pipeline = Pipeline(workspace, runtime_dir=".ree_concurrent")
    digest, path, _ = pipeline.artifacts.save("parse", {"probe": "concurrent"})
    context = multiprocessing.get_context("fork")
    barrier = context.Barrier(2)
    queue = context.Queue()
    processes = [
        context.Process(
            target=_concurrent_publisher,
            args=(
                str(workspace),
                ".ree_concurrent",
                path,
                digest,
                sha256_bytes(b"config"),
                barrier,
                queue,
            ),
        )
        for _ in range(2)
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=20)
    results = sorted(queue.get(timeout=5) for _ in processes)
    assert results == [("error", "E_STALE_CHECKPOINT"), ("ok", 1)]
    assert all(process.exitcode == 0 for process in processes)
