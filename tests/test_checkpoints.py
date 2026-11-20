from __future__ import annotations

import json
import multiprocessing
from pathlib import Path

import pytest

from xrel_workbench.checkpoints import CheckpointStore
from xrel_workbench.config import load_config, config_version
from xrel_workbench.errors import BusinessError, E_INVALID_STAGE, E_STALE_CHECKPOINT
from xrel_workbench.util import read_json
from xrel_workbench.workspace import initialize_workspace, input_digests


def test_checkpoint_sequence_and_duplicate_protection(workspace) -> None:
    store = CheckpointStore(workspace.root)
    records = store.records()
    assert [record["seq"] for record in records] == list(range(1, 14))
    assert all(not path.name.endswith(".tmp") for path in workspace.checkpoints.iterdir())
    manifest = read_json(workspace.manifest)
    artifact = manifest["artifacts"]["stage1"]["part-001"]
    shard_input = input_digests(workspace)["shards"]["part-001"]
    config = load_config(workspace.scoring)
    with pytest.raises(BusinessError) as raised:
        store.publish(
            stage="stage1",
            shard="part-001",
            input_digest=shard_input,
            config_version=config_version(config),
            artifact_path=artifact["path"],
            fingerprint=artifact["fingerprint"],
        )
    assert raised.value.code == E_STALE_CHECKPOINT

    with pytest.raises(BusinessError) as stale_seq:
        store.publish(
            stage="stage1",
            shard="part-001",
            input_digest=shard_input,
            config_version=config_version(config),
            artifact_path=artifact["path"],
            fingerprint=artifact["fingerprint"],
            expected_seq=2,
        )
    assert stale_seq.value.code == E_STALE_CHECKPOINT


def test_missing_prerequisite_is_invalid_stage(tmp_path: Path, generated_data: Path) -> None:
    workspace = initialize_workspace(generated_data, tmp_path / "empty", force=True)
    fake = workspace.root / "state" / "artifacts" / "fake.json"
    fake.parent.mkdir(parents=True, exist_ok=True)
    fake.write_text("{}\n", encoding="utf-8")
    store = CheckpointStore(workspace.root)
    with pytest.raises(BusinessError) as raised:
        store.publish(
            stage="stage2",
            shard="part-001",
            input_digest="0" * 64,
            config_version="test",
            artifact_path="state/artifacts/fake.json",
            fingerprint="f" * 64,
        )
    assert raised.value.code == E_INVALID_STAGE


def test_cross_stage_misalignment_is_stale(workspace) -> None:
    store = CheckpointStore(workspace.root)
    manifest = read_json(workspace.manifest)
    artifact = manifest["artifacts"]["stage3"]["part-001"]
    with pytest.raises(BusinessError) as raised:
        store.publish(
            stage="stage3",
            shard="part-001",
            input_digest="0" * 64,
            config_version="test",
            artifact_path=artifact["path"],
            fingerprint="e" * 64,
        )
    assert raised.value.code == E_STALE_CHECKPOINT


def _publish_worker(
    workspace_root: str,
    barrier,
    queue,
) -> None:
    from xrel_workbench.checkpoints import CheckpointStore

    try:
        barrier.wait(timeout=10)
        record = CheckpointStore(Path(workspace_root)).publish(
            stage="stage1",
            shard="part-concurrent",
            input_digest="a" * 64,
            config_version="test",
            artifact_path="state/artifacts/concurrent.json",
            fingerprint="b" * 64,
        )
        queue.put(("ok", record["seq"]))
    except Exception as exc:  # pragma: no cover - assertion path
        queue.put(("error", getattr(exc, "code", type(exc).__name__)))


def test_concurrent_publishers_allow_only_one_sequence(tmp_path: Path, generated_data: Path) -> None:
    workspace = initialize_workspace(generated_data, tmp_path / "concurrent", force=True)
    artifact = workspace.root / "state" / "artifacts" / "concurrent.json"
    artifact.write_text("{}\n", encoding="utf-8")
    context = multiprocessing.get_context("spawn")
    barrier = context.Barrier(4)
    queue = context.Queue()
    processes = [
        context.Process(target=_publish_worker, args=(str(workspace.root), barrier, queue))
        for _ in range(4)
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(15)
        assert process.exitcode == 0
    results = [queue.get(timeout=2) for _ in processes]
    successes = [result for result in results if result[0] == "ok"]
    failures = [result for result in results if result[0] == "error"]
    assert successes == [("ok", 1)]
    assert len(failures) == 3
    assert all(code == E_STALE_CHECKPOINT for _, code in failures)
    assert [record["seq"] for record in CheckpointStore(workspace.root).records()] == [1]
