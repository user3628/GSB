from __future__ import annotations

import multiprocessing as mp
from pathlib import Path

import pytest

from tonelab.checkpoints import CheckpointStore
from tonelab.engine import BuildEngine
from tonelab.errors import ToneLabError
from tonelab.util import atomic_write_bytes
from tonelab.workspace import Workspace


def _publish_worker(root: str, sequence: int, queue) -> None:
    try:
        record = CheckpointStore(root).publish(
            stage="feature",
            shard_id=f"shard-{sequence}-{mp.current_process().pid}",
            shard_fingerprint="shard-fingerprint",
            structure_version_fingerprint="structure-fingerprint",
            config_version="config-version",
            artifact_summary={"kind": "synthetic"},
            sequence=sequence,
            project_id="concurrency-test",
        )
        queue.put(("ok", record["sequence"]))
    except ToneLabError as exc:
        queue.put(("error", exc.code))


def test_checkpoint_monotonic_and_stale(tmp_path: Path) -> None:
    artifact = tmp_path / "artifact.json"
    artifact.write_text("{}", encoding="utf-8")
    store = CheckpointStore(tmp_path / "checkpoints")
    first = store.publish(
        stage="feature",
        shard_id="s1",
        shard_fingerprint="sf1",
        structure_version_fingerprint="vf",
        config_version="cv",
        artifact_summary={"artifact": 1},
        artifact_paths=[artifact],
    )
    second = store.publish(
        stage="model",
        shard_id="s1",
        shard_fingerprint="sf1",
        structure_version_fingerprint="vf",
        config_version="cv",
        artifact_summary={"artifact": 2},
        artifact_paths=[artifact],
    )
    assert (first["sequence"], second["sequence"]) == (1, 2)
    assert first["artifact_digest"] and first["config_version"] == "cv"
    with pytest.raises(ToneLabError) as stale:
        store.publish(
            stage="feature",
            shard_id="s1",
            shard_fingerprint="sf1",
            structure_version_fingerprint="vf",
            config_version="cv",
            artifact_summary={"artifact": 1},
            sequence=2,
        )
    assert stale.value.code == "E_STALE_CHECKPOINT"
    with pytest.raises(ToneLabError) as duplicate:
        store.publish(
            stage="feature",
            shard_id="s1",
            shard_fingerprint="sf1",
            structure_version_fingerprint="vf",
            config_version="cv",
            artifact_summary={"artifact": 1},
        )
    assert duplicate.value.code == "E_STALE_CHECKPOINT"
    with pytest.raises(ToneLabError) as gap:
        store.publish(
            stage="feature",
            shard_id="s2",
            shard_fingerprint="sf2",
            structure_version_fingerprint="vf",
            config_version="cv",
            artifact_summary={"artifact": 4},
            sequence=9,
        )
    assert gap.value.code == "E_STALE_CHECKPOINT"


def test_checkpoint_invalid_stage_and_missing_artifact(tmp_path: Path) -> None:
    store = CheckpointStore(tmp_path / "checkpoints")
    with pytest.raises(ToneLabError) as missing_upstream:
        store.publish(
            stage="model",
            shard_id="s1",
            shard_fingerprint="sf",
            structure_version_fingerprint="vf",
            config_version="cv",
            artifact_summary={"artifact": 1},
        )
    assert missing_upstream.value.code == "E_INVALID_STAGE"
    with pytest.raises(ToneLabError) as missing_file:
        store.publish(
            stage="feature",
            shard_id="s1",
            shard_fingerprint="sf",
            structure_version_fingerprint="vf",
            config_version="cv",
            artifact_summary={"artifact": 1},
            artifact_paths=[tmp_path / "not-there.json"],
        )
    assert missing_file.value.code == "E_INVALID_STAGE"
    assert store.records() == []


def test_missing_prerequisite_artifact_is_invalid_stage(tmp_path: Path) -> None:
    artifact = tmp_path / "feature.json"
    artifact.write_text("{}", encoding="utf-8")
    store = CheckpointStore(tmp_path / "checkpoints")
    store.publish(
        stage="feature",
        shard_id="s1",
        shard_fingerprint="sf",
        structure_version_fingerprint="vf",
        config_version="cv",
        artifact_summary={"artifact": 1},
        artifact_paths=[artifact],
    )
    artifact.unlink()
    with pytest.raises(ToneLabError) as captured:
        store.publish(
            stage="model",
            shard_id="s1",
            shard_fingerprint="sf",
            structure_version_fingerprint="vf",
            config_version="cv",
            artifact_summary={"artifact": 2},
        )
    assert captured.value.code == "E_INVALID_STAGE"
    assert captured.value.context["missing"]


def test_atomic_write_interruption_leaves_no_half_file(tmp_path: Path, monkeypatch) -> None:
    def interrupted_replace(src, dst):
        raise RuntimeError("simulated process interruption before replace")

    monkeypatch.setattr("tonelab.util.os.replace", interrupted_replace)
    target = tmp_path / "checkpoint.json"
    with pytest.raises(RuntimeError):
        atomic_write_bytes(target, b'{"half":')
    assert not target.exists()
    assert list(tmp_path.glob("*.tmp")) == []
    assert list(tmp_path.glob(".*.tmp")) == []


def test_multiprocess_duplicate_sequence_rejected(tmp_path: Path) -> None:
    ctx = mp.get_context("fork")
    queue = ctx.Queue()
    root = str(tmp_path / "checkpoints")
    processes = [
        ctx.Process(target=_publish_worker, args=(root, 1, queue))
        for _ in range(2)
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=10)
        assert process.exitcode == 0
    outcomes = [queue.get(timeout=2) for _ in range(2)]
    assert sorted(status for status, _value in outcomes) == ["error", "ok"]
    assert any(value == "E_STALE_CHECKPOINT" for status, value in outcomes if status == "error")
    records = CheckpointStore(root).records()
    assert len(records) == 1
    assert records[0]["sequence"] == 1


def test_stop_finishes_current_shard_and_resume_matches_full(
    tmp_path: Path,
    demo_data: Path,
    baseline,
    monkeypatch,
) -> None:
    workspace = Workspace(tmp_path / "stopped")
    workspace.import_manifests(
        [demo_data / "manifest_v2.json", demo_data / "manifest_v1.json"],
        project_id="test-demo",
    )
    engine = BuildEngine(workspace)
    import tonelab.engine as engine_module

    original = engine_module.extract_features
    calls = {"count": 0}

    def request_stop_during_first_shard(shard, config):
        artifact = original(shard, config)
        calls["count"] += 1
        if calls["count"] == 1:
            workspace.stop.request("test stop after first shard")
        return artifact

    monkeypatch.setattr(engine_module, "extract_features", request_stop_during_first_shard)
    stopped = engine.build("incremental")
    assert stopped["stopped"] is True
    assert stopped["next_shard"] == workspace.load_project().shards[1].shard_id
    records_after_stop = workspace.checkpoints.records()
    assert len(records_after_stop) == 4
    assert [record["stage"] for record in records_after_stop] == ["feature", "model", "quantize", "export"]
    monkeypatch.undo()
    resumed = engine.resume()
    assert resumed["stopped"] is False
    assert resumed["deterministic_fingerprint"] == baseline[2]["deterministic_fingerprint"]
    records = workspace.checkpoints.records()
    assert [record["sequence"] for record in records] == list(range(1, len(records) + 1))
    assert len(records) == 16


def test_named_stage_execution_then_resume_matches_full(
    tmp_path: Path,
    demo_data: Path,
    baseline,
) -> None:
    workspace = Workspace(tmp_path / "staged")
    workspace.import_manifests(
        [demo_data / "manifest_v2.json", demo_data / "manifest_v1.json"],
        project_id="test-demo",
    )
    engine = BuildEngine(workspace)
    feature_report = engine.run_stage("feature")
    assert feature_report["complete"] is False
    assert feature_report["run_stages"] == ["feature"]
    assert all(stages == ["feature"] for stages in feature_report["recomputed_stages"].values())
    assert len(workspace.checkpoints.records()) == 4
    resumed = engine.build("incremental")
    assert resumed["complete"] is True
    assert resumed["deterministic_fingerprint"] == baseline[2]["deterministic_fingerprint"]
    assert len(workspace.checkpoints.records()) == 16

