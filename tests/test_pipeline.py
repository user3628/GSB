"""Pipeline: determinism, graceful stop/resume, invalidation, error codes."""
import json
from pathlib import Path

import pytest

from sft_workbench.errors import E_INVALID_STAGE, E_STALE_CHECKPOINT, E_UNSUPPORTED_VERSION, WorkbenchError
from sft_workbench.generator import generate_shards
from sft_workbench.pipeline import Pipeline, PipelineConfig
from sft_workbench.util import digest_obj, sha256_hex


def tree_digest(root: Path) -> str:
    entries = {}
    for path in sorted(root.rglob("*")):
        if path.is_file():
            entries[str(path.relative_to(root))] = sha256_hex(path.read_bytes())
    return digest_obj(entries)


@pytest.fixture(scope="module")
def shards(tmp_path_factory):
    data_dir = tmp_path_factory.mktemp("data")
    return generate_shards(data_dir)


def test_full_run_determinism(tmp_path, shards):
    ws1, ws2 = tmp_path / "ws1", tmp_path / "ws2"
    result1 = Pipeline(ws1).run(shard_paths=[str(p) for p in shards])
    result2 = Pipeline(ws2).run(shard_paths=[str(p) for p in shards])
    assert result1["status"] == result2["status"] == "completed"
    assert result1["version_fingerprint"] == result2["version_fingerprint"]
    assert tree_digest(ws1 / "artifacts") == tree_digest(ws2 / "artifacts")
    summary = json.loads((ws1 / "artifacts/dedup_summary.json").read_text())
    assert len(summary["kept"]) > 0
    assert len(summary["removed"]) > 0
    assert any(e["code"] == "E_EMPTY_SAMPLE" for e in summary["errors"])


def test_graceful_stop_and_resume_byte_identical(tmp_path, shards):
    full = tmp_path / "full"
    Pipeline(full).run(shard_paths=[str(p) for p in shards])
    expected = tree_digest(full / "artifacts")

    calls = {"n": 0}

    def stop_after_ten():
        calls["n"] += 1
        return calls["n"] > 10

    interrupted = tmp_path / "interrupted"
    pipe = Pipeline(interrupted, stop_checker=stop_after_ten)
    stopped = pipe.run(shard_paths=[str(p) for p in shards])
    assert stopped["status"] == "stopped"
    assert (interrupted / "HEAD.json").exists() is False  # no stage completed

    resumed = Pipeline(interrupted).resume()
    assert resumed["status"] == "completed"
    assert tree_digest(interrupted / "artifacts") == expected
    seqs = [r.seq for r in Pipeline(interrupted).store.list_records()]
    assert seqs == [1, 2, 3, 4, 5]


def test_incremental_invalidation_propagation(tmp_path, shards):
    ws = tmp_path / "ws"
    pipe = Pipeline(ws)
    first = pipe.run(shard_paths=[str(p) for p in shards])
    config = PipelineConfig()
    config.scoring.weights = {"length": 0.7, "overlap": 0.1, "template": 0.1, "source": 0.1}
    rerun = Pipeline(ws, config=config).run()
    statuses = {s["stage"]: s["status"] for s in rerun["stages"]}
    assert statuses["import"] == "cached"
    assert statuses["dedup"] == "cached"
    assert statuses["score"] == "executed"
    assert statuses["sample"] == "executed"
    assert statuses["export"] == "executed"
    assert rerun["version_fingerprint"] != first["version_fingerprint"]
    seqs = [r.seq for r in Pipeline(ws).store.list_records()]
    assert seqs == [1, 2, 3, 4, 5]


def test_missing_prerequisite_stage(tmp_path):
    pipe = Pipeline(tmp_path / "ws")
    with pytest.raises(WorkbenchError) as excinfo:
        pipe.run(until="dedup")
    assert excinfo.value.code == E_INVALID_STAGE


def test_unsupported_export_version(tmp_path, shards):
    config = PipelineConfig(export_version=3)
    pipe = Pipeline(tmp_path / "ws", config=config)
    with pytest.raises(WorkbenchError) as excinfo:
        pipe.run(shard_paths=[str(p) for p in shards])
    assert excinfo.value.code == E_UNSUPPORTED_VERSION


def test_tampered_workspace_rejected_on_run(tmp_path, shards):
    ws = tmp_path / "ws"
    pipe = Pipeline(ws)
    pipe.run(shard_paths=[str(p) for p in shards])
    quality = ws / "artifacts/quality.json"
    quality.write_bytes(quality.read_bytes() + b" ")
    with pytest.raises(WorkbenchError) as excinfo:
        Pipeline(ws).run()
    err = excinfo.value
    assert err.code == E_STALE_CHECKPOINT
    assert err.details["kind"] == "tamper"
    assert err.details["expected_fingerprint"] != err.details["actual_fingerprint"]


def test_second_run_is_fully_cached(tmp_path, shards):
    ws = tmp_path / "ws"
    Pipeline(ws).run(shard_paths=[str(p) for p in shards])
    second = Pipeline(ws).run()
    assert all(s["status"] == "cached" for s in second["stages"])
