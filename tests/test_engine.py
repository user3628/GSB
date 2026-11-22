from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest

from embedding_gather.digest import deterministic_npz_write, load_npz, sha256_file
from embedding_gather.engine import open_engine
from embedding_gather.errors import BusinessError
from embedding_gather.kernel import GatherConfig


SAFE = ["batch-ok", "batch-cross-shard", "batch-duplicate", "batch-negative", "batch-pad"]
CONFIG = GatherConfig(output_scale=0.0001)


def _append_batch(data_root: Path, batch_id: str, ids: np.ndarray) -> None:
    relative = f"index_batches/{batch_id}.npz"
    deterministic_npz_write(
        data_root / relative,
        {
            "ids": ids,
            "batch_id": np.asarray(batch_id),
            "batch_number": np.asarray(99, dtype=np.int64),
            "table_id": np.asarray("demo-table"),
            "gather_axis": np.asarray(0, dtype=np.int64),
        },
    )
    manifest_path = data_root / "manifest_v2.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["index_batches"].append(
        {
            "batch_id": batch_id,
            "table_id": "demo-table",
            "file": relative,
            "batch_number": 99,
            "gather_axis": 0,
        }
    )
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, sort_keys=True), encoding="utf-8")


def test_full_run_and_stop_resume_bundle_are_byte_identical(sample_copy, tmp_path):
    full = open_engine(
        sample_copy / "manifest_v2.json",
        sample_copy,
        tmp_path / "full",
        gather_config=CONFIG,
    ).run(SAFE)
    assert full.status == "complete"
    assert full.bundle_digest is not None

    stopped_engine = open_engine(
        sample_copy / "manifest_v2.json",
        sample_copy,
        tmp_path / "resume",
        gather_config=CONFIG,
    )
    stopped = stopped_engine.run(SAFE, stop_after_batches=2)
    assert stopped.status == "stopped"
    assert stopped.completed_batches == tuple(SAFE[:2])
    resumed = stopped_engine.run(SAFE)
    assert resumed.status == "complete"

    full_bytes = Path(full.bundle_path).read_bytes()
    resumed_bytes = Path(resumed.bundle_path).read_bytes()
    assert full_bytes == resumed_bytes
    assert full.bundle_digest == resumed.bundle_digest
    assert [item.status for item in resumed.results[:2]] == ["reused", "reused"]
    assert [item.status for item in resumed.results[2:]] == ["built", "built", "built"]
    for left, right in zip(full.results, resumed.results, strict=True):
        assert left.artifact_digest == right.artifact_digest
        assert sha256_file(left.artifact_path) == sha256_file(right.artifact_path)


def test_fresh_workspaces_produce_byte_identical_bundles(sample_copy, tmp_path):
    first = open_engine(
        sample_copy / "manifest_v2.json",
        sample_copy,
        tmp_path / "a",
        gather_config=CONFIG,
    ).run(["batch-ok", "batch-duplicate"])
    second = open_engine(
        sample_copy / "manifest_v2.json",
        sample_copy,
        tmp_path / "b",
        gather_config=CONFIG,
    ).run(["batch-duplicate", "batch-ok"])
    assert Path(first.bundle_path).read_bytes() == Path(second.bundle_path).read_bytes()


def test_invalid_batches_do_not_publish_output(sample_copy, tmp_path):
    engine = open_engine(
        sample_copy / "manifest_v2.json",
        sample_copy,
        tmp_path / "state",
        gather_config=CONFIG,
    )
    for batch_id in ("batch-oob", "batch-zero-weight"):
        link = engine.project.batch_map()[batch_id]
        with pytest.raises(BusinessError):
            engine.process_batch(link)
        assert not engine._artifact_path(engine._store(link), "verify").exists()
    assert not (tmp_path / "state" / "final" / "bundle.npz").exists()


def test_weight_change_invalidates_gather_not_validate(sample_copy, tmp_path):
    workspace = tmp_path / "state"
    engine = open_engine(sample_copy / "manifest_v2.json", sample_copy, workspace, gather_config=CONFIG)
    engine.run(["batch-duplicate"])

    path = sample_copy / "weights" / "shard-000.npz"
    arrays = load_npz(path)
    arrays["weights_q"] = arrays["weights_q"].copy()
    arrays["weights_q"][0, 0] = int(arrays["weights_q"][0, 0]) + 9
    deterministic_npz_write(path, arrays)

    changed = open_engine(sample_copy / "manifest_v2.json", sample_copy, workspace, gather_config=CONFIG)
    report = changed.inspect(["batch-duplicate"])[0]
    assert report["earliest_invalid_stage"] == "gather"
    assert "validate" in report["reused_stages"]
    assert "changed:weight_content" in report["reasons"]
    assert report["expected_fingerprints"]["gather"] != report["stage_details"][1]["recorded_input_fingerprint"]

    result = changed.run(["batch-duplicate"])
    sequences = result.results[0].stage_sequences
    assert sequences["validate"] == 1
    assert sequences["gather"] > 4
    invalidations = changed.inspect(["batch-duplicate"])[0]["invalidations"]
    assert invalidations[-1]["invalidated_stage"] == "gather"
    assert "changed:weight_content" in invalidations[-1]["invalidation_reasons"]


def test_quantization_width_change_invalidates_only_quantize(sample_copy, tmp_path):
    workspace = tmp_path / "state"
    open_engine(sample_copy / "manifest_v2.json", sample_copy, workspace, gather_config=CONFIG).run(["batch-ok"])
    manifest_path = sample_copy / "manifest_v2.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["quantization_bits"] = 13
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, sort_keys=True), encoding="utf-8")

    changed = open_engine(manifest_path, sample_copy, workspace, gather_config=CONFIG)
    report = changed.inspect(["batch-ok"])[0]
    assert report["earliest_invalid_stage"] == "quantize"
    assert report["reused_stages"] == ["validate", "gather"]
    assert "changed:quantization_bits" in report["reasons"]


def test_pad_change_invalidates_validate_stage(sample_copy, tmp_path):
    workspace = tmp_path / "state"
    open_engine(sample_copy / "manifest_v2.json", sample_copy, workspace, gather_config=CONFIG).run(["batch-pad"])
    manifest_path = sample_copy / "manifest_v2.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["shards"][0]["pad_ranges"] = [[27, 32]]
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, sort_keys=True), encoding="utf-8")

    changed = open_engine(manifest_path, sample_copy, workspace, gather_config=CONFIG)
    report = changed.inspect(["batch-pad"])[0]
    assert report["earliest_invalid_stage"] == "validate"
    assert report["reused_stages"] == []


def test_weights_not_used_by_batch_do_not_invalidate_it(sample_copy, tmp_path):
    _append_batch(sample_copy, "batch-shard1-only", np.asarray([33, 40], dtype=np.int64))
    workspace = tmp_path / "state"
    open_engine(sample_copy / "manifest_v2.json", sample_copy, workspace, gather_config=CONFIG).run(
        ["batch-shard1-only"]
    )
    path = sample_copy / "weights" / "shard-000.npz"
    arrays = load_npz(path)
    arrays["weights_q"] = arrays["weights_q"].copy()
    arrays["weights_q"][0, 0] = int(arrays["weights_q"][0, 0]) + 3
    deterministic_npz_write(path, arrays)

    changed = open_engine(sample_copy / "manifest_v2.json", sample_copy, workspace, gather_config=CONFIG)
    report = changed.inspect(["batch-shard1-only"])[0]
    assert report["all_valid"] is True
    result = changed.run(["batch-shard1-only"])
    assert result.results[0].status == "reused"


def test_manifest_structure_upgrade_invalidates_validate(sample_copy, tmp_path):
    workspace = tmp_path / "state"
    v1_engine = open_engine(
        sample_copy / "manifest_v1.json",
        sample_copy,
        workspace,
        gather_config=CONFIG,
    )
    v1_engine.run(["batch-ok"])
    v2_engine = open_engine(
        sample_copy / "manifest_v2.json",
        sample_copy,
        workspace,
        gather_config=CONFIG,
    )
    report = v2_engine.inspect(["batch-ok"])[0]
    assert report["earliest_invalid_stage"] == "validate"
    assert "changed:manifest_structure_version" in report["reasons"]


def test_stop_file_stops_at_batch_boundary(sample_copy, tmp_path):
    stop_file = tmp_path / "STOP"
    engine = open_engine(
        sample_copy / "manifest_v2.json",
        sample_copy,
        tmp_path / "state",
        gather_config=CONFIG,
    )
    stop_file.write_text("stop", encoding="utf-8")
    report = engine.run(SAFE, stop_file=stop_file)
    assert report.status == "stopped"
    assert report.completed_batches == ()


def test_zero_scale_from_weight_file_returns_numeric_with_hit_context(sample_copy, tmp_path):
    path = sample_copy / "weights" / "shard-000.npz"
    arrays = load_npz(path)
    arrays["scale"] = np.asarray(0.0, dtype=np.float64)
    deterministic_npz_write(path, arrays)
    engine = open_engine(
        sample_copy / "manifest_v2.json",
        sample_copy,
        tmp_path / "state",
        gather_config=CONFIG,
    )
    with pytest.raises(BusinessError) as error:
        engine.process_batch(engine.project.batch_map()["batch-duplicate"])
    assert error.value.code == "E_NUMERIC"
    assert error.value.context["shard_id"] == "shard-000"
    assert error.value.context["index_id"] == 3
    assert error.value.context["component"] == "scale"
    assert error.value.context["ids_digest"]


def test_missing_stage_artifact_returns_invalid_stage(sample_copy, tmp_path):
    engine = open_engine(
        sample_copy / "manifest_v2.json",
        sample_copy,
        tmp_path / "state",
        gather_config=CONFIG,
    )
    engine.run(["batch-ok"])
    link = engine.project.batch_map()["batch-ok"]
    engine._artifact_path(engine._store(link), "gather").unlink()
    with pytest.raises(BusinessError) as error:
        engine.process_batch(link)
    assert error.value.code == "E_INVALID_STAGE"
    assert error.value.context["stage"] == "gather"


def test_manifest_structure_rollback_is_rejected(sample_copy, tmp_path):
    workspace = tmp_path / "state"
    open_engine(sample_copy / "manifest_v2.json", sample_copy, workspace, gather_config=CONFIG).run(["batch-ok"])
    v1_engine = open_engine(sample_copy / "manifest_v1.json", sample_copy, workspace, gather_config=CONFIG)
    with pytest.raises(BusinessError) as error:
        v1_engine.process_batch(v1_engine.project.batch_map()["batch-ok"])
    assert error.value.code == "E_STALE_CHECKPOINT"
    assert error.value.context["reason"] == "rollback"


def test_interruption_between_artifact_and_record_recovers_deterministically(sample_copy, tmp_path):
    class InjectedCrash(RuntimeError):
        pass

    crashed = {"done": False}

    def crash_once(kind, label):
        if not crashed["done"]:
            crashed["done"] = True
            raise InjectedCrash(label)

    interrupted = open_engine(
        sample_copy / "manifest_v2.json",
        sample_copy,
        tmp_path / "interrupted",
        gather_config=CONFIG,
        fault_hook=crash_once,
    )
    with pytest.raises(InjectedCrash):
        interrupted.process_batch(interrupted.project.batch_map()["batch-ok"])
    link = interrupted.project.batch_map()["batch-ok"]
    store = interrupted._store(link)
    assert store.read_records() == []
    assert interrupted._artifact_path(store, "validate").exists()

    resumed_engine = open_engine(
        sample_copy / "manifest_v2.json",
        sample_copy,
        tmp_path / "interrupted",
        gather_config=CONFIG,
    )
    resumed = resumed_engine.process_batch(link)
    full = open_engine(
        sample_copy / "manifest_v2.json",
        sample_copy,
        tmp_path / "full",
        gather_config=CONFIG,
    ).process_batch(link)
    assert resumed.artifact_digest == full.artifact_digest
    assert Path(resumed.artifact_path).read_bytes() == Path(full.artifact_path).read_bytes()
