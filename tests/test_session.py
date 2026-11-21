from __future__ import annotations

import json
import zipfile
from pathlib import Path

import numpy as np
import pytest

from rope_diag.artifacts import ArtifactRef, ArtifactStore
from rope_diag.config import RopeConfig
from rope_diag.errors import RopeError
from rope_diag.session import DiagnosticSession


def _new_session(tmp_path: Path, assets: dict, name: str = "ws") -> DiagnosticSession:
    return DiagnosticSession.create(
        tmp_path / name, assets["manifest"], assets["config"], reset=True
    )


def test_full_run_persists_status_and_all_stage_artifacts(tmp_path: Path, assets: dict) -> None:
    session = _new_session(tmp_path, assets)
    status = session.run()
    assert status["status"] == "completed"
    assert all(
        stage["status"] == "completed"
        for shard in status["shards"].values()
        for stage in shard["stages"].values()
    )
    loaded = DiagnosticSession.load(session.workspace)
    assert loaded is not None
    assert loaded.status == "completed"
    assert loaded.config.version == session.config.version
    assert session.verify()["all_byte_identical"] is True


def test_two_fresh_workspaces_have_identical_artifact_digests(tmp_path: Path, assets: dict) -> None:
    first = _new_session(tmp_path, assets, "first")
    second = _new_session(tmp_path, assets, "second")
    first.run()
    second.run()
    first_digests = {
        shard_id: {
            stage: ref.artifact_digest for stage, ref in stages.items()
        }
        for shard_id, stages in first.stage_refs.items()
    }
    second_digests = {
        shard_id: {
            stage: ref.artifact_digest for stage, ref in stages.items()
        }
        for shard_id, stages in second.stage_refs.items()
    }
    assert first_digests == second_digests


def test_odd_dimension_metadata_is_explicit(tmp_path: Path, assets: dict) -> None:
    session = _new_session(tmp_path, assets)
    session.run()
    metadata = ArtifactStore(session.workspace).metadata(
        session.stage_refs["odd"]["frequency_generation"]
    )
    assert metadata["unrotated_channels"] == [8]
    assert metadata["odd_head_dim_recorded"] is True


def test_query_cache_is_byte_identical_to_bypass_recompute(tmp_path: Path, assets: dict) -> None:
    session = _new_session(tmp_path, assets)
    session.run()
    first = session.query(shard_id="long", query_type="weight_stats")
    cached = session.query(shard_id="long", query_type="weight_stats")
    recomputed = session.query(
        shard_id="long", query_type="weight_stats", bypass_cache=True
    )
    assert first["cache_hit"] is False
    assert cached["cache_hit"] is True
    assert first["payload_digest"] == cached["payload_digest"] == recomputed["payload_digest"]
    assert first["cache_key"] == cached["cache_key"] == recomputed["cache_key"]
    assert {
        "shard_fingerprint",
        "config_version",
        "angle_table_fingerprint",
    } <= set(first["cache_components"])


def test_latency_budget_returns_query_id_and_elapsed(tmp_path: Path, assets: dict) -> None:
    session = _new_session(tmp_path, assets)
    session.run()
    with pytest.raises(RopeError) as caught:
        session.query(
            shard_id="short",
            query_type="angles",
            latency_budget_ms=0,
            bypass_cache=True,
        )
    assert caught.value.code == "E_LATENCY_BUDGET"
    assert caught.value.details["query_id"].startswith("qry-")
    assert caught.value.details["elapsed_ms"] >= 0
    assert caught.value.details["budget_ms"] == 0.0


def test_missing_prerequisite_stage_is_invalid(tmp_path: Path, assets: dict) -> None:
    session = _new_session(tmp_path, assets)
    with pytest.raises(RopeError) as query_error:
        session.query(shard_id="short", query_type="angles")
    assert query_error.value.code == "E_INVALID_STAGE"
    assert query_error.value.details["missing_stage"] == "frequency_generation"

    from rope_diag.engine import RopeEngine

    engine = RopeEngine(session.workspace, session.shards, session.config)
    with pytest.raises(RopeError) as stage_error:
        engine._compute_stage(
            session.shard_by_id["short"], "block_rotation", {}
        )
    assert stage_error.value.code == "E_INVALID_STAGE"
    assert stage_error.value.details["missing_stage"] == "frequency_generation"


def test_incremental_block_change_reuses_frequency_and_matches_full(tmp_path: Path, assets: dict) -> None:
    session = _new_session(tmp_path, assets)
    session.run()
    old_frequency = {
        shard_id: stages["frequency_generation"].artifact_digest
        for shard_id, stages in session.stage_refs.items()
    }
    result = session.update_config({"rotation_block_size": 4}, run=True, verify=True)
    assert result["status"] == "completed"
    summary = result["recompute_report"]["stage_summary"]
    assert summary["frequency_generation"]["statuses"] == {
        "short": "reused",
        "long": "reused",
        "odd": "reused",
    }
    assert set(summary["block_rotation"]["statuses"].values()) == {"computed"}
    assert set(summary["scoring_normalization"]["statuses"].values()) == {"computed"}
    assert {
        shard_id: stages["frequency_generation"].artifact_digest
        for shard_id, stages in session.stage_refs.items()
    } == old_frequency
    assert result["consistency"]["all_byte_identical"] is True


def test_frequency_change_recomputes_every_stage(tmp_path: Path, assets: dict) -> None:
    session = _new_session(tmp_path, assets)
    session.run()
    result = session.update_config({"base_frequency": 12000.0}, run=True, verify=True)
    summary = result["recompute_report"]["stage_summary"]
    assert result["recompute_report"]["earliest_affected_stage"] == "frequency_generation"
    assert set(summary["frequency_generation"]["statuses"].values()) == {"computed"}
    assert result["consistency"]["all_byte_identical"] is True


def test_scheduler_change_reuses_all_compute_stages(tmp_path: Path, assets: dict) -> None:
    session = _new_session(tmp_path, assets)
    session.run()
    result = session.update_config(
        {"memory_budget_bytes": 1_250_000, "row_chunk_size": 4},
        run=True,
        verify=True,
    )
    summary = result["recompute_report"]["stage_summary"]
    assert result["recompute_report"]["earliest_affected_stage"] is None
    for stage in ("frequency_generation", "block_rotation", "scoring_normalization"):
        assert set(summary[stage]["statuses"].values()) == {"reused"}


def test_stop_and_resume_match_fresh_run(tmp_path: Path, assets: dict) -> None:
    reference = _new_session(tmp_path, assets, "reference")
    reference.run()
    reference_digests = {
        shard_id: {stage: ref.artifact_digest for stage, ref in stages.items()}
        for shard_id, stages in reference.stage_refs.items()
    }

    session = _new_session(tmp_path, assets, "resume")
    stopped = session.run(stop_after_stage="frequency_generation")
    assert stopped["status"] == "stopped"
    assert session.is_stop_requested()
    assert session.stage_refs["short"]["frequency_generation"]
    resumed = session.resume()
    assert resumed["status"] == "completed"
    resumed_digests = {
        shard_id: {stage: ref.artifact_digest for stage, ref in stages.items()}
        for shard_id, stages in session.stage_refs.items()
    }
    assert resumed_digests == reference_digests
    assert session.verify()["all_byte_identical"] is True


def test_incremental_mismatch_reports_differing_stage(tmp_path: Path, assets: dict) -> None:
    session = _new_session(tmp_path, assets)
    session.run()
    original = session.stage_refs["short"]["scoring_normalization"]
    session.stage_refs["short"]["scoring_normalization"] = ArtifactRef(
        stage=original.stage,
        shard_id=original.shard_id,
        artifact_digest="0" * 64,
        path=original.path,
        arrays=original.arrays,
    )
    with pytest.raises(RopeError) as caught:
        session.verify()
    assert caught.value.code == "E_INCREMENTAL_MISMATCH"
    assert caught.value.details["differing_stage"] == "scoring_normalization"
    assert caught.value.details["shard_id"] == "short"


def test_export_zip_contains_artifacts_and_verified_digest(tmp_path: Path, assets: dict) -> None:
    session = _new_session(tmp_path, assets)
    session.run()
    output = tmp_path / "bundle.zip"
    result = session.export(output)
    assert output.exists()
    with zipfile.ZipFile(output) as archive:
        names = archive.namelist()
        assert "summary.json" in names
        assert any(name.endswith("attention_weights.npy") for name in names)
        summary = json.loads(archive.read("summary.json"))
        assert summary["session"]["status"] == "completed"


def test_nan_input_returns_numeric_error_with_shard_and_stage_context(
    tmp_path: Path, assets: dict
) -> None:
    bad_query = np.zeros((1, 1, 3, 4), dtype=np.float32)
    bad_query[0, 0, 1, 0] = np.nan
    shard_path = tmp_path / "bad.npz"
    np.savez(shard_path, q=bad_query, k=np.zeros_like(bad_query))
    manifest = tmp_path / "bad_manifest.json"
    manifest.write_text(
        json.dumps(
            {"shards": [{"id": "bad", "file": str(shard_path), "shape": list(bad_query.shape)}]}
        ),
        encoding="utf-8",
    )
    session = DiagnosticSession.create(tmp_path / "bad_ws", manifest, assets["config"], reset=True)
    with pytest.raises(RopeError) as caught:
        session.run()
    assert caught.value.code == "E_NUMERIC"
    assert caught.value.details["shard_id"] == "bad"
    assert {"row_block_index", "step_index"} <= set(caught.value.details)
    assert session.status == "failed"
    assert session.stage_refs["bad"].get("frequency_generation") is not None
    assert session.stage_refs["bad"].get("block_rotation") is not None
    assert session.stage_refs["bad"].get("scoring_normalization") is None
