from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest

from sampling_engine.config import EngineConfig
from sampling_engine.engine import SamplingEngine
from sampling_engine.errors import EngineError, E_SNAPSHOT_CONFLICT

from conftest import SMALL_VOCAB, request_entry, write_logits, write_v2_manifest


def _matrix(seed: int = 1, steps: int = 4) -> np.ndarray:
    return np.round(np.random.default_rng(seed).normal(size=(steps, len(SMALL_VOCAB))), 6)


def _two_request_manifest(tmp_path: Path) -> tuple[Path, Path]:
    logits = write_logits(tmp_path / "logits.npy", _matrix())
    requests = [
        request_entry(
            request_id="req_a",
            logits_path=str(logits),
            top_p=0.85,
            seed=11,
            steps=4,
        ),
        request_entry(
            request_id="req_b",
            logits_path=str(logits),
            top_p=0.95,
            seed=12,
            steps=4,
        ),
    ]
    manifest = write_v2_manifest(tmp_path / "manifest.json", logits, requests=requests)
    return manifest, logits


def test_stop_and_resume_matches_full_run(tmp_path):
    manifest, _ = _two_request_manifest(tmp_path)
    full = SamplingEngine(tmp_path / "full", EngineConfig())
    full.import_manifest_path(manifest)
    full_result = full.run()["result"]

    stopped = SamplingEngine(tmp_path / "resumed", EngineConfig())
    stopped.import_manifest_path(manifest)
    first = stopped.run(stop_after_requests=1)
    assert first["stopped"] is True
    assert first["processed_count"] == 1
    second = stopped.resume()
    assert second["result"]["result_digest"] == full_result["result_digest"]
    for request_id in ("req_a", "req_b"):
        status = stopped.request_status(request_id)
        assert status["status"] == "complete"
        assert status["sequence"]["contiguous"] is True
        assert status["sequence"]["all_sequences"] == [1, 2, 3, 4]


def test_incremental_invalidation_preserves_unaffected_request_and_upstream(tmp_path):
    manifest, _ = _two_request_manifest(tmp_path)
    engine = SamplingEngine(tmp_path / "workspace", EngineConfig())
    engine.import_manifest_path(manifest)
    engine.run()
    before_a = engine.request_status("req_a")
    before_b = engine.request_status("req_b")

    payload = json.loads(manifest.read_text(encoding="utf-8"))
    payload["requests"][0]["top_p"] = 0.75
    manifest.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    engine.import_manifest_path(manifest)
    result = engine.run(request_ids=["req_a"])["processed"][0]
    assert result["reused_stages"] == ["postprocess"]
    assert result["rebuilt_stages"] == ["normalize", "decode", "summarize"]
    after_a = engine.request_status("req_a")
    after_b = engine.request_status("req_b")
    assert after_b["version_fingerprint"] == before_b["version_fingerprint"]
    assert after_b["sequence"] == before_b["sequence"]
    assert after_a["version_fingerprint"] != before_a["version_fingerprint"]
    invalidation = after_a["invalidations"][-1]
    assert invalidation["earliest_stage"] == "normalize"
    assert invalidation["invalidated_stages"] == [
        "normalize",
        "decode",
        "summarize",
    ]
    assert invalidation["changed_fields"] == ["top_p"]
    assert after_a["sequence"]["contiguous"] is True
    assert after_a["sequence"]["history_sequences"] == [2, 3, 4]


def test_seed_change_invalidates_decode_first(tmp_path):
    manifest, _ = _two_request_manifest(tmp_path)
    engine = SamplingEngine(tmp_path / "workspace", EngineConfig())
    engine.import_manifest_path(manifest)
    engine.run(request_ids=["req_a"])
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    payload["requests"][0]["seed"] = 999
    manifest.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    engine.import_manifest_path(manifest)
    result = engine.run(request_ids=["req_a"])["processed"][0]
    assert result["reused_stages"] == ["postprocess", "normalize"]
    assert result["rebuilt_stages"] == ["decode", "summarize"]
    invalidation = engine.request_status("req_a")["invalidations"][-1]
    assert invalidation["earliest_stage"] == "decode"
    assert invalidation["changed_fields"] == ["seed"]


def test_logits_content_change_invalidates_postprocess(tmp_path):
    manifest, logits = _two_request_manifest(tmp_path)
    engine = SamplingEngine(tmp_path / "workspace", EngineConfig())
    engine.import_manifest_path(manifest)
    engine.run(request_ids=["req_a"])
    write_logits(logits, _matrix(seed=99))
    result = engine.run(request_ids=["req_a"])["processed"][0]
    assert result["reused_stages"] == []
    assert result["rebuilt_stages"] == [
        "postprocess",
        "normalize",
        "decode",
        "summarize",
    ]
    invalidation = engine.request_status("req_a")["invalidations"][-1]
    assert invalidation["earliest_stage"] == "postprocess"
    assert "logits_content_sha256" in invalidation["changed_fields"]


def test_structure_version_change_invalidates_postprocess(tmp_path):
    manifest, logits = _two_request_manifest(tmp_path)
    engine = SamplingEngine(tmp_path / "workspace", EngineConfig(v1_default_top_p=0.85))
    engine.import_manifest_path(manifest)
    engine.run(request_ids=["req_a"])
    v1 = {
        "manifest_version": 1,
        "vocab": SMALL_VOCAB,
        "requests": [
            {
                "request_id": "req_a",
                "logits_path": str(logits),
                "temp_percent": 80,
                "top_k": 4,
                "repetition_penalty": 1.1,
                "seed": 11,
                "steps": 4,
                "prompt_tokens": [],
            },
            json.loads(manifest.read_text())["requests"][1],
        ],
    }
    # The second request keeps v2 fields, so remove it for this focused test.
    v1["requests"] = [v1["requests"][0]]
    manifest.write_text(json.dumps(v1, ensure_ascii=False), encoding="utf-8")
    engine.import_manifest_path(manifest)
    result = engine.run(request_ids=["req_a"])["processed"][0]
    assert result["rebuilt_stages"] == [
        "postprocess",
        "normalize",
        "decode",
        "summarize",
    ]
    invalidation = engine.request_status("req_a")["invalidations"][-1]
    assert invalidation["earliest_stage"] == "postprocess"
    assert "manifest_version" in invalidation["changed_fields"]


def test_snapshot_artifact_digest_mismatch_is_reported(tmp_path):
    manifest, _ = _two_request_manifest(tmp_path)
    engine = SamplingEngine(tmp_path / "workspace", EngineConfig())
    engine.import_manifest_path(manifest)
    engine.run(request_ids=["req_a"])
    artifact = engine.workspace / "requests" / "req_a" / "artifacts" / "postprocess.npy"
    with open(artifact, "ab") as handle:
        handle.write(b"tampered")
    with pytest.raises(EngineError) as info:
        engine.request_status("req_a")
    assert info.value.code == E_SNAPSHOT_CONFLICT
    assert info.value.context["reason"] == "digest_mismatch"


def test_snapshot_export_detects_rollback_and_digest_mismatch(tmp_path):
    manifest, _ = _two_request_manifest(tmp_path)
    engine = SamplingEngine(tmp_path / "workspace", EngineConfig())
    engine.import_manifest_path(manifest)
    engine.run(request_ids=["req_a"])
    snapshot_path = tmp_path / "snapshot.json"
    engine.export_workspace_snapshot(snapshot_path)
    old_generation = json.loads(snapshot_path.read_text())["generation"]
    engine.run(request_ids=["req_b"])
    with pytest.raises(EngineError) as info:
        engine.validate_workspace_snapshot(snapshot_path)
    assert info.value.code == E_SNAPSHOT_CONFLICT
    assert info.value.context["reason"] == "version_rollback"
    assert info.value.context["incoming_generation"] == old_generation

    payload = json.loads(snapshot_path.read_text())
    payload["generation"] += 1
    snapshot_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(EngineError) as info:
        engine.validate_workspace_snapshot(snapshot_path)
    assert info.value.code == E_SNAPSHOT_CONFLICT
    assert info.value.context["reason"] == "digest_mismatch"


def test_whole_workspace_generation_rollback_is_detected(tmp_path):
    manifest, _ = _two_request_manifest(tmp_path)
    workspace = tmp_path / "workspace"
    engine = SamplingEngine(workspace, EngineConfig())
    engine.import_manifest_path(manifest)
    engine.run(request_ids=["req_a"])
    meta_path = workspace / "workspace_meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    assert meta["generation"] > 1
    meta["generation"] = 1
    meta_path.write_text(json.dumps(meta), encoding="utf-8")
    with pytest.raises(EngineError) as info:
        SamplingEngine(workspace, EngineConfig())
    assert info.value.code == E_SNAPSHOT_CONFLICT
    assert info.value.context["reason"] == "version_rollback"
