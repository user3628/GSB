from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
import pytest

from anomaly_workbench.checkpoints import CheckpointStore
from anomaly_workbench.data import DEMO_SEED, generate_demo, mutate_shard_copy
from anomaly_workbench.demo import run_demo
from anomaly_workbench.errors import WorkbenchError
from anomaly_workbench.pipeline import Pipeline
from anomaly_workbench.reference import generate_reference, verify_reference
from anomaly_workbench.util import atomic_write_bytes, read_json
from anomaly_workbench.web import create_server


def artifact_bytes(name: str) -> bytes:
    return json.dumps(
        {"payload": {"name": name}}, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def test_checkpoint_store_guards_and_recovery(tmp_path: Path) -> None:
    store = CheckpointStore(tmp_path)

    with pytest.raises(WorkbenchError) as exc:
        store.publish(
            stage="fit",
            tier="small",
            input_fingerprint="fit-input",
            config_version="cfg",
            artifact_bytes=artifact_bytes("fit"),
            upstream_fingerprints={"preprocess": "pre-input"},
        )
    assert exc.value.code == "E_INVALID_STAGE"

    first = store.publish(
        stage="preprocess",
        tier="small",
        input_fingerprint="pre-input",
        config_version="cfg",
        artifact_bytes=artifact_bytes("pre"),
    )
    assert first["seq"] == 1

    with pytest.raises(WorkbenchError) as exc:
        store.publish(
            stage="preprocess",
            tier="small",
            input_fingerprint="pre-input",
            config_version="cfg",
            artifact_bytes=artifact_bytes("pre"),
            expected_seq=1,
        )
    assert exc.value.code == "E_STALE_CHECKPOINT"

    with pytest.raises(WorkbenchError) as exc:
        store.publish(
            stage="fit",
            tier="small",
            input_fingerprint="fit-input",
            config_version="cfg",
            artifact_bytes=artifact_bytes("fit"),
            upstream_fingerprints={"preprocess": "wrong-pre-input"},
        )
    assert exc.value.code == "E_STALE_CHECKPOINT"

    second = store.publish(
        stage="fit",
        tier="small",
        input_fingerprint="fit-input",
        config_version="cfg",
        artifact_bytes=artifact_bytes("fit"),
        upstream_fingerprints={"preprocess": "pre-input"},
    )
    assert second["seq"] == 2
    assert [record["seq"] for record in store.scan()["records"]] == [1, 2]

    # A torn/corrupt record is ignored and the consistent prefix stops before it.
    atomic_write_bytes(store.records_dir / "00000002.json", b'{"seq":2,')
    scan = store.scan()
    assert scan["max_seq"] == 1
    assert any(item.get("seq") == 2 for item in scan["ignored"])

    # Publishing at the first inconsistent sequence quarantines the damaged tail.
    repaired = store.publish(
        stage="fit",
        tier="small",
        input_fingerprint="fit-input",
        config_version="cfg",
        artifact_bytes=artifact_bytes("fit"),
        upstream_fingerprints={"preprocess": "pre-input"},
    )
    assert repaired["seq"] == 2
    assert repaired["quarantined"] == ["00000002.json"]
    assert [record["seq"] for record in store.scan()["records"]] == [1, 2]


def test_duplicate_sequence_is_ignored(tmp_path: Path) -> None:
    store = CheckpointStore(tmp_path)
    store.publish(
        stage="preprocess",
        tier="small",
        input_fingerprint="pre-input",
        config_version="cfg",
        artifact_bytes=artifact_bytes("pre"),
    )
    original = (store.records_dir / "00000001.json").read_bytes()
    (store.records_dir / "00000001.dup.json").write_bytes(original)
    scan = store.scan()
    assert scan["max_seq"] == 0
    assert any(item.get("reason") == "duplicate sequence records" for item in scan["ignored"])


def test_concurrent_publishers_allow_only_one_next_sequence(tmp_path: Path) -> None:
    store = CheckpointStore(tmp_path)
    store.publish(
        stage="preprocess",
        tier="small",
        input_fingerprint="pre-input",
        config_version="cfg",
        artifact_bytes=artifact_bytes("pre"),
    )
    barrier = threading.Barrier(2)

    def contender(index: int) -> str:
        local = CheckpointStore(tmp_path)
        barrier.wait(timeout=5)
        try:
            local.publish(
                stage="fit",
                tier="small",
                input_fingerprint=f"fit-input-{index}",
                config_version="cfg",
                artifact_bytes=artifact_bytes(f"fit-{index}"),
                upstream_fingerprints={"preprocess": "pre-input"},
                expected_seq=2,
            )
            return "published"
        except WorkbenchError as exc:
            return exc.code

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = sorted(executor.map(contender, [0, 1]))
    assert results == ["E_STALE_CHECKPOINT", "published"]
    assert [record["seq"] for record in store.scan()["records"]] == [1, 2]


def test_complete_demo_and_incremental_properties(tmp_path: Path) -> None:
    report = tmp_path / "acceptance-report.json"
    result = run_demo(tmp_path / "workspace", seed=DEMO_SEED, force=True, report_path=report)
    assert result["reference_verified"] is True
    assert result["full_equivalence"] == {"small": True, "medium": True}
    assert result["checkpoint_sequence_contiguous"] is True
    assert result["invalid_threshold_error"] == "E_INVALID_CONFIG"
    assert result["config_change"]["earliest_stage"] == "score"
    assert result["config_change"]["actions"]["small"] == {
        "preprocess": "reused",
        "fit": "reused",
        "score": "rebuilt",
        "diagnostics": "rebuilt",
    }
    assert result["shard_replacement"]["actions"] == {
        "preprocess": "rebuilt",
        "fit": "rebuilt",
        "score": "rebuilt",
        "diagnostics": "rebuilt",
    }
    stored = read_json(report)
    assert stored["tier_observations"]["small"]["rows"] == 800
    assert stored["tier_observations"]["small"]["columns"] == 10
    assert stored["tier_observations"]["medium"]["rows"] == 6000
    assert stored["tier_observations"]["medium"]["columns"] == 30
    assert stored["tier_observations"]["small"]["missing_column_indices"]
    assert stored["tier_observations"]["medium"]["heavy_tail_column_indices"]
    for tier in ("small", "medium"):
        for filename in (
            "preprocess-summary.json",
            "model-summary.json",
            "scores.json",
            "diagnostics.json",
            "manifest.json",
        ):
            assert (tmp_path / "workspace" / "exports" / tier / filename).exists()


def test_import_full_declaration_with_one_replacement_shard(tmp_path: Path) -> None:
    generate_demo(tmp_path, force=True)
    pipeline = Pipeline(tmp_path)
    assert pipeline.run(tiers=["small"])["status"] == "complete"
    replacement = tmp_path / "replacement.npz"
    mutate_shard_copy(tmp_path, "small", 1, replacement)
    declaration = tmp_path / "data" / "small" / "declaration.json"
    result = pipeline.import_dataset(
        "small", declaration, [replacement], replace=True
    )
    assert result["invalidation"]["earliest_stage"] == "preprocess"
    rerun = pipeline.run(tiers=["small"])
    assert {
        action["stage"]: action["action"] for action in rerun["actions"]
    } == {
        "preprocess": "rebuilt",
        "fit": "rebuilt",
        "score": "rebuilt",
        "diagnostics": "rebuilt",
    }
    assert pipeline.verify_full_equivalence("small")["byte_identical"] is True


def test_negative_weight_is_numeric_error_then_recover(tmp_path: Path) -> None:
    generate_demo(tmp_path, force=True)
    pipeline = Pipeline(tmp_path)
    pipeline.run(tiers=["small"])
    before = pipeline.current_artifacts("small")
    pipeline.update_config({"scoring": {"weights": {"covariance": -0.2, "forest": 0.5}}})
    with pytest.raises(WorkbenchError) as exc:
        pipeline.run(tiers=["small"])
    assert exc.value.code == "E_NUMERIC"
    assert exc.value.stage == "score"
    assert exc.value.sample_id
    assert exc.value.details["detector"] == "covariance"

    pipeline.update_config({"scoring": {"weights": {"covariance": 0.5, "forest": 0.5}}})
    rerun = pipeline.run(tiers=["small"])
    assert rerun["status"] == "complete"
    after = pipeline.current_artifacts("small")
    assert before["preprocess"]["artifact_digest"] == after["preprocess"]["artifact_digest"]
    assert before["fit"]["artifact_digest"] == after["fit"]["artifact_digest"]
    assert before["score"]["artifact_digest"] == after["score"]["artifact_digest"]
    assert pipeline.verify_full_equivalence("small")["byte_identical"] is True


def _http_call(port: int, path: str, body: dict | None = None) -> tuple[int, dict]:
    data = None if body is None else json.dumps(body).encode("utf-8")
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}",
        data=data,
        headers={"content-type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


def test_http_stop_resume_and_invalid_threshold(tmp_path: Path) -> None:
    generate_demo(tmp_path, force=True)
    server, _ = create_server(tmp_path, "127.0.0.1", 0)
    port = int(server.server_address[1])
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        status, health = _http_call(port, "/api/health")
        assert status == 200 and health["ok"] is True
        assert _http_call(port, "/api/stop", {})[0] == 200
        status, execute = _http_call(
            port, "/api/execute", {"tiers": ["small"], "wait": True}
        )
        assert status == 200
        execution = execute["execution"]
        assert execution["result"]["status"] == "stopped"
        assert execution["result"]["stopped_after"] == {"tier": "small", "stage": "preprocess"}

        status, resumed = _http_call(
            port, "/api/resume", {"tiers": ["small"], "wait": True}
        )
        assert status == 200
        assert resumed["execution"]["result"]["status"] == "complete"
        status, sensitivity = _http_call(port, "/api/sensitivity?tier=small")
        assert status == 200 and sensitivity["ok"] is True

        status, invalid = _http_call(
            port, "/api/config", {"patch": {"scoring": {"threshold_quantile": 1.1}}}
        )
        assert status == 400
        assert invalid["error"]["code"] == "E_INVALID_CONFIG"
        status, exported = _http_call(port, "/api/export", {"tier": "small"})
        assert status == 200 and exported["ok"] is True
    finally:
        server.shutdown()
        server.server_close()


def test_reference_is_independent_and_matches(tmp_path: Path) -> None:
    generate_demo(tmp_path, force=True)
    pipeline = Pipeline(tmp_path)
    pipeline.run(tiers=["small"])
    generated = generate_reference(tmp_path, tiers=["small"])
    verification = verify_reference(tmp_path)
    assert generated["config_version"]
    assert verification["verified"] is True
    assert all(
        stage["match"]
        for stage in verification["tiers"]["small"].values()
    )


def test_fixed_seed_regenerates_identical_dataset(tmp_path: Path) -> None:
    first_root = tmp_path / "first"
    second_root = tmp_path / "second"
    generate_demo(first_root, seed=DEMO_SEED, force=True)
    generate_demo(second_root, seed=DEMO_SEED, force=True)
    from anomaly_workbench.data import load_tier

    for tier in ("small", "medium"):
        first = load_tier(first_root, tier)
        second = load_tier(second_root, tier)
        assert first.data_fingerprint == second.data_fingerprint
        assert first.declaration_digest == second.declaration_digest
        assert first.features.shape == second.features.shape
        assert first.source_ids.tolist() == second.source_ids.tolist()
        assert first.batch_ids.tolist() == second.batch_ids.tolist()
        assert ((first.features == second.features) | (np.isnan(first.features) & np.isnan(second.features))).all()


def test_through_stage_execution_can_resume_to_complete(tmp_path: Path) -> None:
    generate_demo(tmp_path, force=True)
    pipeline = Pipeline(tmp_path)
    first = pipeline.run(tiers=["small"], through_stage="preprocess")
    assert first["status"] == "partial"
    assert first["completed_through"] == "preprocess"
    stages = pipeline.status()["tiers"]["small"]["stages"]
    assert stages["preprocess"]["status"] == "complete"
    assert stages["fit"]["status"] == "missing"

    second = pipeline.run(tiers=["small"], through_stage="fit")
    assert second["status"] == "partial"
    stages = pipeline.status()["tiers"]["small"]["stages"]
    assert stages["preprocess"]["status"] == "complete"
    assert stages["fit"]["status"] == "complete"
    assert stages["score"]["status"] == "missing"

    final = pipeline.run(tiers=["small"])
    assert final["status"] == "complete"
    assert [action["action"] for action in final["actions"]] == [
        "reused",
        "reused",
        "rebuilt",
        "rebuilt",
    ]
    assert pipeline.verify_full_equivalence("small")["byte_identical"] is True


def test_incremental_mismatch_reports_stage(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    generate_demo(tmp_path, force=True)
    pipeline = Pipeline(tmp_path)
    pipeline.run(tiers=["small"])
    original = Pipeline._build_payload

    def altered_build(self, stage, data, upstream_artifacts):
        payload = original(self, stage, data, upstream_artifacts)
        if stage == "score" and "awb-full-verify-" in str(self.root):
            payload["threshold"]["value"] = payload["threshold"]["value"] + 1.0
        return payload

    monkeypatch.setattr(Pipeline, "_build_payload", altered_build)
    with pytest.raises(WorkbenchError) as exc:
        pipeline.verify_full_equivalence("small")
    assert exc.value.code == "E_INCREMENTAL_MISMATCH"
    assert exc.value.stage == "score"
    assert exc.value.details["tier"] == "small"
    assert exc.value.details["stage"] == "score"
