from __future__ import annotations

import base64
import hashlib
import json
import threading
import urllib.error
import urllib.request
from pathlib import Path

import pytest

from vcworkbench.canonical import atomic_write_bytes
from vcworkbench.demo import generate_demo
from vcworkbench.errors import WorkbenchError
from vcworkbench.pipeline import PipelineRunner, load_source, verify_streaming
from vcworkbench.server import WorkbenchHTTPServer
from vcworkbench.storage import CheckpointStore
from vcworkbench.workspace import Workspace


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@pytest.fixture()
def workspace(tmp_path: Path) -> Workspace:
    ws = Workspace(tmp_path / "workspace")
    generate_demo(ws)
    return ws


def signatures(result: dict) -> dict[str, dict]:
    return {
        item["shard_id"]: {
            "pcm_sha256": item["output"]["pcm_sha256"],
            "wav_sha256": item["output"]["wav_sha256"],
            "sample_count": item["output"]["sample_count"],
            "diagnostics": item["diagnostics"],
            "f0": item["stream_consistency"]["f0_sequence_digest"],
            "mel": item["stream_consistency"]["mel_energy_digest"],
        }
        for item in result["results"]
    }


def test_deterministic_stream_and_block_invariance(workspace: Workspace) -> None:
    before = {
        path.name: path.read_bytes() for path in sorted(workspace.samples_dir.glob("*.wav"))
    }
    manifest_before = workspace.manifest_path.read_bytes()
    generate_demo(workspace, force=True)
    assert manifest_before == workspace.manifest_path.read_bytes()
    assert before == {
        path.name: path.read_bytes() for path in sorted(workspace.samples_dir.glob("*.wav"))
    }
    manifest = workspace.require_manifest()
    baseline = PipelineRunner(workspace).convert(block_frames=5, verify_stream=True)
    baseline_signatures = signatures(baseline)
    assert len(baseline["results"]) == 3
    assert {spec.target_speaker for spec in manifest.shards} >= {"T1", "T2"}
    for item in baseline["results"]:
        assert item["output"]["sample_count"] == item["output"]["expected_output_samples"]
        assert item["stream_consistency"]["verified"] is True

    for block_frames in (1, 7, 1000):
        repeated = PipelineRunner(workspace).convert(
            block_frames=block_frames, verify_stream=True
        )
        assert signatures(repeated) == baseline_signatures
        assert all(item["earliest_invalidated_stage"] is None for item in repeated["results"])

    runner = PipelineRunner(workspace)
    streamed = runner.stream_check("zh_en_01", block_frames=2)
    pcm = b"".join(base64.b64decode(chunk["pcm_base64"]) for chunk in streamed["chunks"])
    assert sha(pcm) == baseline_signatures["zh_en_01"]["pcm_sha256"]
    assert len(pcm) == baseline_signatures["zh_en_01"]["sample_count"] * 2


def test_injection_business_codes(workspace: Workspace) -> None:
    cases = [
        ({"channels": 2}, "E_CHANNEL_UNSUPPORTED", "analysis"),
        ({"declared_sample_rate": 44100}, "E_SAMPLE_RATE_MISMATCH", "analysis"),
        ({"nan_index": 111}, "E_NUMERIC", "analysis"),
        ({"sample_count_delta": 9}, "E_MANIFEST_MISMATCH", "analysis"),
        ({"pitch_shift": 0.0}, "E_INVALID_PARAM", "mapping"),
        ({"mel_warp": -0.5}, "E_INVALID_PARAM", "mapping"),
        ({"pitch_shift": 0.5}, "E_PITCH_OUT_OF_RANGE", "validation"),
        ({"remove_reference": True}, "E_REFERENCE_MISSING", "validation"),
    ]
    shard_id = "zh_en_01"
    for injection, code, stage in cases:
        with pytest.raises(WorkbenchError) as caught:
            PipelineRunner(workspace).convert(
                shard_ids=[shard_id],
                block_frames=3,
                injections={shard_id: injection},
            )
        assert caught.value.code == code
        assert caught.value.stage == stage
        assert caught.value.shard_id == shard_id


def test_incremental_invalidation_and_stop_resume(workspace: Workspace) -> None:
    baseline = PipelineRunner(workspace).convert(block_frames=4, verify_stream=True)
    baseline_signatures = signatures(baseline)

    quantized = PipelineRunner(workspace).convert(
        shard_ids=["zh_en_01"],
        block_frames=4,
        injections={"zh_en_01": {"quant_bits": 12}},
    )["results"][0]
    assert quantized["earliest_invalidated_stage"] == "reconstruction"
    assert [item["status"] for item in quantized["stages"]] == [
        "reused",
        "reused",
        "built",
        "built",
    ]
    quant_stream = PipelineRunner(workspace).stream_check("zh_en_01", block_frames=2)
    assert quant_stream["verified"] is True
    assert quant_stream["pcm_sha256"] == quantized["output"]["pcm_sha256"]

    shifted = PipelineRunner(workspace).convert(
        shard_ids=["zh_en_01"],
        block_frames=4,
        injections={
            "zh_en_01": {
                "target_speaker": "T2",
                "reference_pitch_hz": {"min_hz": 160.0, "max_hz": 195.0},
            }
        },
    )["results"][0]
    assert shifted["earliest_invalidated_stage"] == "mapping"
    assert [item["status"] for item in shifted["stages"]] == [
        "reused",
        "built",
        "built",
        "built",
    ]

    PipelineRunner(workspace).convert(shard_ids=["zh_en_01"], block_frames=4)
    validation_only = PipelineRunner(workspace).convert(
        shard_ids=["zh_en_01"],
        block_frames=4,
        injections={"zh_en_01": {"reference_tolerance_hz": 6.0}},
    )["results"][0]
    assert validation_only["earliest_invalidated_stage"] == "validation"
    assert [item["status"] for item in validation_only["stages"]] == [
        "reused",
        "reused",
        "reused",
        "built",
    ]

    # Use a clean state for the stop/resume comparison.
    import shutil

    shutil.rmtree(workspace.state_dir)
    from vcworkbench.errors import OperationStopped

    with pytest.raises(OperationStopped) as stopped:
        PipelineRunner(workspace, stop_after_blocks=1).convert(
            block_frames=2, verify_stream=True
        )
    assert stopped.value.stage == "reconstruction"
    assert stopped.value.blocks_completed == 1
    status = CheckpointStore(workspace.state_dir).status("zh_en_01")
    assert status["stage_status"]["analysis"] == "complete"
    assert status["stage_status"]["mapping"] == "complete"
    assert status["stage_status"]["reconstruction"] == "pending"
    workspace.clear_stop()
    resumed = PipelineRunner(workspace).convert(
        block_frames=9, verify_stream=True, clear_stop=True
    )
    assert signatures(resumed) == baseline_signatures


def test_stream_mismatch_has_first_frame(workspace: Workspace) -> None:
    runner = PipelineRunner(workspace)
    runner.convert(shard_ids=["zh_en_01"], block_frames=4, verify_stream=True)
    store = runner.store
    analysis_artifact = store.load_current_artifact("zh_en_01", "analysis")
    mapping_artifact = store.load_current_artifact("zh_en_01", "mapping")
    reconstruction_artifact = store.load_current_artifact("zh_en_01", "reconstruction")
    assert analysis_artifact and mapping_artifact and reconstruction_artifact
    analysis = runner._read_artifact_json(analysis_artifact, "analysis.json")
    mapping = runner._read_artifact_json(mapping_artifact, "mapping.json")
    expected = bytearray(reconstruction_artifact.path("output.pcm").read_bytes())
    expected[-1] ^= 0xFF
    spec = workspace.require_manifest().by_id("zh_en_01")
    source = load_source(workspace, spec, None)
    with pytest.raises(WorkbenchError) as caught:
        verify_streaming(
            spec,
            source,
            analysis,
            mapping,
            bytes(expected),
            block_frames=4,
            config=runner.dsp,
        )
    assert caught.value.code == "E_STREAM_MISMATCH"
    assert caught.value.shard_id == "zh_en_01"
    assert isinstance(caught.value.details["first_mismatch_frame"], int)


def test_checkpoint_tamper_and_stale_publication(workspace: Workspace) -> None:
    PipelineRunner(workspace).convert(shard_ids=["zh_en_01"], block_frames=4)
    store = CheckpointStore(workspace.state_dir)

    ledger = store.read_ledger("zh_en_01")
    assert ledger is not None
    old_ledger = store._ledger_path("zh_en_01").read_bytes()
    # A structurally valid older ledger still loses to the high-water mark.
    ledger["seq"] -= 1
    ledger["commits"] = ledger["commits"][:-1]
    ledger["last_record_digest"] = ledger["commits"][-1]["record_digest"]
    atomic_write_bytes(store._ledger_path("zh_en_01"), json.dumps(ledger).encode())
    with pytest.raises(WorkbenchError) as caught:
        store.read_ledger("zh_en_01")
    assert caught.value.code == "E_STALE_CHECKPOINT"
    atomic_write_bytes(store._ledger_path("zh_en_01"), old_ledger)

    reconstruction = store.load_current_artifact("zh_en_01", "reconstruction")
    assert reconstruction is not None
    pcm = reconstruction.path("output.pcm").read_bytes()
    reconstruction.path("output.pcm").write_bytes(pcm[:-1] + bytes([pcm[-1] ^ 1]))
    with pytest.raises(WorkbenchError) as caught:
        store.load_current_artifact("zh_en_01", "reconstruction")
    assert caught.value.code == "E_STALE_CHECKPOINT"
    reconstruction.path("output.pcm").write_bytes(pcm)

    analysis = store.load_current_artifact("zh_en_01", "analysis")
    assert analysis is not None
    fake = "__cross_shard__"
    rebuild = store.begin_rebuild(fake, "analysis")
    with pytest.raises(WorkbenchError) as caught:
        store.publish(
            shard_id=fake,
            stage="analysis",
            expected_seq=rebuild["next_seq"],
            expected_epoch=rebuild["epoch"],
            input_fingerprint="cross",
            artifact=analysis,
            prerequisite_digest=None,
        )
    assert caught.value.code == "E_STALE_CHECKPOINT"

    fake_missing = "__missing_prereq__"
    rebuild = store.begin_rebuild(fake_missing, "analysis")
    with pytest.raises(WorkbenchError) as caught:
        store.publish(
            shard_id=fake_missing,
            stage="mapping",
            expected_seq=rebuild["next_seq"],
            expected_epoch=rebuild["epoch"],
            input_fingerprint="missing",
            artifact=analysis,
            prerequisite_digest=None,
        )
    assert caught.value.code == "E_INVALID_STAGE"


def _http_json(url: str, method: str = "GET", body: dict | None = None) -> tuple[int, dict]:
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"} if body is not None else {},
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


def test_http_diagnostics_budget_and_export(workspace: Workspace) -> None:
    PipelineRunner(workspace).convert(block_frames=4, verify_stream=True)
    server = WorkbenchHTTPServer(("127.0.0.1", 0), workspace)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        status, health = _http_json(base + "/api/health")
        assert status == 200 and health["deterministic"] is True
        status, state = _http_json(base + "/api/state")
        assert status == 200 and len(state["shards"]) == 3
        status, frame = _http_json(
            base
            + "/api/diagnostics/zh_en_01/frame?frame_index=20&latency_budget_ms=1000"
        )
        assert status == 200 and frame["analysis_frame"]["frame_index"] == 20
        status, error = _http_json(
            base + "/api/diagnostics/zh_en_01/frame?frame_index=20&latency_budget_ms=0"
        )
        assert status == 504 and error["error"]["code"] == "E_LATENCY_BUDGET"
        with urllib.request.urlopen(base + "/api/export/zh_en_01.wav", timeout=10) as response:
            wav = response.read()
        assert wav[:4] == b"RIFF" and len(wav) > 44
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
