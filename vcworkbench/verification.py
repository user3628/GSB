from __future__ import annotations

import base64
import json
import shutil
import tempfile
import threading
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from .canonical import atomic_write_bytes
from .demo import generate_demo
from .errors import OperationStopped, WorkbenchError
from .pipeline import PipelineRunner, load_source, verify_streaming
from .server import WorkbenchHTTPServer
from .storage import CheckpointStore
from .workspace import Workspace


def _result_signature(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "pcm_sha256": result["output"]["pcm_sha256"],
        "wav_sha256": result["output"]["wav_sha256"],
        "sample_count": result["output"]["sample_count"],
        "diagnostics": result["diagnostics"],
        "f0_sequence_digest": result["stream_consistency"].get("f0_sequence_digest"),
        "mel_energy_digest": result["stream_consistency"].get("mel_energy_digest"),
    }


def _capture_error(callable_) -> WorkbenchError | None:  # type: ignore[no-untyped-def]
    try:
        callable_()
    except WorkbenchError as exc:
        return exc
    return None


def _http_json(url: str, *, method: str = "GET", body: dict[str, Any] | None = None) -> tuple[int, Any]:
    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


def run_acceptance() -> dict[str, Any]:
    checks: list[dict[str, Any]] = []

    def check(name: str, condition: bool, details: Any = None) -> None:
        checks.append({"name": name, "passed": bool(condition), "details": details})
        if not condition:
            raise AssertionError(f"verification check failed: {name}: {details}")

    with tempfile.TemporaryDirectory(prefix="vc-workbench-verify-") as temp:
        root = Path(temp) / "workspace"
        workspace = Workspace(root)
        generated = generate_demo(workspace)
        check(
            "three deterministic mixed-language shards",
            len(generated["shard_ids"]) == 3
            and len(generated["target_speakers"]) >= 2
            and generated["seed"] == 20261122,
            generated,
        )
        manifest = workspace.require_manifest()
        baseline = PipelineRunner(workspace).convert(block_frames=5, verify_stream=True)
        baseline_by_id = {item["shard_id"]: item for item in baseline["results"]}
        signatures = {key: _result_signature(value) for key, value in baseline_by_id.items()}
        check(
            "all shards complete with exact sample bounds",
            all(
                item["output"]["sample_count"]
                == item["output"]["expected_output_samples"]
                == manifest.by_id(item["shard_id"]).sample_count
                for item in baseline["results"]
            ),
            {item["shard_id"]: item["output"]["sample_count"] for item in baseline["results"]},
        )
        check(
            "full inference stream self-check verified",
            all(item["stream_consistency"].get("verified") for item in baseline["results"]),
            {item["shard_id"]: item["stream_consistency"] for item in baseline["results"]},
        )

        runner = PipelineRunner(workspace)
        analysis_artifact = runner.store.load_current_artifact("zh_en_01", "analysis")
        mapping_artifact = runner.store.load_current_artifact("zh_en_01", "mapping")
        reconstruction_artifact = runner.store.load_current_artifact("zh_en_01", "reconstruction")
        assert analysis_artifact and mapping_artifact and reconstruction_artifact
        tampered_expected = bytearray(reconstruction_artifact.path("output.pcm").read_bytes())
        tampered_expected[-1] ^= 0xFF
        spec_01 = manifest.by_id("zh_en_01")
        mismatch_error = _capture_error(
            lambda: verify_streaming(
                spec_01,
                load_source(workspace, spec_01, None),
                runner._read_artifact_json(analysis_artifact, "analysis.json"),
                runner._read_artifact_json(mapping_artifact, "mapping.json"),
                bytes(tampered_expected),
                block_frames=4,
                config=runner.dsp,
            )
        )
        check(
            "stream mismatch returns E_STREAM_MISMATCH with first frame",
            mismatch_error is not None
            and mismatch_error.code == "E_STREAM_MISMATCH"
            and mismatch_error.shard_id == "zh_en_01"
            and isinstance(mismatch_error.details.get("first_mismatch_frame"), int),
            None if mismatch_error is None else mismatch_error.to_dict()["error"],
        )

        # Block length must not alter final bytes or invalidate checkpoints.
        for block_frames in (1, 3, 997):
            repeated = PipelineRunner(workspace).convert(
                block_frames=block_frames, verify_stream=True
            )
            repeated_by_id = {item["shard_id"]: item for item in repeated["results"]}
            check(
                f"block size {block_frames} is byte invariant",
                all(
                    _result_signature(repeated_by_id[shard_id]) == signatures[shard_id]
                    for shard_id in signatures
                )
                and all(item["earliest_invalidated_stage"] is None for item in repeated["results"]),
                {
                    item["shard_id"]: item["output"]["pcm_sha256"]
                    for item in repeated["results"]
                },
            )

        # Explicitly reconstruct each chunk from its base64 PCM payload and hash it.
        runner = PipelineRunner(workspace)
        chunk_report = {}
        for shard_id, expected in signatures.items():
            streamed = runner.stream_check(shard_id, block_frames=4)
            pcm = b"".join(
                base64.b64decode(chunk["pcm_base64"]) for chunk in streamed["chunks"]
            )
            from .canonical import sha256_bytes

            chunk_report[shard_id] = {
                "chunks": len(streamed["chunks"]),
                "bytes": len(pcm),
                "sha256": sha256_bytes(pcm),
            }
            check(
                f"stream chunk concatenation matches full PCM for {shard_id}",
                sha256_bytes(pcm) == expected["pcm_sha256"]
                and len(pcm) == expected["sample_count"] * 2,
                chunk_report[shard_id],
            )

        # Fresh-state reproducibility.
        shutil.rmtree(workspace.state_dir, ignore_errors=True)
        fresh = PipelineRunner(workspace).convert(block_frames=3, verify_stream=True)
        fresh_by_id = {item["shard_id"]: item for item in fresh["results"]}
        check(
            "fresh run is byte-identical to baseline",
            all(
                _result_signature(fresh_by_id[shard_id]) == signatures[shard_id]
                for shard_id in signatures
            ),
            {key: fresh_by_id[key]["output"]["pcm_sha256"] for key in fresh_by_id},
        )

        # Stable injection business codes.
        expected_injections = [
            ("channels", {"channels": 2}, "E_CHANNEL_UNSUPPORTED", "analysis"),
            (
                "sample_rate",
                {"declared_sample_rate": 44100},
                "E_SAMPLE_RATE_MISMATCH",
                "analysis",
            ),
            ("nan", {"nan_index": 321}, "E_NUMERIC", "analysis"),
            ("manifest_count", {"sample_count_delta": 7}, "E_MANIFEST_MISMATCH", "analysis"),
            ("zero_pitch", {"pitch_shift": 0.0}, "E_INVALID_PARAM", "mapping"),
            ("negative_warp", {"mel_warp": -1.0}, "E_INVALID_PARAM", "mapping"),
            ("pitch_range", {"pitch_shift": 0.5}, "E_PITCH_OUT_OF_RANGE", "validation"),
            (
                "missing_truth",
                {"remove_reference": True},
                "E_REFERENCE_MISSING",
                "validation",
            ),
        ]
        injection_results = {}
        shard_id = "zh_en_01"
        for name, injection, expected_code, expected_stage in expected_injections:
            error = _capture_error(
                lambda injection=injection: PipelineRunner(workspace).convert(
                    shard_ids=[shard_id],
                    block_frames=4,
                    injections={shard_id: injection},
                )
            )
            injection_results[name] = None if error is None else error.to_dict()["error"]
            check(
                f"injection {name} returns {expected_code}",
                error is not None
                and error.code == expected_code
                and error.stage == expected_stage
                and error.shard_id == shard_id,
                injection_results[name],
            )

        # Incremental invalidation: quant_bits starts at reconstruction, target starts at mapping.
        shutil.rmtree(workspace.state_dir, ignore_errors=True)
        PipelineRunner(workspace).convert(block_frames=6, verify_stream=True)
        quant = PipelineRunner(workspace).convert(
            shard_ids=[shard_id],
            block_frames=6,
            injections={shard_id: {"quant_bits": 12}},
        )["results"][0]
        check(
            "quant_bits invalidates reconstruction only",
            quant["earliest_invalidated_stage"] == "reconstruction"
            and [(stage["stage"], stage["status"]) for stage in quant["stages"]]
            == [
                ("analysis", "reused"),
                ("mapping", "reused"),
                ("reconstruction", "built"),
                ("validation", "built"),
            ],
            [(stage["stage"], stage["status"]) for stage in quant["stages"]],
        )
        target = PipelineRunner(workspace).convert(
            shard_ids=[shard_id],
            block_frames=6,
            injections={
                shard_id: {
                    "target_speaker": "T2",
                    "reference_pitch_hz": {"min_hz": 160.0, "max_hz": 195.0},
                }
            },
        )["results"][0]
        check(
            "target speaker invalidates mapping and downstream while reusing analysis",
            target["earliest_invalidated_stage"] == "mapping"
            and target["stages"][0]["status"] == "reused",
            [(stage["stage"], stage["status"]) for stage in target["stages"]],
        )

        PipelineRunner(workspace).convert(shard_ids=[shard_id], block_frames=6)
        pitch = PipelineRunner(workspace).convert(
            shard_ids=[shard_id],
            block_frames=6,
            injections={shard_id: {"pitch_shift": 1.061}},
        )["results"][0]
        check(
            "pitch_shift invalidates mapping",
            pitch["earliest_invalidated_stage"] == "mapping"
            and pitch["stages"][0]["status"] == "reused",
            [(stage["stage"], stage["status"]) for stage in pitch["stages"]],
        )
        PipelineRunner(workspace).convert(shard_ids=[shard_id], block_frames=6)
        warp = PipelineRunner(workspace).convert(
            shard_ids=[shard_id],
            block_frames=6,
            injections={shard_id: {"mel_warp": 0.96}},
        )["results"][0]
        check(
            "mel_warp invalidates mapping",
            warp["earliest_invalidated_stage"] == "mapping"
            and warp["stages"][0]["status"] == "reused",
            [(stage["stage"], stage["status"]) for stage in warp["stages"]],
        )
        PipelineRunner(workspace).convert(shard_ids=[shard_id], block_frames=6)
        hop = PipelineRunner(workspace).convert(
            shard_ids=[shard_id],
            block_frames=6,
            injections={shard_id: {"frame_shift": 144}},
        )["results"][0]
        check(
            "frame_shift invalidates analysis",
            hop["earliest_invalidated_stage"] == "analysis"
            and hop["stages"][0]["status"] == "built",
            [(stage["stage"], stage["status"]) for stage in hop["stages"]],
        )
        wav_path = workspace.samples_dir / f"{shard_id}.wav"
        original_wav = wav_path.read_bytes()
        mutated = bytearray(original_wav)
        mutated[100] ^= 0x01
        wav_path.write_bytes(bytes(mutated))
        try:
            sample_change = PipelineRunner(workspace).convert(
                shard_ids=[shard_id],
                block_frames=6,
                injections={shard_id: {"reference_tolerance_hz": 1000.0}},
            )["results"][0]
        finally:
            wav_path.write_bytes(original_wav)
        check(
            "sample content invalidates analysis",
            sample_change["earliest_invalidated_stage"] == "analysis"
            and sample_change["stages"][0]["status"] == "built",
            [(stage["stage"], stage["status"]) for stage in sample_change["stages"]],
        )

        # Graceful stop and resume from the reconstruction stage.
        shutil.rmtree(workspace.state_dir, ignore_errors=True)
        stopped_error: OperationStopped | None = None
        try:
            PipelineRunner(workspace, stop_after_blocks=1).convert(
                block_frames=3, verify_stream=True
            )
        except OperationStopped as exc:
            stopped_error = exc
        check(
            "graceful stop occurs at a block boundary",
            stopped_error is not None
            and stopped_error.stage == "reconstruction"
            and stopped_error.blocks_completed == 1,
            None if stopped_error is None else stopped_error.to_dict(),
        )
        store = CheckpointStore(workspace.state_dir)
        pre_resume_status = store.status("zh_en_01")
        old_ledger_bytes = store._ledger_path("zh_en_01").read_bytes()
        check(
            "stop leaves analysis+mapping checkpoints and no reconstruction",
            pre_resume_status["stage_status"]["analysis"] == "complete"
            and pre_resume_status["stage_status"]["mapping"] == "complete"
            and pre_resume_status["stage_status"]["reconstruction"] == "pending",
            pre_resume_status,
        )
        workspace.clear_stop()
        resumed = PipelineRunner(workspace).convert(
            block_frames=7, verify_stream=True, clear_stop=True
        )
        resumed_by_id = {item["shard_id"]: item for item in resumed["results"]}
        check(
            "resume is byte-identical to complete run",
            all(
                _result_signature(resumed_by_id[shard_id]) == signatures[shard_id]
                for shard_id in signatures
            ),
            {key: resumed_by_id[key]["output"]["pcm_sha256"] for key in resumed_by_id},
        )
        check(
            "resume rebuilds reconstruction while reusing analysis+mapping",
            resumed_by_id["zh_en_01"]["earliest_invalidated_stage"] == "reconstruction"
            and resumed_by_id["zh_en_01"]["stages"][0]["status"] == "reused",
            [(s["stage"], s["status"]) for s in resumed_by_id["zh_en_01"]["stages"]],
        )

        # Artifact tamper is never silently reused.
        recon_artifact = store.load_current_artifact("zh_en_01", "reconstruction")
        assert recon_artifact is not None
        original_pcm = recon_artifact.path("output.pcm").read_bytes()
        recon_artifact.path("output.pcm").write_bytes(
            original_pcm[:-1] + bytes([original_pcm[-1] ^ 0xFF])
        )
        tamper_error = _capture_error(lambda: store.load_current_artifact("zh_en_01", "reconstruction"))
        check(
            "artifact payload tamper returns E_STALE_CHECKPOINT",
            tamper_error is not None and tamper_error.code == "E_STALE_CHECKPOINT",
            None if tamper_error is None else tamper_error.to_dict()["error"],
        )
        recon_artifact.path("output.pcm").write_bytes(original_pcm)

        # Replacing the ledger with an older valid checkpoint is caught by highwater.
        atomic_write_bytes(store._ledger_path("zh_en_01"), old_ledger_bytes)
        rollback_error = _capture_error(lambda: store.read_ledger("zh_en_01"))
        check(
            "checkpoint ledger rollback is rejected",
            rollback_error is not None and rollback_error.code == "E_STALE_CHECKPOINT",
            None if rollback_error is None else rollback_error.to_dict()["error"],
        )

        # Restore the post-resume ledger for HTTP checks.
        atomic_write_bytes(store._ledger_path("zh_en_01"), old_ledger_bytes)
        # Re-run resume is no longer possible after the rollback test, so use a clean
        # workspace pass for the HTTP smoke test.
        shutil.rmtree(workspace.state_dir, ignore_errors=True)
        PipelineRunner(workspace).convert(block_frames=5, verify_stream=True)

        server = WorkbenchHTTPServer(("127.0.0.1", 0), workspace)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        try:
            status, health = _http_json(f"{base_url}/api/health")
            check(
                "HTTP health endpoint",
                status == 200 and health.get("deterministic") is True,
                health,
            )
            status, state_payload = _http_json(f"{base_url}/api/state")
            check(
                "HTTP state endpoint exposes three complete shards",
                status == 200
                and len(state_payload.get("shards", [])) == 3
                and all(
                    item["checkpoints"]["stage_status"]["validation"] == "complete"
                    for item in state_payload["shards"]
                ),
                {"shards": len(state_payload.get("shards", []))},
            )
            status, diag = _http_json(
                f"{base_url}/api/diagnostics/zh_en_01/frame?frame_index=20&latency_budget_ms=1000"
            )
            check(
                "HTTP frame diagnostic within latency budget",
                status == 200
                and diag["frame_index"] == 20
                and diag["diagnostic_frame"]["alignment"] in {"interpolation", "extrapolation", "unvoiced"},
                {"latency_ms": diag.get("latency_ms"), "frame": diag.get("diagnostic_frame")},
            )
            status, latency_error = _http_json(
                f"{base_url}/api/diagnostics/zh_en_01/frame?frame_index=20&latency_budget_ms=0"
            )
            check(
                "HTTP latency budget returns E_LATENCY_BUDGET",
                status == 504
                and latency_error["error"]["code"] == "E_LATENCY_BUDGET"
                and latency_error["error"]["shard_id"] == "zh_en_01",
                latency_error,
            )
            status, injection_error = _http_json(
                f"{base_url}/api/inject",
                method="POST",
                body={
                    "shard_id": "zh_en_01",
                    "injection": {"channels": 2},
                    "verify_stream": False,
                },
            )
            check(
                "HTTP injection returns stable channel code",
                status == 422
                and injection_error["error"]["code"] == "E_CHANNEL_UNSUPPORTED",
                injection_error,
            )
            request = urllib.request.Request(
                f"{base_url}/api/export/zh_en_01.wav"
            )
            with urllib.request.urlopen(request, timeout=10) as response:
                wav = response.read()
                wav_type = response.headers.get("Content-Type")
            check(
                "HTTP WAV export",
                len(wav) > 44 and wav[:4] == b"RIFF" and wav_type == "audio/wav",
                {"bytes": len(wav), "content_type": wav_type},
            )
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

        # Checkpoint stale/missing-prerequisite primitives on isolated shard ids.
        stale_shard = "__verify_stale__"
        store.begin_rebuild(stale_shard, "analysis")
        ledger = store.read_ledger(stale_shard)
        assert ledger is not None
        current_analysis = store.load_current_artifact("zh_en_01", "analysis")
        assert current_analysis is not None
        missing_prereq = _capture_error(
            lambda: store.publish(
                shard_id=stale_shard,
                stage="mapping",
                expected_seq=ledger["seq"] + 1,
                expected_epoch=ledger["epoch"],
                input_fingerprint="missing-prereq",
                artifact=current_analysis,
                prerequisite_digest=None,
            )
        )
        check(
            "missing prerequisite checkpoint returns E_INVALID_STAGE",
            missing_prereq is not None and missing_prereq.code == "E_INVALID_STAGE",
            None if missing_prereq is None else missing_prereq.to_dict()["error"],
        )
        stale_publish = _capture_error(
            lambda: store.publish(
                shard_id=stale_shard,
                stage="analysis",
                expected_seq=99,
                expected_epoch=ledger["epoch"],
                input_fingerprint="stale",
                artifact=current_analysis,
                prerequisite_digest=None,
            )
        )
        check(
            "stale checkpoint sequence returns E_STALE_CHECKPOINT",
            stale_publish is not None and stale_publish.code == "E_STALE_CHECKPOINT",
            None if stale_publish is None else stale_publish.to_dict()["error"],
        )
        cross_shard = "__cross_shard__"
        cross_rebuild = store.begin_rebuild(cross_shard, "analysis")
        cross_error = _capture_error(
            lambda: store.publish(
                shard_id=cross_shard,
                stage="analysis",
                expected_seq=cross_rebuild["next_seq"],
                expected_epoch=cross_rebuild["epoch"],
                input_fingerprint="cross-shard",
                artifact=current_analysis,
                prerequisite_digest=None,
            )
        )
        check(
            "cross-shard checkpoint publication returns E_STALE_CHECKPOINT",
            cross_error is not None and cross_error.code == "E_STALE_CHECKPOINT",
            None if cross_error is None else cross_error.to_dict()["error"],
        )

    passed = all(item["passed"] for item in checks)
    return {
        "ok": passed,
        "status": "passed" if passed else "failed",
        "check_count": len(checks),
        "checks": checks,
        "baseline": {
            shard_id: {
                "pcm_sha256": signature["pcm_sha256"],
                "sample_count": signature["sample_count"],
                "diagnostics": signature["diagnostics"],
            }
            for shard_id, signature in signatures.items()
        },
    }
