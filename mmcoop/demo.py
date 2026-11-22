from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

from .canonical import atomic_write_json, canonical_bytes, digest_json
from .config import load_config, write_projection_matrix
from .errors import AppError
from .service import WorkspaceService


INJECTION_CASES = (
    ("projection_dim", "i01"),
    ("invalid_operator", "i01"),
    ("text_too_long", "t01"),
    ("similarity_nan", "i01"),
    ("missing_ref", "i01"),
)


def _ensure_matrix_file(service: WorkspaceService) -> None:
    config = load_config(service.config_path)
    matrix_file = config.projection.get("matrix_file")
    if not matrix_file:
        return
    candidate = Path(str(matrix_file))
    if not candidate.is_absolute():
        candidate = config.path.parent / candidate
    if not candidate.exists():
        write_projection_matrix(service.config_path, candidate)


def _stage_digests(service: WorkspaceService) -> dict[str, str]:
    return {
        stage: value["artifact_sha256"]
        for stage, value in sorted(service.store.current_stages().items())
    }


def _fresh_full_fingerprint(service: WorkspaceService, temporary_root: Path) -> str:
    fresh = WorkspaceService(service.config_path, service.data_dir, temporary_root / "workspace")
    result = fresh.build(verify_incremental=False)
    return result["pipeline_fingerprint"]


def run_demo(service: WorkspaceService, *, seed: int = 20261123, report_path: str | Path | None = None) -> dict[str, Any]:
    _ensure_matrix_file(service)
    generated = service.generate_demo(seed=seed, fresh_workspace=True)
    initial_data_fingerprint = generated["data_fingerprint"]
    baseline = service.build(verify_incremental=True)
    baseline_digests = _stage_digests(service)
    with tempfile.TemporaryDirectory(prefix="mmcoop-full-") as temporary:
        full_scratch_fingerprint = _fresh_full_fingerprint(service, Path(temporary))
    full_scratch_match = full_scratch_fingerprint == baseline["pipeline_fingerprint"]

    injection_results: list[dict[str, Any]] = []
    for kind, sample_id in INJECTION_CASES:
        sequence_before = service.store.scan().latest_sequence
        try:
            service.build(injections=[{"kind": kind, "sample_id": sample_id}])
        except AppError as error:
            sequence_after = service.store.scan().latest_sequence
            injection_results.append(
                {
                    "kind": kind,
                    "sample_id": sample_id,
                    "code": error.code,
                    "stage": error.stage,
                    "checkpoint_sequence_unchanged": sequence_before == sequence_after,
                    "diagnostic_keys": sorted(error.diagnostics),
                }
            )
        else:
            raise AssertionError(f"injection {kind} did not fail")

    dataset = service._load()[1]
    original_text = dataset.text_map()["t03"]["text"]
    original_feature = list(dataset.image_map()["i01"]["feature"])

    edited_feature = list(original_feature)
    edited_feature[0] = float(edited_feature[0]) + 0.0001
    service.edit_image("i01", {"feature": edited_feature})
    image_incremental = service.build(verify_incremental=True)
    service.edit_image("i01", {"feature": original_feature})
    image_restored = service.build(verify_incremental=True)

    changed_text = original_text + " [incremental-demo]"
    service.edit_text("t03", {"text": changed_text})
    text_incremental = service.build(verify_incremental=True)
    service.edit_text("t03", {"text": original_text})
    text_restored = service.build(verify_incremental=True)

    sequence_before_stop = service.store.scan().latest_sequence
    service.edit_text("t03", {"text": original_text + " [stop-resume-demo]"})
    service.request_stop()
    stopped = service.build(verify_incremental=True)
    sequence_after_stop = service.store.scan().latest_sequence
    resumed = service.resume(verify_incremental=True)
    with tempfile.TemporaryDirectory(prefix="mmcoop-resume-") as temporary:
        resumed_full_fingerprint = _fresh_full_fingerprint(service, Path(temporary))
    resumed_match = resumed_full_fingerprint == resumed["pipeline_fingerprint"]
    service.edit_text("t03", {"text": original_text})
    final_restore = service.build(verify_incremental=True)
    final_digests = _stage_digests(service)
    status = service.status()

    report_core = {
        "schema_version": "mmcoop.demo-report.v1",
        "seed": seed,
        "initial_data_fingerprint": initial_data_fingerprint,
        "baseline": {
            "status": baseline["status"],
            "version_fingerprint": baseline["version_fingerprint"],
            "pipeline_fingerprint": baseline["pipeline_fingerprint"],
            "stage_artifact_sha256": baseline_digests,
            "full_scratch_match": full_scratch_match,
        },
        "injections": injection_results,
        "incremental": {
            "image_feature": {
                "earliest_affected_stage": image_incremental["rebuilt_stages"][0],
                "reused_stages": image_incremental["reused_stages"],
                "rebuilt_stages": image_incremental["rebuilt_stages"],
                "restore_pipeline_fingerprint_matches_baseline": image_restored["pipeline_fingerprint"]
                == baseline["pipeline_fingerprint"],
            },
            "text_content": {
                "earliest_affected_stage": text_incremental["rebuilt_stages"][0],
                "reused_stages": text_incremental["reused_stages"],
                "rebuilt_stages": text_incremental["rebuilt_stages"],
                "restore_pipeline_fingerprint_matches_baseline": text_restored["pipeline_fingerprint"]
                == baseline["pipeline_fingerprint"],
            },
        },
        "stop_resume": {
            "stop_status": stopped["status"],
            "checkpoint_sequence_unchanged": sequence_before_stop == sequence_after_stop,
            "resume_status": resumed["status"],
            "resumed_pipeline_fingerprint": resumed["pipeline_fingerprint"],
            "resume_matches_full_run": resumed_match,
            "restored_inmemory_render_ready": status["status"] == "ready",
        },
        "final": {
            "status": status["status"],
            "pipeline_fingerprint": final_restore["pipeline_fingerprint"],
            "matches_baseline": final_digests == baseline_digests,
            "integrity_verdict": status["integrity"]["verdict"],
            "aggregation_verdict_counts": service.results()["result"]["summary"]["verdict_counts"],
            "missing_link_text_ids": service.results()["result"]["summary"]["missing_link_text_ids"],
        },
    }
    report = {**report_core, "deterministic_summary_sha256": digest_json(report_core)}
    if report_path is not None:
        atomic_write_json(Path(report_path), report, indent=2)
    return report

