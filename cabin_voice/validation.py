"""Reproducible end-to-end validation used by the demo and acceptance report."""

from __future__ import annotations

import platform
import shutil
import wave
from pathlib import Path
from typing import Any

import numpy as np

from . import __version__
from .engine import Engine
from .errors import CabinVoiceError
from .samplegen import generate_demo
from .storage import Workspace
from .util import atomic_write_json, sha256_file


def _reference_summary(workspace: Workspace, result: dict[str, Any]) -> dict[str, Any]:
    manifest = workspace.require_manifest()
    max_start_delta = 0
    max_end_delta = 0
    mismatches = 0
    comparisons = 0
    for chunk in manifest["chunks"]:
        chunk_id = chunk["chunk_id"]
        for segment in chunk["segments"]:
            zone_id = segment["zone_id"]
            actual = result["activity_intervals"][chunk_id][zone_id]
            expected = segment.get("reference_active_ms", [])
            if len(actual) != len(expected):
                mismatches += 1
                continue
            for detected, reference in zip(actual, expected):
                comparisons += 1
                start_delta = abs(int(detected["start_ms"]) - int(reference[0]))
                end_delta = abs(int(detected["end_ms"]) - int(reference[1]))
                max_start_delta = max(max_start_delta, start_delta)
                max_end_delta = max(max_end_delta, end_delta)
                if start_delta > 30 or end_delta > 30:
                    mismatches += 1
    review = [
        command["event_id"]
        for command in result["command_attributions"]
        if command["status"] == "needs_review"
    ]
    assigned = [
        command["event_id"]
        for command in result["command_attributions"]
        if command["status"] == "assigned"
    ]
    return {
        "comparisons": comparisons,
        "interval_count_mismatches": mismatches,
        "max_start_delta_ms": max_start_delta,
        "max_end_delta_ms": max_end_delta,
        "overlap_markers": len(result["overlap_markers"]),
        "assigned_commands": len(assigned),
        "review_commands": review,
    }


def _scaled_wav(source: Path, target: Path, factor: float) -> None:
    with wave.open(str(source), "rb") as handle:
        params = handle.getparams()
        samples = np.frombuffer(handle.readframes(handle.getnframes()), dtype="<i2").copy()
    changed = np.clip((samples.astype(np.float64) * factor).round(), -32768, 32767).astype("<i2")
    with wave.open(str(target), "wb") as handle:
        handle.setparams(params)
        handle.writeframes(changed.tobytes())


def run_validation(output_dir: str | Path, *, seed: int = 20261123) -> dict[str, Any]:
    output = Path(output_dir).resolve()
    if output.exists():
        shutil.rmtree(output)
    output.mkdir(parents=True)
    generated = generate_demo(output / "demo", seed=seed)
    checks: list[dict[str, Any]] = []

    full_workspace = Workspace(output / "full").ensure()
    full_workspace.import_bundle(generated["manifest"])
    Engine(full_workspace).build(mode="full", verify=True)
    full_result = Engine(full_workspace).result()
    reference_metrics = _reference_summary(full_workspace, full_result)
    checks.append(
        {
            "name": "reference_activity_and_overlap_arbitration",
            "ok": reference_metrics["interval_count_mismatches"] == 0
            and reference_metrics["max_start_delta_ms"] <= 30
            and reference_metrics["max_end_delta_ms"] <= 30
            and reference_metrics["review_commands"] == ["chunk_002:open_window"],
            "metrics": reference_metrics,
        }
    )

    resumed_workspace = Workspace(output / "resumed").ensure()
    resumed_workspace.import_bundle(generated["manifest"])
    partial = Engine(resumed_workspace).build(mode="full", stop_after_units=5)
    resumed = Engine(resumed_workspace).build(mode="incremental", resume=True, verify=True)
    resume_equal = full_workspace.read_final_bytes() == resumed_workspace.read_final_bytes()
    resume_sequences = [
        record["seq"] for record in Engine(resumed_workspace).checkpoints.load_consistent()["prefix"]
    ]
    checks.append(
        {
            "name": "graceful_stop_and_resume",
            "ok": partial["status"] == "stopped"
            and partial["last_seq"] == 5
            and resumed["status"] == "complete"
            and resume_equal
            and resume_sequences == list(range(1, 13)),
            "metrics": {
                "stop_sequence": partial["last_seq"],
                "resumed_sequence": resumed["last_seq"],
                "byte_equal": resume_equal,
                "sequences": resume_sequences,
            },
        }
    )

    incremental_workspace = Workspace(output / "incremental").ensure()
    incremental_workspace.import_bundle(generated["manifest"])
    Engine(incremental_workspace).build(mode="full")
    from .timeline import EditModel

    EditModel(incremental_workspace.edits_path, incremental_workspace.require_manifest()).set_priority(
        "passenger", 96
    )
    incremental = Engine(incremental_workspace).build(mode="incremental", verify=True)
    priority_report = incremental["invalidation"]["chunks"]["chunk_001"]
    checks.append(
        {
            "name": "priority_incremental_reuse_and_full_equality",
            "ok": incremental["verification"]["equal"]
            and priority_report["earliest_stage"] == "attribution"
            and priority_report["sources"]["detection"] == "cache"
            and priority_report["sources"]["delay"] == "cache",
            "metrics": {
                "earliest_stage": priority_report["earliest_stage"],
                "sources": priority_report["sources"],
                "rebuilt_stages": incremental["invalidation"]["rebuilt_stages"],
                "sha256_equal": incremental["verification"]["incremental_sha256"]
                == incremental["verification"]["full_sha256"],
            },
        }
    )

    replacement_workspace = Workspace(output / "replacement").ensure()
    replacement_workspace.import_bundle(generated["manifest"])
    Engine(replacement_workspace).build(mode="full")
    original_chunk = replacement_workspace.require_manifest()["chunks"][1]
    replacement_wav = output / "replacement_chunk_002.wav"
    _scaled_wav(replacement_workspace.chunk_wav_path(original_chunk), replacement_wav, 0.97)
    replacement_workspace.replace_chunk_sample_file("chunk_002", replacement_wav)
    replacement = Engine(replacement_workspace).build(mode="incremental", verify=True)
    replacement_report = replacement["invalidation"]["chunks"]["chunk_002"]
    checks.append(
        {
            "name": "sample_replacement_invalidates_detection_chain",
            "ok": replacement["verification"]["equal"]
            and replacement_report["earliest_stage"] == "detection"
            and "SAMPLE_CONTENT_CHANGED" in replacement_report["reason_codes"],
            "metrics": {
                "earliest_stage": replacement_report["earliest_stage"],
                "reason_codes": replacement_report["reason_codes"],
                "rebuilt_stages": replacement["invalidation"]["rebuilt_stages"],
            },
        }
    )

    corrupt_workspace = Workspace(output / "corrupt_recovery").ensure()
    corrupt_workspace.import_bundle(generated["manifest"])
    Engine(corrupt_workspace).build(mode="full", stop_after_units=7)
    (corrupt_workspace.checkpoint_dir / "000000000003.json").write_text("{broken", encoding="utf-8")
    shutil.copyfile(
        corrupt_workspace.checkpoint_dir / "000000000001.json",
        corrupt_workspace.checkpoint_dir / "duplicate-seq-one.json",
    )
    scan = Engine(corrupt_workspace).checkpoints.load_consistent()
    corrupt_resumed = Engine(corrupt_workspace).build(mode="incremental", resume=True)
    corrupt_equal = full_workspace.read_final_bytes() == corrupt_workspace.read_final_bytes()
    checks.append(
        {
            "name": "corrupt_and_duplicate_checkpoint_recovery",
            "ok": scan["prefix"] == []
            and corrupt_resumed["status"] == "complete"
            and corrupt_equal,
            "metrics": {
                "ignored_records": scan["invalid"],
                "resumed_sequence": corrupt_resumed["last_seq"],
                "byte_equal": corrupt_equal,
            },
        }
    )

    invalid_offset_details: dict[str, Any] = {}
    try:
        bad_workspace = Workspace(output / "invalid_offset").ensure()
        bad_workspace.import_bundle(generated["invalid_offset_manifest"])
        invalid_offset_code = None
    except CabinVoiceError as exc:
        invalid_offset_code = exc.code
        invalid_offset_details = exc.details
    checks.append(
        {
            "name": "offset_injection_error_code",
            "ok": invalid_offset_code == "E_ZONE_OFFSET_OUT_OF_BOUNDS",
            "metrics": {"code": invalid_offset_code, "details": invalid_offset_details},
        }
    )

    report = {
        "schema_version": 1,
        "engine_version": __version__,
        "python": platform.python_version(),
        "numpy": np.__version__,
        "seed": seed,
        "output": str(output),
        "all_checks_passed": all(check["ok"] for check in checks),
        "checks": checks,
        "artifacts": {
            "demo_manifest": generated["manifest"],
            "full_final": str(full_workspace.final_path),
            "full_final_sha256": sha256_file(full_workspace.final_path),
            "summary_digest": full_result["summary"]["session_digest"],
            "incremental_final_sha256": sha256_file(incremental_workspace.final_path),
            "replacement_final_sha256": sha256_file(replacement_workspace.final_path),
        },
    }
    atomic_write_json(output / "validation_report.json", report)
    return report
