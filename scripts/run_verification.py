#!/usr/bin/env python3
"""Run the deterministic acceptance demonstration and record results."""

from __future__ import annotations

import hashlib
import json
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any

import numpy as np


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from audiofp import (  # noqa: E402
    CheckpointStore,
    FingerprintError,
    get_report,
    import_dataset,
    request_stop,
    resume_pipeline,
    run_pipeline,
)
from audiofp.jsonutil import atomic_write_bytes, sha256_file  # noqa: E402
from audiofp.samples import generate_samples  # noqa: E402


def _workspace_artifact_hashes(workspace: Path, include_checkpoints: bool = False) -> dict[str, str]:
    hashes: dict[str, str] = {}
    directories = ["artifacts", "reports", "errors"]
    if include_checkpoints:
        directories.append("checkpoints")
    for directory in directories:
        base = workspace / directory
        if not base.exists():
            continue
        for path in sorted(base.rglob("*")):
            if path.is_file():
                relative = path.relative_to(workspace).as_posix()
                hashes[relative] = sha256_file(path)
    return hashes


def _hash_map_digest(hashes: dict[str, str]) -> str:
    encoded = json.dumps(hashes, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _error_codes(report: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for error in report["pipeline_errors"]:
        row = {
            "shard_id": error["shard_id"],
            "code": error["code"],
            "stage": error.get("stage"),
        }
        if error.get("details", {}).get("strongest_bucket") is not None:
            row["strongest_bucket"] = error["details"]["strongest_bucket"]
        rows.append(row)
    return rows


def _public_summary(report: dict[str, Any]) -> dict[str, Any]:
    return {
        "audited_shard_count": report["audited_shard_count"],
        "failed_shard_count": report["failed_shard_count"],
        "suspicious_shard_count": report["suspicious_shard_count"],
        "failed_shards": [
            {
                "shard_id": item["shard_id"],
                "kind": item.get("kind"),
                "code": item.get("code"),
            }
            for item in report["failed_shards"]
        ],
        "suspicious_shards": [
            {
                "shard_id": item["shard_id"],
                "confidence_ppm": item["confidence_ppm"],
                "confidence_threshold_ppm": item["confidence_threshold_ppm"],
            }
            for item in report["suspicious_shards"]
        ],
        "shards": [
            {
                key: item.get(key)
                for key in (
                    "shard_id",
                    "status",
                    "reference_pair_count",
                    "generated_vector_count",
                    "hit_rate_ppm",
                    "false_match_rate_ppm",
                    "average_dt_shift_frames",
                    "confidence_ppm",
                    "weight_ppm",
                    "suspicious",
                    "error_code",
                )
            }
            for item in report["shards"]
        ],
        "corpus": report["corpus"],
    }


def _checkpoint_guard_demo(workspace: Path) -> dict[str, str]:
    store = CheckpointStore(workspace)

    def record(shard_id: str, stage: str) -> dict[str, Any]:
        return {
            "schema": "audiofp.checkpoint.v1",
            "shard_id": shard_id,
            "stage": stage,
            "input_digest": "demo-input",
            "config_version": "demo-config",
            "artifact_digest": "demo-artifact",
            "stage_version": "demo-version",
            "dependencies": {},
            "params": {},
        }

    codes: dict[str, str] = {}
    try:
        store.publish("guard", record("guard", "quantize"), expected_seq=1)
    except FingerprintError as error:
        codes["illegal_stage_jump"] = error.code
    store.publish("guard", record("guard", "peaks"), expected_seq=1)
    try:
        store.publish("guard", record("guard", "peaks"), expected_seq=2)
    except FingerprintError as error:
        codes["duplicate_stage"] = error.code
    try:
        store.publish("guard", record("other-shard", "pairs"), expected_seq=2)
    except FingerprintError as error:
        codes["cross_shard_misalignment"] = error.code
    return codes


def main() -> int:
    samples_dir = ROOT / "samples"
    generation = generate_samples(samples_dir, 20261123)
    manifest_path = Path(generation["manifest"])
    sample_hashes_first = {
        path.name: sha256_file(path)
        for path in sorted(samples_dir.iterdir())
        if path.is_file()
    }
    generate_samples(samples_dir, 20261123)
    sample_hashes_second = {
        path.name: sha256_file(path)
        for path in sorted(samples_dir.iterdir())
        if path.is_file()
    }
    reference_payload = json.loads((samples_dir / "reference_pairs.json").read_text(encoding="utf-8"))
    sample_hashes = {
        path.name: sha256_file(path)
        for path in sorted(samples_dir.glob("*.wav"))
    }

    temporary_root = Path(tempfile.mkdtemp(prefix="audiofp-verification-"))
    try:
        full = temporary_root / "full"
        resumed = temporary_root / "resumed"
        stop_marker = temporary_root / "stop-marker"
        incremental = temporary_root / "incremental"
        incremental_fresh = temporary_root / "incremental-fresh"
        guard_workspace = temporary_root / "checkpoint-guards"
        for workspace in (full, resumed, stop_marker, incremental):
            import_dataset(workspace, manifest_path)

        full_result = run_pipeline(full)
        full_report = get_report(full)["summary"]
        full_hashes = _workspace_artifact_hashes(full, include_checkpoints=True)

        partial_result = run_pipeline(resumed, stop_after_stage="quantize")
        partial_sequences = {
            shard_id: CheckpointStore(resumed).read_state(shard_id)["next_seq"]
            for shard_id in ("music_a", "music_b", "music_c")
        }
        resume_result = resume_pipeline(resumed)
        resumed_report = get_report(resumed)["summary"]
        resumed_hashes = _workspace_artifact_hashes(resumed, include_checkpoints=True)
        resumed_sequences = {
            shard_id: {
                stage: CheckpointStore(resumed).read_state(shard_id)["stages"][stage]["seq"]
                for stage in ("peaks", "pairs", "quantize", "audit")
            }
            for shard_id in ("music_a", "music_b", "music_c")
        }

        request_stop(stop_marker)
        marker_result = run_pipeline(stop_marker)
        marker_resume_result = resume_pipeline(stop_marker)
        marker_report = get_report(stop_marker)["summary"]
        marker_hashes = _workspace_artifact_hashes(stop_marker, include_checkpoints=True)

        run_pipeline(incremental)
        before_incremental = CheckpointStore(incremental).read_state("music_c")
        incremental_manifest = incremental / "manifest.json"
        manifest_payload = json.loads(incremental_manifest.read_text(encoding="utf-8"))
        for shard in manifest_payload["shards"]:
            if shard["shard_id"] == "music_c":
                shard["bucket_width"] = 8192
        incremental_manifest.write_text(
            json.dumps(manifest_payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )
        run_pipeline(incremental)
        after_incremental = CheckpointStore(incremental).read_state("music_c")
        incremental_hashes = _workspace_artifact_hashes(incremental)

        import_dataset(incremental_fresh, incremental_manifest)
        run_pipeline(incremental_fresh)
        incremental_fresh_hashes = _workspace_artifact_hashes(incremental_fresh)
        incremental_fresh_report = get_report(incremental_fresh)["summary"]
        checkpoint_guard_errors = _checkpoint_guard_demo(guard_workspace)

        checks = {
            "full_run_completed": not full_result.stopped and full_result.summary_digest is not None,
            "sample_generation_is_byte_identical": sample_hashes_first == sample_hashes_second,
            "has_three_audited_music_shards": full_report["audited_shard_count"] >= 3,
            "silence_returned_e_no_peak": any(
                item["shard_id"] == "silence" and item["code"] == "E_NO_PEAK"
                for item in full_report["pipeline_errors"]
            ),
            "low_bucket_returned_e_bucket_collision": any(
                item["shard_id"] == "bucket_collision" and item["code"] == "E_BUCKET_COLLISION"
                for item in full_report["pipeline_errors"]
            ),
            "suspicious_shards_are_separate": full_report["suspicious_shard_count"] >= 1,
            "stage_stop_was_graceful": partial_result.stopped and partial_result.report_path is None,
            "stop_then_resume_is_byte_identical": full_hashes == resumed_hashes,
            "stop_marker_was_respected": marker_result.stopped
            and marker_result.stop_reason == "stop_requested",
            "stop_marker_resume_is_byte_identical": full_hashes == marker_hashes,
            "incremental_reused_peaks_and_pairs": (
                before_incremental["stages"]["peaks"]["seq"]
                == after_incremental["stages"]["peaks"]["seq"]
                and before_incremental["stages"]["pairs"]["seq"]
                == after_incremental["stages"]["pairs"]["seq"]
                and after_incremental["last_invalidation"]["from_stage"] == "quantize"
            ),
            "incremental_outputs_match_fresh_build": incremental_hashes == incremental_fresh_hashes,
            "checkpoint_guard_errors_are_stable": (
                checkpoint_guard_errors.get("illegal_stage_jump") == "E_INVALID_STAGE"
                and checkpoint_guard_errors.get("duplicate_stage") == "E_STALE_CHECKPOINT"
                and checkpoint_guard_errors.get("cross_shard_misalignment") == "E_STALE_CHECKPOINT"
            ),
            "summary_matches_fresh_build": incremental_hashes.get("reports/summary.json")
            == incremental_fresh_hashes.get("reports/summary.json"),
            "all_audited_hit_rates_are_100_percent": all(
                row["hit_rate_ppm"] == 1_000_000
                for row in full_report["shards"]
                if row["status"] == "passed"
            ),
        }

        audit_fields = {}
        for shard_id in ("music_a", "music_b", "music_c"):
            status = None
            for row in full_report["shards"]:
                if row["shard_id"] == shard_id:
                    status = row
            if status is None:
                continue
            artifact = json.loads(
                next(
                    path.read_text(encoding="utf-8")
                    for path in (full / "reports" / "shards").glob("*.json")
                    if json.loads(path.read_text(encoding="utf-8"))["shard_id"] == shard_id
                )
            )
            fields = artifact["quantization_error"]["fields"]
            audit_fields[shard_id] = {
                field: {
                    "upper_bound": value["upper_bound"],
                    "actual_max_abs_error": value["actual_max_abs_error"],
                    "effective_bits": value["effective_bits"],
                    "within_bound": value["actual_max_abs_error"] <= value["upper_bound"] + 1e-12,
                }
                for field, value in fields.items()
            }
            checks[f"quantization_bounds_hold_{shard_id}"] = all(
                item["within_bound"] for item in audit_fields[shard_id].values()
            )

        result = {
            "schema": "audiofp.verification.v1",
            "environment": {
                "python": sys.version.split()[0],
                "numpy": np.__version__,
                "platform": sys.platform,
            },
            "seed": 20261123,
            "sample_files": sample_hashes,
            "reference_pair_counts": {
                shard_id: len(entries)
                for shard_id, entries in sorted(reference_payload["shards"].items())
            },
            "full_run": {
                "summary_digest": full_result.summary_digest,
                "artifact_map_digest": _hash_map_digest(full_hashes),
                "errors": _error_codes(full_report),
                "summary": _public_summary(full_report),
                "quantization_fields": audit_fields,
            },
            "stop_and_resume": {
                "partial_processed_shards": partial_result.processed_shards,
                "partial_next_seq": partial_sequences,
                "resumed_summary_digest": resume_result.summary_digest,
                "full_resume_artifact_maps_equal": full_hashes == resumed_hashes,
                "final_stage_sequences": resumed_sequences,
            },
            "stop_marker": {
                "run_stopped": marker_result.stopped,
                "stop_reason": marker_result.stop_reason,
                "resumed": not marker_resume_result.stopped,
                "artifact_maps_equal_to_full": full_hashes == marker_hashes,
            },
            "incremental_invalidation": {
                "changed": {"shard_id": "music_c", "field": "bucket_width", "old": 4096, "new": 8192},
                "invalidation": after_incremental["last_invalidation"],
                "upstream_sequences_before": {
                    "peaks": before_incremental["stages"]["peaks"]["seq"],
                    "pairs": before_incremental["stages"]["pairs"]["seq"],
                },
                "stage_sequences_after": {
                    stage: after_incremental["stages"][stage]["seq"]
                    for stage in ("peaks", "pairs", "quantize", "audit")
                },
                "artifact_maps_equal_to_fresh": incremental_hashes == incremental_fresh_hashes,
            },
            "checkpoint_guard_errors": checkpoint_guard_errors,
            "checks": checks,
            "passed": all(checks.values()),
        }
        encoded = (json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode("utf-8")
        atomic_write_bytes(ROOT / "artifacts" / "verification.json", encoded)
        lines = [
            "Audio fingerprint verification",
            f"seed: 20261123",
            f"python: {result['environment']['python']}",
            f"numpy: {result['environment']['numpy']}",
            f"passed: {result['passed']}",
            "",
            "checks:",
        ]
        for name, value in sorted(checks.items()):
            lines.append(f"- {name}: {value}")
        lines.extend(["", "full-run errors:"] + [f"- {item}" for item in result["full_run"]["errors"]])
        lines.extend(["", "audited shards:"])
        for row in full_report["shards"]:
            lines.append(f"- {row}")
        lines.extend(
            [
                "",
                f"summary digest: {full_result.summary_digest}",
                f"artifact map digest: {_hash_map_digest(full_hashes)}",
                f"checkpoint guard errors: {checkpoint_guard_errors}",
                f"invalidation: {after_incremental['last_invalidation']}",
                "stop/resume byte-identical: "
                + str(full_hashes == resumed_hashes and full_hashes == marker_hashes),
                "incremental/fresh byte-identical: " + str(incremental_hashes == incremental_fresh_hashes),
            ]
        )
        atomic_write_bytes(
            ROOT / "artifacts" / "verification.txt",
            ("\n".join(lines) + "\n").encode("utf-8"),
        )
        print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2))
        return 0 if result["passed"] else 1
    finally:
        shutil.rmtree(temporary_root, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())

