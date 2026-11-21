#!/usr/bin/env python3
"""Repeatable demonstration of the complete delivery's three required paths.

Run order
---------
1. Generates missing deterministic samples/declarations.
2. Runs small/medium/large end-to-end in workdir/demo/full.
3. Repeats each scale with a graceful stop at a different stage, then resumes,
   and compares final samples.jsonl/stats.json byte digests with the full run.
4. Changes persisted config variants to demonstrate version invalidation:
   a) numeric boundary change invalidates normalize and all downstream stages;
   b) scoring-parameter change invalidates decision onward while normalize is reused.
5. Writes workdir/demo/demo_summary.json and prints the same summary.

Error-code meanings (shared by CLI/HTTP)
---------------------------------------
E_INVALID_FEATURE        empty feature region/value or non-monotonic boundaries
E_HASH_DIM_ZERO          zero category/cross hash dimension
E_CROSS_LIMIT            crossing cap exceeded
E_VECTOR_LENGTH_MISMATCH fixed vector length differs from declaration
E_BUDGET_EXCEEDED        selected/actual plan exceeds the computed budget
E_INVALID_STAGE          illegal stage jump, missing prerequisite, count mismatch
E_STALE_CHECKPOINT       stale/duplicate/behind/non-contiguous/cross-scale publish
E_NOT_FOUND              requested completed artifact is unavailable
E_INVALID_REQUEST        malformed CLI/HTTP request
"""

from __future__ import annotations

import copy
import json
import shutil
import sys
from pathlib import Path
from typing import Any

import yaml


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from recsys_pipeline.config import Config  # noqa: E402
from recsys_pipeline.generator import SCALE_SIZES, generate_all  # noqa: E402
from recsys_pipeline.pipeline import PipelineManager  # noqa: E402
from recsys_pipeline.util import (  # noqa: E402
    read_json,
    sha256_file,
    write_bytes_atomic,
    write_json_atomic,
)


STOP_STAGE = {"small": "normalize", "medium": "decision", "large": "encode"}


def _write_yaml_variant(path: Path, data: dict[str, Any]) -> None:
    payload = yaml.safe_dump(data, allow_unicode=True, sort_keys=False).encode("utf-8")
    write_bytes_atomic(path, payload)


def _artifact_digest(manager: PipelineManager, scale: str) -> dict[str, Any]:
    files = manager._stage_files(scale, "assemble")
    stats = read_json(files["stats"])
    decision = manager.get_decision(scale)
    selected = decision["selected"]
    return {
        "samples_sha256": sha256_file(files["samples"]),
        "stats_sha256": sha256_file(files["stats"]),
        "stats_content_digest": stats["content_digest"],
        "assembled_count": stats["assembled_count"],
        "discarded_count": stats["discarded_count"],
        "discarded_reasons": stats["discarded_reasons"],
        "decision": {
            "encoding_strategy": selected["encoding_strategy"],
            "assembly_strategy": selected["assembly_strategy"],
            "active_categorical_field_count": len(selected["active_categorical_fields"]),
            "cross_count": selected["cross_count"],
            "expected_loss_upper_bound": selected["expected_loss_upper_bound"],
            "estimated_cost_units": selected["estimated_cost_units"],
            "batch_size": selected["batch_size"],
        },
        "budget": decision["budget"],
    }


def main() -> int:
    base_config = Config.load(ROOT)
    generate_all(ROOT, base_config, force=False)
    demo_root = base_config.workdir / "demo"
    if demo_root.exists():
        shutil.rmtree(demo_root)
    demo_root.mkdir(parents=True, exist_ok=True)
    config_dir = ROOT / "config" / "generated"
    config_dir.mkdir(parents=True, exist_ok=True)

    summary: dict[str, Any] = {
        "schema_version": 1,
        "demo_root": str(demo_root),
        "scales": {},
        "version_invalidation": {},
        "upstream_reuse": {},
    }

    full_manager = PipelineManager(ROOT, workdir_override=demo_root / "full")
    full_result = full_manager.run_all()
    if full_result["state"] != "completed":
        raise RuntimeError(f"full run did not complete: {full_result['state']}")

    for scale in ("small", "medium", "large"):
        scale_data = full_manager.repository.load_scale(scale)
        full_digest = _artifact_digest(full_manager, scale)

        resume_manager = PipelineManager(ROOT, workdir_override=demo_root / f"resume_{scale}")
        stopped = resume_manager.run(scale, stop_after_stage=STOP_STAGE[scale])
        resumed = resume_manager.resume(scale)
        resumed_digest = _artifact_digest(resume_manager, scale)
        consistent = (
            full_digest["samples_sha256"] == resumed_digest["samples_sha256"]
            and full_digest["stats_sha256"] == resumed_digest["stats_sha256"]
            and full_digest["stats_content_digest"] == resumed_digest["stats_content_digest"]
        )
        summary["scales"][scale] = {
            "declaration": {
                "sample_count": scale_data.declaration["sample_count"],
                "feature_cardinality": scale_data.declaration["feature_cardinality"],
                "cross_limit": scale_data.declaration["cross_limit"],
                "budget_units": scale_data.declaration["budget_units"],
            },
            "full_run": full_digest,
            "stop_resume": {
                "stop_after_stage": STOP_STAGE[scale],
                "stopped_state": stopped["state"],
                "stopped_stages": stopped["stages_executed"],
                "resume_state": resumed["state"],
                "reused_stages": resumed["stages_reused"],
                "executed_stages": resumed["stages_executed"],
                "samples_sha256": resumed_digest["samples_sha256"],
                "stats_sha256": resumed_digest["stats_sha256"],
                "stats_content_digest": resumed_digest["stats_content_digest"],
                "byte_equal_to_full_run": consistent,
            },
        }
        if not consistent:
            raise RuntimeError(f"stop/resume mismatch for {scale}")

    boundary_data = copy.deepcopy(base_config.data)
    boundary_data["features"]["numeric"]["user.age"]["boundaries"] = [18, 25, 34, 50, 65]
    boundary_config_path = config_dir / "pipeline_boundary_change.yaml"
    _write_yaml_variant(boundary_config_path, boundary_data)

    version_manager = PipelineManager(ROOT, workdir_override=demo_root / "version_boundary")
    version_manager.run("small")
    normalized_before = sha256_file(
        version_manager._stage_files("small", "normalize")["normalized"]
    )
    boundary_manager = PipelineManager(
        ROOT,
        config_path=boundary_config_path,
        workdir_override=demo_root / "version_boundary",
    )
    invalid_status = boundary_manager.status("small")
    invalidated_result = boundary_manager.run("small")
    normalized_after = sha256_file(
        boundary_manager._stage_files("small", "normalize")["normalized"]
    )
    summary["version_invalidation"] = {
        "change": "features.numeric.user.age.boundaries [18,25,35,50,65] -> [18,25,34,50,65]",
        "config_variant": str(boundary_config_path.relative_to(ROOT)),
        "earliest_invalid_stage": invalid_status["earliest_invalid_stage"],
        "invalidated_stages": invalid_status["invalidated_stages"],
        "normalized_sha256_before": normalized_before,
        "normalized_sha256_after": normalized_after,
        "rebuild_state": invalidated_result["state"],
        "rebuilt_stages": invalidated_result["stages_executed"],
        "final_status_fully_complete": invalidated_result["status"]["fully_complete"],
    }

    decision_data = copy.deepcopy(base_config.data)
    decision_data["decision"]["score_weights"]["cost"] = 0.81
    decision_config_path = config_dir / "pipeline_decision_change.yaml"
    _write_yaml_variant(decision_config_path, decision_data)

    reuse_manager = PipelineManager(ROOT, workdir_override=demo_root / "version_reuse")
    reuse_manager.run("small")
    normalized_sha = sha256_file(
        reuse_manager._stage_files("small", "normalize")["normalized"]
    )
    decision_manager = PipelineManager(
        ROOT,
        config_path=decision_config_path,
        workdir_override=demo_root / "version_reuse",
    )
    reuse_status = decision_manager.status("small")
    reuse_result = decision_manager.run("small")
    normalized_sha_after = sha256_file(
        decision_manager._stage_files("small", "normalize")["normalized"]
    )
    summary["upstream_reuse"] = {
        "change": "decision.score_weights.cost 0.80 -> 0.81",
        "config_variant": str(decision_config_path.relative_to(ROOT)),
        "earliest_invalid_stage": reuse_status["earliest_invalid_stage"],
        "normalize_stage_state_before_rebuild": reuse_status["stages"][0]["state"],
        "invalidated_stages": reuse_status["invalidated_stages"],
        "reused_stages": reuse_result["stages_reused"],
        "executed_stages": reuse_result["stages_executed"],
        "normalized_sha256_before": normalized_sha,
        "normalized_sha256_after": normalized_sha_after,
        "normalize_reused_byte_equal": normalized_sha == normalized_sha_after,
        "final_status_fully_complete": reuse_result["status"]["fully_complete"],
    }

    summary["all_checks_passed"] = all(
        item["stop_resume"]["byte_equal_to_full_run"] for item in summary["scales"].values()
    ) and summary["version_invalidation"]["earliest_invalid_stage"] == "normalize" and summary[
        "upstream_reuse"
    ][
        "normalize_reused_byte_equal"
    ]
    summary_path = demo_root / "demo_summary.json"
    write_json_atomic(summary_path, summary, pretty=True)
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True, indent=2))
    return 0 if summary["all_checks_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
