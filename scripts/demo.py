#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import shutil
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from halu_pipeline.canonical import (  # noqa: E402
    GracefulStop,
    PipelineError,
    atomic_write_json,
    canonical_bytes,
)
from halu_pipeline.pipeline import Pipeline  # noqa: E402
from halu_pipeline.query import QueryService  # noqa: E402


DEMO_DATA = ROOT / "data" / "demo"
ARTIFACTS = ROOT / "artifacts"


def _reset(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)


def _shards() -> list[Path]:
    return sorted(DEMO_DATA.glob("shard-*.jsonl"))


def _stage_bytes(root: Path) -> dict[str, str]:
    return {
        str(stage): hashlib.sha256(
            (root / "stages" / f"stage{stage}.json").read_bytes()
        ).hexdigest()
        for stage in range(1, 5)
    }


def _base_config(manifest: dict[str, Any], *, workers: int = 1) -> dict[str, Any]:
    return {
        "declared_strata": manifest["declared_strata"],
        "latency_budget_ms": 100.0,
        "parallel_workers": workers,
        "sentence_support_threshold": 0.82,
        "consistency_min": 0.80,
        "min_annotation_coverage": 0.60,
    }


def main() -> int:
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    manifest = json.loads((DEMO_DATA / "manifest.json").read_text(encoding="utf-8"))
    demo_run = ARTIFACTS / "demo-run"
    fresh_run = ARTIFACTS / ".demo-fresh"
    stop_run = ARTIFACTS / ".demo-stop"
    parallel_run = ARTIFACTS / ".demo-parallel"
    for path in (demo_run, fresh_run, stop_run, parallel_run):
        _reset(path)

    checks: dict[str, Any] = {}
    pipeline = Pipeline(demo_run)
    pipeline.initialize(_shards(), _base_config(manifest))
    baseline = pipeline.run()
    checks["full_run"] = baseline["verification"] == "PASS"
    baseline_hashes = _stage_bytes(demo_run)
    baseline_sample = QueryService(demo_run).query_sample(
        "mix-002", budget_ms=1000, use_cache=False
    )

    threshold_edit = pipeline.update_config(assignments=["sentence_support_threshold=0.84"])
    threshold_run = pipeline.run()
    checks["threshold_edit_earliest_stage_2"] = (
        threshold_edit["earliest_affected_stage"] == 2
    )
    checks["threshold_edit_reuses_stage_1"] = threshold_run["reused_stages"] == [1]

    weight_edit = pipeline.update_config(assignments=["evidence_weights.wiki_mixed=0.75"])
    weight_run = pipeline.run()
    checks["weight_edit_earliest_stage_2"] = weight_edit["earliest_affected_stage"] == 2
    checks["weight_edit_reuses_stage_1"] = weight_run["reused_stages"] == [1]

    validation_edit = pipeline.update_config(assignments=["consistency_min=0.82"])
    validation_run = pipeline.run()
    checks["validation_edit_earliest_stage_4"] = (
        validation_edit["earliest_affected_stage"] == 4
    )
    checks["validation_edit_reuses_stages_1_2_3"] = validation_run["reused_stages"] == [1, 2, 3]

    final_config = pipeline.load_config().to_dict()
    fresh = Pipeline(fresh_run)
    fresh.initialize(_shards(), final_config)
    fresh.run()
    final_hashes = _stage_bytes(demo_run)
    checks["incremental_matches_fresh_full_run"] = final_hashes == _stage_bytes(fresh_run)

    parallel_root = parallel_run
    parallel_config = dict(final_config)
    parallel_config["parallel_workers"] = 4
    parallel = Pipeline(parallel_root)
    parallel.initialize(_shards(), parallel_config)
    parallel_result = parallel.run()
    checks["parallel_run_self_verification"] = parallel_result["verification"] == "PASS"
    checks["parallel_matches_sequential"] = _stage_bytes(parallel_root) == final_hashes

    stop = Pipeline(stop_run)
    stop.initialize(_shards(), final_config)
    stop.run(until_stage=1)
    stage1_before_stop = (stop_run / "stages" / "stage1.json").read_bytes()
    stop.request_stop()
    stopped_code = None
    try:
        stop.run()
    except GracefulStop as exc:
        stopped_code = exc.code
    checks["graceful_stop_code"] = stopped_code == "E_STOPPED"
    stop.resume()
    stop_result = stop.run()
    checks["stop_resume_verification"] = stop_result["verification"] == "PASS"
    checks["stop_resume_stage1_reused"] = stop_result["reused_stages"] == [1]
    checks["stop_resume_matches_fresh"] = _stage_bytes(stop_run) == final_hashes
    checks["stop_resume_stage1_bytes_stable"] = (
        stage1_before_stop == (stop_run / "stages" / "stage1.json").read_bytes()
    )

    service = QueryService(demo_run)
    service.clear_cache()
    sample_first = service.query_sample("mix-002", budget_ms=1000)
    sample_cached = service.query_sample("mix-002", budget_ms=1000)
    service.clear_cache()
    sample_recomputed = service.query_sample("mix-002", budget_ms=1000)
    checks["sample_cache_hit_byte_equal"] = (
        canonical_bytes(sample_first) == canonical_bytes(sample_cached)
    )
    checks["sample_cache_recompute_byte_equal"] = (
        canonical_bytes(sample_first) == canonical_bytes(sample_recomputed)
    )
    checks["sample_detected_hallucination"] = bool(
        sample_first["detection"]["hallucination"]
    )
    checks["evidence_weight_changes_score"] = (
        sample_first["detection"]["consistency_score"]
        != baseline_sample["detection"]["consistency_score"]
    )

    summary_first = service.query_summary(budget_ms=1000)
    service.clear_cache()
    summary_recomputed = service.query_summary(budget_ms=1000)
    checks["summary_cache_recompute_byte_equal"] = (
        canonical_bytes(summary_first) == canonical_bytes(summary_recomputed)
    )
    empty_layers = [
        {"source": item["source"], "lang": item["lang"]}
        for item in summary_first["strata"]
        if item["sample_count"] == 0
    ]
    checks["empty_strata_preserved"] = len(empty_layers) == 2

    latency = service.query_sample("mix-002", budget_ms=0, query_id="demo-budget-0")
    checks["latency_budget_error"] = (
        latency["code"] == "E_LATENCY_BUDGET"
        and latency.get("query_id") == "demo-budget-0"
        and "elapsed_ms" in latency
    )

    invalid_code = None
    invalid_details: Any = None
    try:
        pipeline.update_config(assignments=["sentence_support_threshold=1.5"])
    except PipelineError as exc:
        invalid_code = exc.code
        invalid_details = exc.details
    checks["invalid_threshold_error"] = invalid_code == "E_INVALID_CONFIG"

    status = pipeline.status()
    validation = pipeline.load_stage_payload(4)
    summary = pipeline.load_stage_payload(3)
    checkpoints = pipeline.checkpoints.list()
    sequences = [item["sequence"] for item in checkpoints]
    checks["checkpoint_monotonic"] = sequences == sorted(set(sequences))
    checks["checkpoint_atomic_no_tmp"] = not list(
        (demo_run / "checkpoints").glob("*.tmp")
    )

    report_digest = pipeline.write_report(ARTIFACTS / "demo_report_full.json")
    report = {
        "code": "OK",
        "seed": manifest["seed"],
        "shard_count": len(_shards()),
        "record_count": manifest["record_count"],
        "checks": checks,
        "all_checks_pass": all(bool(value) for value in checks.values()),
        "baseline_run": baseline,
        "incremental_runs": {
            "threshold": threshold_run,
            "evidence_weight": weight_run,
            "validation_threshold": validation_run,
        },
        "stage_hashes_final": final_hashes,
        "baseline_stage_hashes": baseline_hashes,
        "status": {
            "earliest_invalidated_stage": status["earliest_invalidated_stage"],
            "config_version": status["config_version"],
            "checkpoint_last_sequence": status["checkpoint_last_sequence"],
            "stage_config_fingerprints": status["stage_config_fingerprints"],
        },
        "summary_totals": summary["totals"],
        "empty_strata": empty_layers,
        "validation_overall": validation["overall_confusion"],
        "source_reviews": [
            {
                "source": item["source"],
                "coverage": item["coverage"],
                "accuracy": item["confusion"]["accuracy"],
                "review_required": item["review_required"],
                "review_reasons": item["review_reasons"],
                "missing_annotation_sample_ids": item["missing_annotation_sample_ids"],
                "recommendations": item["recommendations"],
            }
            for item in validation["source_reviews"]
        ],
        "sample_mix_002": {
            "hallucination": sample_first["detection"]["hallucination"],
            "consistency_score": sample_first["detection"]["consistency_score"],
            "stage_fingerprint": sample_first["stage_fingerprint"],
            "sentence_support": sample_first["detection"]["sentence_support"],
        },
        "latency_error": latency,
        "invalid_threshold": {"code": invalid_code, "details": invalid_details},
        "checkpoint_sequences": sequences,
        "full_report": {
            "path": str(ARTIFACTS / "demo_report_full.json"),
            "sha256": report_digest,
        },
    }
    atomic_write_json(ARTIFACTS / "demo_report.json", report)

    for path in (fresh_run, stop_run, parallel_run):
        shutil.rmtree(path)
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if report["all_checks_pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
