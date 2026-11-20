#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import shutil
import sys
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from recall_lab.engine import Lab  # noqa: E402
from recall_lab.generate import generate_dataset  # noqa: E402
from recall_lab.util import atomic_write_bytes, canonical_json_bytes  # noqa: E402


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _compact_scale(summary: dict) -> dict:
    return {
        "scale": summary["scale"],
        "seed": summary["seed"],
        "item_count": summary["item_count"],
        "query_count": summary["query_count"],
        "enabled_sources": summary["decision"]["enabled_sources"],
        "disabled_sources": summary["decision"]["disabled_sources"],
        "route_depths": {
            source: route["effective_depth"] for source, route in summary["decision"]["routes"].items()
        },
        "used_evaluations": summary["decision"]["used_evaluations"],
        "global_budget": summary["decision"]["global_budget"],
        "precision_at_k": summary["fusion"]["precision_at_k"],
        "recall_at_k": summary["fusion"]["recall_at_k"],
        "candidate_coverage": summary["candidate_coverage"],
        "average_rank_deviation": summary["fusion"]["average_rank_deviation"],
        "route_reference_hits": {
            source: route["reference_hits"] for source, route in summary["routes"].items()
        },
        "eliminated_routes": summary["decision"]["disabled_sources"],
    }


def main() -> int:
    demo_root = ROOT / "artifacts" / "demo"
    if demo_root.exists():
        shutil.rmtree(demo_root)
    data_dir = demo_root / "data"
    manifest = generate_dataset(data_dir, seed=20261120)
    workspace_root = demo_root / "workspaces"

    full = Lab.initialize(workspace_root / "full", manifest, ROOT / "configs" / "fusion.yaml", force=True)
    full_run = full.run()
    full_export = full.root / "exports" / "experiment-summary.json"
    published_summary = ROOT / "artifacts" / "experiment-summary.json"
    atomic_write_bytes(published_summary, full_export.read_bytes())
    full_summary = json.loads(full_export.read_text(encoding="utf-8"))

    stopped = Lab.initialize(workspace_root / "stopped", manifest, ROOT / "configs" / "fusion.yaml", force=True)
    partial_run = stopped.run(stop_after="medium:fusion")
    resumed_run = stopped.resume()
    stopped_export = stopped.root / "exports" / "experiment-summary.json"
    resume_equal = full_export.read_bytes() == stopped_export.read_bytes()

    budget_lab = Lab.initialize(
        workspace_root / "budget-tight",
        manifest,
        ROOT / "configs" / "budget-tight.yaml",
        force=True,
    )
    budget_lab.run()
    budget_summary = json.loads((budget_lab.root / "exports" / "experiment-summary.json").read_text(encoding="utf-8"))

    weights_lab = Lab.initialize(
        workspace_root / "weights",
        manifest,
        ROOT / "configs" / "fusion.yaml",
        force=True,
    )
    weights_lab.run(scales=["small"])
    before = weights_lab.status(scales=["small"])["scales"][0]
    before_fusion = weights_lab.artifact("small", "fusion")
    alt_raw = yaml.safe_load((ROOT / "configs" / "fusion.yaml").read_text(encoding="utf-8"))
    alt_raw["fusion"]["weights"] = {"lexical": 2.5, "category": 0.1, "vector": 0.2}
    alt_config = demo_root / "alt-weights.yaml"
    alt_config.write_text(yaml.safe_dump(alt_raw, sort_keys=False, allow_unicode=True), encoding="utf-8")
    altered = Lab(weights_lab.root, config_path=alt_config)
    invalid_before_run = altered.status(scales=["small"])["scales"][0]["earliest_invalid_stage"]
    altered.run(scales=["small"])
    after = altered.status(scales=["small"])["scales"][0]
    after_fusion = altered.artifact("small", "fusion")
    changed_queries = sum(
        [item["item_id"] for item in left["fused"]] != [item["item_id"] for item in right["fused"]]
        for left, right in zip(before_fusion["queries"], after_fusion["queries"])
    )

    rrf_lab = Lab.initialize(
        workspace_root / "rrf",
        manifest,
        ROOT / "configs" / "fusion-rrf.yaml",
        force=True,
    )
    rrf_lab.run(scales=["small"])
    rrf_fusion = rrf_lab.artifact("small", "fusion")

    small_recall = full.artifact("small", "recall")
    cross_route_duplicate_count = sum(
        query["merged_duplicate_count"] for query in full.artifact("small", "fusion")["queries"]
    )
    mixed_query = next(
        query
        for query in small_recall["queries"]
        if any(ord(char) > 127 for char in query["query_text"])
        and any("a" <= char.lower() <= "z" for char in query["query_text"])
    )

    report = {
        "format": "verification-report-v1",
        "seed": 20261120,
        "manifest": {
            "path": str(manifest.relative_to(ROOT)),
            "sha256": _sha256(manifest),
            "scales": [
                {
                    "scale": row["scale"],
                    "item_count": row["item_count"],
                    "query_count": row["query_count"],
                    "expected_candidate_depth": row["expected_candidate_depth"],
                }
                for row in (
                    json.loads(line)
                    for line in manifest.read_text(encoding="utf-8").splitlines()
                    if line.strip()
                )
            ],
        },
        "full_run": {
            "status": full_run["status"],
            "completed_stage_count": len(full_run["completed"]),
            "summary_path": str(published_summary.relative_to(ROOT)),
            "summary_sha256": _sha256(full_export),
            "aggregate": full_summary["aggregate"],
            "scales": [_compact_scale(item) for item in full_summary["scales"]],
        },
        "budget_injection": {
            "config": "configs/budget-tight.yaml",
            "config_version": budget_summary["config_version"],
            "summary_sha256": _sha256(budget_lab.root / "exports" / "experiment-summary.json"),
            "scales": [_compact_scale(item) for item in budget_summary["scales"]],
        },
        "version_invalidation": {
            "earliest_invalid_stage_after_weight_change": invalid_before_run,
            "recall_sequence_before": before["stages"]["recall"]["checkpoint"]["sequence"],
            "recall_sequence_after": after["stages"]["recall"]["checkpoint"]["sequence"],
            "fusion_sequence_before": before["stages"]["fusion"]["checkpoint"]["sequence"],
            "fusion_sequence_after": after["stages"]["fusion"]["checkpoint"]["sequence"],
            "config_version_before": before["version"]["config_version"],
            "config_version_after": after["version"]["config_version"],
            "changed_top_k_queries": changed_queries,
        },
        "stop_and_resume": {
            "stopped_status": partial_run["status"],
            "stop_boundary": f"{partial_run['last']['scale']}:{partial_run['last']['stage']}",
            "resumed_status": resumed_run["status"],
            "full_summary_sha256": _sha256(full_export),
            "resumed_summary_sha256": _sha256(stopped_export),
            "byte_identical": resume_equal,
        },
        "fusion_strategies": {
            "weighted_sum": {
                "config_version": full_summary["config_version"],
                "cross_route_merged_duplicates_small": cross_route_duplicate_count,
            },
            "rrf": {
                "config_version": rrf_lab.config.fingerprint,
                "strategy": rrf_fusion["strategy"],
                "top_item": rrf_fusion["queries"][0]["fused"][0]["item_id"],
            },
        },
        "mixed_language_query_example": {
            "query_id": mixed_query["query_id"],
            "query_text": mixed_query["query_text"],
            "route_candidate_counts": {
                source: len(mixed_query["routes"][source]["candidates"]) for source in ("lexical", "category", "vector")
            },
        },
        "all_checks_passed": bool(
            resume_equal
            and full_run["status"] == "completed"
            and resumed_run["status"] == "completed"
            and invalid_before_run == "fusion"
            and before["stages"]["recall"]["checkpoint"]["sequence"]
            == after["stages"]["recall"]["checkpoint"]["sequence"]
            and changed_queries > 0
            and rrf_fusion["strategy"] == "rrf"
            and cross_route_duplicate_count > 0
        ),
    }
    report_path = ROOT / "artifacts" / "verification-report.json"
    atomic_write_bytes(report_path, canonical_json_bytes(report) + b"\n")
    print(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2))
    return 0 if report["all_checks_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
