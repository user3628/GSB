#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from feature_workbench import Workbench
from feature_workbench.budget import plan_schedule
from feature_workbench.config import dump_config, load_yaml, normalize_config
from feature_workbench.dataset import build_shard_specs, load_manifest
from feature_workbench.generator import generate_demo


def main() -> int:
    parser = argparse.ArgumentParser(description="Deterministic local workbench demonstration")
    parser.add_argument("--root", default=".", help="Workspace root")
    parser.add_argument("--workdir", default="artifacts/demo", help="Output directory")
    parser.add_argument("--force", action="store_true", help="Replace an existing demo directory")
    args = parser.parse_args()
    root = Path(args.root).resolve()
    workdir = (root / args.workdir).resolve()
    data_dir = root / "data"
    if not (data_dir / "manifest.json").is_file():
        generate_demo(data_dir)
    elif args.force:
        generate_demo(data_dir)

    if workdir.exists():
        if not args.force:
            raise SystemExit(f"{workdir} exists; use --force to replace it")
        shutil.rmtree(workdir)
    workdir.mkdir(parents=True)

    full = Workbench(
        root=root,
        state_dir=workdir / "full" / "state",
        export_dir=workdir / "full" / "exports",
    )
    full_result = full.run(worker_mode="process")

    resumed = Workbench(
        root=root,
        state_dir=workdir / "resume" / "state",
        export_dir=workdir / "resume" / "exports",
    )
    stopped_result = resumed.run(worker_mode="sequential", stop_after_shards=2)
    resumed_result = resumed.run(worker_mode="process", resume=True)
    full_summary = full.read_selection_summary()
    resumed_summary = resumed.read_selection_summary()
    assert full_summary is not None and resumed_summary is not None
    same_tiers = full_summary["tiers"] == resumed_summary["tiers"]

    target_config = normalize_config(load_yaml(data_dir / "config.yaml"))
    target_config["encoding"]["strategy"] = "target"
    target_config["selection"]["method"] = "mutual_info"
    target_config["selection"]["threshold"] = 0.0
    target_config_path = workdir / "target" / "config.yaml"
    dump_config(target_config_path, target_config)
    target = Workbench(
        root=root,
        config_path=target_config_path,
        state_dir=workdir / "target" / "state",
        export_dir=workdir / "target" / "exports",
    )
    target_result = target.run(worker_mode="sequential")
    target_encoding_file = workdir / "target" / "exports" / "small" / "encoding_summaries.json"
    target_encodings = json.loads(target_encoding_file.read_text(encoding="utf-8"))
    diagnostics = target_encodings["shards"][0]["target_encoding_diagnostics"][0]
    sample_category = diagnostics["categories"][0]

    manifest = load_manifest(data_dir / "manifest.json")
    specs = build_shard_specs(manifest, data_dir)
    constrained = normalize_config(load_yaml(data_dir / "config.yaml"))
    constrained["budget"]["global_feature_evaluations"] = 200_000
    constrained_plan = plan_schedule(specs, constrained)

    output = {
        "ok": True,
        "data_dir": str(data_dir),
        "full": {
            "result_digest": full_result["result_digest"],
            "checkpoints": full_result["checkpoints"],
            "status": full_result["status"],
        },
        "stop_resume": {
            "stopped_status": stopped_result["status"],
            "stopped_checkpoints": stopped_result["checkpoints"],
            "resumed_result_digest": resumed_result["result_digest"],
            "reused": resumed_result["reused"],
            "checkpoints": resumed_result["checkpoints"],
            "matches_full_run": resumed_result["result_digest"] == full_result["result_digest"],
            "tier_payloads_match": same_tiers,
        },
        "target_encoding_demo": {
            "result_digest": target_result["result_digest"],
            "column": diagnostics["column"],
            "fallback": diagnostics["fallback"],
            "category": sample_category["value"],
            "encoded_by_fold": [
                {
                    "fold": item["fold"],
                    "encoded": item["encoded"],
                    "seen_in_training": item["seen_in_training"],
                }
                for item in sample_category["folds"]
            ],
        },
        "constrained_budget_demo": {
            "global_budget": constrained_plan["global_budget"],
            "global_used": constrained_plan["global_used"],
            "admitted_shards": [item["shard_index"] for item in constrained_plan["admitted"]],
            "unprocessed": [
                {
                    "shard_index": item["shard_index"],
                    "file": item["file"],
                    "reason": item["reason"],
                    "estimated_evaluations": item["estimated_evaluations"],
                }
                for item in constrained_plan["unprocessed"]
            ],
        },
        "outputs": {
            "full": str(workdir / "full" / "exports" / "selection.json"),
            "resume": str(workdir / "resume" / "exports" / "selection.json"),
            "target": str(target_encoding_file),
        },
    }
    print(json.dumps(output, ensure_ascii=False, indent=2))
    return 0 if output["stop_resume"]["matches_full_run"] and same_tiers else 1


if __name__ == "__main__":
    raise SystemExit(main())

