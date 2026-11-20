#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.generate_demo import generate  # noqa: E402
from xrel_workbench.bridge import load_bridge  # noqa: E402
from xrel_workbench.checkpoints import CheckpointStore  # noqa: E402
from xrel_workbench.pipeline import run_pipeline, workspace_status  # noqa: E402
from xrel_workbench.util import atomic_write, atomic_write_json, read_json  # noqa: E402
from xrel_workbench.workspace import (  # noqa: E402
    WorkspacePaths,
    clear_stop,
    initialize_workspace,
    update_bridge_entry,
)


def _copy_bytes(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    atomic_write(destination, source.read_bytes())


def _sequence_is_contiguous(paths: WorkspacePaths) -> bool:
    records = CheckpointStore(paths.root).records()
    return [int(record["seq"]) for record in records] == list(range(1, len(records) + 1))


def run_demo(data_dir: Path, artifacts_dir: Path, *, seed: int = 20261121) -> dict[str, Any]:
    data_dir = data_dir.resolve()
    artifacts_dir = artifacts_dir.resolve()
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    generate(data_dir, seed, force=True)

    main_root = artifacts_dir / "workspace"
    main = initialize_workspace(data_dir, main_root, force=True)
    initial = run_pipeline(main, full=True, verify_incremental=False)
    if initial["stopped"]:
        raise RuntimeError("initial full build unexpectedly stopped")
    initial_summary = initial["summary"]
    initial_checkpoint_seq = workspace_status(main)["checkpoints"]["last_seq"]
    _copy_bytes(main.reports / "summary.json", artifacts_dir / "full_baseline_summary.json")
    _copy_bytes(main.reports / "validation.json", artifacts_dir / "full_baseline_validation.json")

    resume_root = artifacts_dir / "stop-resume-workspace"
    resume_ws = initialize_workspace(data_dir, resume_root, force=True)
    stopped = run_pipeline(
        resume_ws,
        full=True,
        verify_incremental=False,
        stop_after_units=4,
    )
    if not stopped["stopped"]:
        raise RuntimeError("stop demonstration did not stop at the requested boundary")
    stopped_status = workspace_status(resume_ws)
    stopped_seq = stopped_status["checkpoints"]["last_seq"]
    clear_stop(resume_ws)
    resumed = run_pipeline(resume_ws, verify_incremental=True)
    resumed_summary = resumed["summary"]
    if resumed_summary["build_id"] != initial_summary["build_id"]:
        raise RuntimeError("stop/resume build id differs from the full baseline")
    if not _sequence_is_contiguous(resume_ws):
        raise RuntimeError("checkpoint sequence is not contiguous after stop/resume")
    _copy_bytes(
        resume_ws.reports / "summary.json",
        artifacts_dir / "stop_resume_summary.json",
    )

    bridge_before = load_bridge(main.bridge)
    before_entry = next(entry for entry in bridge_before["entries"] if entry["id"] == "br-003")
    old_weight = float(before_entry["weight"])
    new_weight = round(old_weight - 0.21, 2)
    update_bridge_entry(main, "br-003", weight=new_weight)
    incremental = run_pipeline(main, full=False, verify_incremental=True)
    if not incremental["verification"] or not incremental["verification"]["match"]:
        raise RuntimeError("incremental build did not match the full shadow build")
    final_summary = incremental["summary"]
    _copy_bytes(main.reports / "summary.json", artifacts_dir / "final_incremental_summary.json")
    _copy_bytes(main.reports / "validation.json", artifacts_dir / "final_validation.json")
    _copy_bytes(main.reports / "error_report.json", artifacts_dir / "final_error_report.json")
    _copy_bytes(main.exports / "scores.jsonl", artifacts_dir / "final_scores.jsonl")

    final_status = workspace_status(main)
    report = {
        "ok": True,
        "seed": seed,
        "commands": {
            "generate": f"python scripts/generate_demo.py --out {data_dir} --seed {seed} --force",
            "run": "python -m xrel_workbench run --workspace <workspace>",
            "demo": "python -m xrel_workbench demo",
        },
        "initial_full_build": {
            "build_id": initial_summary["build_id"],
            "shards": initial_summary["counts"]["shards"],
            "pairs": initial_summary["counts"]["pairs"],
            "grade_counts": initial_summary["grades"]["counts"],
            "review_required": initial_summary["review_required"],
            "checkpoint_sequence": initial_checkpoint_seq,
        },
        "stop_resume": {
            "stopped_after_units": stopped["completed_units"] if "completed_units" in stopped else stopped["rebuilt_units"],
            "stopped_checkpoint_seq": stopped_seq,
            "resumed_build_id": resumed_summary["build_id"],
            "matches_full_baseline": resumed_summary["build_id"] == initial_summary["build_id"],
            "checkpoint_sequence_contiguous": _sequence_is_contiguous(resume_ws),
            "verification": resumed.get("verification"),
        },
        "bridge_edit_incremental": {
            "entry_id": "br-003",
            "old_weight": old_weight,
            "new_weight": new_weight,
            "earliest_affected_stages": incremental["invalidation_plan"]["summary"],
            "rebuilt_units": incremental["rebuilt_units"],
            "reused_units": incremental["reused_units"],
            "build_id": final_summary["build_id"],
            "grade_counts": final_summary["grades"]["counts"],
            "review_required": final_summary["review_required"],
            "incremental_vs_full": incremental["verification"],
            "checkpoint_sequence_contiguous": _sequence_is_contiguous(main),
            "final_checkpoint_seq": final_status["checkpoints"]["last_seq"],
        },
        "artifacts": {
            "workspace": str(main.root),
            "full_baseline_summary": str(artifacts_dir / "full_baseline_summary.json"),
            "final_summary": str(artifacts_dir / "final_incremental_summary.json"),
            "final_validation": str(artifacts_dir / "final_validation.json"),
            "final_error_report": str(artifacts_dir / "final_error_report.json"),
            "final_scores": str(artifacts_dir / "final_scores.jsonl"),
            "verification_report": str(artifacts_dir / "verification_report.json"),
        },
    }
    atomic_write_json(artifacts_dir / "verification_report.json", report, indent=2)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the deterministic end-to-end demonstration")
    parser.add_argument("--seed", type=int, default=20261121)
    parser.add_argument("--data", type=Path, default=ROOT / "data")
    parser.add_argument("--artifacts", type=Path, default=ROOT / "artifacts" / "demo")
    args = parser.parse_args()
    result = run_demo(args.data, args.artifacts, seed=args.seed)
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
