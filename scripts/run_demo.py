"""Deterministic end-to-end demo of the voxel reconstruction engine.

Flow (all local, no GPU / network / external assets):
  1. generate the fixed-seed sample dataset (5 view shards + manifest);
  2. full build in a reference workspace;
  3. stop-between-stages + recovery + resume in a second workspace, then
     byte-compare against the reference digests;
  4. incremental updates in a third workspace - wholesale replacement of one
     shard, then a voxel-resolution change, then a sparsity-threshold change -
     each followed by the incremental-vs-full consistency check;
  5. export the global voxel summary and print the fixed demo digest.

Run from the repository root:  python -m scripts.run_demo --dir demo_out
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from voxel_engine.engine import Engine
from voxel_engine.generator import DEFAULT_SEED, generate_dataset
from voxel_engine.serialization import hash_json


def _digests(engine: Engine) -> dict:
    return engine._collect_digests()["_summary"]


def run_demo(out_dir, seed: int = DEFAULT_SEED) -> dict:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    data_dir = out / "data"
    manifest = generate_dataset(data_dir, seed=seed, num_views=5)
    view_ids = [view["view_id"] for view in manifest["views"]]
    replaced_view = view_ids[2]
    alt_image = data_dir / "shards_alt" / f"{replaced_view}.png"
    reference = Engine(out / "ws_reference")
    reference.import_dataset(data_dir / "manifest.json")
    reference.build(mode="full")
    reference_digests = _digests(reference)
    stopped = Engine(out / "ws_stopped")
    stopped.import_dataset(data_dir / "manifest.json")
    stop_report = stopped.build(mode="full", stop_after="voxelize:view_01")
    assert stop_report["status"] == "stopped", stop_report
    recovery = stopped.recover()
    resume_report = stopped.build(mode="resume")
    assert resume_report["status"] == "completed", resume_report
    resumed_digests = _digests(stopped)
    stop_resume_matches = resumed_digests == reference_digests
    incremental = Engine(out / "ws_incremental")
    incremental.import_dataset(data_dir / "manifest.json")
    incremental.build(mode="full")
    incremental.replace_view(replaced_view, alt_image)
    report_replace = incremental.build(mode="incremental")
    check_replace = incremental.verify_consistency()
    incremental.update_config({"voxel_resolution": 0.4})
    report_resolution = incremental.build(mode="incremental")
    check_resolution = incremental.verify_consistency()
    incremental.update_config({"sparsity_threshold": 0.05})
    report_threshold = incremental.build(mode="incremental")
    check_threshold = incremental.verify_consistency()
    export = incremental.export_summary()
    final_digests = _digests(incremental)
    demo_digest = hash_json(
        {
            "reference": reference_digests,
            "resumed": resumed_digests,
            "incremental_final": final_digests,
        }
    )
    summary = {
        "seed": seed,
        "views": view_ids,
        "replaced_view": replaced_view,
        "reference_digests": reference_digests,
        "stop_resume": {
            "stopped_at": stop_report["stopped_at"],
            "recovery": recovery,
            "matches_reference": stop_resume_matches,
        },
        "incremental": {
            "replace_view": {
                "rebuilt": report_replace["rebuilt"],
                "reused": report_replace["reused"],
                "verify_match": check_replace["match"],
            },
            "voxel_resolution_0.4": {
                "rebuilt": report_resolution["rebuilt"],
                "reused": report_resolution["reused"],
                "verify_match": check_resolution["match"],
            },
            "sparsity_threshold_0.05": {
                "rebuilt": report_threshold["rebuilt"],
                "reused": report_threshold["reused"],
                "verify_match": check_threshold["match"],
            },
        },
        "final_digests": final_digests,
        "export": export,
        "demo_digest": demo_digest,
    }
    (out / "demo_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True, ensure_ascii=False),
        encoding="utf-8",
    )
    return summary


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dir", required=True)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    args = parser.parse_args(argv)
    summary = run_demo(args.dir, seed=args.seed)
    print(json.dumps(summary, indent=2, sort_keys=True, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
