"""Fixed, repeatable stop/resume demonstration."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from .canonical import atomic_write_json
from .engine import Engine
from .generator import DEMO_SEED, generate_demo_data


def run_demo(root: Path | str = ".") -> dict[str, Any]:
    base = Path(root).resolve()
    demo_root = base / "runtime" / "demo"
    shutil.rmtree(demo_root, ignore_errors=True)
    generate_demo_data(demo_root, seed=DEMO_SEED, overwrite=True)
    engine = Engine(
        demo_root,
        state_dir="state",
        views_dir="views",
        config_path="config/analysis.yaml",
        manifest_path="data/manifest.json",
    )

    stopped = engine.build(stop_after_shards=1)
    stopped_checkpoint = stopped["checkpoint"]
    resumed = engine.build()
    resumed_checkpoint = resumed["checkpoint"]
    verification = engine.verify_against_full()
    summary = engine.get_summary()
    final_checkpoints = engine.checkpoint_summary()

    report = {
        "schema_version": 1,
        "ok": True,
        "status": "ok",
        "seed": DEMO_SEED,
        "shard_count": summary["shard_count"],
        "sentence_count": summary["sentence_count"],
        "token_count": summary["token_count"],
        "stats": {
            "tree_count": summary["tree_count"],
            "error_count": summary["error_count"],
            "error_counts": summary["error_counts"],
            "constraint_violation_count": summary["constraint_violation_count"],
            "constraint_violation_counts": summary["constraint_violation_counts"],
            "low_confidence_step_count": summary["low_confidence_step_count"],
        },
        "digests": {
            "tree_digest": summary["tree_digest"],
            "diagnostic_digest": summary["diagnostic_digest"],
            "summary_digest": summary["summary_digest"],
        },
        "stop_resume": {
            "stopped_status": stopped["status"],
            "stopped_processed_shards": stopped["processed_shards"],
            "stopped_checkpoint_sequence": stopped_checkpoint["last_sequence"],
            "resume_status": resumed["status"],
            "resume_computed_stage_count": len(resumed["computed"]),
            "resume_reused_stage_count": len(resumed["reused"]),
            "final_checkpoint_sequence": resumed_checkpoint["last_sequence"],
            "final_record_count": final_checkpoints["record_count"],
            "checkpoint_ignored": final_checkpoints["ignored"],
        },
        "consistency": {
            "incremental_equals_full": verification["full_match"],
            "matched_artifacts": verification["artifacts"],
        },
    }
    atomic_write_json(demo_root / "report.json", report)
    return report
