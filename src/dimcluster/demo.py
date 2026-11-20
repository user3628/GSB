from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

from .config import load_config
from .envcheck import environment_check
from .errors import WorkbenchError
from .pipeline import Workbench
from .samples import generate_samples, verify_samples
from .util import atomic_write_bytes, atomic_write_json, canonical_json_bytes, sha256_bytes


def run_demo(root: Path, base_config: dict[str, Any] | None = None) -> dict[str, Any]:
    root = Path(root).resolve()
    config = copy.deepcopy(base_config) if base_config is not None else load_config()
    samples_dir = root / "samples"
    manifest_path = samples_dir / "manifest.json"
    generate_summary = generate_samples(samples_dir, seed=int(config["seed"]))
    sample_verification = verify_samples(manifest_path)

    demo_state = root / "state" / "demo"
    demo_output = root / "demo"
    demo_output.mkdir(parents=True, exist_ok=True)
    fixed_workbench = Workbench(
        root,
        copy.deepcopy(config),
        state_dir=demo_state,
        output_dir=demo_output,
        manifest_path=manifest_path,
    )
    fixed_workbench.state.reset()

    stop_run = fixed_workbench.run(
        mode="full",
        stop_after_tier="small",
        verify_full=False,
    )
    partial_status = fixed_workbench.state.status()
    partial_report_written = stop_run["report_written"]

    resumed_run = fixed_workbench.run(mode="auto", verify_full=True)
    fixed_report_bytes = fixed_workbench.report_bytes()
    fixed_report = json.loads(fixed_report_bytes.decode("utf-8"))
    atomic_write_bytes(demo_output / "fixed_report.json", fixed_report_bytes)

    incremental_config = copy.deepcopy(config)
    incremental_config["clustering"]["n_clusters"] = 4
    incremental_workbench = Workbench(
        root,
        incremental_config,
        state_dir=demo_state,
        output_dir=demo_output,
        manifest_path=manifest_path,
    )
    incremental_run = incremental_workbench.run(mode="auto", verify_full=True)
    incremental_report_bytes = incremental_workbench.report_bytes()
    atomic_write_bytes(
        demo_output / "incremental_report.json", incremental_report_bytes
    )

    invalid_config: dict[str, Any]
    try:
        load_config(overrides=["reduction.target_dimension=1"])
        invalid_config = {
            "ok": False,
            "error": {
                "code": "E_EXPECTED_INVALID_CONFIG_NOT_RAISED",
                "message": "invalid config injection did not raise",
            },
        }
    except WorkbenchError as exc:
        invalid_config = exc.to_dict()

    env = environment_check(root)
    # The ephemeral probe port is intentionally omitted from the fixed demo summary;
    # binding success remains recorded as a stable boolean.
    env["http"].pop("ephemeral_port", None)
    summary = {
        "schema_version": "demo-summary-v1",
        "ok": bool(
            stop_run["stopped"]
            and not partial_report_written
            and resumed_run["report_written"]
            and resumed_run.get("incremental_verification", {}).get("ok")
            and incremental_run.get("incremental_verification", {}).get("ok")
            and invalid_config.get("error", {}).get("code") == "E_INVALID_CONFIG"
            and sample_verification["ok"]
        ),
        "seed": config["seed"],
        "samples": {
            "manifest": str(manifest_path),
            "generation_summary": generate_summary,
            "verification": sample_verification,
        },
        "stop_resume": {
            "stop_run": {
                "stopped": stop_run["stopped"],
                "stopped_after_tier": stop_run["stopped_after_tier"],
                "completed_tiers": stop_run["completed_tiers"],
                "report_written": stop_run["report_written"],
                "actions": stop_run["actions"],
            },
            "partial_checkpoint_status": partial_status,
            "resume_run": {
                "stopped": resumed_run["stopped"],
                "completed_tiers": resumed_run["completed_tiers"],
                "report_written": resumed_run["report_written"],
                "reused_stage_count": resumed_run["reused_stage_count"],
                "recomputed_stage_count": resumed_run["recomputed_stage_count"],
                "report_sha256": resumed_run["report_sha256"],
                "incremental_verification": resumed_run.get(
                    "incremental_verification"
                ),
            },
        },
        "fixed_summary": {
            "report_sha256": fixed_report["report_sha256"],
            "report_bytes_sha256": sha256_bytes(fixed_report_bytes),
            "report_path": str(demo_output / "fixed_report.json"),
            "tier_algorithm_matrix": fixed_report["overall"]["algorithm_matrix"],
            "flagged_tiers": fixed_report["overall"]["flagged_tiers"],
        },
        "incremental_demo": {
            "config_change": {"clustering.n_clusters": 4},
            "earliest_recomputed_stage": incremental_run.get(
                "earliest_recomputed_stage"
            ),
            "reused_stage_count": incremental_run["reused_stage_count"],
            "recomputed_stage_count": incremental_run["recomputed_stage_count"],
            "report_sha256": incremental_run["report_sha256"],
            "verification": incremental_run.get("incremental_verification"),
            "report_path": str(demo_output / "incremental_report.json"),
        },
        "invalid_config_injection": invalid_config,
        "environment": env,
        "state_dir": str(demo_state),
        "output_dir": str(demo_output),
    }
    summary["demo_sha256"] = sha256_bytes(canonical_json_bytes(summary))
    atomic_write_json(demo_output / "demo_summary.json", summary, pretty=True)
    return summary
