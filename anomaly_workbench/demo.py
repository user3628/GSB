"""Deterministic end-to-end acceptance demonstration."""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Any

from .config import STAGES
from .data import DEMO_SEED, generate_demo, mutate_shard_copy
from .errors import WorkbenchError
from .pipeline import Pipeline
from .reference import generate_reference, verify_reference
from .util import atomic_write_json


def _reset_workspace(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    for relative in (
        "data",
        "checkpoints",
        "control",
        "exports",
        "reference",
        "state",
        "locks",
    ):
        path = root / relative
        if path.is_dir():
            shutil.rmtree(path)
    for name in ("config.json", "manifest.json", "demo-report.json"):
        path = root / name
        if path.exists():
            path.unlink()


def _digest_map(pipeline: Pipeline, tier: str) -> dict[str, str]:
    artifacts = pipeline.current_artifacts(tier)
    return {stage: artifacts[stage]["artifact_digest"] for stage in STAGES}


def run_demo(
    root: str | os.PathLike[str],
    *,
    seed: int = DEMO_SEED,
    force: bool = False,
    report_path: str | os.PathLike[str] | None = None,
) -> dict[str, Any]:
    root_path = Path(root)
    if force:
        _reset_workspace(root_path)
    elif (root_path / "manifest.json").exists():
        raise WorkbenchError(
            "E_DATA", "demo root already contains data; rerun with force=True", stage="demo"
        )
    generate_demo(root_path, seed=seed, force=force)
    pipeline = Pipeline(root_path)

    def stop_after_small_fit(stage: str, tier: str, record: dict[str, Any]) -> None:
        if (tier, stage) == ("small", "fit"):
            pipeline.request_stop()

    first_run = pipeline.run(tiers=["small", "medium"], stage_hook=stop_after_small_fit)
    if first_run["status"] != "stopped":
        raise WorkbenchError("E_INTERNAL", "demo stop request did not stop the pipeline", stage="demo")
    stop_snapshot = {
        "status": first_run["status"],
        "stopped_after": first_run["stopped_after"],
        "checkpoint_seq": first_run["seq"],
        "checkpoint_count": first_run["checkpoints"]["max_seq"],
    }
    resumed = pipeline.resume(tiers=["small", "medium"])
    if resumed["status"] != "complete":
        raise WorkbenchError("E_INTERNAL", "demo resume did not complete", stage="demo")
    stop_resume = {
        **stop_snapshot,
        "resumed_status": resumed["status"],
        "resumed_action_counts": {
            "reused": resumed["reused_stages"],
            "rebuilt": resumed["rebuilt_stages"],
            "republished": resumed["republished_stages"],
        },
        "checkpoint_count": resumed["checkpoints"]["max_seq"],
    }

    reference_result = generate_reference(root_path)
    reference_verification = verify_reference(root_path)
    full_equivalence = {
        tier: pipeline.verify_full_equivalence(tier) for tier in ("small", "medium")
    }
    baseline_digests = {
        tier: _digest_map(pipeline, tier) for tier in ("small", "medium")
    }

    replacement_file = root_path / "exports" / "replacement-small-batch-001.npz"
    mutation = mutate_shard_copy(root_path, "small", 1, replacement_file)
    replacement = pipeline.replace_shard("small", replacement_file)
    replacement_run = pipeline.run(tiers=["small"])
    replacement_verification = pipeline.verify_full_equivalence("small")
    replacement_actions = {
        action["stage"]: action["action"] for action in replacement_run["actions"]
    }

    config_change = pipeline.update_config(
        {"scoring": {"weights": {"covariance": 0.8, "forest": 0.2}}}
    )
    config_run = pipeline.run(tiers=["small", "medium"])
    config_verification = {
        tier: pipeline.verify_full_equivalence(tier) for tier in ("small", "medium")
    }
    config_actions: dict[str, dict[str, str]] = {"small": {}, "medium": {}}
    for action in config_run["actions"]:
        config_actions[action["tier"]][action["stage"]] = action["action"]

    invalid_threshold_error: dict[str, Any] | None = None
    try:
        pipeline.update_config({"scoring": {"threshold_quantile": 1.5}})
    except WorkbenchError as exc:
        invalid_threshold_error = exc.to_dict()

    exports = {
        tier: pipeline.export_tier(tier) for tier in ("small", "medium")
    }
    checkpoint_records = pipeline.store.status()["records"]
    sequences = [record["seq"] for record in checkpoint_records]
    sequence_contiguous = sequences == list(range(1, len(sequences) + 1))
    status = pipeline.status(include_records=False)
    final_digests = {tier: _digest_map(pipeline, tier) for tier in ("small", "medium")}

    report = {
        "schema_version": 1,
        "demo": "deterministic anomaly detection incremental workbench",
        "seed": int(seed),
        "stop_and_resume": stop_resume,
        "reference": {
            "generated": reference_result,
            "verification": reference_verification,
        },
        "full_equivalence_after_resume": {
            tier: {
                "byte_identical": result["byte_identical"],
                "stages": result["stages"],
            }
            for tier, result in full_equivalence.items()
        },
        "baseline_artifact_digests": baseline_digests,
        "incremental_shard_replacement": {
            "mutation": mutation,
            "replaced_batch": replacement["replaced_batch"],
            "invalidation": replacement["invalidation"],
            "actions": replacement_actions,
            "verification": replacement_verification,
        },
        "incremental_config_change": {
            "change": config_change,
            "actions": config_actions,
            "verification": config_verification,
        },
        "invalid_threshold": invalid_threshold_error,
        "checkpoints": {
            "sequence_contiguous": sequence_contiguous,
            "sequences": sequences,
            "max_seq": pipeline.store.status(include_records=False)["max_seq"],
            "ignored": pipeline.store.status(include_records=False)["ignored"],
        },
        "final_artifact_digests": final_digests,
        "exports": exports,
        "tier_observations": {
            tier: status["tiers"][tier]["data"] for tier in ("small", "medium")
        },
    }
    destination = Path(report_path) if report_path is not None else root_path / "demo-report.json"
    atomic_write_json(destination, report)
    return {
        "report_path": str(destination),
        "stop_and_resume": stop_resume,
        "reference_verified": reference_verification.get("verified"),
        "full_equivalence": {
            tier: result["byte_identical"] for tier, result in full_equivalence.items()
        },
        "shard_replacement": {
            "actions": replacement_actions,
            "byte_identical": replacement_verification["byte_identical"],
        },
        "config_change": {
            "earliest_stage": config_change["earliest_stage"],
            "actions": config_actions,
            "byte_identical": {
                tier: result["byte_identical"]
                for tier, result in config_verification.items()
            },
        },
        "invalid_threshold_error": (
            None if invalid_threshold_error is None else invalid_threshold_error["code"]
        ),
        "checkpoint_sequence_contiguous": sequence_contiguous,
    }
