from __future__ import annotations

import contextlib
from pathlib import Path
from typing import Any

from .generator import DEFAULT_SEED, generate_demo
from .injection import append_injection, build_operation, clear_injections
from .pipeline import read_report, run_pipeline
from .store import CheckpointStore
from .util import WorkbenchError, atomic_write_json, ensure_dir


def run_demo(root: Path, *, seed: int = DEFAULT_SEED, stop_after: int = 5) -> dict[str, Any]:
    root = root.resolve()
    generated = generate_demo(root, seed=seed, clean=True)
    clear_injections(root)
    reset_outputs(root)

    baseline = run_pipeline(root, mode="run")
    if baseline.status != "completed" or not baseline.report_sha256:
        raise WorkbenchError("E_DEMO_INVARIANT", "baseline run did not complete", baseline.as_dict())
    baseline_sha = baseline.report_sha256

    reset_outputs(root)
    stopped = run_pipeline(root, mode="run", stop_after_checkpoints=stop_after)
    if stopped.status != "stopped" or stopped.stop_reason != "stop_after_checkpoints":
        raise WorkbenchError("E_DEMO_INVARIANT", "configured stop did not interrupt the pipeline", stopped.as_dict())
    resumed = run_pipeline(root, mode="resume")
    if resumed.status != "completed" or resumed.report_sha256 != baseline_sha:
        raise WorkbenchError(
            "E_DEMO_INVARIANT",
            "resumed report differs from the from-scratch report",
            {
                "baseline_sha256": baseline_sha,
                "resumed_sha256": resumed.report_sha256,
                "stopped": stopped.as_dict(),
                "resumed": resumed.as_dict(),
            },
        )

    store = CheckpointStore(root)
    unaffected_before = store.get("score", "s01_dual")["artifact_digest"]
    injection = build_operation("score_length", "s08_multi", label="cat")
    append_injection(root, injection)
    injected = run_pipeline(root, mode="run")
    if injected.status != "completed":
        raise WorkbenchError("E_DEMO_INVARIANT", "injected run did not complete", injected.as_dict())
    injected_report = read_report(root)
    if injected_report is None:
        raise WorkbenchError("E_DEMO_INVARIANT", "injected run did not emit a report")
    score_length_failures = [
        failure
        for failure in injected_report["failures"]
        if failure.get("code") == "E_SCORE_LENGTH" and failure.get("sample_key") == "s08_multi"
    ]
    if not score_length_failures:
        raise WorkbenchError("E_DEMO_INVARIANT", "score_length injection was not located in the report")
    unaffected_after = store.get("score", "s01_dual")["artifact_digest"]
    if unaffected_before != unaffected_after:
        raise WorkbenchError("E_DEMO_INVARIANT", "injection changed an unaffected sample's score artifact")
    invalidations = store.invalidations(100)
    latest_invalidation = next(
        (entry for entry in reversed(invalidations) if entry.get("sample_key") == "s08_multi"),
        None,
    )
    if not latest_invalidation or latest_invalidation.get("earliest_stage") != "score":
        raise WorkbenchError("E_DEMO_INVARIANT", "injection did not invalidate the score stage")

    clear_injections(root)
    restored = run_pipeline(root, mode="run")
    if restored.status != "completed" or restored.report_sha256 != baseline_sha:
        raise WorkbenchError(
            "E_DEMO_INVARIANT",
            "clearing the injection did not restore the deterministic report",
            {
                "baseline_sha256": baseline_sha,
                "restored_sha256": restored.report_sha256,
            },
        )

    summary = {
        "schema_version": 1,
        "demo": "deterministic-multilabel-workbench",
        "seed": seed,
        "generated_samples": generated,
        "baseline": {
            "status": baseline.status,
            "published": baseline.published,
            "reused": baseline.reused,
            "report_sha256": baseline_sha,
            "failures": baseline.failures,
            "conflicts": baseline.conflicts,
            "metrics_micro": baseline.metrics["micro"],
            "metrics_macro": baseline.metrics["macro"],
            "cooccurrence_consistency": baseline.metrics["cooccurrence"]["consistency"],
        },
        "stop_resume": {
            "stop_after_checkpoints": stop_after,
            "stopped_status": stopped.status,
            "stopped_reason": stopped.stop_reason,
            "stopped_published": stopped.published,
            "resumed_status": resumed.status,
            "resumed_published": resumed.published,
            "resumed_reused": resumed.reused,
            "resumed_report_sha256": resumed.report_sha256,
            "byte_identical_to_baseline": resumed.report_sha256 == baseline_sha,
        },
        "injection": {
            "operation": injection,
            "error_code": score_length_failures[0]["code"],
            "failure_sample": score_length_failures[0]["sample_key"],
            "earliest_invalidated_stage": latest_invalidation["earliest_stage"],
            "invalidated_stages": latest_invalidation["invalidated_stages"],
            "unaffected_sample": "s01_dual",
            "unaffected_score_artifact_unchanged": unaffected_before == unaffected_after,
            "report_sha256_after_injection": injected.report_sha256,
            "restored_report_sha256": restored.report_sha256,
            "restored_byte_identical_to_baseline": restored.report_sha256 == baseline_sha,
        },
        "failure_codes": sorted({item["code"] for item in read_report(root)["failures"]}),
        "conflict_codes": sorted({item["code"] for item in read_report(root)["conflicts"]}),
    }
    atomic_write_json(ensure_dir(root / "reports") / "demo_summary.json", summary, pretty=True)
    return summary


def reset_outputs(root: Path) -> None:
    root = root.resolve()
    CheckpointStore(root).clear_all()
    for relative in (
        "checkpoints/control/run_state.json",
        "checkpoints/control/stop.request",
        "reports/report.json",
        "reports/report.sha256",
        "reports/failures.json",
        "reports/conflicts.json",
    ):
        with contextlib.suppress(FileNotFoundError):
            (root / relative).unlink()
    ensure_dir(root / "reports")


__all__ = ["reset_outputs", "run_demo"]
