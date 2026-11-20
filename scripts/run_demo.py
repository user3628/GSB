#!/usr/bin/env python3
"""Run the complete deterministic local acceptance demo."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil
import sys
from typing import Any

import numpy as np


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from spmm_ops.engine import RunOptions, spmm  # noqa: E402
from spmm_ops.samples import generate_samples, load_manifest  # noqa: E402
from spmm_ops.util import atomic_write_bytes, json_digest, json_safe  # noqa: E402
from spmm_ops.version_store import VersionStore  # noqa: E402


def _strategy_for(sample: dict[str, Any]) -> tuple[str, str]:
    density = float(sample["actual_density"])
    if density < 0.05:
        return "sequential", "ascending"
    if density < 0.20:
        return "compensated", "ascending"
    return "compensated", "block"


def _audit_compact(result: Any) -> dict[str, Any]:
    report = result.audit_report
    if report is None:
        return {"present": False}
    finite_blocks = [block for block in report["blocks"] if block["theoretical_abs_error_bound"] is not None]
    return {
        "present": True,
        "passed": report["passed"],
        "all_bounds_hold": all(block["bound_holds"] for block in report["blocks"]),
        "block_count": len(report["blocks"]),
        "sampled_position_count": report["sampling"]["actual_count"],
        "sampling_seed": report["sampling"]["seed"],
        "self_check_passed": report["self_check"]["passed"],
        "max_observed_abs_error": max(
            (block["observed_max_abs_error"] or 0.0 for block in report["blocks"]), default=0.0
        ),
        "max_theoretical_abs_error_bound": max(
            (block["theoretical_abs_error_bound"] for block in finite_blocks), default=0.0
        ),
        "nonfinite_source_count": report["nonfinite"]["source_count"],
        "bound_model": report["bound_model"],
    }


def _run_compact(
    result: Any,
    sample: dict[str, Any],
    *,
    strategy: str,
    order: str,
    memory_limit: int,
) -> dict[str, Any]:
    return {
        "sample": sample["name"],
        "kind": sample["kind"],
        "shape": sample["shape"],
        "dense_shape": sample["dense_shape"],
        "density": sample["actual_density"],
        "strategy": strategy,
        "order": order,
        "scheme": result.scheme.to_dict(),
        "cache_hit": result.cache["hit"],
        "cache_lookup_reason": result.cache["lookup_reason"],
        "cache_version_sequence": result.cache["cache_record_sequence"],
        "scheme_version_sequence": None if result.version is None else result.version["sequence"],
        "scheme_version_digest": None if result.version is None else result.version["digest"],
        "memory_limit_bytes": memory_limit,
        "estimated_peak_bytes": result.schedule.estimated_peak_bytes,
        "split_count": result.schedule.split_count,
        "logical_tile_count": result.schedule.logical_tile_count,
        "executed_tile_count": result.schedule.executed_tile_count,
        "executed_terms": result.kernel_diagnostics.get("executed_terms"),
        "output_digest": result.output_digest,
        "audit": _audit_compact(result),
    }


def _plan_change_demo(sample_dir: Path, store_dir: Path, sample: dict[str, Any]) -> dict[str, Any]:
    sparse = sample_dir / sample["sparse_file"]
    dense = sample_dir / sample["dense_file"]
    common = dict(
        sum_strategy="sequential",
        order="block",
        cache_budget_bytes=64 * 1024,
        memory_limit_bytes=4 * 1024 * 1024,
        version_store=store_dir,
        audit=True,
        sampling_count=16,
        self_check_blocks=2,
    )

    def run(plan: tuple[int, int]) -> Any:
        return spmm(sparse, dense, options=RunOptions(block_scheme=plan, **common))

    plan_a = (16, 24)
    plan_b = (24, 40)
    a_first = run(plan_a)
    a_hit = run(plan_a)
    b_first = run(plan_b)
    a_again = run(plan_a)
    return {
        "sample": sample["name"],
        "strategy": "sequential",
        "order": "block",
        "plan_a": list(plan_a),
        "plan_b": list(plan_b),
        "steps": [
            {
                "step": "A_initial",
                "cache_hit": a_first.cache["hit"],
                "cache_lookup_reason": a_first.cache["lookup_reason"],
                "scheme_version_sequence": a_first.version["sequence"],
                "cache_version_sequence": a_first.cache["cache_record_sequence"],
                "output_digest": a_first.output_digest,
            },
            {
                "step": "A_repeat",
                "cache_hit": a_hit.cache["hit"],
                "cache_lookup_reason": a_hit.cache["lookup_reason"],
                "scheme_version_sequence": a_hit.version["sequence"],
                "cache_version_sequence": a_hit.cache["cache_record_sequence"],
                "output_digest": a_hit.output_digest,
            },
            {
                "step": "B_initial",
                "cache_hit": b_first.cache["hit"],
                "cache_lookup_reason": b_first.cache["lookup_reason"],
                "scheme_version_sequence": b_first.version["sequence"],
                "cache_version_sequence": b_first.cache["cache_record_sequence"],
                "output_digest": b_first.output_digest,
            },
            {
                "step": "A_return",
                "cache_hit": a_again.cache["hit"],
                "cache_lookup_reason": a_again.cache["lookup_reason"],
                "scheme_version_sequence": a_again.version["sequence"],
                "cache_version_sequence": a_again.cache["cache_record_sequence"],
                "output_digest": a_again.output_digest,
            },
        ],
        "old_cache_reused_after_plan_return": a_again.cache["hit"],
        "a_output_stable_after_rebuild": a_first.output_digest == a_again.output_digest,
        "a_and_b_output_differ": a_first.output_digest != b_first.output_digest,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--samples", default=str(ROOT / "samples"))
    parser.add_argument("--runtime", default=str(ROOT / "artifacts" / "demo_runtime"))
    parser.add_argument("--summary", default=str(ROOT / "artifacts" / "demo_summary.json"))
    args = parser.parse_args()

    sample_dir = Path(args.samples)
    runtime_dir = Path(args.runtime)
    if runtime_dir.exists():
        shutil.rmtree(runtime_dir)
    runtime_dir.mkdir(parents=True, exist_ok=True)
    store_dir = runtime_dir / "version_store"

    manifest = generate_samples(sample_dir)
    loaded_manifest = load_manifest(sample_dir / "manifest.json")
    if manifest["manifest_digest"] != loaded_manifest["manifest_digest"]:
        raise RuntimeError("sample manifest failed immediate round-trip verification")

    runs: list[dict[str, Any]] = []
    for sample in manifest["samples"]:
        strategy, order = _strategy_for(sample)
        memory_limit = 48 * 1024
        result = spmm(
            sample_dir / sample["sparse_file"],
            sample_dir / sample["dense_file"],
            options=RunOptions(
                sum_strategy=strategy,
                order=order,
                duplicate_tolerance=1,
                cache_budget_bytes=64 * 1024,
                memory_limit_bytes=memory_limit,
                version_store=store_dir,
                audit=True,
                sampling_count=16,
                self_check_blocks=2,
            ),
        )
        runs.append(_run_compact(result, sample, strategy=strategy, order=order, memory_limit=memory_limit))

    plan_sample = next(sample for sample in manifest["samples"] if sample["name"] == "degenerate_struct")
    plan_change = _plan_change_demo(sample_dir, store_dir, plan_sample)
    store = VersionStore(store_dir)
    final_status = store.status()

    core: dict[str, Any] = {
        "schema": "spmm-ops-demo",
        "schema_version": 1,
        "manifest_digest": manifest["manifest_digest"],
        "sample_count": len(runs),
        "sample_runs": runs,
        "plan_change_demo": plan_change,
        "final_version": {
            "sequence": final_status["sequence"],
            "digest": final_status["digest"],
            "state_digest": final_status["state_digest"],
            "active_scheme": final_status["active_scheme"],
            "record_count": final_status["record_count"],
            "scheme_version_count": final_status["scheme_version_count"],
            "cache_version_count": final_status["cache_version_count"],
            "cache_file_count": final_status["cache_file_count"],
            "cache_bytes": final_status["cache_bytes"],
        },
    }
    summary: dict[str, Any] = dict(core)
    summary["summary_digest"] = json_digest(core)
    encoded = json.dumps(json_safe(summary), sort_keys=True, indent=2, ensure_ascii=False, allow_nan=False).encode("utf-8") + b"\n"
    atomic_write_bytes(args.summary, encoded)

    print(f"summary_digest={summary['summary_digest']}")
    print(f"manifest_digest={manifest['manifest_digest']}")
    print(f"samples={len(runs)} version_sequence={final_status['sequence']}")
    print("name                      shape       density strategy      order      split  peak_B  cache  audit  output_digest")
    for run in runs:
        print(
            f"{run['sample']:<25} {str(run['shape']):<10} {run['density']:<7.4f} "
            f"{run['strategy']:<13} {run['order']:<10} {run['split_count']:<6} "
            f"{run['estimated_peak_bytes']:<7} {str(run['cache_hit']):<6} "
            f"{str(run['audit']['passed']):<6} {run['output_digest'][:16]}"
        )
    print(
        "plan_change: "
        f"A_initial_hit={plan_change['steps'][0]['cache_hit']} "
        f"A_repeat_hit={plan_change['steps'][1]['cache_hit']} "
        f"B_initial_hit={plan_change['steps'][2]['cache_hit']} "
        f"A_return_hit={plan_change['steps'][3]['cache_hit']} "
        f"stable={plan_change['a_output_stable_after_rebuild']} "
        f"A_ne_B={plan_change['a_and_b_output_differ']}"
    )
    print(f"summary={args.summary}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

