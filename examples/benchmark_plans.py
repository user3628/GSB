#!/usr/bin/env python3
"""Measure real local block-plan and summation tradeoffs.

Timings are machine observations and are intentionally stored in a separate
measurement artifact, not in the deterministic correctness summary.
"""

from __future__ import annotations

import os

os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"

import argparse
import json
import statistics
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from gemm_lab import BlockPlan, gemm, select_plan  # noqa: E402
from gemm_lab.storage import digest_value, json_safe  # noqa: E402


BENCHMARKS = {
    "small_64x64x64": [
        BlockPlan(16, 32, 64, 64, 64),
        BlockPlan(16, 32, 64, 32, 64),
        BlockPlan(4, 8, 32, 16, 32),
    ],
    "medium_512x256x384": [
        BlockPlan(16, 32, 256, 64, 256),
        BlockPlan(16, 32, 128, 64, 128),
        BlockPlan(16, 32, 64, 64, 64),
    ],
    "large_1024x1024x1024": [
        BlockPlan(16, 32, 256, 64, 256),
        BlockPlan(16, 32, 256, 32, 256),
        BlockPlan(16, 32, 128, 64, 128),
    ],
}


def _measure(a: np.ndarray, b: np.ndarray, plan: BlockPlan, repeats: int) -> dict[str, Any]:
    reference = gemm(a, b, plan=plan, sum_strategy="pairwise", verify=True)
    durations = []
    last = None
    for _ in range(repeats):
        started = time.perf_counter()
        last = gemm(a, b, plan=plan, sum_strategy="pairwise", verify=False)
        durations.append(time.perf_counter() - started)
    assert last is not None
    selection = select_plan(a.shape[0], b.shape[1], a.shape[1], plan=plan)
    flops = 2.0 * a.shape[0] * b.shape[1] * a.shape[1]
    median = statistics.median(durations)
    return {
        "plan": plan.short,
        "signature": plan.signature,
        "durations_seconds": [round(value, 6) for value in durations],
        "median_seconds": round(median, 6),
        "gflops": round(flops / median / 1e9, 4),
        "cache_workspace_bytes": selection.metrics["cache_workspace_bytes"],
        "microkernel_calls": selection.metrics["microkernel_calls"],
        "max_abs_difference_vs_verified_reference": float(
            np.max(np.abs(last.output - reference.output))
        ),
    }


def run(data_dir: Path, repeats: int, output: Path) -> dict[str, Any]:
    report: dict[str, Any] = {
        "measurement_method": (
            "CPU single process; BLAS thread env vars set to 1; warm verification run; "
            "timed calls use verify=False; median of repeats with time.perf_counter"
        ),
        "repeats": repeats,
        "scales": {},
    }
    for name, plans in BENCHMARKS.items():
        a = np.load(data_dir / "samples" / f"{name}_a.npy", allow_pickle=False)
        b = np.load(data_dir / "samples" / f"{name}_b.npy", allow_pickle=False)
        entries = [_measure(a, b, plan, repeats) for plan in plans]
        report["scales"][name] = entries

    small_a = np.load(data_dir / "samples" / "small_64x64x64_a.npy", allow_pickle=False)
    small_b = np.load(data_dir / "samples" / "small_64x64x64_b.npy", allow_pickle=False)
    small_plan = BlockPlan(16, 32, 64, 64, 64)
    strategy_entries = {}
    for strategy in ("sequential", "pairwise", "compensated"):
        durations = []
        last = None
        for _ in range(repeats):
            started = time.perf_counter()
            last = gemm(
                small_a,
                small_b,
                plan=small_plan,
                sum_strategy=strategy,
                accumulation_mode="scalar",
                verify=False,
            )
            durations.append(time.perf_counter() - started)
        assert last is not None
        strategy_entries[strategy] = {
            "durations_seconds": [round(value, 6) for value in durations],
            "median_seconds": round(statistics.median(durations), 6),
            "output_digest": last.summary()["output_digest"],
        }
    report["small_scalar_strategies"] = strategy_entries
    report["measurement_digest"] = digest_value(report)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(json_safe(report), indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=Path, default=ROOT / "data")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--output", type=Path, default=ROOT / "artifacts" / "plan_benchmark.json")
    args = parser.parse_args()
    report = run(args.data, args.repeats, args.output)
    print(
        json.dumps(
            {
                "ok": True,
                "output": str(args.output),
                "measurement_digest": report["measurement_digest"],
                "scales": {
                    name: [
                        {
                            "plan": entry["plan"],
                            "median_seconds": entry["median_seconds"],
                            "gflops": entry["gflops"],
                            "max_difference": entry["max_abs_difference_vs_verified_reference"],
                        }
                        for entry in entries
                    ]
                    for name, entries in report["scales"].items()
                },
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

