#!/usr/bin/env python3
"""End-to-end local GEMM/BMM demonstration.

Run from the repository root:

    python examples/full_demo.py

The script uses only the generated local corpus, NumPy, and the standard
library.  It prints measurement timings but writes a deterministic semantic
summary whose digest is stable for the same inputs, plans, and NumPy baseline.
"""

from __future__ import annotations

import os

os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"

import argparse
import json
import shutil
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from gemm_lab import OperatorWorkspace, bmm, gemm  # noqa: E402
from gemm_lab.errors import GemmError  # noqa: E402
from gemm_lab.shapes import shape_fingerprint  # noqa: E402
from gemm_lab.storage import digest_value  # noqa: E402


STRATEGY_BY_SAMPLE = {
    "small_64x64x64": "pairwise",
    "medium_512x256x384": "pairwise",
    "large_1024x1024x1024": "pairwise",
    "nonsquare_128x96x80": "compensated",
    "trans_a_96x128x64": "sequential",
    "trans_b_80x64x96": "pairwise",
    "trans_ab_72x88x64": "compensated",
    "batch_4x32x24x40": "compensated",
    "batch_trans_3x17x29x23": "pairwise",
    "anomaly_nan_inf_48x40x36": "sequential",
    "batch_anomaly_2x12x10x8": "sequential",
}


def _load_manifest(data_dir: Path) -> dict[str, Any]:
    return json.loads((data_dir / "manifest.json").read_text(encoding="utf-8"))


def _load_sample(data_dir: Path, record: dict[str, Any]) -> tuple[np.ndarray, np.ndarray]:
    return (
        np.load(data_dir / record["a_file"], allow_pickle=False),
        np.load(data_dir / record["b_file"], allow_pickle=False),
    )


def _capture_error(callable_object) -> dict[str, Any]:
    try:
        callable_object()
    except GemmError as exc:
        return exc.to_dict()
    return {"ok": True, "code": "OK", "message": "no error"}


def _prepare_workspace(path: Path) -> OperatorWorkspace:
    path.mkdir(parents=True, exist_ok=True)
    for child in ("config_store", "cache", "locks"):
        target = path / child
        if target.exists():
            shutil.rmtree(target)
    anchor = path / ".config_store.anchor.json"
    anchor.unlink(missing_ok=True)
    return OperatorWorkspace(path)


def _record_fingerprint(record: dict[str, Any]) -> str:
    return shape_fingerprint(
        batch=int(record["batch"]),
        m=int(record["m"]),
        n=int(record["n"]),
        k=int(record["k"]),
        trans_a=record["trans_a"],
        trans_b=record["trans_b"],
        dtype=record.get("dtype", "float64"),
        is_batched=record["mode"] == "bmm",
    )


def _sample_summary(record: dict[str, Any], result) -> dict[str, Any]:
    error = result.error_report
    output_nonfinite = result.nonfinite["output"]
    return {
        "name": record["name"],
        "mode": record["mode"],
        "logical_shape": [record["m"], record["n"], record["k"]],
        "batch": record["batch"],
        "transpose_flags": f"{record['trans_a']}/{record['trans_b']}",
        "strategy": result.kernel.sum_strategy,
        "accumulation_mode": result.kernel.accumulation_mode,
        "plan": result.plan_selection.plan.short,
        "plan_signature": result.plan_selection.plan.signature,
        "workload_class": result.plan_selection.workload_class,
        "selection_score": round(result.plan_selection.score, 8),
        "plan_reason": result.plan_selection.reason,
        "cache_workspace_bytes": result.plan_selection.metrics["cache_workspace_bytes"],
        "microkernel_calls": result.plan_selection.metrics["microkernel_calls"],
        "max_abs_error_vs_high_precision": error["max_abs_error_vs_high_precision"],
        "max_rel_error_vs_high_precision": error["max_rel_error_vs_high_precision"],
        "max_abs_error_vs_pow2": error["max_abs_error_vs_pow2"],
        "theoretical_abs_bound_max": error["theoretical_abs_bound_max"],
        "theoretical_bound_violations": error["theoretical_bound_violations"],
        "verification_passed": error["passed"],
        "nonfinite_output": {
            "nan": output_nonfinite["nan_count"],
            "pos_inf": output_nonfinite["pos_inf_count"],
            "neg_inf": output_nonfinite["neg_inf_count"],
        },
        "output_digest": result.summary()["output_digest"],
    }


def _strategy_comparison(data_dir: Path, small_record: dict[str, Any]) -> dict[str, Any]:
    a, b = _load_sample(data_dir, small_record)
    plan = gemm(a, b, sum_strategy="pairwise").plan_selection.plan
    runs = {}
    outputs = {}
    for strategy in ("sequential", "pairwise", "compensated"):
        result = gemm(
            a,
            b,
            plan=plan,
            sum_strategy=strategy,
            accumulation_mode="scalar",
            verify=True,
        )
        outputs[strategy] = result.output
        runs[strategy] = {
            "max_abs_error_vs_high_precision": result.error_report[
                "max_abs_error_vs_high_precision"
            ],
            "max_abs_error_vs_pow2": result.error_report["max_abs_error_vs_pow2"],
            "theoretical_abs_bound_max": result.error_report["theoretical_abs_bound_max"],
            "output_digest": result.summary()["output_digest"],
        }
    return {
        "plan": plan.short,
        "runs": runs,
        "pairwise_equals_sequential": bool(
            np.array_equal(outputs["pairwise"], outputs["sequential"])
        ),
        "max_pairwise_vs_compensated": float(
            np.max(np.abs(outputs["pairwise"] - outputs["compensated"]))
        ),
    }


def _bmm_isolation(data_dir: Path, record: dict[str, Any], batched_output: np.ndarray) -> dict[str, Any]:
    a, b = _load_sample(data_dir, record)
    comparisons = []
    for index in range(a.shape[0]):
        single = gemm(
            a[index],
            b[index],
            trans_a=record["trans_a"],
            trans_b=record["trans_b"],
            sum_strategy=STRATEGY_BY_SAMPLE[record["name"]],
        )
        comparisons.append(bool(np.array_equal(single.output, batched_output[index])))
    return {
        "sample": record["name"],
        "batches": int(a.shape[0]),
        "batch_equals_standalone_gemm": comparisons,
        "all_exact": all(comparisons),
    }


def _cache_consistency_demo(
    workspace: OperatorWorkspace,
    data_dir: Path,
    record: dict[str, Any],
) -> dict[str, Any]:
    fingerprint = _record_fingerprint(record)
    current = workspace.config_store.verify_state().seq
    workspace.config_store.publish(
        fingerprint=fingerprint,
        plan=gemm(
            *_load_sample(data_dir, record),
            sum_strategy="pairwise",
            verify=False,
        ).plan_selection.plan,
        sum_strategy="pairwise",
        cache_budget={"l1_bytes": 32 * 1024, "l2_bytes": 1024 * 1024},
        selection_source="demo",
        metadata={"demo": "cache-version"},
        expected_seq=current,
    )
    a, b = _load_sample(data_dir, record)
    first = gemm(
        a,
        b,
        trans_a=record["trans_a"],
        trans_b=record["trans_b"],
        sum_strategy="pairwise",
        workspace=workspace,
        use_cache=True,
        publish_config=False,
    )
    second = gemm(
        a,
        b,
        trans_a=record["trans_a"],
        trans_b=record["trans_b"],
        sum_strategy="pairwise",
        workspace=workspace,
        use_cache=True,
        publish_config=False,
    )
    return {
        "config_seq": workspace.config_store.verify_state().seq,
        "stored_plan_adopted": first.config["adopted_stored_plan"],
        "first_lookup_status": first.cache.get("lookup_status"),
        "first_lookup_reason": first.cache.get("lookup_reason"),
        "first_rebuilt": first.cache.get("rebuild"),
        "second_lookup_status": second.cache.get("status"),
        "second_source": second.execution["source"],
        "same_output_after_rebuild": bool(np.array_equal(first.output, second.output)),
    }


def _config_integrity_demo(workspace: OperatorWorkspace, snapshot_root: Path) -> dict[str, Any]:
    store = workspace.config_store
    normal_seq = store.verify_state().seq
    plan = store.load_latest().plan
    fingerprint = store.load_latest().fingerprint

    def publish_next() -> int:
        record = store.publish(
            fingerprint=fingerprint,
            plan=plan,
            sum_strategy="pairwise",
            cache_budget={"l1_bytes": 32 * 1024, "l2_bytes": 1024 * 1024},
            selection_source="demo",
            metadata={"demo": "config-integrity"},
        )
        return record.seq

    snapshot_v12 = snapshot_root / "seq_current"
    if snapshot_v12.exists():
        shutil.rmtree(snapshot_v12)
    shutil.copytree(store.root, snapshot_v12)
    seq_13 = publish_next()
    snapshot_v13 = snapshot_root / "seq_latest"
    if snapshot_v13.exists():
        shutil.rmtree(snapshot_v13)
    shutil.copytree(store.root, snapshot_v13)

    shutil.rmtree(store.root)
    shutil.copytree(snapshot_v12, store.root)
    rollback = _capture_error(store.verify_state)
    shutil.rmtree(store.root)
    shutil.copytree(snapshot_v13, store.root)
    restored = store.verify_state()

    head = json.loads((store.root / "HEAD.json").read_text(encoding="utf-8"))
    latest_record_path = store.records_dir / head["record_file"]
    original_bytes = latest_record_path.read_bytes()
    tampered = json.loads(original_bytes)
    tampered["payload"]["plan"]["kc"] = int(tampered["payload"]["plan"]["kc"]) + 1
    latest_record_path.write_text(json.dumps(tampered), encoding="utf-8")
    tamper = _capture_error(store.verify_state)
    latest_record_path.write_bytes(original_bytes)
    final_state = store.verify_state()
    stale_publish = _capture_error(
        lambda: store.publish(
            fingerprint=fingerprint,
            plan=plan,
            sum_strategy="pairwise",
            cache_budget={"l1_bytes": 32 * 1024, "l2_bytes": 1024 * 1024},
            selection_source="demo",
            metadata={"demo": "stale"},
            expected_seq=1,
        )
    )
    return {
        "normal_sequence_before": normal_seq,
        "normal_sequence_after": seq_13,
        "rollback_code": rollback["code"],
        "rollback_reason": rollback.get("details", {}).get("reason"),
        "restored_status": restored.status,
        "tamper_code": tamper["code"],
        "tamper_reason": tamper.get("details", {}).get("reason"),
        "final_status_after_restore": final_state.status,
        "final_sequence": final_state.seq,
        "stale_publish_code": stale_publish["code"],
        "stale_publish_reason": stale_publish.get("details", {}).get("reason"),
    }


def _memory_budget_demo(data_dir: Path, record: dict[str, Any]) -> dict[str, Any]:
    a, b = _load_sample(data_dir, record)
    full = bmm(
        a,
        b,
        trans_a=record["trans_a"],
        trans_b=record["trans_b"],
        sum_strategy="compensated",
        verify=True,
    )
    base = (
        full.memory.output_bytes
        + full.memory.cache_workspace_bytes
        + full.memory.register_workspace_bytes
        + full.memory.fixed_overhead_bytes
    )
    tight_budget = base + 2 * full.memory.per_batch_operand_bytes
    sliced = bmm(
        a,
        b,
        trans_a=record["trans_a"],
        trans_b=record["trans_b"],
        sum_strategy="compensated",
        verify=True,
        memory_budget_bytes=tight_budget,
    )
    failure = _capture_error(
        lambda: bmm(
            a,
            b,
            trans_a=record["trans_a"],
            trans_b=record["trans_b"],
            sum_strategy="compensated",
            memory_budget_bytes=1024,
        )
    )
    recovered = bmm(
        a,
        b,
        trans_a=record["trans_a"],
        trans_b=record["trans_b"],
        sum_strategy="compensated",
        verify=True,
        memory_budget_bytes=tight_budget,
    )
    return {
        "sample": record["name"],
        "budget_bytes": tight_budget,
        "unsliced_full_peak_bytes": full.memory.full_batch_peak_bytes,
        "slice_size": sliced.memory.slice_size,
        "sliced": sliced.memory.sliced,
        "shards": [shard.to_dict() for shard in sliced.memory.shards],
        "slice_equals_full_exact": bool(np.array_equal(full.output, sliced.output)),
        "budget_failure_code": failure["code"],
        "budget_failure_breakdown": failure.get("details", {}).get("breakdown"),
        "recovery_exact": bool(np.array_equal(full.output, recovered.output)),
    }


def run_demo(data_dir: Path, workspace_root: Path, output: Path) -> dict[str, Any]:
    started = time.perf_counter()
    manifest = _load_manifest(data_dir)
    workspace = _prepare_workspace(workspace_root)
    samples = manifest["samples"]
    results = {}
    outputs = {}
    timings = {}
    sample_summaries = []
    for record in samples:
        a, b = _load_sample(data_dir, record)
        strategy = STRATEGY_BY_SAMPLE[record["name"]]
        kwargs = {
            "trans_a": record["trans_a"],
            "trans_b": record["trans_b"],
            "sum_strategy": strategy,
            "workspace": workspace,
            "use_cache": True,
            "publish_config": True,
            "selection_metadata": {"demo_sample": record["name"]},
        }
        result = bmm(a, b, **kwargs) if record["mode"] == "bmm" else gemm(a, b, **kwargs)
        results[record["name"]] = result
        outputs[record["name"]] = result.output
        timings[record["name"]] = round(float(result.execution["elapsed_seconds"]), 6)
        summary = _sample_summary(record, result)
        sample_summaries.append(summary)
        print(
            f"{record['name']:32s} plan={summary['plan']:22s} "
            f"strategy={summary['strategy']:12s} "
            f"max_abs_hp={summary['max_abs_error_vs_high_precision']:.3e} "
            f"time={timings[record['name']]:.4f}s"
        )

    small_record = next(record for record in samples if record["name"] == "small_64x64x64")
    batch_record = next(record for record in samples if record["name"] == "batch_4x32x24x40")
    strategy_comparison = _strategy_comparison(data_dir, small_record)
    bmm_isolation = _bmm_isolation(data_dir, batch_record, outputs[batch_record["name"]])
    cache_demo = _cache_consistency_demo(workspace, data_dir, small_record)
    integrity = _config_integrity_demo(workspace, workspace_root / "snapshots")
    memory_demo = _memory_budget_demo(data_dir, batch_record)

    scale_decisions = {}
    for name in ("small_64x64x64", "medium_512x256x384", "large_1024x1024x1024"):
        summary = next(item for item in sample_summaries if item["name"] == name)
        scale_decisions[name] = {
            "workload_class": summary["workload_class"],
            "plan": summary["plan"],
            "cache_workspace_bytes": summary["cache_workspace_bytes"],
            "microkernel_calls": summary["microkernel_calls"],
            "reason": summary["plan_reason"],
        }

    semantic = {
        "schema_version": 1,
        "base_seed": manifest["base_seed"],
        "numpy_version": manifest["numpy_version"],
        "measurement_method": (
            "single process, CPU, OPENBLAS_NUM_THREADS=1, deterministic local .npy inputs, "
            "time.perf_counter around the full API call; elapsed values are excluded from the digest"
        ),
        "samples": sample_summaries,
        "scale_decisions": scale_decisions,
        "strategy_comparison": strategy_comparison,
        "bmm_isolation": bmm_isolation,
        "cache_consistency": cache_demo,
        "config_integrity": integrity,
        "memory_budget": memory_demo,
        "fixed_codes": {
            "numeric_mismatch": "E_NUMERIC_MISMATCH",
            "invalid_shape": "E_INVALID_SHAPE",
            "batch_mismatch": "E_BATCH_MISMATCH",
            "stale_config": integrity["rollback_code"],
            "stale_publish": integrity["stale_publish_code"],
            "memory_budget": memory_demo["budget_failure_code"],
        },
    }
    semantic["deterministic_digest"] = digest_value(semantic)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(semantic, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    measurements = {
        "wall_seconds": round(time.perf_counter() - started, 6),
        "sample_seconds": {
            name: elapsed for name, elapsed in sorted(timings.items())
        },
    }
    (output.parent / "demo_measurements.json").write_text(
        json.dumps(measurements, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return semantic


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=Path, default=ROOT / "data")
    parser.add_argument("--workspace", type=Path, default=ROOT / "artifacts" / "demo_workspace")
    parser.add_argument("--output", type=Path, default=ROOT / "artifacts" / "demo_summary.json")
    args = parser.parse_args()
    summary = run_demo(args.data, args.workspace, args.output)
    print(
        json.dumps(
            {
                "ok": True,
                "output": str(args.output),
                "deterministic_digest": summary["deterministic_digest"],
                "samples": len(summary["samples"]),
                "strategy_comparison": summary["strategy_comparison"],
                "bmm_all_exact": summary["bmm_isolation"]["all_exact"],
                "cache": summary["cache_consistency"],
                "config": summary["config_integrity"],
                "memory": {
                    key: summary["memory_budget"][key]
                    for key in (
                        "slice_size",
                        "sliced",
                        "slice_equals_full_exact",
                        "budget_failure_code",
                        "recovery_exact",
                    )
                },
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

