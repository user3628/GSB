#!/usr/bin/env python3
"""Deterministic end-to-end demo: attention, resume, and prefix correction."""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path
from typing import Any

import numpy as np


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from flashblock.config import AttentionConfig  # noqa: E402
from flashblock.core import arrays_byte_equal, full_attention, max_abs_deviation  # noqa: E402
from flashblock.digest import sha256_json  # noqa: E402
from flashblock.engine import AttentionEngine  # noqa: E402
from flashblock.reference import reference_attention  # noqa: E402
from flashblock.samples import generate_samples  # noqa: E402


def _load_npz(path: Path) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    with np.load(path, allow_pickle=False) as payload:
        return payload["q"], payload["k"], payload["v"]


def _max_reference_deviation(result: Any, reference: Any) -> dict[str, float]:
    return {
        "output": max_abs_deviation(result.output, reference.output),
        "row_max": max_abs_deviation(result.row_max, reference.row_max),
        "row_norm": max_abs_deviation(result.row_norm, reference.row_norm),
    }


def _within_tolerance(result: Any, reference: Any, config: AttentionConfig) -> bool:
    return bool(
        np.allclose(result.output, reference.output, atol=config.atol, rtol=config.rtol)
        and np.allclose(result.row_max, reference.row_max, atol=config.atol, rtol=config.rtol)
        and np.allclose(result.row_norm, reference.row_norm, atol=config.atol, rtol=config.rtol)
    )


def run_demo(root: str | Path = ROOT, *, seed: int = 20260301, clean: bool = True) -> dict[str, Any]:
    root = Path(root).resolve()
    config_path = root / "configs" / "operator.yaml"
    manifest_path = root / "data" / "manifest.json"
    if not config_path.exists() or not manifest_path.exists():
        generate_samples(root, seed)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    config = AttentionConfig.from_yaml(config_path)
    artifacts = root / "artifacts"
    runtime = artifacts / "runtime"
    resume_runtime = artifacts / "resume_runtime"
    if clean:
        shutil.rmtree(runtime, ignore_errors=True)
        shutil.rmtree(resume_runtime, ignore_errors=True)
    runtime.mkdir(parents=True, exist_ok=True)
    resume_runtime.mkdir(parents=True, exist_ok=True)

    shard_results: dict[str, Any] = {}
    engines: dict[str, AttentionEngine] = {}
    for shard in manifest["shards"]:
        shard_id = shard["shard_id"]
        input_path = root / shard["path"]
        q, k, v = _load_npz(input_path)
        engine = AttentionEngine(runtime, shard_id, q, k, v, config)
        result = engine.run()
        if not result.complete:
            raise RuntimeError(f"full run unexpectedly paused for {shard_id}")
        reference = reference_attention(q, k, v, config, shard_id=shard_id)
        fresh = full_attention(q, k, v, config, shard_id=shard_id)
        reference_ok = _within_tolerance(result, reference, config)
        fresh_ok = bool(
            arrays_byte_equal(result.output, fresh.output)
            and arrays_byte_equal(result.row_max, fresh.row_max)
            and arrays_byte_equal(result.row_norm, fresh.row_norm)
        )
        cached = AttentionEngine(runtime, shard_id, q, k, v, config).run()
        cached_ok = bool(
            arrays_byte_equal(result.output, cached.output)
            and arrays_byte_equal(result.row_max, cached.row_max)
            and arrays_byte_equal(result.row_norm, cached.row_norm)
        )
        shard_results[shard_id] = {
            "shape": list(q.shape),
            "tags": shard["tags"],
            "result_sha256": result.digest(),
            "reference_within_tolerance": reference_ok,
            "reference_max_abs_deviation": _max_reference_deviation(result, reference),
            "fresh_block_byte_identical": fresh_ok,
            "cached_checkpoint_byte_identical": cached_ok,
            "state": {
                "complete": engine.query_state()["complete"],
                "committed_row_blocks": engine.query_state()["committed_row_blocks"],
                "available_sequence": engine.query_state()["available_sequence"],
            },
            "budget": {
                "budget_bytes": engine.budget_report()["budget_bytes"],
                "estimated_peak_bytes": engine.budget_report()["estimated_peak_bytes"],
                "split_count": engine.budget_report()["split_count"],
            },
        }
        if not (reference_ok and fresh_ok and cached_ok):
            raise RuntimeError(f"validation failed for {shard_id}")
        engines[shard_id] = engine

    resume_shard = manifest["shards"][0]
    resume_id = resume_shard["shard_id"]
    resume_q, resume_k, resume_v = _load_npz(root / resume_shard["path"])
    paused = AttentionEngine(resume_runtime, resume_id, resume_q, resume_k, resume_v, config).run(
        max_new_row_blocks=1
    )
    if paused.status != "paused":
        raise RuntimeError("graceful-stop demo did not pause")
    resumed = AttentionEngine(resume_runtime, resume_id, resume_q, resume_k, resume_v, config).run()
    resume_fresh = full_attention(resume_q, resume_k, resume_v, config, shard_id=resume_id)
    resume_ok = bool(
        arrays_byte_equal(resumed.output, resume_fresh.output)
        and arrays_byte_equal(resumed.row_max, resume_fresh.row_max)
        and arrays_byte_equal(resumed.row_norm, resume_fresh.row_norm)
    )
    if not resume_ok:
        raise RuntimeError("resume result is not byte-identical to full block attention")
    resume_summary = {
        "shard_id": resume_id,
        "paused_after_row_blocks": paused.committed_row_blocks,
        "paused_available_sequence": paused.available_sequence,
        "resumed_reused_row_blocks": resumed.reused_row_blocks,
        "resumed_recomputed_row_blocks": resumed.recomputed_row_blocks,
        "byte_identical_to_full": resume_ok,
        "result_sha256": resumed.digest(),
    }

    update_shard = manifest["shards"][2]
    update_id = update_shard["shard_id"]
    q, old_k, old_v = _load_npz(root / update_shard["path"])
    key_start, key_end = 80, 112
    rng = np.random.default_rng(seed + 9001)
    update_shape = (q.shape[0], q.shape[1], key_end - key_start, q.shape[3])
    k_slice = rng.normal(0.0, 0.6, size=update_shape)
    v_slice = rng.normal(0.0, 0.6, size=update_shape)
    updated_k = old_k.copy()
    updated_v = old_v.copy()
    updated_k[:, :, key_start:key_end, :] = k_slice
    updated_v[:, :, key_start:key_end, :] = v_slice
    update_engine = AttentionEngine(runtime, update_id, q, old_k, old_v, config)
    incremental = update_engine.apply_prefix_update(
        key_start=key_start,
        key_end=key_end,
        k_values=k_slice,
        v_values=v_slice,
        verify=True,
    )
    incremental_fresh = full_attention(q, updated_k, updated_v, config, shard_id=update_id)
    incremental_reference = reference_attention(q, updated_k, updated_v, config, shard_id=update_id)
    incremental_ok = bool(
        arrays_byte_equal(incremental.output, incremental_fresh.output)
        and arrays_byte_equal(incremental.row_max, incremental_fresh.row_max)
        and arrays_byte_equal(incremental.row_norm, incremental_fresh.row_norm)
    )
    incremental_reference_ok = _within_tolerance(incremental, incremental_reference, config)
    if not (incremental_ok and incremental_reference_ok):
        raise RuntimeError("incremental update validation failed")
    invalidation = update_engine.query_invalidations()
    updated_npz = artifacts / "updated_odd_dim_causal.npz"
    np.savez_compressed(
        updated_npz,
        q=q,
        k=updated_k,
        v=updated_v,
        output=incremental.output,
        row_max=incremental.row_max,
        row_norm=incremental.row_norm,
    )
    reloaded_engine = AttentionEngine.from_npz(runtime, update_id, updated_npz, config)
    reloaded = reloaded_engine.run()
    reloaded_ok = bool(
        arrays_byte_equal(reloaded.output, incremental.output)
        and arrays_byte_equal(reloaded.row_max, incremental.row_max)
        and arrays_byte_equal(reloaded.row_norm, incremental.row_norm)
    )
    if not reloaded_ok:
        raise RuntimeError("incremental state could not be recovered from disk")
    incremental_summary = {
        "shard_id": update_id,
        "changed_key_interval": [key_start, key_end],
        "verified": incremental.verified,
        "byte_identical_to_full": incremental_ok,
        "reference_within_tolerance": incremental_reference_ok,
        "reference_max_abs_deviation": _max_reference_deviation(incremental, incremental_reference),
        "reused_row_blocks": incremental.reused_row_blocks,
        "recomputed_row_blocks": incremental.recomputed_row_blocks,
        "earliest_affected_row_block": invalidation["earliest_affected_row_block"],
        "earliest_affected_col_block": invalidation["earliest_affected_col_block"],
        "reused_col_blocks_by_row": invalidation["reused_col_blocks_by_row"],
        "recomputed_col_blocks_by_row": invalidation["recomputed_col_blocks_by_row"],
        "result_sha256": incremental.digest(),
        "reload_byte_identical": reloaded_ok,
        "updated_npz": str(updated_npz.relative_to(root)).replace("\\", "/"),
    }

    summary_core = {
        "format": "flashblock/demo-summary-v1",
        "seed": seed,
        "operator_config": str(config_path.relative_to(root)).replace("\\", "/"),
        "shards": shard_results,
        "resume": resume_summary,
        "incremental_update": incremental_summary,
        "validation": {
            "all_reference_within_tolerance": all(
                item["reference_within_tolerance"] for item in shard_results.values()
            ) and incremental_reference_ok,
            "all_fresh_block_byte_identical": all(
                item["fresh_block_byte_identical"] for item in shard_results.values()
            ),
            "all_cached_checkpoint_byte_identical": all(
                item["cached_checkpoint_byte_identical"] for item in shard_results.values()
            ),
            "resume_byte_identical": resume_ok,
            "incremental_byte_identical": incremental_ok,
            "incremental_reload_byte_identical": reloaded_ok,
        },
    }
    summary = dict(summary_core)
    summary["aggregate_sha256"] = sha256_json(summary_core)
    summary_path = artifacts / "demo_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print("FlashBlock deterministic demo")
    print(f"  aggregate_sha256: {summary['aggregate_sha256']}")
    for shard_id, item in shard_results.items():
        print(
            f"  {shard_id}: shape={item['shape']} result={item['result_sha256'][:16]}... "
            f"reference_ok={item['reference_within_tolerance']} fresh_bytes={item['fresh_block_byte_identical']}"
        )
    print(
        f"  resume: paused={resume_summary['paused_after_row_blocks']} rowblocks, "
        f"reused={resume_summary['resumed_reused_row_blocks']}, bytes_ok={resume_ok}"
    )
    print(
        f"  prefix update: reused_rows={incremental.reused_row_blocks}, "
        f"recomputed_rows={incremental.recomputed_row_blocks}, bytes_ok={incremental_ok}"
    )
    print(f"  summary: {summary_path}")
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=str(ROOT))
    parser.add_argument("--seed", type=int, default=20260301)
    parser.add_argument("--no-clean", action="store_false", dest="clean", default=True)
    args = parser.parse_args()
    run_demo(args.root, seed=args.seed, clean=args.clean)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

