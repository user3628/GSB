#!/usr/bin/env python3
"""Deterministic end-to-end demonstration of the xfpipe workbench.

Scenarios (all local, no external services):
  1. generate the seeded sample CSV (640 rows, 4 categorical columns, a
     column with missing values, one binary label) and the JSON config
  2. full four-stage run -> fixed pipeline digest
  3. interactive queries with cache hits and latency tracing
  4. injected out-of-range target dimension -> E_DIM_INVALID
  5. injected illegal combination order    -> E_INVALID_CONFIG
  6. injected zero latency budget          -> E_LATENCY_BUDGET
  7. config change -> incremental rebuild of affected stages only,
     verified byte-for-byte against a from-scratch full build
  8. stop between stages -> recover -> artefacts identical to a full run
     with continuously advancing checkpoint sequence numbers
"""
from __future__ import annotations

import json
import os
import shutil
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from xfpipe.cache import QueryCache
from xfpipe.errors import PipelineError
from xfpipe.pipeline import Pipeline
from xfpipe.query import QueryEngine
from xfpipe.sample_gen import DEFAULT_ROWS, DEFAULT_SEED, generate_sample_csv, write_config
from xfpipe.workbench import Workbench

DEMO_DIR = os.path.abspath(os.environ.get("XFIPE_DEMO_DIR", "demo_run"))


def banner(title: str) -> None:
    print(f"\n=== {title} " + "=" * max(4, 60 - len(title)))


def show_error(exc: PipelineError) -> None:
    payload = exc.to_dict()["error"]
    print(f"  code    : {payload['code']}")
    print(f"  message : {payload['message']}")
    print(f"  details : {json.dumps(payload['details'], sort_keys=True)[:180]}")


def main() -> int:
    if os.path.exists(DEMO_DIR):
        shutil.rmtree(DEMO_DIR)
    os.makedirs(DEMO_DIR)

    banner("1. generate deterministic sample and config")
    sample = generate_sample_csv(os.path.join(DEMO_DIR, "data", "sample.csv"),
                                 rows=DEFAULT_ROWS, seed=DEFAULT_SEED)
    config_path = write_config(os.path.join(DEMO_DIR, "data", "config.json"))
    print(f"  sample : {sample} ({DEFAULT_ROWS} rows, seed {DEFAULT_SEED})")
    print(f"  config : {config_path}")

    wb = Workbench(DEMO_DIR)
    print(f"  sample fingerprint : {wb.pipeline.sample_fingerprint[:32]}...")
    print(f"  config version     : {wb.pipeline.config_version[:32]}...")

    banner("2. full four-stage run")
    report = wb.pipeline.run()
    print(f"  status     : {report['status']}")
    print(f"  recomputed : {report['recomputed_stages']}")
    summary = wb.pipeline.load_artifact("summary")["artifact"]
    print(f"  pipeline digest    : {summary['pipeline_digest']}")
    print(f"  cross combinations : {summary['cross']['combination_count']} kept, "
          f"{summary['cross']['truncated_count']} truncated, "
          f"{summary['cross']['feature_count']} features")
    for cand in summary["candidates"]:
        ratio = cand["explained_variance_ratio"]
        top = f"{ratio[0]:.4f}" if ratio else "n/a"
        print(f"  {cand['method']:<18} dim={cand['target_dim']} "
              f"top1-var={top} max-err={cand['error_bound']['max_abs_error']:.3e} "
              f"bound={cand['error_bound']['total_bound']:.3e}")

    banner("3. interactive queries (cache + latency)")
    for kind in ("explained_variance", "combinations", "explained_variance"):
        out = wb.query_engine.query(kind)
        print(f"  {out['query_id']} {kind:<20} hit={out['cache_hit']!s:<5} "
              f"elapsed={out['elapsed_ms']:.3f}ms budget={out['budget_ms']:.0f}ms")
    print(f"  cache stats: {wb.cache_stats()}")

    banner("4. injected out-of-range dimension -> E_DIM_INVALID")
    cfg = json.loads(json.dumps(wb.config))
    cfg["reduction"]["candidates"][0]["target_dim"] = 100000
    try:
        from xfpipe.reduce import reduce_features
        reduce_features(wb.pipeline.load_artifact("cross")["artifact"], cfg)
    except PipelineError as exc:
        show_error(exc)

    banner("5. injected illegal combination order -> E_INVALID_CONFIG")
    cfg = json.loads(json.dumps(wb.config))
    cfg["crossing"]["max_order"] = 0
    try:
        from xfpipe.config import validate_config
        validate_config(cfg)
    except PipelineError as exc:
        show_error(exc)

    banner("6. injected zero latency budget -> E_LATENCY_BUDGET")
    engine = QueryEngine(wb.pipeline, QueryCache(4), latency_budget_ms=1e-9)
    try:
        engine.query("feature_summary")
    except PipelineError as exc:
        show_error(exc)

    banner("7. config change -> incremental rebuild + full verification")
    cfg = json.loads(json.dumps(wb.config))
    cfg["crossing"]["max_crosses"] = 6
    cfg["reduction"]["candidates"][1]["target_dim"] = 4
    wb.update_config(cfg)
    report = wb.pipeline.run()
    print(f"  reused     : {report['reused_stages']}")
    print(f"  recomputed : {report['recomputed_stages']}")
    verification = report["incremental_verification"]
    print(f"  incremental vs full rebuild: {verification['status']}")
    print(f"  verified digest: {verification['pipeline_digest'][:48]}...")

    banner("8. stop between stages -> recover -> identical to full run")
    # Simulate a fresh process that gets stopped mid-build: wipe the
    # derived directory so the first two stages are genuinely recomputed
    # and checkpointed before the stop lands.
    shutil.rmtree(os.path.join(DEMO_DIR, "derived"))
    wb = Workbench(DEMO_DIR)
    stopped = wb.pipeline.run(stop_after_stage="cross")
    print(f"  stop after 'cross': status={stopped['status']} "
          f"completed={stopped['completed_stages']}")
    print(f"  journal records so far: {len(wb.pipeline.checkpoints.records())}")
    recovered = wb.pipeline.recover()
    print(f"  recovery: status={recovered['status']} recomputed={recovered['recomputed_stages']}")
    records = wb.pipeline.checkpoints.records()
    print(f"  journal sequence: {[r['seq'] for r in records]} "
          f"({wb.pipeline.checkpoints.classify_snapshot()['status']})")
    final = wb.pipeline.verify_against_full_rebuild()
    print(f"  recovered artefacts vs full run: {final['status']}")

    banner("demo finished")
    print(f"  final pipeline digest: {wb.pipeline.load_artifact('summary')['artifact']['pipeline_digest']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
