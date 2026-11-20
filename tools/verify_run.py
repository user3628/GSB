"""Result verification tool and fixed-digest comparison entry.

Runs the pipeline over the deterministic local sample and verifies:
  1. a fresh parallel run produces the fixed expected summary digest;
  2. a serial run is byte-identical to the parallel run;
  3. a stop/resume run is byte-identical to a from-scratch run.

Usage:
  python tools/verify_run.py --check            # full verification
  python tools/verify_run.py --write-golden     # record the golden digest
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

from rerank_bench.generator import DEFAULT_SEED, generate  # noqa: E402
from rerank_bench.pipeline import Workspace  # noqa: E402
from rerank_bench.runner import run_pipeline  # noqa: E402

GOLDEN_PATH = os.path.join(REPO_ROOT, "tests", "fixtures",
                           "expected_summary.sha256")


def _digest_of_export(ws: Workspace) -> str:
    path = os.path.join(ws.export_dir, "summary.json")
    with open(path, "rb") as fh:
        return hashlib.sha256(fh.read()).hexdigest()


def _fresh_workspace(base: str, name: str, data_dir: str) -> Workspace:
    root = os.path.join(base, name)
    if os.path.exists(root):
        shutil.rmtree(root)
    ws = Workspace(root)
    ws.import_data(data_dir)
    return ws


def run_verification(work_root: str, data_dir: str) -> dict:
    if os.path.exists(work_root):
        shutil.rmtree(work_root)
    os.makedirs(work_root, exist_ok=True)
    generate(data_dir, seed=DEFAULT_SEED, num_shards=4)

    ws_parallel = _fresh_workspace(work_root, "ws-parallel", data_dir)
    run_pipeline(ws_parallel, serial=False, repo_root=REPO_ROOT)
    digest_parallel = _digest_of_export(ws_parallel)

    ws_serial = _fresh_workspace(work_root, "ws-serial", data_dir)
    run_pipeline(ws_serial, serial=True, repo_root=REPO_ROOT)
    digest_serial = _digest_of_export(ws_serial)

    ws_resume = _fresh_workspace(work_root, "ws-resume", data_dir)
    first = run_pipeline(ws_resume, serial=False, repo_root=REPO_ROOT,
                         stop_after=2)  # graceful stop between shards
    assert first["stopped"], "expected the stop request to interrupt the run"
    assert len(first["pending"]) == 2, "expected two shards left pending"
    second = run_pipeline(ws_resume, serial=False, repo_root=REPO_ROOT)
    assert second["aggregated"], "expected resume to finish with aggregation"
    digest_resume = _digest_of_export(ws_resume)

    return {
        "parallel": digest_parallel,
        "serial": digest_serial,
        "resume": digest_resume,
        "parallel_eq_serial": digest_parallel == digest_serial,
        "resume_eq_fresh": digest_resume == digest_parallel,
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="verify deterministic runs")
    parser.add_argument("--work-root", default="verify_work")
    parser.add_argument("--data-dir", default=os.path.join("verify_work", "data"))
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--write-golden", action="store_true")
    args = parser.parse_args(argv)

    report = run_verification(args.work_root, args.data_dir)
    print(json.dumps(report, indent=2, sort_keys=True))

    ok = report["parallel_eq_serial"] and report["resume_eq_fresh"]
    if args.write_golden:
        os.makedirs(os.path.dirname(GOLDEN_PATH), exist_ok=True)
        with open(GOLDEN_PATH, "w", encoding="utf-8") as fh:
            fh.write(report["parallel"] + "\n")
        print(f"golden digest written to {GOLDEN_PATH}")
    if args.check:
        if not os.path.exists(GOLDEN_PATH):
            print("golden digest missing; run with --write-golden first",
                  file=sys.stderr)
            return 2
        with open(GOLDEN_PATH, "r", encoding="utf-8") as fh:
            expected = fh.read().strip()
        if report["parallel"] != expected:
            print(f"digest mismatch: got {report['parallel']}, "
                  f"expected {expected}", file=sys.stderr)
            return 1
        print(f"golden digest match: {expected}")
    if not ok:
        print("determinism check failed", file=sys.stderr)
        return 1
    print("verification passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
