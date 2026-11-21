"""End-to-end acceptance demonstration.

Runs the full deterministic workflow in a throwaway workspace and asserts:
    1. serial and parallel runs produce byte-identical state/checkpoints;
    2. tight-budget scheduling rejects folds with explicit reason codes and
       never exceeds the global fit budget;
    3. a cooperatively stopped run, once resumed, matches a clean run
       byte-for-byte;
    4. reports (JSON, error-curve CSV, budget CSV, HTML dashboard) export.

Run::
    python scripts/demo.py
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__),
                                                "..")))

from src.checkpoints import read_state, request_stop  # noqa: E402
from src.config import BacktestConfig, load_config  # noqa: E402
from src.pipeline import run_pipeline  # noqa: E402
from src.report import export_all  # noqa: E402


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
MANIFEST = os.path.join(REPO_ROOT, "samples", "manifest.json")


def state_hash(workspace: str) -> str:
    state = read_state(workspace)
    state.pop("exports", None)
    payload = json.dumps(state, sort_keys=True, separators=(",", ":"),
                         allow_nan=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def checkpoint_hashes(workspace: str):
    directory = os.path.join(workspace, "checkpoints")
    hashes = {}
    for name in sorted(os.listdir(directory)):
        if not name.startswith("shard_"):
            continue
        with open(os.path.join(directory, name), "rb") as handle:
            hashes[name] = hashlib.sha256(handle.read()).hexdigest()
    return hashes


def main() -> int:
    if not os.path.exists(MANIFEST):
        from scripts.generate_samples import generate
        generate(os.path.join(REPO_ROOT, "samples"))

    root = tempfile.mkdtemp(prefix="wb-demo-")
    try:
        cfg = load_config(os.path.join(REPO_ROOT, "config", "backtest.yaml"))
        serial_ws = os.path.join(root, "serial")
        parallel_ws = os.path.join(root, "parallel")
        stopped_ws = os.path.join(root, "stopped")
        budget_ws = os.path.join(root, "budget")
        os.makedirs(serial_ws)
        os.makedirs(parallel_ws)
        os.makedirs(stopped_ws)
        os.makedirs(budget_ws)

        print("[1/5] serial run ...")
        run_pipeline(serial_ws, MANIFEST, cfg, parallel=False)
        print("[2/5] parallel run ...")
        run_pipeline(parallel_ws, MANIFEST, cfg, parallel=True)
        identical = state_hash(serial_ws) == state_hash(parallel_ws) and \
            checkpoint_hashes(serial_ws) == checkpoint_hashes(parallel_ws)
        print("      serial == parallel (byte-identical):", identical)
        assert identical

        print("[3/5] cooperative stop after 5 folds then resume ...")
        first = run_pipeline(stopped_ws, MANIFEST, cfg, parallel=True,
                             stop_after_folds=5)
        assert first["run_status"] == "stopped"
        request_stop(stopped_ws)  # exercise the real stop-flag path too
        resumed = run_pipeline(stopped_ws, MANIFEST, cfg, parallel=True)
        assert resumed["run_status"] == "completed"
        resume_identical = state_hash(serial_ws) == \
            state_hash(stopped_ws) and \
            checkpoint_hashes(serial_ws) == checkpoint_hashes(stopped_ws)
        print("      resumed == clean run (byte-identical):",
              resume_identical)
        assert resume_identical

        print("[4/5] tight-budget admission ...")
        tight = BacktestConfig(method="exponential_smoothing",
                               max_workers=3, global_fit_budget=20,
                               worker_fit_quota=8)
        budget_state = run_pipeline(budget_ws, MANIFEST, tight,
                                    parallel=False)
        accepted = budget_state["scheduler"]["accepted_folds"]
        rejected = budget_state["rejected_folds"]
        print("      accepted:", accepted, "rejected:",
              sorted({item["code"] for item in rejected}))
        assert accepted <= tight.global_fit_budget
        assert rejected

        print("[5/5] report export ...")
        paths = export_all(serial_ws, read_state(serial_ws))
        for kind, path in paths.items():
            assert os.path.exists(path)
            print(f"      {kind}: {os.path.relpath(path, REPO_ROOT)}")

        summary = read_state(serial_ws)["metrics"]["summary"]
        print("\naggregate metrics:")
        for key in ("fold_count", "step_count", "mae", "rmse", "mape",
                    "coverage", "mape_skipped_zero_denominator"):
            print(f"  {key}: {summary[key]}")
        print("\nALL ACCEPTANCE CHECKS PASSED")
        return 0
    finally:
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
