"""End-to-end deterministic demonstration for the contract risk workbench.

Run with the project virtualenv:
    .venv/bin/python scripts/run_demo.py
"""
from __future__ import annotations

import json
import os
import sys
import threading
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from crwbench.config import load_config  # noqa: E402
from crwbench.generator import generate_dataset  # noqa: E402
from crwbench.pipeline import Pipeline, verify_workdir_outputs  # noqa: E402
from crwbench.server import serve  # noqa: E402
from crwbench.util import canonical_dumps  # noqa: E402

CONFIG = os.path.join(ROOT, "config", "default_config.yaml")
VARIANT = os.path.join(ROOT, "config", "variants", "config_v110.yaml")
PROFILES = {
    "small": {"sizes": {"small": 24}, "expect": "exact_full", "seed": 20261121},
    "medium": {"sizes": {"medium": 120}, "expect": "keyword_priority_truncated", "seed": 20261122},
    "large": {"sizes": {"large": 320}, "expect": "category_tiered_budget", "seed": 20261123},
}


def line(title: str) -> None:
    print(f"\n--- {title} ---")


def main() -> int:
    config = load_config(CONFIG)
    demo_root = os.path.join(ROOT, "work", "demo_runs")
    os.makedirs(demo_root, exist_ok=True)
    fixed_summary = {}

    line("1. deterministic shards: 3 shards, zh/en/mixed, five risk categories")
    for name, spec in PROFILES.items():
        data_dir = os.path.join(ROOT, "data", f"demo_{name}")
        result = generate_dataset(
            seed=spec["seed"], sizes=spec["sizes"], out_dir=data_dir, shard_count=3
        )
        shards = result["paths"]
        workdir = os.path.join(demo_root, name)
        report = Pipeline(config, workdir).run(shards, force=True)
        final = Pipeline(config, workdir).store.load_artifact("summary")
        decision = final["strategy_decision"]
        print(
            f"{name:6s} tier={final['tier']:6s} strategy={final['summary']['strategy']:28s} "
            f"judgments={final['summary']['committed_judgments']:4d} "
            f"clauses={final['summary']['clause_count']:4d} "
            f"levels={final['summary']['level_distribution']}"
        )
        assert final["summary"]["strategy"] == spec["expect"]
        fixed_summary[name] = {
            "tier": final["tier"],
            "strategy": final["summary"]["strategy"],
            "committed_judgments": final["summary"]["committed_judgments"],
            "level_distribution": final["summary"]["level_distribution"],
            "rejected_reasons": decision["rejected_reasons"],
            "report_digest": report["final_report_digest"],
        }

    line("2. reproducibility: repeated run reuses checkpoints and keeps the same digest")
    medium_shards = sorted(
        os.path.join(ROOT, "data", "demo_medium", n)
        for n in os.listdir(os.path.join(ROOT, "data", "demo_medium"))
        if n.endswith(".jsonl")
    )
    workdir = os.path.join(demo_root, "medium")
    first = Pipeline(config, workdir).run(medium_shards)
    second = Pipeline(config, workdir).run(medium_shards)
    print("reused stages on second run:", second["reused_stages"])
    assert first["final_report_digest"] == second["final_report_digest"]
    print("report digest stable:", second["final_report_digest"])

    line("3. graceful stop plus resume, byte-identical to a clean full run")
    baseline = os.path.join(demo_root, "resume_baseline")
    Pipeline(config, baseline).run(medium_shards, force=True)
    interrupted = os.path.join(demo_root, "resume_interrupted")
    pipe = Pipeline(config, interrupted)
    pipe.run(medium_shards, force=True)
    pipe.store.rollback_to(0)  # emulate interruption after stage 1
    pipe.request_stop()
    stopped = pipe.run(medium_shards)
    print("stopped after stages:", stopped["completed_stages"], "inflight:", stopped["stop_stage"])
    resumed = pipe.resume(medium_shards)
    print("resume reused:", resumed["reused_stages"], "recomputed:", resumed["recomputed_stages"])
    verify_workdir_outputs(baseline, interrupted)
    print("resume output byte-identical to clean run: True")

    line("4. incremental recompute after negation/degree/threshold change")
    changed = load_config(VARIANT)
    incr_dir = os.path.join(demo_root, "incremental")
    Pipeline(config, incr_dir).run(medium_shards, force=True)
    incr_report = Pipeline(changed, incr_dir).run(medium_shards)
    print("reused:", incr_report["reused_stages"])
    print("recomputed:", incr_report["recomputed_stages"])
    clean_dir = os.path.join(demo_root, "clean_v110")
    Pipeline(changed, clean_dir).run(medium_shards, force=True)
    verify_workdir_outputs(clean_dir, incr_dir)
    print("incremental output byte-identical to clean full run: True")

    line("5. local HTTP API smoke test on 127.0.0.1 (no external dependency)")
    httpd = serve(CONFIG, os.path.join(demo_root, "http_workspace"), port=0)
    server_thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    server_thread.start()
    try:
        def call(method: str, path: str, payload=None):
            data = canonical_dumps(payload or {})
            req = urllib.request.Request(
                f"http://127.0.0.1:{httpd.actual_port}{path}",
                data=data if method == "POST" else None,
                headers={"Content-Type": "application/json"},
                method=method,
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                return json.loads(resp.read().decode("utf-8"))

        print("health:", call("GET", "/health"))
        call("POST", "/shards/import", {"shard_paths": medium_shards})
        run = call("POST", "/pipeline/run", {"force": True})
        print("pipeline strategy via HTTP:", run["summary"]["strategy"])
        status = call("GET", "/stages/status")
        print("checkpoint stages:", [r["stage"] for r in status["records"]])
        rationale = call("GET", "/strategy/rationale")
        print(
            "rationale chosen:",
            rationale["strategy_decision"]["chosen_strategy"],
            "candidates:",
            rationale["strategy_decision"]["candidate_count"],
        )
        evidence = call("GET", "/levels/evidence")
        print("evidence rows:", len(evidence["evidence"]))
        exported = call("GET", "/levels/export")
        print("exported level rows:", len(exported["level_list"]))
    finally:
        httpd.shutdown()

    line("fixed demo summary")
    summary_path = os.path.join(ROOT, "work", "demo_fixed_summary.json")
    with open(summary_path, "wb") as fh:
        fh.write(canonical_dumps(fixed_summary))
    print(canonical_dumps(fixed_summary).decode("utf-8"))
    print("fixed summary written to:", summary_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
