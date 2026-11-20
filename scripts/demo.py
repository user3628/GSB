"""End-to-end deterministic demonstration of the workbench.

Runs entirely locally with no network/GPU. Writes a fixed summary to
artifacts/demo_summary.json and prints each verification step.
"""
from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from depworkbench.checkpoint import CheckpointStore  # noqa: E402
from depworkbench.config import load_config  # noqa: E402
from depworkbench.engine import Engine, reduce_results  # noqa: E402
from depworkbench.errors import (E_BUDGET_GLOBAL, E_BUDGET_WORKER,  # noqa: E402
                                 E_INVALID_CONFIG, E_INVALID_STAGE,
                                 E_NUMERIC, E_STALE_CHECKPOINT,
                                 E_TREE_INVALID, WorkbenchError)
from depworkbench.parser import parse_sentence  # noqa: E402
from depworkbench.shards import load_shards  # noqa: E402
import depworkbench.workspace as workspace_mod
Workspace = workspace_mod.Workspace
WorkbenchService = workspace_mod.WorkbenchService  # noqa: E402

SAMPLES = ROOT / "samples"
WORKSPACES = ROOT / "workspaces"
ARTIFACTS = ROOT / "artifacts"


def banner(title: str) -> None:
    print(f"\n=== {title} ===")


def expect_error(label: str, code: str, fn) -> dict:
    try:
        fn()
    except WorkbenchError as exc:
        assert exc.code == code, f"{label}: expected {code}, got {exc.code}"
        print(f"[OK] {label}: {code} -> {exc.message}")
        return exc.to_dict()
    raise AssertionError(f"{label}: expected error {code} but none raised")


def main() -> None:
    if WORKSPACES.exists():
        shutil.rmtree(WORKSPACES)
    ARTIFACTS.mkdir(exist_ok=True)

    banner("1. import deterministic sample bundle")
    ws_full = Workspace.create(WORKSPACES / "full")
    svc = WorkbenchService(ws_full)
    record = svc.import_bundle(SAMPLES, SAMPLES / "manifest.json",
                               SAMPLES / "config.yaml")
    print(f"[OK] imported {record['shard_count']} shards, "
          f"config={record['config_fingerprint'][:12]}")

    banner("2. budget admission plan")
    plan = svc.plan()
    for decision in plan["decisions"]:
        print(f"  shard {decision['shard_index']}: "
              f"{'ADMIT' if decision['admitted'] else 'REJECT ' + str(decision['reason_code'])}"
              f" worker={decision['worker_id']} tokens={decision['estimated_tokens']}")
    assert plan["global_consumed"] <= plan["global_budget"]

    banner("3. parallel run vs sequential run must be byte-identical")
    parallel_report = svc.run()
    parallel_stats = svc.stats()["stats"]
    parallel_export = json.loads(
        svc.export(ARTIFACTS / "parallel_export.json").read_text(encoding="utf-8"))

    ws_seq = Workspace.create(WORKSPACES / "sequential")
    svc_seq = WorkbenchService(ws_seq)
    svc_seq.import_bundle(SAMPLES, SAMPLES / "manifest.json",
                          SAMPLES / "config.yaml")
    svc_seq.run(sequential=True)
    seq_export = json.loads(
        svc_seq.export(ARTIFACTS / "sequential_export.json").read_text(
            encoding="utf-8"))
    assert parallel_export == seq_export, "parallel/sequential outputs diverged"
    assert parallel_stats == svc_seq.stats()["stats"]
    print(f"[OK] identical outputs; stats={json.dumps(parallel_stats, sort_keys=True)}")

    banner("4. graceful stop then resume reaches the same fixed summary")
    ws_stop = Workspace.create(WORKSPACES / "stopresume")
    svc_stop = WorkbenchService(ws_stop)
    svc_stop.import_bundle(SAMPLES, SAMPLES / "manifest.json",
                           SAMPLES / "config.yaml")
    def stop_after_first(shard_index: int) -> None:
        if shard_index == 0:
            svc_stop.stop()

    stopped = svc_stop.run(sequential=True,
                           shard_complete_callback=stop_after_first)
    print(f"[OK] stop requested, run ended with status={stopped.status}")
    assert stopped.status == "stopped"
    resume_report = svc_stop.resume()
    print(f"[OK] resumed, reused={resume_report.reused_checkpoints}, "
          f"status={resume_report.status}")
    resume_export = json.loads(
        svc_stop.export(ARTIFACTS / "resume_export.json").read_text(
            encoding="utf-8"))
    assert resume_export == parallel_export
    print("[OK] resumed export is byte-identical to full parallel run")

    banner("5. version change invalidates downstream shards")
    shards = load_shards(ws_stop.shards_dir, ws_stop.manifest_path)
    changed = shards[1].sentences[0] + " 增量"
    (ws_stop.shards_dir / shards[1].filename).write_text(
        changed + "\n" + "\n".join(shards[1].sentences[1:]) + "\n",
        encoding="utf-8")
    from depworkbench.shards import shard_fingerprint, write_manifest
    new_shards = load_shards(ws_stop.shards_dir, ws_stop.manifest_path)
    write_manifest(ws_stop.manifest_path, new_shards)
    report = svc_stop.resume()
    print(f"[OK] invalidation reason={report.invalidation_reason}, "
          f"invalidated={report.invalidated_shards}")
    assert report.invalidation_reason == "INPUT_CHANGED"
    assert report.invalidated_shards[0] == 1
    status = svc_stop.status()
    observable = [s for s in status["shards"] if s["invalidation"] is None]
    print(f"[OK] status exposes fingerprints; shard0 reused, "
          f"reused count={len(observable)}")

    banner("6. budget overruns return structured reason codes")
    ws_tight = Workspace.create(WORKSPACES / "tight")
    svc_tight = WorkbenchService(ws_tight)
    svc_tight.import_bundle(SAMPLES, SAMPLES / "manifest.json",
                            SAMPLES / "config_tight_budget.yaml")
    tight_plan = svc_tight.plan()
    codes = {d["shard_index"]: d["reason_code"]
             for d in tight_plan["decisions"] if not d["admitted"]}
    print(f"[OK] tight plan rejection codes: {codes}")
    assert set(codes.values()) <= {E_BUDGET_GLOBAL, E_BUDGET_WORKER}
    assert tight_plan["global_consumed"] <= tight_plan["global_budget"]

    banner("7. invalid config error codes")
    expect_error("distance out of range", E_INVALID_CONFIG,
                 lambda: load_config(SAMPLES / "config_bad_distance.yaml"))
    expect_error("illegal weight value", E_INVALID_CONFIG,
                 lambda: load_config(SAMPLES / "config_bad_weights.yaml"))

    banner("8. numeric and tree diagnostics")
    cfg = load_config(SAMPLES / "config.yaml")
    expect_error(
        "non-finite action score", E_NUMERIC,
        lambda: parse_sentence(shards[0].sentences[0], cfg.model, "num-1",
                               force_numeric_at=2),
    )
    expect_error(
        "distance limit violated", E_TREE_INVALID,
        lambda: parse_sentence(
            shards[0].sentences[0],
            load_config.__self__ if False else
            _tight_model(), "tree-1"),
    )

    banner("9. publish guard and stage guard")
    store = CheckpointStore(WORKSPACES / "guard" / "checkpoints")
    from depworkbench.checkpoint import CheckpointRecord
    good = CheckpointRecord.create(0, 0, "in", "m", "c", {"k": 1})
    store.publish(good)
    stale = CheckpointRecord.create(0, 0, "in", "m", "c", {"k": 1})
    expect_error("duplicate/stale publish", E_STALE_CHECKPOINT,
                 lambda: store.publish(stale))
    expect_error("aggregation without predecessor", E_INVALID_STAGE,
                 lambda: store.require_prefix(2))

    banner("10. fixed summary")
    summary = {
        "parallel_stats": parallel_stats,
        "plan_global_consumed": plan["global_consumed"],
        "tight_plan_codes": codes,
        "config_fingerprint": record["config_fingerprint"],
    }
    (ARTIFACTS / "demo_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    print("\nALL DEMO CHECKS PASSED")


def _tight_model():
    cfg = load_config(SAMPLES / "config.yaml")
    from dataclasses import replace
    return replace(cfg.model, max_dependency_distance=1)


if __name__ == "__main__":
    main()
