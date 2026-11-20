#!/usr/bin/env python3
"""End-to-end deterministic demonstration and error-code verification.

Running this script twice on the same dataset produces an identical fixed
summary (printed at the end and written to ``demo-summary.json`` in the
workspace).  It exercises:

* 3 shards with 2-3 speakers, silence and overlap regions;
* separation, embedding and recognition determinism;
* incremental recomputation vs. from-scratch byte equality;
* latency-budget structured errors;
* graceful stop + recovery equivalence;
* snapshot rollback/tamper detection (E_STALE_SNAPSHOT);
* concurrent publish protection (E_STALE_VERSION) and invalid stage jumps
  (E_INVALID_STAGE), plus invalid configuration rejection.
"""
from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from diarwb.config import default_config
from diarwb.datagen import build_demo_dataset
from diarwb.errors import (EInvalidConfig, EInvalidStage, ELatencyBudget,
                           EStaleSnapshot, EStaleVersion, WorkbenchError)
from diarwb.pipeline import STAGE_RECOGNITION, STAGE_SEPARATION
from diarwb.workbench import Workbench, atomic_write_json, digest_json, read_json


def fresh_workspace(base: str, name: str) -> str:
    root = os.path.join(base, name)
    os.makedirs(root, exist_ok=True)
    build_demo_dataset(root)
    return root


def expect_error(fn, code: str) -> dict:
    try:
        fn()
    except WorkbenchError as exc:
        assert exc.code == code, f"expected {code}, got {exc.code}"
        return exc.to_dict()
    raise AssertionError(f"expected error {code} but call succeeded")


def main() -> int:
    base = tempfile.mkdtemp(prefix="diarwb-demo-")
    report: dict = {"steps": [], "errors_verified": {}}

    def step(name: str, payload) -> None:
        report["steps"].append({"step": name, "result": payload})

    root = fresh_workspace(base, "primary")
    wb = Workbench(root, default_config())
    step("import", wb.import_workspace())
    step("run_all", wb.run_all())
    step("stage_status", wb.stage_status())

    # Determinism: rebuild into an independent workspace and compare artifacts.
    mirror = fresh_workspace(base, "mirror")
    wb_mirror = Workbench(mirror, default_config())
    wb_mirror.import_workspace()
    wb_mirror.run_all()
    determinism = {}
    for shard_id in sorted(wb.pipeline.infos):
        a = wb.ws.read_stage_result(shard_id, STAGE_SEPARATION)
        b = wb_mirror.ws.read_stage_result(shard_id, STAGE_SEPARATION)
        ra = wb.ws.read_stage_result(shard_id, STAGE_RECOGNITION)
        rb = wb_mirror.ws.read_stage_result(shard_id, STAGE_RECOGNITION)
        determinism[shard_id] = {
            "separation_identical": a["summary"] == b["summary"],
            "recognition_identical":
                digest_json(ra["diagnostics"]) == digest_json(rb["diagnostics"])
                and digest_json(ra["overlaps"]) == digest_json(rb["overlaps"]),
        }
    step("cross_workspace_determinism", determinism)

    # Query latency budget: force an impossible budget.
    lat = expect_error(
        lambda: wb.query("shard-000", "overlaps", budget_ms=-0)
        if False else wb.query("shard-000", "overlaps", budget_ms=0),
        "E_LATENCY_BUDGET")
    assert "elapsed_ms" in lat["details"] and "query_id" in lat["details"]
    report["errors_verified"]["E_LATENCY_BUDGET"] = lat["details"]

    # Incremental edit: split a segment, only recognition should recompute.
    tl_before = wb.get_timeline("shard-000")
    target = tl_before["segments"][0]
    split_at = (target["start_ms"] + target["end_ms"]) // 2
    before_versions = {s["shard_id"]: s["stages"]
                       for s in wb.stage_status()}
    published = wb.publish_edit(
        "shard-000", "split",
        {"seg_id": target["seg_id"], "at_ms": split_at},
        expected_version=0, origin="demo")
    after_versions = {s["shard_id"]: s["stages"]
                      for s in wb.stage_status()}
    sep_unchanged = (
        before_versions["shard-000"]["separation"]["stage_version"]
        == after_versions["shard-000"]["separation"]["stage_version"])
    rec_changed = (
        before_versions["shard-000"]["recognition"]["stage_version"]
        != after_versions["shard-000"]["recognition"]["stage_version"])
    step("incremental_split", {"published": published,
                               "separation_reused": sep_unchanged,
                               "recognition_recomputed": rec_changed})
    consistency = wb.verify_consistency()
    step("incremental_consistency", {"status": consistency["status"]})

    # Cache key includes edit version: stale cached result must not be served.
    q1 = wb.query("shard-000", "segments")
    assert q1["data"]["version"] == 1 and q1["cached"] is False
    q2 = wb.query("shard-000", "segments")
    assert q2["cached"] is True
    step("cache_versioning", {"first_cached": q1["cached"],
                              "second_cached": q2["cached"]})

    # Stale publish: reuse the already-consumed expected version 0.
    stale = expect_error(
        lambda: wb.publish_edit(
            "shard-000", "rename_speaker",
            {"old": "S1", "new": "ALICE"}, expected_version=0),
        "E_STALE_VERSION")
    report["errors_verified"]["E_STALE_VERSION"] = stale["details"]

    # Draft isolation and rollback.
    wb.save_draft("shard-001", "rename_speaker",
                  {"old": "S1", "new": "DRAFT"}, origin="demo")
    published_tl = wb.get_timeline("shard-001")
    wb.discard_draft("shard-001")
    step("draft_rollback", {"published_version_unchanged":
                            published_tl["version"] == 0})

    # Invalid config: threshold outside [0, 1].
    bad_cfg = expect_error(
        lambda: wb.update_config(recognition_margin=1.5), "E_INVALID_CONFIG")
    report["errors_verified"]["E_INVALID_CONFIG"] = bad_cfg["details"]

    # Invalid stage jump: recognition without separation on a fresh workspace.
    fresh = fresh_workspace(base, "stagejump")
    wb_fresh = Workbench(fresh, default_config())
    wb_fresh.import_workspace()
    bad_stage = expect_error(
        lambda: wb_fresh.run_stage("shard-000", STAGE_RECOGNITION),
        "E_INVALID_STAGE")
    report["errors_verified"]["E_INVALID_STAGE"] = bad_stage["details"]

    # Config change drives upstream recompute (separation mask iterations).
    wb.update_config(mask_iterations=default_config().mask_iterations + 4)
    rerun = wb.run_all()
    step("config_driven_recompute", rerun)
    step("config_consistency",
         {"status": wb.verify_consistency()["status"]})
    wb.update_config(mask_iterations=default_config().mask_iterations)

    # Graceful stop: request before run, observe E_STOPPED, then recover.
    stop_root = fresh_workspace(base, "stop")
    wb_stop = Workbench(stop_root, default_config())
    wb_stop.import_workspace()
    wb_stop.request_stop()
    stopped = expect_error(wb_stop.run_all, "E_STOPPED")
    report["errors_verified"]["E_STOPPED"] = stopped["details"]
    recovered = wb_stop.recover()
    step("stop_recover", {"resumed_shards": len(recovered["resumed"])})
    step("recover_consistency",
         {"status": wb_stop.verify_consistency()["status"]})

    # Snapshot replacement: rollback and tamper are rejected, advance accepted.
    snap_path = wb.ws.snapshot_path("shard-000", STAGE_SEPARATION)
    original = read_json(snap_path)
    rolled_back = dict(original)
    rolled_back["stage_version"] = "fp-0000000000000000"
    rolled_back["snapshot_digest"] = digest_json(
        {k: v for k, v in rolled_back.items() if k != "snapshot_digest"})
    atomic_write_json(snap_path, rolled_back)
    rollback_err = expect_error(
        lambda: wb.run_stage("shard-000", STAGE_SEPARATION),
        "E_STALE_SNAPSHOT")
    tampered = dict(original)
    tampered["artifact_digest"] = digest_json({"tampered": True})
    tampered["snapshot_digest"] = digest_json(
        {k: v for k, v in tampered.items() if k != "snapshot_digest"})
    atomic_write_json(snap_path, tampered)
    tamper_err = expect_error(
        lambda: wb.run_stage("shard-000", STAGE_SEPARATION),
        "E_STALE_SNAPSHOT")
    atomic_write_json(snap_path, original)  # restore -> normal advance accepted
    advance = wb.run_stage("shard-000", STAGE_SEPARATION)
    report["errors_verified"]["E_STALE_SNAPSHOT"] = {
        "rollback": rollback_err["details"],
        "tamper": tamper_err["details"],
        "normal_advance_version": advance["stage_version"],
    }
    step("snapshot_replacement", rollback_err["details"]["kind"])

    # Cross-shard identity unification.
    unify = wb.unify_identity({"S1": "ALICE", "S2": "BOB", "S3": "CAROL"})
    step("unify_identity", unify)

    export_path = wb.export_results()
    export_payload = read_json(export_path)
    fixed_summary = {
        "config_version": wb.cfg.fingerprint(),
        "export_digest": export_payload["export_digest"],
        "stage_versions": [
            {"shard_id": s["shard_id"],
             "recognition_stage": s["stage_version"]}
            for s in export_payload["shards"]],
        "stats": {s["shard_id"]: s["stats"]
                  for s in export_payload["shards"]},
        "overlap_counts": {s["shard_id"]: len(s["overlaps"])
                           for s in export_payload["shards"]},
        "errors_verified": sorted(report["errors_verified"]),
    }
    report["fixed_summary"] = fixed_summary
    summary_path = os.path.join(root, "demo-summary.json")
    atomic_write_json(summary_path, report)
    print(json.dumps(fixed_summary, indent=2, sort_keys=True, ensure_ascii=False))
    print(f"\nfull report: {summary_path}")
    print(f"workspaces: {base}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
