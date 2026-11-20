"""Deterministic end-to-end demonstration of the alignment engine.

Steps (all local, fixed seed, no network):
  1. generate samples (3 text + 3 vision shards, mixed zh/en, 1 injected
     dimension-mismatch anomaly);
  2. import v2 manifest, run all four stages -> fixed summary digest;
  3. verify three-format round-trip fidelity;
  4. upgrade v1 manifest and verify byte-level v2 equivalence;
  5. stop/resume byte-identity check;
  6. config change -> invalidation propagation (stage 3+ rebuilt, 1-2 reused);
  7. injected anomalies -> E_DIM_MISMATCH / E_INVALID_STAGE /
     E_STALE_CHECKPOINT.
"""

import json
import os
import shutil

import sample_gen
from .. import service
from ..errors import EngineError
from ..pipeline import Workspace, workspace_output_digests


def _expect_error(fn, code):
    try:
        fn()
    except EngineError as exc:
        return {"expected": code, "got": exc.code, "ok": exc.code == code,
                "details": exc.details}
    return {"expected": code, "got": None, "ok": False}


def run_demo(root):
    root = os.path.abspath(root)
    if os.path.exists(root):
        shutil.rmtree(root)
    os.makedirs(root)
    results = {"root": root, "steps": {}}

    # 1. samples
    samples_dir = os.path.join(root, "samples")
    results["steps"]["1_generate"] = sample_gen.generate(samples_dir)
    manifest_v1 = os.path.join(samples_dir, "manifest_v1.yaml")
    manifest_v2 = os.path.join(samples_dir, "manifest_v2.yaml")
    shards_dir = os.path.join(samples_dir, "shards")
    inject_dir = os.path.join(samples_dir, "inject")

    # 2. import v2 + full run
    ws_main = os.path.join(root, "ws_main")
    results["steps"]["2_import_v2"] = service.do_import(
        ws_main, manifest_v2, shards_dir)
    run_report = service.do_run(ws_main)
    summary = service.do_summary(ws_main)
    results["steps"]["2_run"] = {
        "status": run_report["status"],
        "summary_digest": summary["digest"],
        "totals": summary["totals"],
        "config_version": summary["config_version"],
    }

    # 3. three-format round-trip fidelity
    project_json = os.path.join(ws_main, "project", "project.json")
    results["steps"]["3_roundtrip"] = service.do_verify_roundtrip(
        project_json, "json_project", os.path.join(root, "roundtrip"))

    # 4. v1 upgrade + v2 equivalence
    results["steps"]["4_upgrade"] = service.do_upgrade(
        manifest_v1, manifest_v2, shards_dir,
        os.path.join(root, "upgrade"),
        out_path=os.path.join(root, "upgrade", "reexported_v2.yaml"))

    # 5. stop / resume byte identity
    ws_stop = os.path.join(root, "ws_stop")
    service.do_import(ws_stop, manifest_v2, shards_dir)
    stopped = service.do_run(ws_stop, stop_after_units=3)
    resumed = service.do_run(ws_stop)
    digests_fresh = workspace_output_digests(Workspace(ws_main))
    digests_resumed = workspace_output_digests(Workspace(ws_stop))
    results["steps"]["5_stop_resume"] = {
        "stopped_status": stopped["status"],
        "resumed_status": resumed["status"],
        "byte_identical": digests_fresh == digests_resumed,
        "digests": digests_resumed,
    }

    # 6. invalidation propagation after config change
    service.do_config_set(ws_main, "operator_weights.op-encoder-b", 0.9)
    rerun = service.do_run(ws_main)
    reused = sorted({"{}:{}".format(e["stage"], e["unit"])
                     for e in rerun["events"] if e["event"] == "reuse"})
    rebuilt = sorted({"{}:{}".format(e["stage"], e["unit"])
                      for e in rerun["events"] if e["event"] == "rebuild"})
    summary_after = service.do_summary(ws_main)
    results["steps"]["6_invalidation"] = {
        "reused_units": reused,
        "rebuilt_units": rebuilt,
        "invalidations": service.do_invalidations(ws_main)["invalidations"],
        "new_config_version": summary_after["config_version"],
        "new_summary_digest": summary_after["digest"],
    }

    # 7a. injected dimension mismatch
    ws_bad = os.path.join(root, "ws_bad_dim")
    service.do_import(ws_bad, manifest_v2, shards_dir,
                      extra_shards_dir=inject_dir)
    results["steps"]["7a_dim_mismatch"] = _expect_error(
        lambda: service.do_run(ws_bad), "E_DIM_MISMATCH")

    # 7b. illegal stage jump on a fresh workspace
    ws_fresh = os.path.join(root, "ws_fresh")
    service.do_import(ws_fresh, manifest_v2, shards_dir)
    results["steps"]["7b_invalid_stage"] = _expect_error(
        lambda: service.do_run(ws_fresh, stage="concat"),
        "E_INVALID_STAGE")

    # 7c. checkpoint replacement detection
    ws_tamper = os.path.join(root, "ws_tamper")
    service.do_import(ws_tamper, manifest_v2, shards_dir)
    service.do_run(ws_tamper)
    artifact = os.path.join(ws_tamper, "artifacts", "s2_project",
                            "text-shard-001.json")
    with open(artifact, "ab") as handle:
        handle.write(b"tampered")
    results["steps"]["7c_stale_checkpoint"] = _expect_error(
        lambda: service.do_run(ws_tamper), "E_STALE_CHECKPOINT")

    report_path = os.path.join(root, "demo_report.json")
    with open(report_path, "w", encoding="utf-8") as handle:
        json.dump(results, handle, ensure_ascii=False, indent=2,
                  sort_keys=True)
        handle.write("\n")
    results["report_path"] = report_path
    return results
