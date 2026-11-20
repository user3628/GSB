"""Reproducible end-to-end demonstration.

Runs entirely locally with the fixed-seed dataset under ``data/`` and writes
all run artifacts under ``runs/demo`` plus a human/ machine readable summary
under ``artifacts/demo``.  The script demonstrates:

1. byte-identical serial vs parallel execution,
2. graceful stop + resume equivalence,
3. stage-level version invalidation propagation,
4. the three budget rejection codes,
5. injected business error codes (NaN/Inf, bitwidth, stale publish).
"""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path
from typing import Any, Dict

import yaml

from qoplib.checkpoint import CheckpointStore, build_record
from qoplib.errors import (
    E_INVALID_BITWIDTH,
    E_MEMORY_BUDGET,
    E_NUMERIC_INPUT,
    E_NUMERIC_MISMATCH,
    E_STALE_CHECKPOINT,
    E_WORKER_QUOTA,
    NumericMismatchError,
    StaleCheckpointError,
)
from qoplib.fingerprints import canonical_json_text
from qoplib.manifest import load_manifest
from qoplib.pipeline import run_shard_pipeline
from qoplib.runner import request_stop, run, run_fingerprint

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
RUNS = ROOT / "runs" / "demo"
ART = ROOT / "artifacts" / "demo"


def _hash_tree(root: Path) -> Dict[str, str]:
    return {
        str(path.relative_to(root)): hashlib.sha256(
            path.read_bytes()).hexdigest()
        for path in sorted(root.rglob("*"))
        if path.is_file() and ".lock" not in path.name
    }


def _reset() -> None:
    shutil.rmtree(RUNS, ignore_errors=True)
    shutil.rmtree(ART, ignore_errors=True)
    RUNS.mkdir(parents=True, exist_ok=True)
    ART.mkdir(parents=True, exist_ok=True)


def section(title: str) -> None:
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def main() -> int:
    _reset()
    report: Dict[str, Any] = {"checks": []}

    def record(name: str, ok: bool, detail: Any) -> None:
        report["checks"].append({"name": name, "ok": bool(ok),
                                 "detail": detail})
        print(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}")

    manifest_path = DATA / "manifest.yaml"
    manifest = load_manifest(manifest_path)

    # ---- 1. serial vs parallel determinism ---------------------------
    section("1. serial vs parallel byte-identical execution")
    serial_dir = RUNS / "serial"
    parallel_dir = RUNS / "parallel"
    serial = run(manifest_path, DATA, serial_dir, serial=True)
    parallel = run(manifest_path, DATA, parallel_dir, workers=4)
    same_summary = json.dumps(serial.summary, sort_keys=True) == \
        json.dumps(parallel.summary, sort_keys=True)
    same_files = _hash_tree(serial_dir) == _hash_tree(parallel_dir)
    record("summary byte-identical", same_summary,
           serial.summary["summary_fingerprint"][:16])
    record("artifact tree byte-identical", same_files,
           f"{len(_hash_tree(serial_dir))} files")

    # ---- 2. graceful stop and resume ----------------------------------
    section("2. graceful stop then resume")
    stop_dir = RUNS / "stop_resume"
    request_stop(stop_dir)
    stopped = run(manifest_path, DATA, stop_dir, serial=True)
    (stop_dir / "STOP").unlink()
    record("stop honoured before next shard", stopped.stopped,
           stopped.summary["totals"])
    resumed = run(manifest_path, DATA, stop_dir, serial=True)
    resume_match = json.dumps(resumed.summary, sort_keys=True) == \
        json.dumps(serial.summary, sort_keys=True)
    resume_files = _hash_tree(stop_dir) == _hash_tree(serial_dir)
    record("resume summary matches clean run", resume_match, "")
    record("resume artifacts match clean run", resume_files, "")

    # ---- 3. version invalidation propagation --------------------------
    section("3. version change -> earliest-stage invalidation")
    import copy
    changed = copy.deepcopy(manifest)
    target_layer = changed.shard_by_id("shard_01").layers[0]
    object.__setattr__(target_layer, "bitwidth", 8)
    tmp_manifest = RUNS / "manifest_bitwidth8.yaml"
    tmp_manifest.write_text(yaml.safe_dump(_manifest_dict(changed),
                                           sort_keys=True))
    changed_manifest = load_manifest(tmp_manifest)
    store = CheckpointStore(stop_dir)
    result = run_shard_pipeline(
        changed_manifest.shard_by_id("shard_01"), DATA, changed_manifest,
        store, run_fingerprint(changed_manifest, DATA))
    invalidated = [item["stage"] for item in result.invalidations]
    reused = {stage.stage: stage.reused for stage in result.stages}
    record("validate reused after bitwidth change",
           reused.get("validate") is True, reused)
    record("quantize/linear/verify invalidated",
           invalidated == ["quantize", "linear", "verify"], invalidated)

    # ---- 4. budget rejection codes ------------------------------------
    section("4. budget constrained scheduling")
    tight = run(DATA / "manifest_tight.yaml", DATA,
                RUNS / "tight_budget", serial=True)
    tight_codes = sorted({shard.get("reason_code") for shard in
                          tight.summary["shards"] if shard["status"] ==
                          "rejected"})
    record("compute/quota budget codes observed",
           bool(set(tight_codes) & {E_WORKER_QUOTA, "E_COMPUTE_BUDGET"}),
           tight_codes)
    tiny = run(DATA / "manifest_tiny_memory.yaml", DATA,
               RUNS / "tiny_memory", serial=True)
    mem_codes = {shard.get("reason_code") for shard in
                 tiny.summary["shards"] if shard["status"] == "rejected"}
    record("memory budget code observed", E_MEMORY_BUDGET in mem_codes,
           sorted(mem_codes))

    # ---- 5. injected business error codes -----------------------------
    section("5. injected business error codes")
    nan_run = run(manifest_path, DATA, RUNS / "nan_check", serial=True)
    nan_codes = {error["code"] for error in nan_run.summary["errors"]}
    record("NaN/Inf -> E_NUMERIC_INPUT",
           E_NUMERIC_INPUT in nan_codes, sorted(nan_codes))

    # E_NUMERIC_MISMATCH via impossible tolerance
    bad_tol = copy.deepcopy(manifest)
    for layer in bad_tol.shard_by_id("shard_01").layers:
        object.__setattr__(layer, "rel_tol", 1e-12)
        object.__setattr__(layer, "abs_tol", 1e-12)
    bad_path = RUNS / "manifest_tight_tol.yaml"
    bad_path.write_text(yaml.safe_dump(_manifest_dict(bad_tol),
                                       sort_keys=True))
    bad_manifest = load_manifest(bad_path)
    store2 = CheckpointStore(RUNS / "mismatch_check")
    try:
        run_shard_pipeline(bad_manifest.shard_by_id("shard_01"), DATA,
                           bad_manifest, store2,
                           run_fingerprint(bad_manifest, DATA))
        mismatch_ok = False
    except NumericMismatchError as exc:
        mismatch_ok = exc.code == E_NUMERIC_MISMATCH and \
            exc.details["layer"] == "fc1"
    record("tolerance breach -> E_NUMERIC_MISMATCH", mismatch_ok,
           "layer fc1 max deviation reported")

    # E_INVALID_BITWIDTH
    bad_bw = copy.deepcopy(manifest)
    object.__setattr__(bad_bw.shard_by_id("shard_00").layers[0],
                       "bitwidth", 3)
    bw_path = RUNS / "manifest_bad_bw.yaml"
    bw_path.write_text(yaml.safe_dump(_manifest_dict(bad_bw),
                                      sort_keys=True))
    try:
        load_manifest(bw_path)
        bw_ok = False
    except Exception as exc:  # InvalidBitwidthError
        bw_ok = getattr(exc, "code", None) == E_INVALID_BITWIDTH
    record("illegal bitwidth -> E_INVALID_BITWIDTH", bw_ok, "bitwidth=3")

    # E_STALE_CHECKPOINT via out-of-order publish attempt
    store3 = CheckpointStore(RUNS / "stale_check",
                             enforce_contiguous=True)
    store3.commit(build_record(
        0, "shard_00", 0, "validate", "in", 1, "fp", {}, "run"))
    try:
        store3.commit(build_record(
            9, "shard_02", 2, "validate", "in", 1, "fp", {}, "run"))
        stale_ok = False
    except StaleCheckpointError as exc:
        stale_ok = exc.code == E_STALE_CHECKPOINT
    record("out-of-order publish -> E_STALE_CHECKPOINT", stale_ok,
           "required contiguous sequence")

    # ---- audit export --------------------------------------------------
    section("6. audit report and status")
    from qoplib.audit import export_audit_report, query_status
    report_path = export_audit_report(serial_dir)
    record("audit report exported", report_path.exists(), str(report_path))
    status = query_status(serial_dir)
    record("continuous sequence 0..12",
           status["latest_sequence"] == 12,
           f"latest={status['latest_sequence']}")

    report["all_passed"] = all(item["ok"] for item in report["checks"])
    report["totals"] = serial.summary["totals"]
    out = ART / "demo_report.json"
    out.write_text(canonical_json_text(report) + "\n", encoding="utf-8")
    section(f"demo report -> {out}")
    print(canonical_json_text({"all_passed": report["all_passed"],
                               "checks": len(report["checks"]),
                               "totals": report["totals"]}))
    return 0 if report["all_passed"] else 1


def _manifest_dict(manifest) -> Dict[str, Any]:
    return {
        "config_version": manifest.config_version,
        "budget": manifest.budget.to_dict(),
        "default_tolerance": manifest.default_tolerance,
        "shards": [
            {
                "shard_id": shard.shard_id,
                "index": shard.index,
                "npz_file": shard.npz_file,
                "batch": shard.batch,
                "input_features": shard.input_features,
                "expected_output_shape": shard.expected_output_shape,
                "expected_rel_error_bound": shard.expected_rel_error_bound,
                "layers": [layer.to_dict() for layer in shard.layers],
            }
            for shard in manifest.shards
        ],
    }


if __name__ == "__main__":
    raise SystemExit(main())
