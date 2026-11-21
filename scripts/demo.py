"""Reproducible end-to-end demonstration (loopback only).

Run with::

    python scripts/demo.py

The script uses deterministic in-repo samples and a fresh local data
directory, exercises every required scenario and prints structured results
(import report, cascade rebuild scope, recovery conclusion) for manual review.
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
import urllib.error
import urllib.request

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from exp_tracker import binary, jsonl_fmt, negotiation, samples
from exp_tracker.server import build_server
from exp_tracker.store import ExperimentStore


def call(base_url: str, method: str, path: str, payload=None):
    data = None
    headers = {"Content-Type": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(base_url + path, data=data,
                                     headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


def heading(title: str) -> None:
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def main() -> int:
    workdir = tempfile.mkdtemp(prefix="exp_tracker_demo_")
    sample_dir = os.path.join(workdir, "samples")
    db_path = os.path.join(workdir, "tracker.db")
    print(f"workdir: {workdir}")

    # ---------------------------------------------------------- 1. samples
    heading("1. deterministic samples (seed=%d)" % samples.SEED)
    sample_paths = samples.generate_sample_files(sample_dir)
    runs = samples.build_sample_runs()
    for name, path in sample_paths.items():
        print(f"  {name:24s} -> {os.path.basename(path)} "
              f"({os.path.getsize(path)} bytes)")
    print(f"  generated {len(runs)} runs; run-003 params omitted for back-fill")

    server = build_server(db_path, "127.0.0.1", 0)
    thread = server.serve_in_thread()
    base_url = f"http://127.0.0.1:{server.server_address[1]}"
    print(f"loopback server: {base_url}")

    # ------------------------------------------------ 2. binary export/import
    heading("2. binary export then cross-format import")
    status, pre = call(base_url, "POST", "/precheck",
                       {"path": sample_paths["binary_v2"]})
    print("  precheck binary v2:", status, pre["verdict"], "-", pre["reason"])
    status, report = call(base_url, "POST", "/imports",
                          {"path": sample_paths["binary_v2"], "report_id": "rep-bin"})
    print("  binary import committed:", report["committed"], "counts:", report["counts"])

    # Round-trip fidelity check through both codecs.
    decoded_bin = [run for _, run in binary.decode_file(sample_paths["binary_v2"])]
    decoded_json = [run for _, run in jsonl_fmt.decode_file(sample_paths["jsonl_v2"])]
    assert decoded_bin[0].metric_signature() == decoded_json[0].metric_signature()
    print("  field-level round-trip signature match: binary v2 == jsonl v2")

    # --------------------------------------------- 3. jsonl export + downgrade
    heading("3. jsonl v1 import (downgrade accept, params back-filled)")
    status, pre = call(base_url, "POST", "/precheck",
                       {"path": sample_paths["jsonl_v1_omit_params"]})
    print("  precheck jsonl v1:", status, pre["verdict"], "-", pre["reason"])
    runs_dg, neg = negotiation.decode_with_negotiation(sample_paths["jsonl_v1_omit_params"])
    backfilled = [r.run_id for r in runs_dg if r.downgrades and not r.params.get("__none__")]
    print("  default params back-filled for runs (e.g. run-003):",
          next(r.params for r in runs_dg if r.run_id == "run-003"))

    # Binary v1 uses ms precision -> downgrade path.
    status, pre_v1 = call(base_url, "POST", "/precheck",
                          {"path": sample_paths["binary_v1"]})
    print("  precheck binary v1:", status, pre_v1["verdict"], "-", pre_v1["reason"],
          "precision:", pre_v1["precision"])

    # --------------------------------------------------- 4. CRC tamper failure
    heading("4. tampered CRC import must fail with E_RECORD_CORRUPT")
    tampered = os.path.join(sample_dir, "tampered.etrk")
    shutil.copyfile(sample_paths["binary_v2"], tampered)
    samples.tamper_binary_crc(tampered, target_record=1)
    revision_before = server.store.revision()
    status, err = call(base_url, "POST", "/imports",
                       {"path": tampered, "report_id": "rep-bad"})
    revision_after = server.store.revision()
    print("  HTTP status:", status)
    print("  error code:", err["error"]["code"])
    print("  context:", err["error"]["context"])
    print("  revision unchanged:", revision_before, "->", revision_after)

    # ------------------------------------------ 5. conflict report (skip case)
    heading("5. re-import same batch -> deterministic skip conflict report")
    status, report2 = call(base_url, "POST", "/imports",
                           {"path": sample_paths["binary_v2"], "report_id": "rep-again"})
    print("  committed:", report2["committed"], "counts:", report2["counts"])

    # --------------------------------------------- 5b. normalization
    heading("5b. ms/us timestamp normalization with rounding accounting")
    status, norm = call(base_url, "POST", "/normalize", {
        "rule": "truncate",
        "records": [
            {"run_id": "binary-ms", "unit": "ms", "time_ms": 1234},
            {"run_id": "json-us", "unit": "us", "time_us": 1234700},
        ],
    })
    for item in norm["records"]:
        print(f"  {item['run_id']:10s} -> projected_us={item['projected_us']} "
              f"rounded_digits={item['rounded_digits']} discarded_us={item['discarded_us']} "
              f"cumulative_error_us={item['cumulative_error_us']}")

    # ------------------------------------------------- 6. cascade rebuild
    heading("6. metric summary cascade rebuild")
    status, summaries_before = call(base_url, "GET", "/summaries")
    print("  summary groups before:", len(summaries_before["summaries"]))
    status, cascade = call(base_url, "POST", "/cascade", {"experiment": "vision"})
    print("  rebuilt summaries:", cascade["rebuilt_summaries"])
    print("  reindexed run ids:", cascade["reindexed_run_ids"])
    status, param_index = call(base_url, "GET", "/params?key=batch_size")
    print("  param index 'batch_size' ->", param_index["run_ids"])

    # ------------------------------------------------ 7. snapshot replacement
    heading("7. snapshot whole-replacement (invalid snapshot keeps old state)")
    snap_path = os.path.join(workdir, "snapshot.json")
    status, snap = call(base_url, "POST", "/snapshots/export", {"path": snap_path})
    print("  exported snapshot seq:", snap["snapshot_seq"], "runs:", snap["run_count"])
    bad_snap = os.path.join(workdir, "bad_snapshot.json")
    with open(bad_snap, "w", encoding="utf-8") as handle:
        handle.write("{not valid json")
    status, err = call(base_url, "POST", "/snapshots/load", {"path": bad_snap})
    print("  invalid snapshot ->", err["error"]["code"], "(old state retained)")
    status, loaded = call(base_url, "POST", "/snapshots/load", {"path": snap_path,
                                                                "request_id": "snap-1"})
    print("  valid snapshot reloaded at seq:", loaded["loaded_snapshot_seq"])

    # Idempotent duplicate request id reuses the stored result.
    status, loaded_again = call(base_url, "POST", "/snapshots/load",
                                {"path": snap_path, "request_id": "snap-1"})
    print("  duplicate request id reused same seq:", loaded_again["loaded_snapshot_seq"])

    # ------------------------------------------- 8. forced-kill recovery
    heading("8. forced termination then restart recovery")
    crash_db = os.path.join(workdir, "crash.db")
    crash_store = ExperimentStore(crash_db)
    crash_store.submit_run(runs[0], request_id="req-good")
    rev_before_kill = crash_store.revision()
    # Persist an intent with no following commit, mimicking SIGKILL mid-write.
    crash_store.mark_crash_point("req-trapped", "import", {"report_id": "rep-trap"})
    crash_store.close()
    print(f"  killed while intent 'req-trapped' pending; revision={rev_before_kill}")

    revived = ExperimentStore(crash_db)
    recovery = revived.recovery_report.to_dict()
    print("  recovered revision:", recovery["recovered_revision"])
    print("  aborted intents:", recovery["aborted_intents"])
    for fact in recovery["facts"]:
        print("   -", fact)
    run_still_there = revived.get_run(runs[0].run_id) is not None
    print("  last complete run still present:", run_still_there)
    revived.close()

    heading("DEMO COMPLETE")
    print("All scenarios executed against loopback only; no external services used.")
    server.shutdown()
    thread.join(timeout=5)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
