#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import os
import platform
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import base64
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qos_workbench.codecs import (  # noqa: E402
    decode_binary,
    decode_jsonl,
    encode_binary,
    encode_jsonl,
)
from qos_workbench.store import SnapshotStore  # noqa: E402


SAMPLES = ROOT / "samples"
REPORTS = ROOT / "reports"


def policy_set_sha(snapshot) -> str:
    payload = json.dumps(
        [policy.canonical_dict() for policy in snapshot.policies],
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def dependency_versions() -> dict[str, str]:
    import fastapi
    import numpy
    import pytest
    import uvicorn

    return {
        "python": platform.python_version(),
        "fastapi": fastapi.__version__,
        "uvicorn": uvicorn.__version__,
        "numpy": numpy.__version__,
        "pytest": pytest.__version__,
    }


def run_format_validation() -> dict:
    json_v3 = decode_jsonl((SAMPLES / "policies_v3.jsonl").read_bytes())
    little_v3 = decode_binary((SAMPLES / "policies_v3_little_endian.bin").read_bytes())
    big_v3 = decode_binary((SAMPLES / "policies_v3_big_endian.bin").read_bytes())
    json_v1 = decode_jsonl((SAMPLES / "policies_v1.jsonl").read_bytes())
    binary_v1 = decode_binary((SAMPLES / "policies_v1_little_endian.bin").read_bytes())
    truncated = decode_jsonl(
        (SAMPLES / "policies_v3.jsonl").read_bytes(), timestamp_truncation="floor_ms"
    )
    return {
        "v3_json_vs_little_vs_big_equal": json_v3.snapshot
        == little_v3.snapshot
        == big_v3.snapshot,
        "snapshot_checksum": json_v3.snapshot.checksum,
        "little_endian": little_v3.import_report["endianness"],
        "big_endian": big_v3.import_report["endianness"],
        "jsonl_record_crc_equivalent": json_v3.snapshot.checksum
        == little_v3.snapshot.checksum,
        "v1_json_vs_binary_equal_after_degrade": json_v1.snapshot == binary_v1.snapshot,
        "v1_decisions": {
            "jsonl": json_v1.negotiation.decision,
            "binary": binary_v1.negotiation.decision,
        },
        "v1_degrade_changes": len(json_v1.import_report["degrade_details"]),
        "floor_ms_cumulative_rounding_error_us": truncated.import_report[
            "timestamp_normalization"
        ]["cumulative_rounding_error_us"],
    }


def run_store_and_simulation_validation(workdir: Path) -> tuple[dict, dict]:
    database = workdir / "main.db"
    store = SnapshotStore(database)
    source = (SAMPLES / "policies_v3.jsonl").read_bytes()
    first_import = store.import_policy_content(source)
    first_seq, first_snapshot = store.current_snapshot()

    binary_text = encode_binary(first_snapshot, version=3, endianness="big")
    second_import = store.import_policy_content(
        binary_text, snapshot_no_override=2, format_name="binary"
    )
    json_text = encode_jsonl(first_snapshot, version=3)
    third_import = store.import_policy_content(
        json_text, snapshot_no_override=3, format_name="jsonl"
    )
    current_seq, current_snapshot = store.current_snapshot()
    policy_hashes = {
        first_seq: policy_set_sha(first_snapshot),
        second_import["commit"]["snapshot_seq"]: policy_set_sha(
            decode_binary(binary_text).snapshot
        ),
        third_import["commit"]["snapshot_seq"]: policy_set_sha(
            decode_jsonl(json_text).snapshot
        ),
    }

    traffic = [
        json.loads(line, parse_float=str)
        for line in (SAMPLES / "traffic.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    simulation_one = store.run_simulation(traffic, relative_tolerance="0.10")
    simulation_two = store.run_simulation(traffic, relative_tolerance="0.10")
    packets_match = (
        simulation_one["simulation"]["packets"]
        == simulation_two["simulation"]["packets"]
    )
    per_queue_match = (
        simulation_one["simulation"]["per_queue"]
        == simulation_two["simulation"]["per_queue"]
    )
    diagnostics = simulation_one["diagnostics"]

    store_report = {
        "snapshot_sequence_after_three_imports": current_seq,
        "snapshot_no": current_snapshot.snapshot_no,
        "current_snapshot_checksum": current_snapshot.checksum,
        "policy_content_equal_across_formats": len(set(policy_hashes.values())) == 1,
        "policy_content_sha256": next(iter(policy_hashes.values())),
        "import_report_formats": [
            first_import["import_report"]["format"],
            second_import["import_report"]["format"],
            third_import["import_report"]["format"],
        ],
        "persisted_snapshot_count": len(store.list_snapshots()),
    }
    simulation_report = {
        "batch_ids": [simulation_one["batch_id"], simulation_two["batch_id"]],
        "packet_count": simulation_one["simulation"]["packet_count"],
        "packets_identical_on_repeat": packets_match,
        "per_queue_identical_on_repeat": per_queue_match,
        "egress_color_counts": {
            color: sum(
                1
                for packet in simulation_one["simulation"]["packets"]
                if packet["egress_color"] == color
            )
            for color in ("green", "yellow", "red")
        },
        "per_queue_sent_dropped_marked": {
            queue_id: {
                "sent_bytes": stats["sent_bytes"],
                "dropped_bytes": stats["dropped_bytes"],
                "marked_bytes": stats["marked_bytes"],
            }
            for queue_id, stats in simulation_one["simulation"]["per_queue"].items()
        },
        "diagnostics": {
            "over_shaped_queue_ids": diagnostics["over_shaped_queue_ids"],
            "under_shaped_queue_ids": diagnostics["under_shaped_queue_ids"],
            "stable_queue_ids": diagnostics["stable_queue_ids"],
            "no_traffic_queue_ids": diagnostics["no_traffic_queue_ids"],
            "relative_tolerance": diagnostics["thresholds"]["relative_tolerance"],
            "precision_contract": diagnostics["precision_contract"],
        },
    }
    return store_report, simulation_report


def run_forced_termination_validation(workdir: Path) -> dict:
    database = workdir / "crash.db"
    store = SnapshotStore(database)
    source = decode_jsonl((SAMPLES / "policies_v3.jsonl").read_bytes()).snapshot
    store.commit_snapshot(source)

    child_code = r"""
import os
from qos_workbench.store import SnapshotStore

db = os.environ["QOS_VALIDATION_CRASH_DB"]
store = SnapshotStore(db, recover_on_start=False)
store.begin_intent(
    kind="simulation",
    snapshot_no=1,
    snapshot_seq=1,
    subject_id="validation-crashed-batch",
    payload_sha256="a" * 64,
    payload={
        "sample_sha256": "a" * 64,
        "snapshot_seq": 1,
        "snapshot_no": 1,
        "request": {"samples": [{"queue_id": "voice", "packet_bytes": 1000}]},
    },
)
conn = store._connect()
conn.execute("BEGIN IMMEDIATE")
conn.execute(
    "INSERT INTO snapshots(snapshot_no, format_version, generated_at_us, checksum, policy_count, created_at_us) "
    "VALUES(99, 3, '1700000000000000', 'uncommitted', 1, 1700000000000000)"
)
seq = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
conn.execute(
    "INSERT INTO snapshot_policies(snapshot_seq, queue_id, cir_bps, pir_bps, cbs_bytes, pbs_bytes, priority, "
    "match_rule, parent_queue_id, timestamp_us) VALUES(?,?,?,?,?,?,?,?,?,?)",
    (seq, "ghost", "1", "1", "1", "1", 1, "all", None, "1"),
)
conn.execute(
    "INSERT INTO simulation_batches(batch_id, snapshot_seq, snapshot_no, status, sample_sha256, request_json, "
    "created_at_us) VALUES('uncommitted-running', 1, 1, 'running', 'b', '{}', 1)"
)
os._exit(91)
"""
    env = os.environ.copy()
    env["QOS_VALIDATION_CRASH_DB"] = str(database)
    env["PYTHONPATH"] = str(ROOT)
    process = subprocess.run(
        [sys.executable, "-c", child_code],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    recovered = SnapshotStore(database)
    report = recovered.recovery_report
    conn = sqlite3.connect(database)
    try:
        snapshot_numbers = [
            row[0] for row in conn.execute("SELECT snapshot_no FROM snapshots ORDER BY snapshot_no")
        ]
        batch = conn.execute(
            "SELECT status, result_json, diagnostics_json FROM simulation_batches "
            "WHERE batch_id='validation-crashed-batch'"
        ).fetchone()
        intent = conn.execute(
            "SELECT state FROM intents ORDER BY created_at_us DESC LIMIT 1"
        ).fetchone()
    finally:
        conn.close()
    return {
        "child_exit_code": process.returncode,
        "expected_abrupt_exit_code": 91,
        "recovery_performed": report["recovery_performed"],
        "started_intents_found": report["started_intents_found"],
        "last_complete_snapshot": report["last_complete_snapshot"],
        "active_snapshot_no_after_recovery": recovered.current_snapshot()[1].snapshot_no,
        "persisted_snapshot_numbers": snapshot_numbers,
        "crashed_batch": {
            "status": batch[0],
            "result_json_is_null": batch[1] is None,
            "diagnostics_json_is_null": batch[2] is None,
        },
        "crashed_intent_state": intent[0],
        "facts_only": report["facts_only"],
        "report_limitations": report["report_limitations"],
    }


def run_pytest() -> dict:
    process = subprocess.run(
        [sys.executable, "-m", "pytest"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    output = process.stdout + process.stderr
    (REPORTS / "pytest.txt").write_text(output, encoding="utf-8")
    summary_line = next(
        (line for line in reversed(output.splitlines()) if "passed" in line or "failed" in line),
        "",
    )
    return {
        "exit_code": process.returncode,
        "summary": summary_line,
        "log": "reports/pytest.txt",
    }


def run_live_http_validation(workdir: Path) -> dict:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        port = int(probe.getsockname()[1])
    database = workdir / "live-http.db"
    env = os.environ.copy()
    env.update(
        {
            "QOS_HOST": "127.0.0.1",
            "QOS_PORT": str(port),
            "QOS_DB_PATH": str(database),
            "PYTHONPATH": str(ROOT),
        }
    )
    process = subprocess.Popen(
        [sys.executable, "-m", "qos_workbench"],
        cwd=ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    base_url = f"http://127.0.0.1:{port}"

    def request_json(path: str, *, method: str = "GET", payload: dict | None = None) -> dict:
        data = None
        headers = {}
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(
            base_url + path, data=data, headers=headers, method=method
        )
        with urllib.request.urlopen(request, timeout=5) as response:
            return json.loads(response.read().decode("utf-8"))

    try:
        ready = False
        for _ in range(80):
            if process.poll() is not None:
                break
            try:
                health_before = request_json("/api/health")
                ready = True
                break
            except (urllib.error.URLError, TimeoutError, ConnectionError):
                time.sleep(0.05)
        if not ready:
            raise RuntimeError("live Uvicorn process did not become ready")

        source = (SAMPLES / "policies_v3.jsonl").read_bytes()
        imported = request_json(
            "/api/policies/import",
            method="POST",
            payload={
                "format": "auto",
                "content_base64": base64.b64encode(source).decode("ascii"),
                "apply_snapshot": True,
            },
        )
        simulated = request_json(
            "/api/simulations/from-sample/traffic.jsonl", method="POST"
        )
        with urllib.request.urlopen(
            base_url + "/api/policies/export?format=binary&version=3&endianness=big",
            timeout=5,
        ) as response:
            exported_bytes = response.read()
            exported_snapshot_header = response.headers.get("X-QoS-Snapshot-No")
        diagnostics = request_json("/api/diagnostics/latest")
        health_after = request_json("/api/health")
        return {
            "listen_host": "127.0.0.1",
            "listen_port": port,
            "server_ready": ready,
            "health_before_import_has_snapshot": health_before["active_snapshot"]
            is not None,
            "import_http_ok": imported["ok"],
            "import_snapshot_no": imported["commit"]["snapshot_no"],
            "simulation_http_ok": simulated["ok"],
            "simulation_packet_count": simulated["simulation"]["packet_count"],
            "export_size_bytes": len(exported_bytes),
            "export_magic_and_version": exported_bytes[:5].hex(),
            "export_endian_marker": exported_bytes[5],
            "export_snapshot_header": exported_snapshot_header,
            "diagnostics_batch_id": diagnostics["batch_id"],
            "health_after_import_snapshot_no": health_after["active_snapshot"][
                "snapshot_no"
            ],
        }
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def main() -> None:
    REPORTS.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="qos-validation-") as temporary:
        workdir = Path(temporary)
        store_report, simulation_report = run_store_and_simulation_validation(workdir)
        recovery_report = run_forced_termination_validation(workdir)
    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "environment": dependency_versions(),
        "database_engine": sqlite3.sqlite_version,
        "loopback_only": True,
        "external_services_used": False,
        "samples": {
            "directory": "samples",
            "policy_files": sorted(path.name for path in SAMPLES.glob("policies_*")),
            "traffic_file": "traffic.jsonl",
        },
        "format_validation": run_format_validation(),
        "store_validation": store_report,
        "simulation_validation": simulation_report,
        "forced_termination_recovery_validation": recovery_report,
        "live_http_validation": run_live_http_validation(workdir),
        "pytest": run_pytest(),
    }
    report["acceptance"] = {
        "dual_format_interoperability": report["format_validation"][
            "v3_json_vs_little_vs_big_equal"
        ]
        and report["store_validation"]["policy_content_equal_across_formats"],
        "deterministic_simulation": report["simulation_validation"][
            "packets_identical_on_repeat"
        ]
        and report["simulation_validation"]["per_queue_identical_on_repeat"],
        "forced_termination_recovery": report["forced_termination_recovery_validation"][
            "child_exit_code"
        ]
        == 91
        and report["forced_termination_recovery_validation"]["persisted_snapshot_numbers"]
        == [1]
        and report["forced_termination_recovery_validation"]["crashed_batch"]["status"]
        == "aborted"
        and report["forced_termination_recovery_validation"]["crashed_batch"][
            "result_json_is_null"
        ],
        "pytest": report["pytest"]["exit_code"] == 0,
        "live_loopback_http": report["live_http_validation"]["import_http_ok"]
        and report["live_http_validation"]["simulation_http_ok"]
        and report["live_http_validation"]["export_magic_and_version"]
        == (b"QOSB\x03").hex(),
    }
    report["all_acceptance_checks_passed"] = all(report["acceptance"].values())
    (REPORTS / "validation_report.json").write_text(
        json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    print(
        json.dumps(
            {
                "report": "reports/validation_report.json",
                "acceptance": report["acceptance"],
                "all_acceptance_checks_passed": report["all_acceptance_checks_passed"],
            },
            indent=2,
        )
    )
    if not report["all_acceptance_checks_passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

