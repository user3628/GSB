from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, Optional, Tuple


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from kvstore.codec_binary import decode_binary


SAMPLES = ROOT / "samples"
DEMO_ROOT = ROOT / "artifacts" / "demo"
DATA_DIR = DEMO_ROOT / "data"
EXPORT_PATH = DEMO_ROOT / "exported-segment.kvsb"
CRASH_MARKER = DEMO_ROOT / "crash-marker.json"
REPORT_PATH = DEMO_ROOT / "interop-report.json"


def main() -> int:
    _reset_demo_dir()
    port = _free_port()
    base_url = f"http://127.0.0.1:{port}"
    phases: Dict[str, Any] = {}
    log_handle = None
    server = None
    try:
        server, log_handle = _start_server(port)
        phases["health"] = _http_json("GET", base_url + "/health")[1]

        binary_bytes = (SAMPLES / "binary_sample.kvsb").read_bytes()
        _, binary_import = _http_json(
            "POST",
            base_url + "/v1/import/binary",
            data=binary_bytes,
            headers={
                "Content-Type": "application/octet-stream",
                "X-Batch-Id": "demo-binary-sample-001",
            },
        )
        phases["binary_import"] = {
            "bytes": len(binary_bytes),
            "negotiation": binary_import["negotiation"],
            "summary": binary_import["summary"],
        }

        exported = _http_raw("GET", base_url + "/v1/export/binary")
        EXPORT_PATH.write_bytes(exported)
        decoded_export = decode_binary(exported)
        phases["binary_export"] = {
            "path": str(EXPORT_PATH.relative_to(ROOT)),
            "bytes": len(exported),
            "record_count": len(decoded_export.records),
            "checksums_verified": decoded_export.negotiation["checksums_verified"],
        }

        jsonl_bytes = (SAMPLES / "jsonl_sample.jsonl").read_bytes()
        _, json_import = _http_json(
            "POST",
            base_url + "/v1/import/jsonl",
            data=jsonl_bytes,
            headers={
                "Content-Type": "application/x-ndjson",
                "X-Batch-Id": "demo-jsonl-sample-001",
            },
        )
        phases["json_import"] = {
            "bytes": len(jsonl_bytes),
            "negotiation": json_import["negotiation"],
            "summary": json_import["summary"],
            "conflicts": json_import["conflicts"],
            "records": json_import["records"],
        }

        _, snapshot = _http_json("POST", base_url + "/v1/snapshot/switch", payload={})
        _, index_before_crash = _http_json("GET", base_url + "/v1/index")
        phases["snapshot_switch"] = {"result": snapshot, "index": index_before_crash}

        _stop_process(server)
        server = None
        if log_handle is not None:
            log_handle.close()
            log_handle = None

        crash_result = _force_crash_worker()
        phases["forced_termination"] = crash_result

        server, log_handle = _start_server(port)
        status, recovery = _http_json("GET", base_url + "/v1/recovery/report")
        _, index_after_recovery = _http_json("GET", base_url + "/v1/index")
        pending_status, pending_body = _http_json(
            "GET", base_url + "/v1/kv/crash:pending", allowed_statuses={404}
        )
        phases["recovery"] = {
            "http_status": status,
            "report": recovery["report"],
            "index": index_after_recovery,
            "pending_key_http_status": pending_status,
            "pending_key_response": pending_body,
        }

        checks = {
            "binary_import_committed_4": binary_import["summary"]["committed_records"] == 4,
            "binary_export_roundtrip_4": len(decoded_export.records) == 4,
            "json_import_overwrite_3_skip_2_reject_2": (
                json_import["summary"]["overwrite"],
                json_import["summary"]["skip"],
                json_import["summary"]["reject"],
            )
            == (3, 2, 2),
            "json_ttl_converted": json_import["negotiation"]["expiry_semantics"] == "relative_ttl",
            "snapshot_seq_1": snapshot["snapshot_seq"] == 1,
            "recovery_rolled_back_one": recovery["report"]["counts"]["rolled_back"] == 1,
            "pending_key_absent": pending_body.get("error", {}).get("code") == "E_KEY_NOT_FOUND",
            "final_live_keys_5": index_after_recovery["live_keys"] == 5,
            "all_index_entries_active_segment": all(
                entry["key"] != "crash:pending" for entry in index_after_recovery["keys"]
            ),
        }
        phases["checks"] = checks
        phases["ok"] = all(checks.values())
        REPORT_PATH.write_text(
            json.dumps(phases, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        _print_human_summary(phases)
        return 0 if phases["ok"] else 1
    finally:
        if server is not None:
            _stop_process(server)
        if log_handle is not None:
            log_handle.close()


def _reset_demo_dir() -> None:
    if DEMO_ROOT.exists():
        shutil.rmtree(DEMO_ROOT)
    DEMO_ROOT.mkdir(parents=True, exist_ok=True)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _start_server(port: int) -> Tuple[subprocess.Popen, Any]:
    log_path = DEMO_ROOT / "server.log"
    log_handle = log_path.open("ab")
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT) + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "kvstore.server",
            "--data-dir",
            str(DATA_DIR),
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ],
        cwd=str(ROOT),
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        env=env,
    )
    deadline = time.time() + 15
    while time.time() < deadline:
        if proc.poll() is not None:
            log_handle.flush()
            raise RuntimeError(f"server exited early; see {log_path}")
        try:
            status, body = _http_json("GET", f"http://127.0.0.1:{port}/health", timeout=0.5)
            if status == 200 and body.get("ok"):
                return proc, log_handle
        except Exception:
            time.sleep(0.05)
    proc.kill()
    proc.wait(timeout=5)
    log_handle.close()
    raise RuntimeError(f"server did not become healthy; see {log_path}")


def _stop_process(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def _force_crash_worker() -> Dict[str, Any]:
    if CRASH_MARKER.exists():
        CRASH_MARKER.unlink()
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT) + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "kvstore.crash_worker",
            "--data-dir",
            str(DATA_DIR),
            "--marker",
            str(CRASH_MARKER),
        ],
        cwd=str(ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    deadline = time.time() + 15
    try:
        while time.time() < deadline and not CRASH_MARKER.exists():
            if proc.poll() is not None:
                stdout, stderr = proc.communicate(timeout=1)
                raise RuntimeError(f"crash worker exited early: {stdout}\n{stderr}")
            time.sleep(0.02)
        if not CRASH_MARKER.exists():
            raise RuntimeError("crash worker did not persist an intent before the deadline")
        marker = json.loads(CRASH_MARKER.read_text(encoding="utf-8"))
        pid = proc.pid
        proc.kill()  # TerminateProcess on Windows; SIGKILL on POSIX.
        return_code = proc.wait(timeout=10)
        return {
            "pid": pid,
            "marker": marker,
            "return_code": return_code,
            "forced_kill": True,
        }
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)


def _http_json(
    method: str,
    url: str,
    *,
    payload: Optional[Dict[str, Any]] = None,
    data: Optional[bytes] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: float = 10.0,
    allowed_statuses: Optional[set[int]] = None,
) -> Tuple[int, Dict[str, Any]]:
    request_headers = dict(headers or {})
    body = data
    if payload is not None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request_headers.setdefault("Content-Type", "application/json")
    request = urllib.request.Request(url, data=body, headers=request_headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            status = response.status
            content = response.read()
    except urllib.error.HTTPError as exc:
        status = exc.code
        content = exc.read()
    allowed = allowed_statuses or set(range(200, 300))
    if status not in allowed:
        raise RuntimeError(f"{method} {url} returned {status}: {content.decode('utf-8', 'replace')}")
    return status, json.loads(content.decode("utf-8"))


def _http_raw(method: str, url: str, *, timeout: float = 10.0) -> bytes:
    request = urllib.request.Request(url, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            if response.status != 200:
                raise RuntimeError(f"{method} {url} returned {response.status}")
            return response.read()
    except urllib.error.HTTPError as exc:
        content = exc.read().decode("utf-8", "replace")
        raise RuntimeError(f"{method} {url} returned {exc.code}: {content}") from exc


def _print_human_summary(phases: Dict[str, Any]) -> None:
    print("=== Local segment KV interoperability demo ===")
    print(
        "1. Binary sample import:",
        phases["binary_import"]["summary"]["committed_records"],
        "committed; CRC verified =",
        phases["binary_import"]["negotiation"]["checksums_verified"],
    )
    print(
        "2. Binary export:",
        phases["binary_export"]["path"],
        phases["binary_export"]["bytes"],
        "bytes,",
        phases["binary_export"]["record_count"],
        "records",
    )
    summary = phases["json_import"]["summary"]
    print(
        "3. JSONL import:",
        f"overwrite={summary['overwrite']} skip={summary['skip']} reject={summary['reject']}",
        f"committed={summary['committed_records']} rejected_keys={summary['rejected_keys']}",
    )
    for conflict in phases["json_import"]["conflicts"]:
        print(
            f"   - record {conflict['record_number']} {conflict['key']}: "
            f"{conflict['outcome']} ({conflict['reason']})"
        )
    print(
        "4. Snapshot switch: seq",
        phases["snapshot_switch"]["result"]["snapshot_seq"],
        "live keys",
        phases["snapshot_switch"]["index"]["live_keys"],
    )
    crash = phases["forced_termination"]
    print(
        "5. Forced termination: killed pid",
        crash["pid"],
        "with persisted intent; process return code",
        crash["return_code"],
    )
    report = phases["recovery"]["report"]
    print(
        "6. Restart recovery: rolled back",
        report["counts"]["rolled_back"],
        "transaction(s):",
        [item["txn_id"] for item in report["rolled_back_transactions"]],
    )
    print(
        "   applied transactions:",
        [item["txn_seq"] for item in report["applied_transactions"]],
    )
    print(
        "7. Final index: live keys",
        phases["recovery"]["index"]["live_keys"],
        "snapshot seq",
        phases["recovery"]["index"]["snapshot_seq"],
        "active segment",
        phases["recovery"]["index"]["segment"],
    )
    print("   crash:pending HTTP response:", phases["recovery"]["pending_key_response"]["error"]["code"])
    print("8. All assertions:", "PASS" if phases["ok"] else "FAIL")
    print("Full JSON report:", REPORT_PATH.relative_to(ROOT))


if __name__ == "__main__":
    raise SystemExit(main())
