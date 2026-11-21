"""End-to-end demonstration against a real local HTTP server.

Scenarios required by the task:
  1. binary import
  2. jsonl degraded import (float weights + missing fields)
  3. cyclic import failure (E_DEPENDENCY_CYCLE)
  4. hard-termination recovery (pending intent -> aborted, last snapshot kept)

The script starts uvicorn on 127.0.0.1:8765 itself, so no external service or
network dependency is involved.
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

HOST = "127.0.0.1"


def _free_port() -> int:
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((HOST, 0))
        return sock.getsockname()[1]


PORT = _free_port()
BASE = f"http://{HOST}:{PORT}"
DATA_DIR = ROOT / "data"
DB_PATH = DATA_DIR / "demo.sqlite3"
MARKER = DATA_DIR / "crash.marker"


def http(method: str, path: str, data: bytes | None = None) -> tuple[int, dict | bytes]:
    req = urllib.request.Request(BASE + path, data=data, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = resp.read()
            ctype = resp.headers.get("content-type", "")
            return resp.status, json.loads(body) if "json" in ctype else body
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


def wait_for_server(proc: subprocess.Popen) -> None:
    for _ in range(60):
        if proc.poll() is not None:
            raise RuntimeError("server exited early")
        try:
            status, body = http("GET", "/health")
            if status == 200 and body.get("active_snapshot_seq") is not None or status == 200:
                return
        except OSError:
            time.sleep(0.25)
    raise RuntimeError("server did not become ready")


def start_server() -> subprocess.Popen:
    env = dict(os.environ)
    env["PYTHONPATH"] = str(ROOT)
    env["WORKBENCH_DB"] = str(DB_PATH)
    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "app.main:app",
            "--host",
            HOST,
            "--port",
            str(PORT),
            "--log-level",
            "warning",
        ],
        cwd=ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )


def print_step(title: str, payload: object) -> None:
    print(f"\n=== {title} ===")
    if isinstance(payload, (dict, list)):
        print(json.dumps(payload, indent=2, ensure_ascii=False, default=str))
    else:
        print(payload)


def main() -> int:
    for stale in (
        DB_PATH,
        Path(str(DB_PATH) + "-wal"),
        Path(str(DB_PATH) + "-shm"),
        MARKER,
        ROOT / "samples" / "snapshot_crash_seq4.yaml",
    ):
        stale.unlink(missing_ok=True)

    samples = ROOT / "samples"
    proc = start_server()
    try:
        wait_for_server(proc)

        status, body = http("POST", "/snapshots/import", (samples / "snapshot_v1.bin").read_bytes())
        print_step("1) binary import", {"status": status, "report": body})
        assert status == 200 and body["negotiation"]["decision"] == "accept"

        status, body = http("POST", "/snapshots/import", (samples / "snapshot_v2.jsonl").read_bytes())
        print_step("2) jsonl degraded import", {"status": status, "report": body})
        assert status == 200 and body["negotiation"]["decision"] == "degraded_accept"
        assert body["cumulative_rounding_error"] != 0

        status, body = http("POST", "/snapshots/import", (samples / "snapshot_cycle.jsonl").read_bytes())
        print_step("3) cyclic import rejected", {"status": status, "body": body})
        assert status == 409 and body["error"]["code"] == "E_DEPENDENCY_CYCLE"

        status, body = http("GET", "/snapshots/active")
        print_step("active snapshot still intact after failures", body["stats"])

        status, exported = http("GET", "/snapshots/export/binary")
        print_step("binary export round-trip dry-run", {"status": status, "magic": exported[:4]})
        status, verify = http("POST", "/snapshots/dry-run", exported)
        assert status == 200 and verify["resource_count"] == 5

        status, impact = http("GET", "/diagnostics/impact/svc-gateway")
        print_step("impact analysis", impact)
    finally:
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=10)

    # 4) Hard termination inside an import transaction, then restart + recover.
    crash_code = _run_crashed_import()
    print_step("4a) hard-terminated process exit code", crash_code)
    assert crash_code == 99

    proc = start_server()
    try:
        wait_for_server(proc)
        status, recovery = http("GET", "/recovery")
        print_step("4b) recovery after restart", recovery)
        assert status == 200 and recovery["active_snapshot_seq"] == 2
        assert recovery["aborted_intents"], "expected an aborted pending intent"
        status, active = http("GET", "/snapshots/active")
        print_step("4c) active snapshot after recovery", active["stats"])
        assert active["stats"]["snapshot_seq"] == 2
    finally:
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=10)

    print("\nDEMO OK: all four scenarios completed on 127.0.0.1:%d" % PORT)
    return 0


def _run_crashed_import() -> int:
    """Fork a short-lived process that kills itself mid-import."""
    crash_input = ROOT / "samples" / "snapshot_crash_seq4.yaml"
    yaml_text = (ROOT / "samples" / "snapshot_v3.yaml").read_text(encoding="utf-8")
    crash_input.write_text(yaml_text.replace("@snapshot_seq 3", "@snapshot_seq 4"), encoding="utf-8")
    from app.service import OrchestrationService
    from app.store import Store

    pid = os.fork()
    if pid == 0:
        try:
            store = Store(DB_PATH)
            service = OrchestrationService(store)
            service.crash_during_import(crash_input.read_bytes(), str(MARKER))
        finally:
            os._exit(99)
    _, status = os.waitpid(pid, 0)
    return os.waitstatus_to_exitcode(status)


if __name__ == "__main__":
    raise SystemExit(main())
