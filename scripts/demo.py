from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

SAMPLE_PATH = ROOT / "samples" / "deterministic_leases.json"
RUNTIME_DIR = ROOT / "runtime"
DB_PATH = RUNTIME_DIR / "demo.db"
ARTIFACT_PATH = ROOT / "artifacts" / "demo-report.json"


def main() -> int:
    sample = json.loads(SAMPLE_PATH.read_text(encoding="utf-8"))
    _clean_state()
    process = _start_server()
    base_url = f"http://127.0.0.1:{process.port}"
    steps: list[dict[str, Any]] = []
    try:
        _wait_ready(base_url)

        loaded = _call(base_url, "PUT", "/v1/snapshot/replace", {
            "base_version": 0,
            "leases": sample["leases"],
        })
        _record(steps, "full_snapshot_load", loaded)

        acquired = _call(base_url, "POST", "/v1/locks/acquire", {
            "lock_key": "exclusive",
            "holder": "demo-holder-a",
            "ttl_ms": 5000,
        })
        _record(steps, "acquire_free_lock", acquired)

        held = _call(base_url, "POST", "/v1/locks/acquire", {
            "lock_key": "exclusive",
            "holder": "demo-holder-b",
            "ttl_ms": 5000,
        })
        _record(steps, "acquire_held_lock", held)
        _expect_code(held, "E_LOCK_HELD")

        stale = _call(base_url, "POST", "/v1/locks/renew", {
            "lock_key": "exclusive",
            "holder": "demo-holder-a",
            "lease_version": 999,
            "ttl_ms": 5000,
        })
        _record(steps, "renew_with_stale_version", stale)
        _expect_code(stale, "E_STALE_LEASE")

        renewed = _call(base_url, "POST", "/v1/locks/renew", {
            "lock_key": "exclusive",
            "holder": "demo-holder-a",
            "lease_version": acquired["lease"]["lease_version"],
            "ttl_ms": 5000,
        })
        _record(steps, "renew_current_lease", renewed)

        released = _call(base_url, "POST", "/v1/locks/release", {
            "lock_key": "exclusive",
            "holder": "demo-holder-a",
            "lease_version": renewed["lease"]["lease_version"],
        })
        _record(steps, "release_current_lease", released)

        current = _call(base_url, "GET", "/v1/snapshot")["snapshot_version"]
        incremental = _call(base_url, "POST", "/v1/snapshot/commit", {
            "base_version": current,
            "changes": [sample["incremental_ttl_change"]],
        })
        _record(steps, "incremental_ttl_cascade", incremental)
        expected_cone = {"db", "cache", "reports"}
        if set(incremental["affected_leases"]) != expected_cone:
            raise AssertionError(
                f"unexpected cascade: {incremental['affected_leases']} != {sorted(expected_cone)}"
            )

        cycle_base = incremental["snapshot_version"]
        cycle = _call(base_url, "POST", "/v1/snapshot/commit", {
            "base_version": cycle_base,
            "changes": [sample["cycle_change"]],
        })
        _record(steps, "reject_dependency_cycle", cycle)
        _expect_code(cycle, "E_DEPENDENCY_CYCLE")
        after_cycle = _call(base_url, "GET", "/v1/snapshot")["snapshot_version"]
        if after_cycle != cycle_base:
            raise AssertionError("cycle rejection changed snapshot version")

        clock = _call(base_url, "POST", "/v1/admin/clock", {
            "advance_ms": sample["clock_advance_ms"],
        })
        _record(steps, "advance_logical_clock", clock)

        expired = _call(base_url, "POST", "/v1/admin/sweep", {})
        _record(steps, "expire_parent_and_cascade", expired)
        db_view = _call(base_url, "GET", "/v1/locks/db")
        cache_view = _call(base_url, "GET", "/v1/locks/cache")
        reports_view = _call(base_url, "GET", "/v1/locks/reports")
        if db_view["lease"]["state"] != "released" or db_view["lease"]["release_reason"] != "DEPENDENCY_EXPIRED":
            raise AssertionError("strong child db was not released by dependency expiry")
        if cache_view["lease"]["state"] != "active" or not cache_view["lease"]["dependency_expired"]:
            raise AssertionError("weak child cache did not remain active with dependency_expired=true")
        if reports_view["lease"]["state"] != "released":
            raise AssertionError("strong descendant reports was not released")
        _record(steps, "strong_child_after_expiry", db_view)
        _record(steps, "weak_child_after_expiry", cache_view)

        dependency_error = _call(base_url, "POST", "/v1/locks/renew", {
            "lock_key": "db",
            "holder": db_view["lease"]["holder"],
            "lease_version": db_view["lease"]["lease_version"],
            "ttl_ms": 45000,
        })
        _record(steps, "renew_released_strong_child", dependency_error)
        _expect_code(dependency_error, "E_DEPENDENCY_EXPIRED")

        before_crash = _call(base_url, "GET", "/v1/snapshot")["snapshot_version"]
        crash_result = _call_for_crash(base_url, "POST", "/v1/snapshot/commit", {
            "base_version": before_crash,
            "changes": [sample["recovery_change"]],
            "_debug_fault": "after_intent",
        })
        _record(steps, "forced_termination_during_increment", crash_result)
        exit_code = process.wait(timeout=10)
        if exit_code != 86:
            raise AssertionError(f"fault-injected server exited with {exit_code}, expected 86")
        process = None

        process = _start_server()
        base_url = f"http://127.0.0.1:{process.port}"
        _wait_ready(base_url)
        recovery = _call(base_url, "GET", "/v1/recovery/report")
        _record(steps, "restart_recovery_report", recovery)
        report = recovery["report"]
        if report["complete_exit_report"] is not False:
            raise AssertionError("recovery report incorrectly claims a complete exit report")
        if not report["aborted_intent_count"]:
            raise AssertionError("expected at least one aborted STARTED intent")
        restored = _call(base_url, "GET", "/v1/snapshot")
        if restored["snapshot_version"] != before_crash:
            raise AssertionError(
                f"recovered version {restored['snapshot_version']} != pre-crash {before_crash}"
            )
        _record(steps, "snapshot_after_restart", {
            "snapshot_version": restored["snapshot_version"],
            "recovered_snapshot_version": report["recovered_snapshot_version"],
            "aborted_intent_count": report["aborted_intent_count"],
            "snapshot_restored_from_checkpoint": report["snapshot_restored_from_checkpoint"],
        })

        summary = {
            "ok": True,
            "sample": sample["name"],
            "database": str(DB_PATH.relative_to(ROOT)),
            "steps": steps,
        }
        ARTIFACT_PATH.parent.mkdir(parents=True, exist_ok=True)
        ARTIFACT_PATH.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
        print(json.dumps(summary, indent=2, ensure_ascii=False))
        return 0
    finally:
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()


class ServerProcess:
    def __init__(self, process: subprocess.Popen[str], port: int) -> None:
        self.process = process
        self.port = port

    def wait(self, timeout: float) -> int:
        return self.process.wait(timeout=timeout)

    def poll(self) -> int | None:
        return self.process.poll()

    def terminate(self) -> None:
        self.process.terminate()

    def kill(self) -> None:
        self.process.kill()


def _start_server() -> ServerProcess:
    port = _free_port()
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    env["LEASE_ENABLE_FAULTS"] = "1"
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "lease_lock.server",
            "--db",
            str(DB_PATH),
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--enable-faults",
        ],
        cwd=str(ROOT),
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    return ServerProcess(process, port)


def _clean_state() -> None:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    for suffix in ("", "-wal", "-shm"):
        path = Path(str(DB_PATH) + suffix)
        if path.exists():
            path.unlink()


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_ready(base_url: str) -> None:
    deadline = time.monotonic() + 8.0
    while time.monotonic() < deadline:
        try:
            _call(base_url, "GET", "/healthz")
            return
        except (urllib.error.URLError, ConnectionError, TimeoutError):
            time.sleep(0.05)
    raise RuntimeError("server did not become ready")


def _call(base_url: str, method: str, path: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = None
    headers = {"Accept": "application/json"}
    if body is not None:
        payload = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(base_url + path, data=payload, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=8) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8")
        return json.loads(raw)


def _call_for_crash(
    base_url: str, method: str, path: str, body: dict[str, Any]
) -> dict[str, Any]:
    payload = json.dumps(body).encode("utf-8")
    request = urllib.request.Request(
        base_url + path,
        data=payload,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=8) as response:
            return {
                "ok": False,
                "unexpected_response": response.status,
                "body": response.read().decode("utf-8", errors="replace"),
            }
    except urllib.error.HTTPError as exc:
        return {"ok": False, "http_status": exc.code, "body": exc.read().decode("utf-8")}
    except (urllib.error.URLError, ConnectionError, TimeoutError) as exc:
        return {
            "ok": False,
            "connection_lost": True,
            "exception": type(exc).__name__,
            "detail": str(exc),
        }


def _expect_code(response: dict[str, Any], code: str) -> None:
    if response.get("code") != code:
        raise AssertionError(f"expected {code}, got {response}")


def _record(steps: list[dict[str, Any]], name: str, response: dict[str, Any]) -> None:
    steps.append({"step": name, "response": response})


if __name__ == "__main__":
    raise SystemExit(main())

