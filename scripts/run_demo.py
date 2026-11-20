"""End-to-end local HTTP demonstration using only deterministic fixtures."""

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
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from capacity_ledger.checksum import canonical_json  # noqa: E402


SAMPLES = ROOT / "samples"
REPORT_PATH = ROOT / "artifacts" / "demo_report.json"


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as handle:
        handle.bind(("127.0.0.1", 0))
        return int(handle.getsockname()[1])


def load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def request(
    base_url: str, method: str, path: str, payload: dict[str, Any] | None = None
) -> tuple[int, dict[str, Any]]:
    data = None if payload is None else canonical_json(payload).encode("utf-8")
    req = urllib.request.Request(
        base_url + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


def wait_for_server(base_url: str, process: subprocess.Popen[str]) -> None:
    deadline = time.time() + 30
    while time.time() < deadline:
        if process.poll() is not None:
            output = process.stdout.read() if process.stdout else ""
            raise RuntimeError(f"uvicorn exited early:\n{output}")
        try:
            status, _ = request(base_url, "GET", "/health")
            if status == 200:
                return
        except (urllib.error.URLError, ConnectionError, TimeoutError):
            pass
        time.sleep(0.1)
    raise TimeoutError("uvicorn did not become ready")


def expect(status: int, expected: int, body: dict[str, Any]) -> None:
    if status != expected:
        raise AssertionError(f"expected HTTP {expected}, got {status}: {body}")


def main() -> None:
    runtime = ROOT / "runtime" / "demo"
    db_path = runtime / "capacity_ledger.sqlite3"
    intent_dir = runtime / "intents"
    if runtime.exists():
        shutil.rmtree(runtime)
    runtime.mkdir(parents=True, exist_ok=True)
    intent_dir.mkdir(parents=True, exist_ok=True)

    port = free_port()
    base_url = f"http://127.0.0.1:{port}"
    env = os.environ.copy()
    env["CAPACITY_DB_PATH"] = str(db_path)
    env["CAPACITY_INTENT_DIR"] = str(intent_dir)
    env["CAPACITY_RECOVER_ON_STARTUP"] = "0"
    env["PYTHONPATH"] = str(ROOT) + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    server = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "capacity_ledger.api:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--log-level",
            "warning",
        ],
        cwd=str(ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    report: dict[str, Any] = {
        "environment": {
            "database": str(db_path),
            "intent_dir": str(intent_dir),
            "http_base_url": base_url,
        },
        "phases": [],
    }
    try:
        wait_for_server(base_url, server)

        status, body = request(base_url, "POST", "/v1/snapshots", load(SAMPLES / "snapshot_v1.json"))
        expect(status, 200, body)
        assert body["version"] == 1 and body["verdict"]["status"] == "PASS"
        assert body["metrics"]["aggregate"]["total_cost_cents_per_hour"] == 1866
        report["phases"].append(
            {
                "phase": "full_snapshot_v1",
                "version": body["version"],
                "mode": body["mode"],
                "total_cost_cents_per_hour": body["metrics"]["aggregate"][
                    "total_cost_cents_per_hour"
                ],
                "total_cost_fixed": body["metrics"]["aggregate"][
                    "total_cost_cents_per_hour_fixed"
                ],
                "verdict": body["verdict"]["status"],
                "ledger_checksum": body["ledger_checksum"],
                "rebuilt_artifacts": body["rebuild_scope"]["rebuilt_count"],
            }
        )
        print(
            "phase 1 full snapshot: version=1 total=1866.000000 cents/h "
            f"verdict=PASS rebuilt={body['rebuild_scope']['rebuilt_count']}"
        )

        inc1 = load(SAMPLES / "increments" / "01_auth_price_increment.json")
        status, body = request(base_url, "POST", "/v1/increments", inc1)
        expect(status, 200, body)
        assert body["version"] == 2 and body["mode"] == "incremental"
        assert body["metrics"]["aggregate"]["total_cost_cents_per_hour"] == 1896
        report["phases"].append(
            {
                "phase": "auth_price_increment_v2",
                "version": body["version"],
                "mode": body["mode"],
                "mode_reason": body["mode_reason"],
                "total_cost_cents_per_hour": body["metrics"]["aggregate"][
                    "total_cost_cents_per_hour"
                ],
                "direct_services": body["rebuild_scope"]["direct_services"],
                "rebuilt_count": body["rebuild_scope"]["rebuilt_count"],
                "reused_count": body["rebuild_scope"]["reused_count"],
                "ledger_checksum": body["ledger_checksum"],
            }
        )
        print(
            "phase 2 auth price increment: version=2 mode=incremental "
            f"total={body['metrics']['aggregate']['total_cost_cents_per_hour_fixed']} "
            f"rebuilt={body['rebuild_scope']['rebuilt_count']} reused={body['rebuild_scope']['reused_count']}"
        )

        status, replay = request(base_url, "POST", "/v1/increments", inc1)
        expect(status, 200, replay)
        assert replay["ledger_checksum"] == body["ledger_checksum"]
        report["phases"].append(
            {
                "phase": "idempotent_retry_v2",
                "version": replay["version"],
                "same_ledger_checksum": replay["ledger_checksum"] == body["ledger_checksum"],
            }
        )
        print("phase 3 idempotent retry: reused version=2 result")

        status, body = request(
            base_url,
            "POST",
            "/v1/increments",
            load(SAMPLES / "increments" / "02_catalog_cores_increment.json"),
        )
        expect(status, 200, body)
        assert body["version"] == 3 and body["mode"] == "incremental"
        assert body["metrics"]["aggregate"]["total_cost_cents_per_hour"] == 1926
        report["phases"].append(
            {
                "phase": "catalog_cores_increment_v3",
                "version": body["version"],
                "mode": body["mode"],
                "total_cost_cents_per_hour": body["metrics"]["aggregate"][
                    "total_cost_cents_per_hour"
                ],
                "rebuilt_count": body["rebuild_scope"]["rebuilt_count"],
                "reused_count": body["rebuild_scope"]["reused_count"],
                "ledger_checksum": body["ledger_checksum"],
            }
        )
        print(
            "phase 4 catalog spec increment: version=3 total=1926.000000 cents/h "
            "mode=incremental"
        )

        status, violation = request(
            base_url,
            "POST",
            "/v1/increments",
            load(SAMPLES / "increments" / "03_payments_budget_violation.json"),
        )
        expect(status, 409, violation)
        assert violation["error"]["code"] == "E_BUDGET_EXCEEDED"
        assert violation["error"]["details"]["persisted_state_changed"] is False
        report["phases"].append(
            {
                "phase": "budget_violation_v4_rejected",
                "http_status": status,
                "code": violation["error"]["code"],
                "primary_constraint": violation["error"]["details"]["primary_constraint"],
                "violations": violation["error"]["details"]["violations"],
                "persisted_state_changed": violation["error"]["details"][
                    "persisted_state_changed"
                ],
            }
        )
        print(
            "phase 5 budget violation: E_BUDGET_EXCEEDED "
            f"primary={violation['error']['details']['primary_constraint']} "
            "persisted_state_changed=false"
        )

        status, before = request(base_url, "GET", "/v1/ledger/current")
        expect(status, 200, before)
        assert before["version"] == 3
        crash_command = [
            sys.executable,
            "-m",
            "capacity_ledger.crash_worker",
            "--db",
            str(db_path),
            "--intent-dir",
            str(intent_dir),
            "--increment",
            str(SAMPLES / "increments" / "04_cart_force_terminate.json"),
        ]
        crash = subprocess.run(
            crash_command,
            cwd=str(ROOT),
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
        )
        if crash.returncode != 86:
            raise AssertionError(
                f"crash worker returned {crash.returncode}, expected 86\n"
                f"stdout={crash.stdout}\nstderr={crash.stderr}"
            )
        status, recovery = request(base_url, "POST", "/v1/admin/recover")
        expect(status, 200, recovery)
        assert recovery["integrity"] == "VALID"
        assert recovery["aborted_count"] == 1
        assert recovery["after"]["version"] == 3
        assert recovery["difference"]["state_unchanged"] is True
        assert recovery["forced_termination_exit_report_available"] is False
        report["phases"].append(
            {
                "phase": "forced_termination_recovery",
                "crash_exit_code": crash.returncode,
                "before": recovery["before"],
                "after": recovery["after"],
                "difference": recovery["difference"],
                "aborted_count": recovery["aborted_count"],
                "aborted_intents": recovery["aborted_intents"],
                "report_basis": recovery["report_basis"],
            }
        )
        print(
            "phase 6 forced termination recovery: "
            f"aborted={recovery['aborted_count']} "
            f"version_before={recovery['before']['version']} "
            f"version_after={recovery['after']['version']} "
            f"state_unchanged={recovery['difference']['state_unchanged']}"
        )

        status, after = request(base_url, "GET", "/v1/ledger/current")
        expect(status, 200, after)
        assert after["ledger_checksum"] == before["ledger_checksum"]

        status, full = request(
            base_url,
            "POST",
            "/v1/increments",
            load(SAMPLES / "increments" / "05_notifications_slo_full_rebuild.json"),
        )
        expect(status, 200, full)
        assert full["version"] == 4 and full["mode"] == "full"
        assert full["rebuild_scope"]["full_rebuild"] is True
        report["phases"].append(
            {
                "phase": "slo_global_rebuild_v4",
                "version": full["version"],
                "mode": full["mode"],
                "mode_reason": full["mode_reason"],
                "rebuild_scope": {
                    "full_rebuild": full["rebuild_scope"]["full_rebuild"],
                    "rebuilt_count": full["rebuild_scope"]["rebuilt_count"],
                    "reused_count": full["rebuild_scope"]["reused_count"],
                },
                "total_cost_cents_per_hour": full["metrics"]["aggregate"][
                    "total_cost_cents_per_hour"
                ],
                "ledger_checksum": full["ledger_checksum"],
            }
        )
        print(
            "phase 7 SLO global rebuild: version=4 mode=full "
            f"reason={full['mode_reason']}"
        )

        status, verify = request(base_url, "GET", "/v1/admin/verify")
        expect(status, 200, verify)
        assert verify["status"] == "VALID" and verify["latest_version"] == 4
        report["final"] = {
            "version": full["version"],
            "total_cost_cents_per_hour": full["metrics"]["aggregate"][
                "total_cost_cents_per_hour"
            ],
            "total_cost_fixed": full["metrics"]["aggregate"][
                "total_cost_cents_per_hour_fixed"
            ],
            "weighted_cost_index_fixed_cents": full["metrics"]["aggregate"][
                "weighted_cost_index_fixed_cents"
            ],
            "verdict": full["verdict"]["status"],
            "rounding": full["metrics"]["rounding"],
            "ledger_checksum": full["ledger_checksum"],
            "integrity": verify,
        }
        print(
            "final: version=4 total=1926.000000 cents/h "
            f"weighted_index={full['metrics']['aggregate']['weighted_cost_index_fixed_cents']} "
            "verdict=PASS integrity=VALID"
        )
    finally:
        server.terminate()
        try:
            server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait(timeout=5)

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    print(f"report: {REPORT_PATH}")


if __name__ == "__main__":
    main()

