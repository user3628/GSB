#!/usr/bin/env python3
"""Repeatable end-to-end HTTP interoperability demo.

No external service or network target is used. The script starts local Python
HTTP servers on 127.0.0.1 and drives only the public API.
"""

from __future__ import annotations

import argparse
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

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from slo_budget.models import dumps_json  # noqa: E402


AS_OF_MS = 1_767_229_200_000
FIXED_NOW_MS = AS_OF_MS + 123_456
SAMPLES = ROOT / "samples"


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class ServerSession:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.port = free_port()
        self.process: subprocess.Popen | None = None
        self.base_url = f"http://127.0.0.1:{self.port}"

    def __enter__(self) -> "ServerSession":
        env = dict(**os.environ)
        env["PYTHONPATH"] = str(ROOT)
        env["SLO_FIXED_NOW_MS"] = str(FIXED_NOW_MS)
        self.process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "slo_budget.server",
                "--db",
                str(self.db_path),
                "--host",
                "127.0.0.1",
                "--port",
                str(self.port),
                "--fixed-clock-ms",
                str(FIXED_NOW_MS),
            ],
            cwd=ROOT,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                stderr = self.process.stderr.read() if self.process.stderr else ""
                raise RuntimeError(f"server exited during startup: {stderr}")
            try:
                status, _ = self.request("GET", "/health")
                if status == 200:
                    return self
            except (urllib.error.URLError, ConnectionError, TimeoutError):
                pass
            time.sleep(0.05)
        raise RuntimeError("server did not become healthy in time")

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)

    def request(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
    ) -> tuple[int, dict[str, Any]]:
        data = None
        headers = {"Accept": "application/json"}
        if body is not None:
            data = dumps_json(body).encode("utf-8")
            headers["Content-Type"] = "application/json; charset=utf-8"
        request = urllib.request.Request(
            self.base_url + path,
            data=data,
            headers=headers,
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return response.status, json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as error:
            payload = error.read().decode("utf-8")
            return error.code, json.loads(payload)


def require_ok(status: int, payload: dict[str, Any], step: str) -> dict[str, Any]:
    if status != 200 or payload.get("ok") is not True:
        raise AssertionError(
            f"{step} failed: HTTP {status} "
            f"{json.dumps(payload, ensure_ascii=False, sort_keys=True)}"
        )
    return payload


def require_error(
    status: int,
    payload: dict[str, Any],
    code: str,
    step: str,
) -> dict[str, Any]:
    actual = payload.get("error", {}).get("code")
    if status < 400 or actual != code:
        raise AssertionError(f"{step} expected {code}, got HTTP {status}: {payload}")
    return payload


def metric_signature(result: dict[str, Any], *, include_late: bool = True) -> dict[str, Any]:
    windows = {}
    for window in result["windows"]:
        item = {
            "sli": window["sli"],
            "burn_rate": window["error_budget"]["burn_rate"],
            "good_events": window["counts"]["good_events"],
            "total_events": window["counts"]["total_events"],
        }
        if include_late:
            item["late_count"] = window["counts"]["late_count"]
        windows[str(window["window_ms"])] = item
    return {
        "windows": windows,
        "alert_grade": result["alert"]["grade"],
    }


def query(
    server: ServerSession,
    service: str,
    sli_type: str,
    *,
    include_events: bool = True,
) -> dict[str, Any]:
    suffix = (
        f"?service={service}&sli_type={sli_type}&as_of_ms={AS_OF_MS}"
        + ("&events_path=" + str(SAMPLES / "events" / "requests.jsonl") if include_events else "")
        + "&persist_alert=true"
    )
    status, payload = server.request("GET", "/v1/query" + suffix)
    return require_ok(status, payload, f"query {service}/{sli_type}")


def run_demo(out_dir: Path) -> dict[str, Any]:
    def progress(step: str) -> None:
        print(f"[demo] {step}", file=sys.stderr, flush=True)

    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    db_main = out_dir / "main.db"
    db_overwrite = out_dir / "overwrite.db"
    db_atomic = out_dir / "atomic.db"
    db_roundtrip_bin = out_dir / "roundtrip-bin.db"
    db_roundtrip_json = out_dir / "roundtrip-json.db"
    db_legacy_bin = out_dir / "legacy-bin.db"
    db_legacy_json = out_dir / "legacy-json.db"
    exported_bin = out_dir / "exported-current.bin"
    exported_json = out_dir / "exported-current.jsonl"

    report: dict[str, Any] = {
        "ok": False,
        "fixed_now_ms": FIXED_NOW_MS,
        "as_of_ms": AS_OF_MS,
        "steps": {},
    }
    current_bin = SAMPLES / "windows" / "current-v2.bin"
    current_jsonl = SAMPLES / "windows" / "current-v2.jsonl"
    legacy_bin = SAMPLES / "windows" / "legacy-v1.bin"
    legacy_jsonl = SAMPLES / "windows" / "legacy-v1.jsonl"
    future_bin = SAMPLES / "windows" / "future-v3-required.bin"
    future_jsonl = SAMPLES / "windows" / "future-v3-required.jsonl"
    overwrite_file = SAMPLES / "conflicts" / "merge-overwrite.jsonl"
    reject_file = SAMPLES / "conflicts" / "merge-reject.jsonl"

    with ServerSession(db_main) as server:
        progress("main: compatibility")
        status, compatibility = server.request("GET", "/v1/compatibility")
        report["steps"]["compatibility"] = require_ok(
            status, compatibility, "compatibility"
        )

        preflights = {}
        for path in (
            current_bin,
            current_jsonl,
            legacy_bin,
            legacy_jsonl,
            future_bin,
            future_jsonl,
        ):
            status, payload = server.request(
                "POST", "/v1/preflight", {"path": str(path), "format": "auto"}
            )
            checked = require_ok(status, payload, f"preflight {path.name}")
            preflights[path.name] = {
                "verdict": checked["verdict"],
                "business_code": checked["business_code"],
                "format_version": checked["negotiation"]["format_version"],
            }
        if preflights["legacy-v1.bin"]["verdict"] != "DEGRADE":
            raise AssertionError("legacy binary did not degrade")
        if preflights["legacy-v1.jsonl"]["verdict"] != "DEGRADE":
            raise AssertionError("legacy JSONL did not degrade")
        if preflights["future-v3-required.bin"]["business_code"] != "E_FORMAT_UNSUPPORTED":
            raise AssertionError("future binary was not rejected")
        if preflights["future-v3-required.jsonl"]["business_code"] != "E_FORMAT_UNSUPPORTED":
            raise AssertionError("future JSONL was not rejected")
        report["steps"]["preflight"] = preflights
        progress("main: load current")

        status, payload = server.request(
            "POST",
            "/v1/snapshots/replace",
            {"path": str(current_bin), "format": "auto", "batch_id": "demo-load-current"},
        )
        report["steps"]["load_current_binary"] = require_ok(
            status, payload, "load current binary"
        )

        baseline = query(server, "checkout-api", "availability")
        baseline_search = query(server, "search-api", "latency")
        progress("main: baseline queries")
        if baseline["alert"]["grade"] != "CRITICAL":
            raise AssertionError("expected checkout availability CRITICAL")
        if baseline_search["alert"]["grade"] != "OK":
            raise AssertionError("expected search latency OK")
        report["steps"]["baseline_query"] = {
            "checkout": metric_signature(baseline),
            "search": metric_signature(baseline_search),
            "consistency": baseline["consistency"],
        }

        status, payload = server.request(
            "POST",
            "/v1/export",
            {"path": str(exported_bin), "format": "binary", "endian": "little"},
        )
        report["steps"]["export_binary"] = require_ok(status, payload, "export binary")
        status, payload = server.request(
            "POST",
            "/v1/export",
            {"path": str(exported_json), "format": "jsonl"},
        )
        report["steps"]["export_jsonl"] = require_ok(status, payload, "export JSONL")
        progress("main: exports")

        state_before = server.request("GET", "/v1/state")[1]["active"]
        status, payload = server.request(
            "POST",
            "/v1/snapshots/replace",
            {"path": str(future_bin), "format": "auto", "batch_id": "demo-future-reject"},
        )
        rejected_future = require_error(
            status, payload, "E_FORMAT_UNSUPPORTED", "future snapshot reject"
        )
        state_after = server.request("GET", "/v1/state")[1]["active"]
        if state_before != state_after:
            raise AssertionError("future snapshot rejection changed active state")
        report["steps"]["future_reject"] = {
            "error": rejected_future,
            "state_preserved": True,
        }
        progress("main: future rejection preserved state")

    with ServerSession(db_overwrite) as server:
        progress("overwrite: load base")
        require_ok(
            *server.request(
                "POST",
                "/v1/snapshots/replace",
                {"path": str(current_bin), "format": "auto", "batch_id": "overwrite-base"},
            ),
            "overwrite base",
        )
        status, payload = server.request(
            "POST",
            "/v1/import",
            {"path": str(overwrite_file), "format": "auto", "batch_id": "overwrite-merge"},
        )
        merge = require_ok(status, payload, "overwrite merge")
        counts = merge["report"]["counts"]
        if counts["overwritten"] != 1 or counts["skipped"] != 1:
            raise AssertionError(f"unexpected overwrite counts: {counts}")
        after_overwrite = query(server, "checkout-api", "availability")
        if metric_signature(after_overwrite)["windows"]["3600000"]["sli"] != baseline[
            "windows"
        ][-1]["sli"]:
            raise AssertionError("overwrite changed SLI unexpectedly")
        report["steps"]["conflict_overwrite"] = {
            "report": merge["report"],
            "query_still_consistent": True,
        }
        progress("overwrite: merge report")

    with ServerSession(db_atomic) as server:
        progress("atomic: load base")
        require_ok(
            *server.request(
                "POST",
                "/v1/snapshots/replace",
                {"path": str(current_bin), "format": "auto", "batch_id": "atomic-base"},
            ),
            "atomic base",
        )
        before = server.request("GET", "/v1/state")[1]["active"]
        status, payload = server.request(
            "POST",
            "/v1/import",
            {"path": str(reject_file), "format": "auto", "batch_id": "atomic-reject"},
        )
        atomic_error = require_error(
            status, payload, "E_IMPORT_CONFLICT", "atomic conflict reject"
        )
        after = server.request("GET", "/v1/state")[1]["active"]
        if before != after:
            raise AssertionError("rejected batch changed active state")
        conflict_report = atomic_error["error"]["details"]["report"]
        if conflict_report["status"] != "ROLLED_BACK" or conflict_report["counts"]["rejected"] != 1:
            raise AssertionError("atomic conflict report is not a rollback")
        report["steps"]["atomic_reject"] = {
            "error": atomic_error,
            "state_preserved": True,
        }
        progress("atomic: rollback verified")

    with ServerSession(db_roundtrip_bin) as server:
        progress("roundtrip-bin: import")
        status, payload = server.request(
            "POST",
            "/v1/import",
            {"path": str(exported_bin), "format": "auto", "batch_id": "import-exported-bin"},
        )
        report["steps"]["import_exported_binary"] = require_ok(
            status, payload, "import exported binary"
        )
        roundtrip_bin = query(server, "checkout-api", "availability")
        if metric_signature(roundtrip_bin) != metric_signature(baseline):
            raise AssertionError("binary export/import changed results")
        report["steps"]["binary_roundtrip_query"] = metric_signature(roundtrip_bin)
        progress("roundtrip-bin: verified")

    with ServerSession(db_roundtrip_json) as server:
        progress("roundtrip-json: replace")
        status, payload = server.request(
            "POST",
            "/v1/snapshots/replace",
            {"path": str(exported_json), "format": "auto", "batch_id": "load-exported-jsonl"},
        )
        report["steps"]["load_exported_jsonl"] = require_ok(
            status, payload, "load exported JSONL"
        )
        roundtrip_json = query(server, "checkout-api", "availability")
        if metric_signature(roundtrip_json) != metric_signature(baseline):
            raise AssertionError("JSONL export/import changed results")
        report["steps"]["jsonl_roundtrip_query"] = metric_signature(roundtrip_json)
        progress("roundtrip-json: verified")

    legacy_results = {}
    for label, db_path, path in (
        ("binary", db_legacy_bin, legacy_bin),
        ("jsonl", db_legacy_json, legacy_jsonl),
    ):
        with ServerSession(db_path) as server:
            progress(f"legacy-{label}: load")
            status, payload = server.request(
                "POST",
                "/v1/snapshots/replace",
                {"path": str(path), "format": "auto", "batch_id": f"legacy-{label}"},
            )
            loaded = require_ok(status, payload, f"legacy {label} load")
            if loaded["report"]["degradations"] == []:
                raise AssertionError("legacy load did not report degradations")
            legacy_query = query(server, "checkout-api", "availability")
            if metric_signature(legacy_query, include_late=False) != metric_signature(
                baseline, include_late=False
            ):
                raise AssertionError(f"legacy {label} changed SLI/burn/alert")
            legacy_results[label] = {
                "report": loaded["report"],
                "query": metric_signature(legacy_query),
            }
            progress(f"legacy-{label}: verified")
    report["steps"]["legacy_degrade"] = legacy_results

    before_crash = None
    with ServerSession(db_main) as server:
        progress("recovery: capture pre-crash state")
        before_crash = server.request("GET", "/v1/state")[1]

    progress("recovery: inject forced exit")
    crash = subprocess.run(
        [
            sys.executable,
            "-m",
            "slo_budget.cli",
            "inject-crash",
            "--db",
            str(db_main),
            "--kind",
            "simulated_crash",
            "--batch-id",
            "demo-crash-batch",
            "--fixed-clock-ms",
            str(FIXED_NOW_MS + 1),
        ],
        cwd=ROOT,
        env={**os.environ, "PYTHONPATH": str(ROOT)},
        capture_output=True,
        text=True,
        timeout=30,
    )
    if crash.returncode != 17:
        raise AssertionError(f"crash probe exit code was {crash.returncode}: {crash.stderr}")

    with ServerSession(db_main) as server:
        progress("recovery: query after restart")
        status, recovery = server.request("GET", "/v1/recovery")
        recovery = require_ok(status, recovery, "recovery report")
        matching = [
            item
            for item in recovery["recovery_reports"]
            if item["batch_id"] == "demo-crash-batch"
        ]
        if not matching:
            raise AssertionError("recovery report did not contain crash batch")
        after_crash = server.request("GET", "/v1/state")[1]
        if after_crash["active"] != before_crash["active"]:
            raise AssertionError("crash recovery did not preserve active snapshot")
        if after_crash["window_count"] != before_crash["window_count"]:
            raise AssertionError("crash recovery changed window count")
        report["steps"]["recovery"] = {
            "crash_exit_code": crash.returncode,
            "recovery_reports": matching,
            "active_preserved": True,
            "state": after_crash,
        }

    report["ok"] = True
    report["verification"] = {
        "binary_jsonl_bidirectional": True,
        "legacy_degraded": True,
        "future_rejected_without_state_change": True,
        "conflict_override_and_skip": True,
        "atomic_batch_rollback": True,
        "crash_recovery": True,
        "baseline_alert": baseline["alert"]["grade"],
        "baseline_windows": metric_signature(baseline)["windows"],
    }
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default=str(ROOT / "artifacts" / "demo"))
    args = parser.parse_args()
    out_dir = Path(args.out).resolve()
    try:
        report = run_demo(out_dir)
    except Exception as exc:  # noqa: BLE001 - demo writes a diagnostic report
        out_dir.mkdir(parents=True, exist_ok=True)
        failure = {
            "ok": False,
            "error": {"type": type(exc).__name__, "message": str(exc)},
        }
        (out_dir / "demo-report.json").write_text(
            dumps_json(failure, pretty=True) + "\n", encoding="utf-8"
        )
        print(dumps_json(failure, pretty=True))
        return 1
    (out_dir / "demo-report.json").write_text(
        dumps_json(report, pretty=True) + "\n", encoding="utf-8"
    )
    print(
        dumps_json(
            {
                "ok": True,
                "report": str(out_dir / "demo-report.json"),
                "verification": report["verification"],
                "preflight": report["steps"]["preflight"],
            },
            pretty=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
