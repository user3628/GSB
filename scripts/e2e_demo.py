#!/usr/bin/env python3
"""End-to-end fixed-input demonstration against the real HTTP server."""

from __future__ import annotations

import base64
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections import Counter
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from diaglab.test_service import free_loopback_port  # noqa: E402


SAMPLES = ROOT / "samples"
OUT = ROOT / "artifacts" / "demo"
DB = OUT / "demo.sqlite3"
API_LOG = OUT / "uvicorn.log"
CRASH_MARKER = OUT / "crash.marker"
REPORT_PATH = OUT / "e2e_report.json"


class Demo:
    def __init__(self) -> None:
        self.report: dict[str, Any] = {
            "started_at_ms": int(time.time() * 1000),
            "checks": [],
            "requests": [],
        }
        self.api_process: subprocess.Popen[str] | None = None
        self.echo_process: subprocess.Popen[str] | None = None
        self.hang_process: subprocess.Popen[str] | None = None
        self.refuse_process: subprocess.Popen[str] | None = None
        self.api_base = ""
        self.api_port = 0
        self.echo: dict[str, Any] = {}
        self.hang: dict[str, Any] = {}
        self.refuse: dict[str, Any] = {}
        self.crash_process: subprocess.Popen[str] | None = None

    def check(self, name: str, condition: bool, detail: Any = None) -> None:
        item = {"name": name, "ok": bool(condition), "detail": detail}
        self.report["checks"].append(item)
        status = "PASS" if condition else "FAIL"
        print(f"[{status}] {name}" + (f" · {detail}" if detail is not None else ""))
        if not condition:
            raise AssertionError(f"check failed: {name}: {detail}")

    def reset_outputs(self) -> None:
        OUT.mkdir(parents=True, exist_ok=True)
        for name in (
            "demo.sqlite3",
            "demo.sqlite3-wal",
            "demo.sqlite3-shm",
            "crash.marker",
            "uvicorn.log",
            "cycle1_binary.bin",
            "cycle1_jsonl.jsonl",
            "cycle2_binary.bin",
            "cycle2_jsonl.jsonl",
            "e2e_report.json",
        ):
            path = OUT / name
            if path.exists():
                path.unlink()

    def start_service(self, mode: str) -> tuple[subprocess.Popen[str], dict[str, Any]]:
        process = subprocess.Popen(
            [
                sys.executable,
                str(ROOT / "scripts" / "test_server.py"),
                "--mode",
                mode,
                "--probe-port",
                "0",
                "--admin-port",
                "0",
                "--hang-seconds",
                "20",
            ],
            cwd=str(ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env={**os.environ, "PYTHONPATH": str(ROOT)},
        )
        assert process.stdout is not None
        line = process.stdout.readline().strip()
        if not line:
            stderr = process.stderr.read() if process.stderr else ""
            raise RuntimeError(f"test service failed to start: {stderr}")
        ready = json.loads(line)
        return process, ready

    def start_api(self) -> None:
        self.api_port = free_loopback_port()
        self.api_base = f"http://127.0.0.1:{self.api_port}"
        env = {
            **os.environ,
            "DIAGLAB_DB": str(DB),
            "PYTHONPATH": str(ROOT),
        }
        log = API_LOG.open("a", encoding="utf-8")
        self.api_process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "diaglab.app:app",
                "--host",
                "127.0.0.1",
                "--port",
                str(self.api_port),
                "--log-level",
                "warning",
            ],
            cwd=str(ROOT),
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
        )
        deadline = time.time() + 25
        last_error: Exception | None = None
        while time.time() < deadline:
            if self.api_process.poll() is not None:
                raise RuntimeError(f"uvicorn exited early; see {API_LOG}")
            try:
                status, body = self.request("GET", "/api/health")
                if status == 200 and body.get("ok"):
                    return
            except Exception as exc:  # startup race
                last_error = exc
            time.sleep(0.15)
        raise RuntimeError(f"uvicorn health check timed out: {last_error}")

    def stop_api(self) -> None:
        process = self.api_process
        self.api_process = None
        if process is None:
            return
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)

    def request(self, method: str, path: str, payload: Any = None) -> tuple[int, Any]:
        data = None
        headers = {"Accept": "application/json"}
        if payload is not None:
            data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(
            self.api_base + path, data=data, method=method, headers=headers
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                status = response.status
                raw = response.read()
        except urllib.error.HTTPError as exc:
            status = exc.code
            raw = exc.read()
        content_type = ""
        try:
            body = json.loads(raw.decode("utf-8")) if raw else None
        except (UnicodeDecodeError, json.JSONDecodeError):
            body = raw
        self.report["requests"].append(
            {"method": method, "path": path, "status": status}
        )
        return status, body

    def download(self, path: str, destination: Path) -> bytes:
        request = urllib.request.Request(self.api_base + path, method="GET")
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                status = response.status
                content = response.read()
                version = response.headers.get("X-Diaglab-Snapshot-Version")
                count = response.headers.get("X-Diaglab-Record-Count")
        except urllib.error.HTTPError as exc:
            raise AssertionError(f"download failed {path}: {exc.read().decode()}") from exc
        destination.write_bytes(content)
        self.report["requests"].append(
            {"method": "GET", "path": path, "status": status, "bytes": len(content)}
        )
        self.report.setdefault("downloads", []).append(
            {
                "path": path,
                "file": str(destination.relative_to(ROOT)),
                "bytes": len(content),
                "snapshot_version": version,
                "record_count": count,
            }
        )
        return content

    def import_file(self, path: Path, batch_id: str) -> tuple[int, Any]:
        payload = {
            "content_base64": base64.b64encode(path.read_bytes()).decode("ascii"),
            "filename": path.name,
            "timestamp_policy": "truncate",
            "batch_id": batch_id,
        }
        return self.request("POST", "/api/imports", payload)

    def core_multiset(self, records: list[dict[str, Any]]) -> Counter[str]:
        keys = (
            "timestamp_ms",
            "probe_id",
            "path_node",
            "protocol",
            "success",
            "rtt_us",
            "payload_bytes",
            "failure_type",
        )
        return Counter(
            json.dumps({key: record[key] for key in keys}, sort_keys=True)
            for record in records
        )

    def run(self) -> dict[str, Any]:
        self.reset_outputs()
        print("== Loopback Margin Lab end-to-end demonstration ==")
        self.start_api()
        print(f"workbench http://127.0.0.1:{self.api_port}")
        self.echo_process, self.echo = self.start_service("echo")
        self.hang_process, self.hang = self.start_service("hang")
        self.refuse_process, self.refuse = self.start_service("refuse")
        print(f"test service echo  probe={self.echo['probe_port']} admin={self.echo['admin_port']}")
        print(f"test service hang  probe={self.hang['probe_port']} admin={self.hang['admin_port']}")
        print(f"test service refuse probe={self.refuse['probe_port']} admin={self.refuse['admin_port']}")

        preflight_status, preflight = self.request(
            "POST",
            "/api/preflight",
            {
                "content_base64": base64.b64encode((SAMPLES / "binary_v2.bin").read_bytes()).decode("ascii"),
                "filename": "binary_v2.bin",
                "timestamp_policy": "truncate",
            },
        )
        self.check(
            "binary v2 preflight accepted",
            preflight_status == 200 and preflight["negotiation"]["decision"] == "accept",
            f"decision={preflight['negotiation']['decision']} records={preflight['negotiation']['records_detected']}",
        )

        # Cycle 1: import source binary, export both formats.
        status, imported1 = self.import_file(SAMPLES / "binary_v2.bin", "demo-import-cycle-1")
        self.check(
            "cycle 1 import committed",
            status == 200 and imported1["records_persisted"] == 21,
            f"version={imported1['commit']['version']} records={imported1['records_persisted']}",
        )
        before_cycle2 = self.request("GET", "/api/records?limit=10000")[1]["records"]
        self.download("/api/exports/binary?endian=little", OUT / "cycle1_binary.bin")
        self.download("/api/exports/jsonl", OUT / "cycle1_jsonl.jsonl")
        self.check(
            "cycle 1 exported both formats",
            (OUT / "cycle1_binary.bin").read_bytes()[:4] == b"DXPK"
            and len((OUT / "cycle1_jsonl.jsonl").read_text(encoding="utf-8").strip().splitlines()) == 21,
            "binary=DXPK; jsonl=21 lines",
        )

        # Cycle 2: import exported JSONL, export again, prove normalized equality.
        status, imported2 = self.import_file(OUT / "cycle1_jsonl.jsonl", "demo-import-cycle-2")
        self.check(
            "cycle 2 JSONL re-import committed",
            status == 200 and imported2["records_persisted"] == 21,
            f"version={imported2['commit']['version']} records={imported2['records_persisted']}",
        )
        after_cycle2 = self.request("GET", "/api/records?limit=10000")[1]["records"]
        expected = Counter()
        for key, count in self.core_multiset(before_cycle2).items():
            expected[key] = count * 2
        self.check(
            "export/re-import normalized core fields are identical",
            self.core_multiset(after_cycle2) == expected,
            f"records {len(after_cycle2)} = 2 × {len(before_cycle2)}",
        )
        self.download("/api/exports/binary?endian=big", OUT / "cycle2_binary.bin")
        self.download("/api/exports/jsonl", OUT / "cycle2_jsonl.jsonl")

        # Degradation and rejection codes.
        status, degraded = self.import_file(SAMPLES / "binary_v1_degraded.bin", "demo-degraded")
        degradation = degraded["normalization"]["degradation"]
        self.check(
            "binary v1 degraded acceptance",
            status == 200
            and degraded["negotiation"]["decision"] == "degrade_accept"
            and degradation["missing_rtt_count"] == 2
            and degradation["missing_payload_count"] == 2,
            f"missing_rtt={degradation['missing_rtt_count']} missing_payload={degradation['missing_payload_count']}",
        )
        version_before_reject = self.request("GET", "/api/overview")[1]["snapshot_version"]
        status, bad_crc = self.import_file(SAMPLES / "binary_bad_crc.bin", "demo-bad-crc")
        self.check(
            "corrupt CRC returns E_FRAME_CORRUPT",
            status == 422 and bad_crc["code"] == "E_FRAME_CORRUPT",
            f"HTTP {status} {bad_crc['code']}",
        )
        status, bad_length = self.import_file(SAMPLES / "binary_bad_length.bin", "demo-bad-length")
        self.check(
            "bad length prefix returns E_FRAME_CORRUPT",
            status == 422 and bad_length["code"] == "E_FRAME_CORRUPT",
            f"HTTP {status} {bad_length['code']}",
        )
        status, unsupported = self.import_file(SAMPLES / "binary_v3_unsupported.bin", "demo-v3")
        self.check(
            "high version returns E_FORMAT_UNSUPPORTED",
            status == 415 and unsupported["code"] == "E_FORMAT_UNSUPPORTED",
            f"HTTP {status} {unsupported['code']}",
        )
        version_after_reject = self.request("GET", "/api/overview")[1]["snapshot_version"]
        self.check(
            "rejected imports do not change committed version",
            version_after_reject == version_before_reject,
            f"version={version_after_reject}",
        )

        # Real probes: success, refused, timeout.
        status, success_probe = self.request(
            "POST",
            "/api/probes",
            {
                "host": "127.0.0.1",
                "port": self.echo["probe_port"],
                "path_node": "local-edge-a",
                "probe_id": "demo-probe-success",
                "timeout_ms": 750,
                "payload_bytes": 32,
            },
        )
        self.check(
            "real TCP echo probe succeeds",
            status == 200 and success_probe["probe"]["outcome"] == "success",
            f"rtt={success_probe['probe']['record']['rtt_us']}us closed={success_probe['probe']['socket_closed']}",
        )
        status, refused_probe = self.request(
            "POST",
            "/api/probes",
            {
                "host": "127.0.0.1",
                "port": self.refuse["probe_port"],
                "path_node": "local-edge-b",
                "probe_id": "demo-probe-refused",
                "timeout_ms": 500,
                "payload_bytes": 32,
            },
        )
        self.check(
            "real connection refusal is classified",
            status == 502 and refused_probe["code"] == "E_CONNECTION_REFUSED",
            f"HTTP {status} {refused_probe['code']} closed={refused_probe['probe']['socket_closed']}",
        )
        status, timeout_probe = self.request(
            "POST",
            "/api/probes",
            {
                "host": "127.0.0.1",
                "port": self.hang["probe_port"],
                "path_node": "local-edge-a",
                "probe_id": "demo-probe-timeout",
                "timeout_ms": 180,
                "payload_bytes": 32,
            },
        )
        self.check(
            "real hang probe returns E_PROBE_TIMEOUT and closes socket",
            status == 504
            and timeout_probe["code"] == "E_PROBE_TIMEOUT"
            and timeout_probe["probe"]["socket_closed"] is True,
            f"HTTP {status} {timeout_probe['code']} closed={timeout_probe['probe']['socket_closed']}",
        )

        status, metrics_result = self.request(
            "POST",
            "/api/service-metrics/poll",
            {
                "admin_url": self.echo["metrics_url"],
                "path_node": "local-edge-a",
            },
        )
        metrics = metrics_result["service_metrics"]["metrics"]
        self.check(
            "local test service throughput/error counters persisted",
            status == 200 and metrics["completed_probes"] >= 1 and "throughput_bytes_per_sec" in metrics,
            f"completed={metrics['completed_probes']} errors={metrics['errors']} throughput={metrics['throughput_bytes_per_sec']:.3f}B/s",
        )

        stats1 = self.request("GET", "/api/stats?path_node=local-edge-a&min_samples=5")[1]
        stats2 = self.request("GET", "/api/stats?path_node=local-edge-a&min_samples=5")[1]
        self.check(
            "loss/jitter statistics are deterministic",
            stats1["calculation_digest"] == stats2["calculation_digest"],
            f"loss={stats1['loss']['percent']}% p95={stats1['rtt_us']['p95']}us jitter={stats1['jitter_us']['mean_abs_successive_us']}us",
        )
        roots = self.request("GET", "/api/root-causes?min_samples=5")[1]
        top = roots["candidates"][0]
        self.check(
            "root-cause candidates expose numeric ranking evidence",
            top["path_node"] == "local-edge-b"
            and "loss_fraction" in top["rank_basis"]
            and "failure_types" in top["rank_basis"],
            f"top={top['path_node']} score={top['rank_score']} failures={top['rank_basis']['failure_types']}",
        )
        compare = self.request(
            "GET", "/api/compare?path_a=local-edge-a&path_b=local-edge-b&rel_tol=0.000001"
        )[1]
        self.check(
            "two-path comparison uses declared relative tolerance",
            compare["tolerance"]["relative"] == 0.000001,
            f"loss_equal={compare['comparisons']['loss_fraction_equal']}",
        )

        # Force-kill a real writer process, restart workbench, inspect recovery ledger.
        before_crash = self.request("GET", "/api/overview")[1]
        self.stop_api()
        self.crash_process = subprocess.Popen(
            [
                sys.executable,
                str(ROOT / "scripts" / "crash_worker.py"),
                "--db",
                str(DB),
                "--batch-id",
                "demo-crash-batch",
                "--marker",
                str(CRASH_MARKER),
                "--records",
                "3",
            ],
            cwd=str(ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env={**os.environ, "PYTHONPATH": str(ROOT)},
        )
        deadline = time.time() + 20
        while not CRASH_MARKER.exists() and time.time() < deadline:
            if self.crash_process.poll() is not None:
                stdout, stderr = self.crash_process.communicate()
                raise RuntimeError(f"crash worker exited early: {stdout}\n{stderr}")
            time.sleep(0.05)
        self.check("forced-termination marker published", CRASH_MARKER.exists(), str(CRASH_MARKER))
        self.crash_process.kill()
        self.crash_process.wait(timeout=10)
        crash_payload = json.loads(CRASH_MARKER.read_text(encoding="utf-8"))
        print(
            f"force-killed pid={crash_payload['pid']} after intent {crash_payload['intent_id']} "
            f"with uncommitted v{crash_payload['uncommitted_version']}"
        )

        self.start_api()
        after_crash = self.request("GET", "/api/overview")[1]
        recovery = self.request("GET", "/api/recovery/report")[1]
        latest_diff = recovery["difference_report"][0] if recovery["difference_report"] else {}
        self.check(
            "restart rolls back to last complete version and reports differences",
            after_crash["snapshot_version"] == before_crash["snapshot_version"]
            and after_crash["record_count"] == before_crash["record_count"]
            and latest_diff.get("planned_records") == 3
            and latest_diff.get("committed_records") == 0
            and latest_diff.get("difference_records") == -3,
            f"v{before_crash['snapshot_version']}->v{after_crash['snapshot_version']} records={after_crash['record_count']} diff={latest_diff.get('difference_records')}",
        )
        aborted_batch = self.request("GET", "/api/imports/demo-crash-batch")[1]
        self.check(
            "interrupted batch is durably marked aborted",
            aborted_batch["status"] == "aborted"
            and aborted_batch["result"]["code"] == "E_BATCH_ABORTED",
            f"status={aborted_batch['status']} code={aborted_batch['result']['code']}",
        )

        self.report["final"] = {
            "format_negotiation": preflight["negotiation"],
            "degraded_conclusion": degraded["negotiation"],
            "loss_percent": stats1["loss"]["percent"],
            "p95_us": stats1["rtt_us"]["p95"],
            "jitter_mean_us": stats1["jitter_us"]["mean_abs_successive_us"],
            "root_cause_top": top,
            "recovery_difference": latest_diff,
            "versions": {
                "after_cycle1": imported1["commit"]["version"],
                "after_cycle2": imported2["commit"]["version"],
                "before_crash": before_crash["snapshot_version"],
                "after_restart": after_crash["snapshot_version"],
            },
            "all_checks_passed": all(item["ok"] for item in self.report["checks"]),
        }
        self.report["finished_at_ms"] = int(time.time() * 1000)
        REPORT_PATH.write_text(
            json.dumps(self.report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        print("\n== Summary ==")
        print(f"format negotiation: {preflight['negotiation']['decision']} (v{preflight['negotiation']['version']})")
        print(f"degraded conclusion: {degraded['negotiation']['decision']} · missing fields audited")
        print(f"loss rate: {stats1['loss']['percent']}% · p95 {stats1['rtt_us']['p95']}µs · jitter {stats1['jitter_us']['mean_abs_successive_us']}µs")
        print(f"root cause top: {top['path_node']} · score {top['rank_score']}")
        print(f"recovery: version {before_crash['snapshot_version']} preserved · planned 3 / committed 0 / difference -3")
        print(f"report: {REPORT_PATH.relative_to(ROOT)}")
        return self.report

    def cleanup(self) -> None:
        self.stop_api()
        if self.crash_process is not None and self.crash_process.poll() is None:
            self.crash_process.kill()
            self.crash_process.wait(timeout=10)
        for process in (self.echo_process, self.hang_process, self.refuse_process):
            if process is not None and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)


def main() -> int:
    demo = Demo()
    try:
        report = demo.run()
        return 0 if report["final"]["all_checks_passed"] else 1
    finally:
        demo.cleanup()


if __name__ == "__main__":
    raise SystemExit(main())
