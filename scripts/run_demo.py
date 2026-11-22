from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import socket
import sqlite3
import subprocess
import sys
import threading
import time
from contextlib import suppress
from pathlib import Path
from typing import Any

import httpx


ROOT = Path(__file__).resolve().parent.parent
RUNTIME = ROOT / "runtime"


def preflight(host: str, ports: list[int]) -> None:
    for port in ports:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind((host, port))
            except OSError as exc:
                raise RuntimeError(
                    f"port {host}:{port} is already in use; run scripts/check_ports.py"
                ) from exc


def wait_http(url: str, timeout: float = 12.0) -> None:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            response = httpx.get(url, timeout=1.0)
            if response.status_code == 200:
                return
        except Exception as exc:
            last_error = exc
        time.sleep(0.08)
    raise RuntimeError(f"service did not become ready: {url}: {last_error}")


def wait_tcp(host: str, port: int, timeout: float = 8.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.3):
                return
        except OSError:
            time.sleep(0.08)
    raise RuntimeError(f"TCP listener did not become ready: {host}:{port}")


def wait_port_free(host: str, port: int, timeout: float = 8.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind((host, port))
                return
            except OSError:
                time.sleep(0.08)
    raise RuntimeError(f"port did not become free: {host}:{port}")


def stop_process(process: subprocess.Popen | None, *, force: bool = False) -> None:
    if process is None or process.poll() is not None:
        return
    if force:
        process.kill()
    else:
        process.terminate()
    try:
        process.wait(timeout=4.0)
    except subprocess.TimeoutExpired:
        process.kill()
        with suppress(subprocess.TimeoutExpired):
            process.wait(timeout=3.0)


def log_tail(path: Path, lines: int = 20) -> str:
    if not path.exists():
        return "<missing log>"
    content = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return "\n".join(content[-lines:])


def materialize_routes(data: dict[str, Any], ports: argparse.Namespace) -> dict[str, Any]:
    clone = json.loads(json.dumps(data))
    routes = clone.get("routes", clone.get("upserts", []))
    for route in routes:
        upstream = route.get("upstream", "")
        upstream = upstream.replace("127.0.0.1:18081", f"127.0.0.1:{ports.backend_port}")
        upstream = upstream.replace("127.0.0.1:18082", f"127.0.0.1:{ports.egress_port}")
        upstream = upstream.replace("127.0.0.1:18083", f"127.0.0.1:{ports.alt_backend_port}")
        route["upstream"] = upstream
        if route.get("target_port") == 18081:
            route["target_port"] = ports.backend_port
    return clone


def print_step(number: int, title: str, detail: str = "") -> None:
    print(f"\n[{number}] {title}")
    if detail:
        print(detail)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the complete loopback proxy workbench demo")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--workbench-port", type=int, default=18080)
    parser.add_argument("--backend-port", type=int, default=18081)
    parser.add_argument("--egress-port", type=int, default=18082)
    parser.add_argument("--alt-backend-port", type=int, default=18083)
    parser.add_argument("--db", default=str(RUNTIME / "demo.sqlite3"))
    args = parser.parse_args()

    RUNTIME.mkdir(parents=True, exist_ok=True)
    db_path = Path(args.db)
    for suffix in ("", "-wal", "-shm"):
        Path(str(db_path) + suffix).unlink(missing_ok=True)
    upstream_log = RUNTIME / "demo_upstreams.log"
    workbench_log = RUNTIME / "demo_workbench.log"
    upstream_log.write_text("", encoding="utf-8")
    workbench_log.write_text("", encoding="utf-8")
    log_handles: list[Any] = []
    upstream_process: subprocess.Popen | None = None
    workbench_process: subprocess.Popen | None = None
    base_url = f"http://{args.host}:{args.workbench_port}"
    client = httpx.Client(timeout=6.0)
    crash_thread: threading.Thread | None = None
    crash_errors: list[str] = []
    report: dict[str, Any] = {"facts": {}, "version_trajectory": [], "cascade_trace": []}

    try:
        preflight(
            args.host,
            [args.workbench_port, args.backend_port, args.egress_port, args.alt_backend_port],
        )

        upstream_handle = upstream_log.open("a", encoding="utf-8")
        log_handles.append(upstream_handle)
        upstream_process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "tools.local_upstreams",
                "--mode",
                "all",
                "--host",
                args.host,
                "--backend-port",
                str(args.backend_port),
                "--alt-backend-port",
                str(args.alt_backend_port),
                "--egress-port",
                str(args.egress_port),
                "--log-level",
                "warning",
            ],
            cwd=ROOT,
            stdout=upstream_handle,
            stderr=subprocess.STDOUT,
        )
        wait_http(f"http://{args.host}:{args.backend_port}/health")
        wait_http(f"http://{args.host}:{args.alt_backend_port}/health")
        wait_tcp(args.host, args.egress_port)

        env = os.environ.copy()
        env.update(
            {
                "WORKBENCH_DB": str(db_path),
                "WORKBENCH_PORT": str(args.workbench_port),
                "WORKBENCH_ENABLE_FAILPOINTS": "1",
            }
        )
        workbench_handle = workbench_log.open("a", encoding="utf-8")
        log_handles.append(workbench_handle)
        workbench_process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "app.main:app",
                "--host",
                args.host,
                "--port",
                str(args.workbench_port),
                "--log-level",
                "warning",
            ],
            cwd=ROOT,
            env=env,
            stdout=workbench_handle,
            stderr=subprocess.STDOUT,
        )
        wait_http(base_url + "/workbench/api/health")

        print("=" * 88)
        print("LOOPBACK PROXY DIAGNOSTICS WORKBENCH :: END-TO-END DEMO")
        print("=" * 88)

        full_snapshot = materialize_routes(
            json.loads((ROOT / "samples" / "routes_snapshot_v1.json").read_text(encoding="utf-8")),
            args,
        )
        response = client.post(base_url + "/workbench/api/config/snapshot", json=full_snapshot)
        response.raise_for_status()
        full_commit = response.json()
        report["facts"]["full_commit"] = {
            key: full_commit[key]
            for key in ("version", "digest", "route_count", "artifact_count", "event_count")
        }
        print_step(
            1,
            "全量快照 v1 原子提交",
            f"version={full_commit['version']} digest={full_commit['digest'][:20]}… "
            f"routes={full_commit['route_count']} artifacts={full_commit['artifact_count']} "
            f"events={full_commit['event_count']}",
        )

        reverse = client.post(
            base_url + "/svc/echo?demo=reverse",
            headers={"X-Demo": "reverse", "Connection": "X-Strip-Me", "X-Strip-Me": "no"},
            content=b"reverse-body",
        )
        reverse.raise_for_status()
        reverse_payload = reverse.json()
        assert reverse_payload["instance"] == "primary"
        assert "x-strip-me" not in reverse_payload["headers"]
        print_step(
            2,
            "反向转发",
            f"status={reverse.status_code} backend={reverse_payload['instance']} "
            f"path={reverse_payload['path']} query={reverse_payload['query']} "
            f"body={reverse_payload['body_text']}",
        )

        forward = client.get(
            base_url + "/forward/echo?demo=forward",
            headers={
                "X-Forward-Host": args.host,
                "X-Forward-Port": str(args.backend_port),
            },
        )
        forward.raise_for_status()
        forward_payload = forward.json()
        assert forward_payload["instance"] == "primary"
        assert "x-local-egress" in forward_payload["headers"]
        print_step(
            3,
            "正向转发（经本地出口）",
            f"status={forward.status_code} backend={forward_payload['instance']} "
            f"egress={forward_payload['headers']['x-local-egress']}",
        )

        budget = client.get(
            base_url + "/workbench/api/budgets/explain?route_id=reverse-main"
        ).json()["route"]["budget"]
        step_errors = [
            f"{step['name']}={step['rounding_error_q6']['decimal']}"
            for step in budget["steps"]
        ]
        print_step(
            4,
            "Q6 定点预算与误差",
            f"total_wait={budget['display']['total_wait_ms']} ms "
            f"cumulative_error={budget['cumulative_rounding_error_q6']['decimal']} Q6\n"
            + "  " + " | ".join(step_errors),
        )
        report["facts"]["budget"] = {
            "total_wait_q6": budget["total_wait_q6"],
            "cumulative_rounding_error_q6": budget["cumulative_rounding_error_q6"],
            "steps": budget["steps"],
        }

        benchmark = client.get(base_url + "/workbench/api/budgets/benchmark").json()
        print_step(
            5,
            "混合量级求和对比",
            f"naive_error={benchmark['naive_float64']['error_q6']} Q6 | "
            f"kahan_error={benchmark['kahan_float64']['error_q6']} Q6 | "
            f"naive_bound={benchmark['error_bounds']['naive_gamma_bound_q6']}",
        )
        report["facts"]["benchmark"] = benchmark

        started = time.monotonic()
        hang = client.get(base_url + "/hang")
        elapsed = time.monotonic() - started
        hang_payload = hang.json()
        assert hang.status_code == 504
        assert hang_payload["business_code"] == "E_UPSTREAM_TIMEOUT"
        print_step(
            6,
            "真实读取超时",
            f"status={hang.status_code} elapsed={elapsed:.3f}s "
            f"stage={hang_payload['details']['stage']} code={hang_payload['business_code']}",
        )
        report["facts"]["timeout"] = {
            "status": hang.status_code,
            "elapsed_seconds": elapsed,
            "business_code": hang_payload["business_code"],
            "stage": hang_payload["details"]["stage"],
        }

        stats_before = client.get(
            f"http://{args.host}:{args.backend_port}/__stats"
        ).json()["forwarded_requests"]

        def same_request() -> int:
            return client.get(
                base_url + "/slow", headers={"X-Request-Id": "demo-idempotent-1"}
            ).status_code

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            same_statuses = list(pool.map(lambda _: same_request(), range(4)))
        stats_after = client.get(
            f"http://{args.host}:{args.backend_port}/__stats"
        ).json()["forwarded_requests"]
        assert same_statuses == [200, 200, 200, 200]
        assert stats_after - stats_before == 1
        conflict = client.post(
            base_url + "/svc/echo",
            headers={"X-Request-Id": "demo-idempotent-1"},
            content=b"different",
        )
        assert conflict.status_code == 409
        assert conflict.json()["business_code"] == "E_CONFLICTING_REQUEST"
        print_step(
            7,
            "请求标识去重",
            f"4 concurrent results={same_statuses} upstream_delta={stats_after - stats_before} "
            f"conflicting_reuse={conflict.status_code}/{conflict.json()['business_code']}",
        )
        report["facts"]["idempotency"] = {
            "concurrent_results": same_statuses,
            "upstream_forward_count": stats_after - stats_before,
            "conflict_business_code": conflict.json()["business_code"],
        }

        incremental = materialize_routes(
            json.loads((ROOT / "samples" / "routes_incremental_v2.json").read_text(encoding="utf-8")),
            args,
        )
        response = client.post(base_url + "/workbench/api/config/incremental", json=incremental)
        response.raise_for_status()
        incremental_commit = response.json()
        assert incremental_commit["version"] == 2
        after_increment = client.post(base_url + "/svc/echo", content=b"v2").json()
        assert after_increment["instance"] == "secondary"
        stale = client.post(base_url + "/workbench/api/config/incremental", json=incremental)
        assert stale.status_code == 409
        assert stale.json()["business_code"] == "E_STALE_BASE"
        report["facts"]["incremental_commit"] = {
            key: incremental_commit[key]
            for key in ("version", "digest", "route_count", "artifact_count", "event_count")
        }
        print_step(
            8,
            "单路由上游增量提交 v2",
            f"version={incremental_commit['version']} events={incremental_commit['event_count']} "
            f"reverse-main backend={after_increment['instance']} | stale={stale.status_code}/"
            f"{stale.json()['business_code']}",
        )

        cascade_groups: dict[tuple[str, str], int] = {}
        for event in incremental_commit["events"]:
            key = (event["artifact_kind"], event["action"])
            cascade_groups[key] = cascade_groups.get(key, 0) + 1
            if event["route_id"] == "reverse-main":
                report["cascade_trace"].append(event)
        print_step(
            9,
            "级联失效/重建轨迹（摘要）",
            "\n".join(
                f"  {kind:18s} {action:11s} count={count}"
                for (kind, action), count in sorted(cascade_groups.items())
            ),
        )

        crash_payload = materialize_routes(
            json.loads((ROOT / "samples" / "routes_incremental_v2.json").read_text(encoding="utf-8")),
            args,
        )
        crash_payload["base_version"] = 2
        crash_payload["upserts"][0]["timeout_ms"] = 777

        def blocked_commit() -> None:
            try:
                httpx.post(
                    base_url + "/workbench/api/config/incremental",
                    json=crash_payload,
                    headers={"X-Workbench-Failpoint": "crash-after-intent"},
                    timeout=30.0,
                )
            except Exception as exc:
                crash_errors.append(type(exc).__name__)

        crash_thread = threading.Thread(target=blocked_commit, daemon=True)
        crash_thread.start()

        intent_seen = False
        deadline = time.monotonic() + 8.0
        while time.monotonic() < deadline:
            with sqlite3.connect(str(db_path), timeout=2.0) as connection:
                row = connection.execute(
                    "SELECT id, state, target_version FROM intents WHERE state='PREPARED' ORDER BY id DESC LIMIT 1"
                ).fetchone()
            if row:
                intent_seen = True
                report["facts"]["orphan_intent"] = {
                    "intent_id": row[0],
                    "state": row[1],
                    "target_version": row[2],
                }
                break
            time.sleep(0.05)
        if not intent_seen:
            raise RuntimeError("failpoint did not persist a PREPARED intent")

        pid = workbench_process.pid
        workbench_process.kill()
        workbench_process.wait(timeout=5.0)
        workbench_process = None
        crash_thread.join(timeout=5.0)
        wait_port_free(args.host, args.workbench_port)
        print_step(
            10,
            "强制终止",
            f"killed pid={pid} while intent v{report['facts']['orphan_intent']['target_version']} "
            f"was PREPARED; configuration transaction never committed",
        )

        restart_handle = workbench_log.open("a", encoding="utf-8")
        log_handles.append(restart_handle)
        workbench_process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "app.main:app",
                "--host",
                args.host,
                "--port",
                str(args.workbench_port),
                "--log-level",
                "warning",
            ],
            cwd=ROOT,
            env=env,
            stdout=restart_handle,
            stderr=subprocess.STDOUT,
        )
        wait_http(base_url + "/workbench/api/health")
        recovery = client.get(base_url + "/workbench/api/recovery").json()
        startup = recovery["startup_recovery"]
        current_version = client.get(base_url + "/workbench/api/health").json()[
            "current_config_version"
        ]
        versions = client.get(base_url + "/workbench/api/versions").json()
        trajectory = [item["version"] for item in versions["versions"]]
        assert current_version == 2
        assert startup["last_complete_version"] == 2
        assert startup["rolled_back_intent_count"] >= 1
        assert trajectory == [1, 2]
        print_step(
            11,
            "重启扫描未完成意图并恢复",
            f"current_version={current_version} last_complete_version={startup['last_complete_version']} "
            f"orphans_rolled_back={startup['rolled_back_intent_count']} "
            f"orphan_ids={startup['orphan_intent_ids']}",
        )
        print_step(
            12,
            "版本轨迹",
            " -> ".join(f"v{version}" for version in trajectory)
            + f" (incomplete v3 rolled back, crash request error={crash_errors or ['none']})",
        )

        report["facts"]["recovery"] = startup
        report["version_trajectory"] = trajectory
        report_path = RUNTIME / "demo_report.json"
        report_path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
        )

        print("\n" + "=" * 88)
        print(f"DEMO PASSED. Report: {report_path}")
        print("=" * 88)
        return 0
    except Exception as exc:
        print(f"\nDEMO FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
        print("\n--- upstream log tail ---\n" + log_tail(upstream_log), file=sys.stderr)
        print("\n--- workbench log tail ---\n" + log_tail(workbench_log), file=sys.stderr)
        return 1
    finally:
        if crash_thread is not None and crash_thread.is_alive():
            crash_thread.join(timeout=1.0)
        client.close()
        stop_process(workbench_process, force=True)
        stop_process(upstream_process)
        if upstream_process is not None:
            with suppress(Exception):
                upstream_process.wait(timeout=3.0)
        for handle in log_handles:
            with suppress(Exception):
                handle.close()


if __name__ == "__main__":
    raise SystemExit(main())
