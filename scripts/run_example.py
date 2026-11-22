"""End-to-end demo against a real loopback HTTP server.

The script intentionally uses only fixed local JSON samples and standard
library HTTP clients. It records the complete result in
``artifacts/example_report.json``.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SAMPLES = ROOT / "samples"
ARTIFACTS = ROOT / "artifacts"


def load(name: str) -> dict[str, Any]:
    return json.loads((SAMPLES / name).read_text(encoding="utf-8"))


def port_is_available(host: str, port: int) -> bool:
    family = socket.AF_INET6 if ":" in host else socket.AF_INET
    with socket.socket(family, socket.SOCK_STREAM) as sock:
        try:
            sock.bind((host, port))
        except OSError:
            return False
    return True


def choose_port(host: str, preferred: int) -> int:
    if port_is_available(host, preferred):
        return preferred
    for port in range(preferred + 1, preferred + 50):
        if port_is_available(host, port):
            return port
    raise RuntimeError(f"no bindable loopback port in {preferred}-{preferred + 49}")


class Client:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url

    def request(
        self, method: str, path: str, body: dict[str, Any] | None = None
    ) -> tuple[int, dict[str, Any]]:
        data = None if body is None else json.dumps(body).encode("utf-8")
        request = urllib.request.Request(
            self.base_url + path,
            data=data,
            method=method,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                return response.status, json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            payload = json.loads(exc.read().decode("utf-8"))
            return exc.code, payload

    def get(self, path: str) -> dict[str, Any]:
        status, payload = self.request("GET", path)
        if status >= 400:
            raise RuntimeError(f"GET {path} failed: {status} {payload}")
        return payload

    def post(self, path: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
        status, payload = self.request("POST", path, body or {})
        if status >= 400:
            raise RuntimeError(f"POST {path} failed: {status} {payload}")
        return payload


def wait_until_ready(client: Client, timeout: float = 20.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            return client.get("/health")
        except Exception:
            time.sleep(0.1)
    raise RuntimeError("HTTP service did not become healthy before timeout")


def batch_summary(result: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "batch_id": batch["batch_id"],
            "batch_index": batch["batch_index"],
            "app_count": len(batch["app_ids"]),
            "apps": batch["apps"],
        }
        for batch in result["batches"]
    ]


def state_for_apps(client: Client, version: int, app_ids: list[str]) -> dict[str, Any]:
    return {app_id: client.get(f"/v1/snapshots/{version}/apps/{app_id}") for app_id in app_ids}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the fixed-sample loopback demo")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8876)
    parser.add_argument("--db", default=str(ARTIFACTS / "example_run.db"))
    args = parser.parse_args(argv)

    if args.host not in {"127.0.0.1", "localhost", "::1"}:
        raise SystemExit("only loopback hosts are supported")
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    db_path = Path(args.db).resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    for suffix in ("", "-wal", "-shm"):
        candidate = Path(str(db_path) + suffix)
        if candidate.exists():
            candidate.unlink()

    chosen_port = choose_port(args.host, args.port)
    url_host = f"[{args.host}]" if ":" in args.host else args.host
    base_url = f"http://{url_host}:{chosen_port}"
    log_path = ARTIFACTS / "example_server.log"
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(ROOT)
    log_file = log_path.open("w", encoding="utf-8")
    server = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "gitops_convergence.server",
            "--host",
            args.host,
            "--port",
            str(chosen_port),
            "--db",
            str(db_path),
        ],
        cwd=ROOT,
        env=environment,
        stdout=log_file,
        stderr=subprocess.STDOUT,
    )
    client = Client(base_url)
    report: dict[str, Any] = {
        "base_url": base_url,
        "db_path": str(db_path),
        "bind_check": {"host": args.host, "port": chosen_port, "bindable": True},
    }
    try:
        report["health"] = wait_until_ready(client)

        report["snapshot_v1"] = client.post(
            "/v1/snapshots", load("large_cluster_graph.json")
        )
        report["full_convergence_v1"] = client.post(
            "/v1/snapshots/1/convergence", {"mode": "auto"}
        )

        report["increment_v2"] = client.post(
            "/v1/snapshots/1/increments", load("increment_single_app.json")
        )
        report["incremental_convergence_v2"] = client.post(
            "/v1/snapshots/2/convergence", {"mode": "auto"}
        )

        report["rollback_increment_v3"] = client.post(
            "/v1/snapshots/2/increments", load("rollback_cascade_snapshot.json")
        )
        report["persisted_cascade_v3"] = client.get("/v1/snapshots/3/cascades")
        report["rollback_convergence_v3"] = client.post(
            "/v1/snapshots/3/convergence", {"mode": "auto"}
        )

        barrier = threading.Barrier(3)

        def send_conflict(payload: dict[str, Any]) -> tuple[int, dict[str, Any]]:
            barrier.wait()
            return client.request("POST", "/v1/snapshots/3/increments", payload)

        with ThreadPoolExecutor(max_workers=2) as executor:
            future_a = executor.submit(send_conflict, load("increment_conflict_a.json"))
            future_b = executor.submit(send_conflict, load("increment_conflict_b.json"))
            barrier.wait()
            conflict_outcomes = [future_a.result(), future_b.result()]
        report["concurrent_increment_conflict"] = conflict_outcomes
        success = next(payload for status, payload in conflict_outcomes if status == 201)
        stale = next(payload for status, payload in conflict_outcomes if status == 409)
        assert stale["code"] == "E_STALE_BASE"

        plan_before = client.post("/v1/snapshots/4/plan-preview", {"mode": "auto"})
        actionable = plan_before["actionable_app_ids"]
        states_before = state_for_apps(client, 4, actionable)

        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "gitops_convergence.crash_worker",
                "--db",
                str(db_path),
                "--snapshot",
                "4",
                "--mode",
                "auto",
            ],
            cwd=ROOT,
            env=environment,
            capture_output=True,
            text=True,
            timeout=30,
        )
        report["forced_termination_exit_code"] = completed.returncode
        assert completed.returncode != 0
        report["recovery"] = client.post("/v1/recovery", {})
        assert report["recovery"]["aborted_convergence_versions"] == [4]
        assert report["recovery"]["aborted_batch_ids"]
        states_after = state_for_apps(client, 4, actionable)
        plan_after = client.post("/v1/snapshots/4/plan-preview", {"mode": "auto"})
        report["recovery_before_after"] = {
            "states_before": states_before,
            "states_after": states_after,
            "state_diff_app_ids": sorted(
                app_id
                for app_id in actionable
                if states_before[app_id]["state"] != states_after[app_id]["state"]
            ),
            "plan_hash_before": plan_before["plan_hash"],
            "plan_hash_after": plan_after["plan_hash"],
            "plan_unchanged": plan_before["plan_hash"] == plan_after["plan_hash"],
        }
        report["convergence_after_recovery_v4"] = client.post(
            "/v1/snapshots/4/convergence", {"mode": "auto"}
        )
        report["final_snapshot"] = client.get("/v1/snapshots/current")
        assert report["final_snapshot"]["state_counts"] == {"CONVERGED": 3000}
        report["successful_increment_payload"] = success
        report["sample_counts"] = {
            "applications": report["snapshot_v1"]["application_count"],
            "clusters": len(report["snapshot_v1"]["cluster_ids"]),
            "initial_pending": report["snapshot_v1"]["state_counts"].get("PENDING", 0),
            "rollback_cascade_size": report["rollback_increment_v3"][
                "rollback_cascade"
            ]["affected_app_count"],
        }
        report["batch_states"] = {
            "v1_full": batch_summary(report["full_convergence_v1"]),
            "v2_incremental": batch_summary(report["incremental_convergence_v2"]),
            "v3_rollback": batch_summary(report["rollback_convergence_v3"]),
            "v4_after_recovery": batch_summary(report["convergence_after_recovery_v4"]),
        }
        report["rollback_cascade"] = report["rollback_increment_v3"]["rollback_cascade"]

        output_path = ARTIFACTS / "example_report.json"
        output_path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        summary = {
            "base_url": base_url,
            "output": str(output_path),
            "sample_counts": report["sample_counts"],
            "v1_path": report["full_convergence_v1"]["recompute"],
            "v2_path": report["incremental_convergence_v2"]["recompute"],
            "v3_path": report["rollback_convergence_v3"]["recompute"],
            "rollback_cascade_order": report["rollback_cascade"]["ordered_app_ids"],
            "concurrent_conflict_codes": sorted(
                payload.get("code", "OK") for _, payload in conflict_outcomes
            ),
            "batch_states": report["batch_states"],
            "rollback_cascade": report["rollback_cascade"],
            "recovery": {
                "last_complete_snapshot_version": report["recovery"][
                    "last_complete_snapshot_version"
                ],
                "aborted_convergence_versions": report["recovery"][
                    "aborted_convergence_versions"
                ],
                "aborted_batch_ids": report["recovery"]["aborted_batch_ids"],
                "state_diff_app_ids": report["recovery_before_after"][
                    "state_diff_app_ids"
                ],
                "plan_unchanged": report["recovery_before_after"]["plan_unchanged"],
            },
            "recovery_before_after": report["recovery_before_after"],
            "final_state_counts": report["final_snapshot"]["state_counts"],
        }
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0
    finally:
        server.terminate()
        try:
            server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait(timeout=5)
        log_file.close()


if __name__ == "__main__":
    raise SystemExit(main())
