from __future__ import annotations

import argparse
import hashlib
import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import httpx


ROOT = Path(__file__).resolve().parents[1]
SAMPLES = ROOT / "samples"


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class Server:
    def __init__(self, db_path: Path, log_path: Path, extra_env: dict[str, str] | None = None):
        self.db_path = db_path
        self.port = free_port()
        self.base_url = f"http://127.0.0.1:{self.port}"
        self.log_path = log_path
        self.log_file = log_path.open("a", encoding="utf-8")
        env = os.environ.copy()
        env.update(extra_env or {})
        env["PYTHONPATH"] = str(ROOT)
        self.process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "h2workbench.server",
                "--db",
                str(db_path),
                "--host",
                "127.0.0.1",
                "--port",
                str(self.port),
            ],
            cwd=ROOT,
            env=env,
            stdout=self.log_file,
            stderr=subprocess.STDOUT,
        )
        self.client = httpx.Client(base_url=self.base_url, timeout=10.0, trust_env=False)

    def wait_ready(self) -> None:
        deadline = time.time() + 15
        while time.time() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(
                    f"server exited early ({self.process.returncode}); log={self.log_path}"
                )
            try:
                response = self.client.get("/api/health")
                if response.status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            time.sleep(0.1)
        raise RuntimeError(f"server did not become ready; log={self.log_path}")

    def stop(self) -> None:
        try:
            self.client.close()
        except Exception:
            pass
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        self.log_file.close()


def load_sample(name: str) -> dict[str, Any]:
    return json.loads((SAMPLES / name).read_text(encoding="utf-8"))


def canonical_digest(value: Any) -> str:
    blob = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def post_sample(client: httpx.Client, sample: dict[str, Any]) -> dict[str, Any]:
    response = client.post("/api/samples", json=sample)
    response.raise_for_status()
    return response.json()


def print_trace(run: dict[str, Any]) -> None:
    for item in run.get("trace", []):
        print(
            f"  round={item['round_index']} seq={item['seq']} stream={item['stream_id']} "
            f"bytes={item['bytes']} priority={item['priority']} weight={item['weight']} "
            f"state={item['state_before']}->{item['state_after']}"
        )
        print(f"    basis: {item['reason']}")


def run_demo(db_path: Path, keep: bool) -> None:
    for suffix in ("", "-wal", "-shm"):
        candidate = Path(str(db_path) + suffix)
        if candidate.exists():
            candidate.unlink()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    log_path = db_path.with_suffix(".server.log")
    if log_path.exists():
        log_path.unlink()

    server: Server | None = None
    summary: dict[str, Any] = {}
    try:
        server = Server(db_path, log_path)
        server.wait_ready()
        print(f"[startup] uvicorn listening on {server.base_url}; db={db_path}")

        # 1. Complete normal multiplexing schedule.
        normal = load_sample("normal_multiplex.json")
        ingest = post_sample(server.client, normal)
        require(ingest["queued_data"]["bytes"] == 12_500, "normal sample queued byte mismatch")
        normal_run = server.client.post(
            "/api/samples/demo-normal-multiplex/schedule", json={}
        )
        normal_run.raise_for_status()
        normal_result = normal_run.json()
        require(normal_result["status"] == "COMPLETED", "normal run did not complete")
        require(normal_result["selected_bytes"] == 12_500, "normal selected byte mismatch")
        snapshot = server.client.get("/api/connections/demo-normal").json()
        require(snapshot["connection_window"] == 53_035, "normal connection window mismatch")
        stream3 = server.client.get("/api/connections/demo-normal/streams/3").json()["stream"]
        require(stream3["state"] == "HALF_CLOSED_LOCAL", "END_STREAM transition mismatch")
        transitions = server.client.get(
            "/api/connections/demo-normal/transitions?sample_id=demo-normal-multiplex"
        ).json()["transitions"]
        print("\n[1] 完整多路复用调度 COMPLETED")
        print_trace(normal_result)
        print(
            f"  window: connection={snapshot['connection_window']}/"
            f"{snapshot['initial_connection_window']}, selected_bytes={normal_result['selected_bytes']}"
        )
        print(
            "  transitions: "
            + "; ".join(
                f"s{item['stream_id']} {item['from_state']}->{item['to_state']} ({item['trigger']})"
                for item in transitions
            )
        )
        summary["normal_trace_digest"] = canonical_digest(
            {"trace": normal_result["trace"], "connection": snapshot}
        )
        summary["normal_window"] = snapshot["connection_window"]

        # 2. Flow-control blocked sample.
        blocked = load_sample("blocked_window.json")
        post_sample(server.client, blocked)
        blocked_response = server.client.post(
            "/api/samples/demo-window-blocked/schedule", json={}
        )
        require(blocked_response.status_code == 409, "blocked schedule must return HTTP 409")
        blocked_error = blocked_response.json()["error"]
        require(
            blocked_error["code"] == "E_FLOW_CONTROL_BLOCKED",
            "blocked schedule returned the wrong business code",
        )
        blocked_trace = server.client.get(
            "/api/samples/demo-window-blocked/trace"
        ).json()
        require(blocked_trace["status"] == "BLOCKED", "blocked run status mismatch")
        first_block = blocked_trace["blocked"][0]
        print("\n[2] 窗口不足 E_FLOW_CONTROL_BLOCKED")
        print(
            f"  stream={first_block['stream_id']} connection_window="
            f"{first_block['connection_window']} stream_window={first_block['stream_window']} "
            f"required={first_block['required_bytes']} pending={first_block['pending_bytes']}"
        )
        print(f"  reason: {first_block['reason']}")
        summary["blocked_code"] = blocked_error["code"]

        # 3. RST_STREAM plus END_STREAM.
        reset = load_sample("reset_end_stream.json")
        post_sample(server.client, reset)
        reset_run = server.client.post(
            "/api/samples/demo-reset-end-stream/schedule", json={}
        )
        reset_run.raise_for_status()
        reset_result = reset_run.json()
        require(
            {item["stream_id"] for item in reset_result["trace"]} == {9},
            "reset stream was scheduled or live stream was omitted",
        )
        reset_conn = server.client.get("/api/connections/demo-reset").json()
        states = {item["stream_id"]: item["state"] for item in reset_conn["streams"]}
        require(states[7] == "RESET", "RST_STREAM state mismatch")
        require(states[9] == "HALF_CLOSED_LOCAL", "END_STREAM state mismatch")
        reset_transitions = server.client.get(
            "/api/connections/demo-reset/transitions?sample_id=demo-reset-end-stream"
        ).json()["transitions"]
        print("\n[3] RST_STREAM 与 END_STREAM")
        print_trace(reset_result)
        print(
            "  states: "
            + ", ".join(f"s{stream_id}={state}" for stream_id, state in sorted(states.items()))
        )
        print(
            "  transitions: "
            + "; ".join(
                f"s{item['stream_id']} {item['from_state']}->{item['to_state']} ({item['trigger']})"
                for item in reset_transitions
            )
        )
        summary["reset_states"] = states

        # 4. Optional extra: WINDOW_UPDATE truncation.
        wu = load_sample("window_update_cap.json")
        post_sample(server.client, wu)
        server.client.post("/api/samples/demo-window-update-cap/schedule", json={}).raise_for_status()
        violations = server.client.get(
            "/api/connections/demo-window-cap/violations"
        ).json()["violations"]
        require(len(violations) == 2, "expected two WINDOW_UPDATE truncation violations")
        print("\n[4] WINDOW_UPDATE 超初始窗口截断")
        for item in violations:
            print(
                f"  stream={item['stream_id']} code={item['code']} requested={item['requested_increment']} "
                f"applied={item['applied_increment']} limit={item['initial_limit']}"
            )
        summary["window_update_violations"] = len(violations)

        # 5. Real forced-termination test with a fresh server process.
        server.stop()
        server = None
        crash_server = Server(db_path, log_path, {"H2WB_CRASH_AT_ROUND": "2"})
        crash_server.wait_ready()
        recovery_sample = load_sample("recovery_crash.json")
        post_sample(crash_server.client, recovery_sample)
        try:
            crash_server.client.post(
                "/api/samples/demo-recovery-crash/schedule", json={}, timeout=5.0
            )
            raise AssertionError("crash server did not terminate")
        except httpx.HTTPError:
            pass
        crash_code = crash_server.process.wait(timeout=15)
        crash_server.stop()
        require(crash_code != 0, "forced termination returned exit code 0")
        print(f"\n[5] 调度第 2 轮前强制终止进程，exit={crash_code}")

        server = Server(db_path, log_path)
        server.wait_ready()
        recovery = server.client.get("/api/recovery").json()
        require(recovery["scope"] == "persisted_facts_only", "recovery scope mismatch")
        require(len(recovery["reports"]) >= 1, "recovery report was not generated")
        report = next(
            item
            for item in recovery["reports"]
            if item["run_id"] == "run-sample-demo-recovery-crash"
        )
        facts = report["facts"]
        require(facts["committed_round_count"] == 1, "recovery commit boundary mismatch")
        require(facts["last_committed_round"] == 1, "recovery last round mismatch")
        recovered_trace = server.client.get(
            "/api/samples/demo-recovery-crash/trace"
        ).json()
        require(recovered_trace["status"] == "ABORTED", "recovered run was not aborted")
        require(recovered_trace["round_count"] == 1, "incomplete round leaked into trace")
        print(f"  action={report['action']}")
        print(
            f"  committed_rounds={facts['committed_round_count']} "
            f"last_round={facts['last_committed_round']} "
            f"connection_window={facts['connection_window']} pending_bytes={facts['pending_bytes']}"
        )
        print(
            f"  trace_status={recovered_trace['status']} trace_rounds={recovered_trace['round_count']} "
            f"pending_intents={len(recovery['pending_intents'])}"
        )
        require(recovery["pending_intents"] == [], "pending intent was not resolved")
        summary["recovery"] = {
            "action": report["action"],
            "committed_rounds": facts["committed_round_count"],
            "connection_window": facts["connection_window"],
            "trace_status": recovered_trace["status"],
        }

        print("\n[核验摘要]")
        print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
        print("\nDEMO OK")
    finally:
        if server is not None:
            server.stop()
        if not keep:
            for suffix in ("", "-wal", "-shm"):
                path = Path(str(db_path) + suffix)
                if path.exists():
                    path.unlink()


def main() -> None:
    parser = argparse.ArgumentParser(description="Repeatable local workbench demo")
    parser.add_argument("--db", default=str(ROOT / "var" / "demo.db"))
    parser.add_argument("--keep", action="store_true", help="keep the demo SQLite database")
    args = parser.parse_args()
    try:
        run_demo(Path(args.db).resolve(), args.keep)
    except Exception as exc:
        print(f"DEMO FAILED: {exc}", file=sys.stderr)
        raise


if __name__ == "__main__":
    main()
