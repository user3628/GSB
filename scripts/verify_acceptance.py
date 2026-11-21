"""Repeatable end-to-end acceptance run over the local HTTP service.

The script starts real local server processes, injects a forced process exit
after intent persistence, restarts on the same SQLite database, and writes a
machine-readable acceptance report under artifacts/.
"""

from __future__ import annotations

import argparse
import copy
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx


ROOT = Path(__file__).resolve().parent.parent
SAMPLES = ROOT / "samples"
ARTIFACTS = ROOT / "artifacts"


def sample(name: str) -> dict[str, Any]:
    return json.loads((SAMPLES / name).read_text(encoding="utf-8"))


def port_available(host: str, port: int) -> bool:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
        return True
    except OSError:
        return False
    finally:
        sock.close()


class Server:
    def __init__(self, db_path: Path, host: str, port: int, log_path: Path, fault_injection: bool):
        self.db_path = db_path
        self.host = host
        self.port = port
        self.log_path = log_path
        self.fault_injection = fault_injection
        self.process: subprocess.Popen | None = None
        self.log_file = None

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    def start(self) -> None:
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        if self.fault_injection:
            env["SCALE_ENABLE_FAULT_INJECTION"] = "1"
        else:
            env.pop("SCALE_ENABLE_FAULT_INJECTION", None)
        self.log_file = self.log_path.open("ab")
        self.process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "app.cli",
                "serve",
                "--db",
                str(self.db_path),
                "--host",
                self.host,
                "--port",
                str(self.port),
                "--log-level",
                "warning",
            ],
            cwd=ROOT,
            env=env,
            stdout=self.log_file,
            stderr=subprocess.STDOUT,
        )
        deadline = time.time() + 20
        last_error: Exception | None = None
        while time.time() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(
                    f"server exited early with {self.process.returncode}; see {self.log_path}"
                )
            try:
                response = httpx.get(self.base_url + "/api/v1/health", timeout=0.5)
                if response.status_code == 200:
                    return
            except Exception as exc:  # startup race
                last_error = exc
            time.sleep(0.15)
        raise RuntimeError(f"server did not become healthy: {last_error}")

    def stop(self) -> None:
        if self.process is not None and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        if self.log_file is not None:
            self.log_file.close()
            self.log_file = None


def request(
    server: Server,
    method: str,
    path: str,
    *,
    payload: dict[str, Any] | None = None,
    expected: int = 200,
) -> tuple[httpx.Response, dict[str, Any]]:
    response = httpx.request(
        method,
        server.base_url + path,
        json=payload,
        timeout=20.0,
    )
    try:
        data = response.json()
    except Exception:
        data = {"raw": response.text}
    if response.status_code != expected:
        raise AssertionError(
            f"{method} {path}: expected {expected}, got {response.status_code}: {data}"
        )
    return response, data


def run(host: str, port: int, keep_db: bool) -> dict[str, Any]:
    if not port_available(host, port):
        raise RuntimeError(f"port {host}:{port} is not available")
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    temp_root = Path(tempfile.mkdtemp(prefix="scaling-arbiter-accept-"))
    db_path = temp_root / "scale.db"
    report: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "python": sys.version.split()[0],
        "host": host,
        "port": port,
        "port_available_before_run": True,
        "scenarios": [],
        "success": False,
    }

    def scenario(name: str, **facts: Any) -> None:
        report["scenarios"].append({"name": name, **facts})

    server = Server(db_path, host, port, temp_root / "server-1.log", fault_injection=True)
    try:
        server.start()
        _, health = request(server, "GET", "/api/v1/health")
        assert health["pending_intents"] == 0
        scenario("service_start", status=health["status"], storage=health["storage"])

        _, policy = request(server, "POST", "/api/v1/policy", payload=sample("policy_v1.json"))
        initial_revision = policy["revision"]
        assert initial_revision == 1
        scenario(
            "policy_commit",
            policy_version=policy["policy_version"],
            revision=initial_revision,
            services=[
                {"id": item["id"], "replicas": item["current_replicas"], "state": item["state"]}
                for item in policy["state"]["services"]
            ],
        )

        _, stale = request(
            server, "POST", "/api/v1/policy", payload=sample("policy_v1.json"), expected=409
        )
        assert stale["error"]["code"] == "E_STALE_SNAPSHOT"
        _, state_after_stale = request(server, "GET", "/api/v1/state")
        assert state_after_stale["revision"] == initial_revision
        scenario(
            "stale_snapshot",
            code=stale["error"]["code"],
            revision_unchanged=state_after_stale["revision"] == initial_revision,
        )

        invalid_policy = sample("policy_invalid_unit.json")
        _, invalid = request(server, "POST", "/api/v1/policy", payload=invalid_policy, expected=400)
        assert invalid["error"]["code"] == "E_SPEC_INVALID"
        scenario("invalid_unit", code=invalid["error"]["code"])

        derive_payload = {
            "policy_version": 1,
            "observations": sample("observations_01.json"),
        }
        _, derive_a = request(server, "POST", "/api/v1/derive", payload=derive_payload)
        _, derive_b = request(server, "POST", "/api/v1/derive", payload=derive_payload)
        assert json.dumps(derive_a, sort_keys=True) == json.dumps(derive_b, sort_keys=True)
        scenario(
            "fixed_point_derivation",
            deterministic=derive_a == derive_b,
            derivations=[
                {
                    "service_id": item["service_id"],
                    "concurrency_replicas": item["concurrency_replicas"],
                    "latency_ratio_milli": item["latency_ratio_milli"],
                    "desired_replicas": item["desired_replicas"],
                    "clamped": item["clamp_min_applied"] or item["clamp_max_applied"],
                }
                for item in derive_a["derivations"]
            ],
        )

        missing_obs = sample("observations_01.json")
        missing_obs["services"] = missing_obs["services"][:-1]
        _, missing = request(
            server,
            "POST",
            "/api/v1/rounds",
            payload={"round_id": "missing-obs", "policy_version": 1, "observations": missing_obs},
            expected=400,
        )
        assert missing["error"]["code"] == "E_OBSERVATION_INVALID"
        scenario("invalid_observation", code=missing["error"]["code"])

        queue_body = {
            "round_id": "queue-full",
            "policy_version": 1,
            "observations": sample("observations_05_queue_full.json"),
        }
        _, queue_full = request(server, "POST", "/api/v1/rounds", payload=queue_body, expected=429)
        assert queue_full["error"]["code"] == "E_SCALE_QUEUE_FULL"
        _, state_after_queue = request(server, "GET", "/api/v1/state")
        assert state_after_queue["revision"] == initial_revision
        scenario(
            "queue_full",
            code=queue_full["error"]["code"],
            queued_replicas=queue_full["error"]["details"]["queued_replicas"],
            wait_queue_limit=queue_full["error"]["details"]["wait_queue_limit"],
            revision_unchanged=True,
        )

        crash_body = {
            "round_id": "crash-forced",
            "policy_version": 1,
            "observations": sample("observations_01.json"),
            "fault_injection": {"crash_after": "INTENT_COMMITTED"},
        }
        crash_observed = False
        try:
            httpx.post(server.base_url + "/api/v1/rounds", json=crash_body, timeout=5.0)
        except (httpx.ReadError, httpx.RemoteProtocolError, httpx.ConnectError):
            crash_observed = True
        assert crash_observed, "fault injection did not terminate the connection"
        assert server.process is not None
        exit_code = server.process.wait(timeout=10)
        assert exit_code == 42, f"unexpected crash exit code {exit_code}"
        scenario(
            "forced_termination",
            crash_observed=True,
            process_exit_code=exit_code,
            phase="after durable round + intent, before revision commit",
        )
        server.stop()

        restarted = Server(db_path, host, port, temp_root / "server-2.log", fault_injection=False)
        server = restarted
        server.start()
        _, recovered_round = request(server, "GET", "/api/v1/rounds/crash-forced")
        assert recovered_round["status"] == "ABORTED"
        _, recovery = request(server, "GET", "/api/v1/recovery/report")
        _, recovered_state = request(server, "GET", "/api/v1/state")
        assert recovered_state["revision"] == initial_revision
        assert any(item["round_id"] == "crash-forced" for item in recovery["items"])
        scenario(
            "startup_recovery",
            round_status=recovered_round["status"],
            recovery_actions=[item["action"] for item in recovery["items"]],
            latest_committed_revision=recovery["latest_committed_revision"],
            partial_revision_visible=False,
        )

        round_bodies: dict[str, dict[str, Any]] = {}
        round_results: dict[str, dict[str, Any]] = {}
        for round_id, observation_name in (
            ("round-001", "observations_01.json"),
            ("round-002", "observations_02.json"),
            ("round-003", "observations_03.json"),
        ):
            body = {
                "round_id": round_id,
                "policy_version": 1,
                "observations": sample(observation_name),
            }
            _, result = request(server, "POST", "/api/v1/rounds", payload=body)
            round_bodies[round_id] = body
            round_results[round_id] = result

        first = round_results["round-001"]
        assert first["summary"]["scale_up_granted"] == 3
        assert first["summary"]["scale_up_queued"] == 3
        assert [item["service_id"] for item in first["actions"]] == ["chat", "chat", "rerank"]
        statuses = {item["service_id"]: item for item in first["services"]}
        assert statuses["rerank"]["state"] == "SCALING_UP"
        assert statuses["embed"]["state"] == "SCALING_UP"
        scenario(
            "bounded_parallel_placement",
            grant_slots=first["summary"]["admission_slots_available"],
            granted=first["summary"]["scale_up_granted"],
            queued=first["summary"]["scale_up_queued"],
            priority_order=[item["service_id"] for item in first["actions"]],
            incomplete_services={
                key: value["state"]
                for key, value in statuses.items()
                if value["current_replicas"] < value["desired_replicas"]
            },
        )

        _, replay = request(server, "POST", "/api/v1/rounds", payload=round_bodies["round-001"])
        assert replay["replayed"] is True
        conflict_body = dict(round_bodies["round-001"])
        conflict_body["observations"] = sample("observations_02.json")
        _, conflict = request(server, "POST", "/api/v1/rounds", payload=conflict_body, expected=409)
        assert conflict["error"]["code"] == "E_CONFLICTING_ROUND"
        scenario(
            "round_idempotency",
            replay=True,
            conflicting_round_code=conflict["error"]["code"],
        )

        third = round_results["round-003"]
        chat = next(item for item in third["services"] if item["service_id"] == "chat")
        assert chat["state"] == "DEGRADED"
        assert chat["degraded_domains"]
        assert third["summary"]["scale_down_removed"] == 2
        scenario(
            "fault_domain_degradation",
            service="chat",
            state=chat["state"],
            replicas=chat["current_replicas"],
            conflicts=chat["degraded_domains"],
            scale_down_reclaimed=third["summary"]["scale_down_removed"],
            scale_up_granted=third["summary"]["scale_up_granted"],
        )

        failover_results = []
        for round_id in ("failover-1", "failover-2"):
            body = {
                "round_id": round_id,
                "policy_version": 1,
                "observations": sample("observations_04_failover.json"),
            }
            _, result = request(server, "POST", "/api/v1/rounds", payload=body)
            failover_results.append(result)
        _, final_state = request(server, "GET", "/api/v1/state")
        node_health = {item["id"]: item["healthy"] for item in final_state["nodes"]}
        assert all(node_health[item["node_id"]] for item in final_state["placements"])
        assert failover_results[0]["summary"]["migrations_completed"] > 0
        assert failover_results[1]["summary"]["migrations_completed"] > 0
        scenario(
            "unhealthy_node_migration",
            first_round_migrated=failover_results[0]["summary"]["migrations_completed"],
            first_round_queued=failover_results[0]["summary"]["migrations_queued"],
            second_round_migrated=failover_results[1]["summary"]["migrations_completed"],
            all_placements_on_healthy_nodes=True,
            gpu_b1_healthy=node_health["gpu-b1"],
        )

        current_chat = next(item for item in final_state["services"] if item["id"] == "chat")
        _, transition = request(
            server,
            "POST",
            "/api/v1/services/chat/transition",
            payload={
                "target_state": current_chat["state"],
                "expected_revision": final_state["revision"],
            },
            expected=409,
        )
        assert transition["error"]["code"] == "E_INVALID_TRANSITION"
        scenario("invalid_transition", code=transition["error"]["code"])

        _, scaling_down = request(
            server,
            "POST",
            "/api/v1/services/chat/transition",
            payload={
                "target_state": "SCALING_DOWN",
                "expected_revision": final_state["revision"],
                "reason": "acceptance demonstration of a legal state transition",
            },
        )
        assert scaling_down["to_state"] == "SCALING_DOWN"
        scenario(
            "legal_state_transition",
            from_state=scaling_down["from_state"],
            to_state=scaling_down["to_state"],
            revision=scaling_down["revision"],
        )

        _, absent = request(server, "GET", "/api/v1/rounds/absent", expected=404)
        assert absent["error"]["code"] == "E_ROUND_NOT_FOUND"

        _, policy_v2 = request(
            server, "POST", "/api/v1/policy", payload=sample("policy_v2_budget.json")
        )
        assert policy_v2["revision"] > final_state["revision"]
        assert len(policy_v2["state"]["placements"]) == len(final_state["placements"])
        scenario(
            "policy_version_update",
            policy_version=policy_v2["policy_version"],
            revision=policy_v2["revision"],
            placements_preserved=len(policy_v2["state"]["placements"]),
        )

        tight = copy.deepcopy(sample("policy_v2_budget.json"))
        tight["version"] = 3
        tight["generated_at"] = "2026-11-21T10:30:00+08:00"
        for node in tight["nodes"]:
            node["memory"] = "512Mi"
            node["reserved_memory"] = "0Mi"
        tight.pop("initial_placements", None)
        _, insufficient = request(server, "POST", "/api/v1/policy", payload=tight, expected=409)
        assert insufficient["error"]["code"] == "E_INSUFFICIENT_CAPACITY"
        _, after_insufficient = request(server, "GET", "/api/v1/state")
        assert after_insufficient["revision"] == policy_v2["revision"]
        scenario(
            "insufficient_capacity",
            code=insufficient["error"]["code"],
            details=insufficient["error"]["details"],
            revision_unchanged=True,
        )

        request(server, "GET", "/api/v1/rounds/absent", expected=404)
        scenario("round_not_found", code="E_ROUND_NOT_FOUND")

        final_health = request(server, "GET", "/api/v1/health")[1]
        assert final_health["pending_intents"] == 0
        report["final_health"] = final_health
        report["error_codes_verified"] = [
            "E_SPEC_INVALID",
            "E_STALE_SNAPSHOT",
            "E_INVALID_TRANSITION",
            "E_SCALE_QUEUE_FULL",
            "E_INSUFFICIENT_CAPACITY",
            "E_OBSERVATION_INVALID",
            "E_CONFLICTING_ROUND",
            "E_ROUND_NOT_FOUND",
        ]
        report["success"] = True
        return report
    finally:
        server.stop()
        if keep_db:
            target = ARTIFACTS / "acceptance-demo.db"
            for suffix in ("", "-wal", "-shm"):
                src = Path(str(db_path) + suffix)
                if src.exists():
                    shutil.copy2(src, Path(str(target) + suffix))
            report["kept_db"] = str(target)
        shutil.rmtree(temp_root, ignore_errors=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=8000, type=int)
    parser.add_argument("--keep-db", action="store_true")
    args = parser.parse_args()
    report_path = ARTIFACTS / "acceptance_report.json"
    report: dict[str, Any] = {}
    try:
        report = run(args.host, args.port, args.keep_db)
    except Exception as exc:
        report = {
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "success": False,
            "error": {"type": exc.__class__.__name__, "message": str(exc)},
        }
        ARTIFACTS.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(report, ensure_ascii=False, indent=2), file=sys.stderr)
        return 1
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    for item in report.get("scenarios", []):
        print(f"PASS {item['name']}: {json.dumps({k: v for k, v in item.items() if k != 'name'}, ensure_ascii=False)}")
    print(f"ACCEPTANCE_OK report={report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
