from __future__ import annotations

import argparse
import json
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

import httpx
import uvicorn

from confighub.api import create_app as create_hub_app
from confighub.client_endpoint import create_client_app
from confighub.errors import ConfigHubError
from confighub.models import SampleSpec
from confighub.pipeline import compute_pipeline
from confighub.service import ConfigService, snapshot_digest


CONFIG_V1 = {
    "w0": 1.2,
    "w1": -0.5,
    "w2": 0.25,
    "normalization_scale": 1.5,
    "threshold": 0.1,
}
CONFIG_V2 = {
    "w0": 2.0,
    "w1": 0.7,
    "w2": -0.3,
    "normalization_scale": 0.8,
    "threshold": 0.25,
}
CONFIG_V3 = {
    "w0": 0.9,
    "w1": 1.1,
    "w2": 0.4,
    "normalization_scale": 1.2,
    "threshold": 0.35,
}
CONFIG_KEYS = list(CONFIG_V1)
SAMPLE = SampleSpec(seed=7, sample_size=16, feature_count=3)


def free_loopback_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", 0))
    port = int(sock.getsockname()[1])
    sock.close()
    return port


class ServerThread:
    def __init__(self, app: Any, port: int) -> None:
        self.server = uvicorn.Server(
            uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error")
        )
        self.thread = threading.Thread(target=self.server.run, daemon=True)

    def start(self) -> None:
        self.thread.start()
        deadline = time.monotonic() + 10
        while not self.server.started and time.monotonic() < deadline:
            if not self.thread.is_alive():
                raise RuntimeError("server thread exited before startup")
            time.sleep(0.02)
        if not self.server.started:
            raise RuntimeError("server did not start")

    def stop(self) -> None:
        self.server.should_exit = True
        self.thread.join(timeout=10)


def post(client: httpx.Client, base_url: str, path: str, payload: dict[str, Any]) -> dict[str, Any]:
    response = client.post(base_url + path, json=payload)
    if response.status_code >= 400:
        try:
            body = response.json()
        except ValueError:
            body = {"message": response.text}
        raise RuntimeError(f"POST {path} failed ({response.status_code}): {body}")
    return response.json()


def get(client: httpx.Client, base_url: str, path: str) -> dict[str, Any]:
    response = client.get(base_url + path)
    response.raise_for_status()
    return response.json()


def snapshot_body(version: int, config: dict[str, float], second: int) -> dict[str, Any]:
    generated_at = f"2026-01-01T00:00:{second:02d}+00:00"
    return {
        "version": version,
        "generated_at": generated_at,
        "config": config,
        "digest": snapshot_digest(version, generated_at, config),
    }


def cleanup_sqlite(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    for suffix in ("", "-wal", "-shm"):
        candidate = Path(str(path) + suffix)
        if candidate.exists():
            candidate.unlink()


def recover_crash_database(crash_db: Path) -> dict[str, Any]:
    restarted = ConfigService(crash_db)
    report = restarted.recover()
    batch = restarted.storage.query_one("SELECT * FROM batches WHERE target_version=2")
    pushes = restarted.storage.query_all(
        "SELECT client_id, version, state, last_error FROM push_records ORDER BY version"
    )
    crash_client = restarted.get_client("crash-client")
    return {
        "recovery_report": report,
        "crashed_batch": dict(batch) if batch else None,
        "push_records": pushes,
        "client_after_restart": crash_client,
        "visibility_violations": restarted.storage.visibility_invariant_violations(),
    }


def run() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--report", default="artifacts/demo-report.json")
    parser.add_argument("--data-dir", default="data")
    args = parser.parse_args()

    root = Path(__file__).resolve().parent
    data_dir = (root / args.data_dir).resolve()
    data_dir.mkdir(parents=True, exist_ok=True)
    demo_db = data_dir / "demo.sqlite"
    crash_db = data_dir / "crash.sqlite"
    cleanup_sqlite(demo_db)
    cleanup_sqlite(crash_db)

    client_port = free_loopback_port()
    hub_port = free_loopback_port()
    while hub_port == client_port:
        hub_port = free_loopback_port()
    client_base = f"http://127.0.0.1:{client_port}"
    hub_base = f"http://127.0.0.1:{hub_port}"

    client_server = ServerThread(
        create_client_app(
            delays={"slow": 0.0},
            modes={"lagging": "lagging"},
        ),
        client_port,
    )
    hub_server = ServerThread(create_hub_app(demo_db), hub_port)
    output: dict[str, Any] = {
        "environment": {
            "client_base": client_base,
            "hub_base": hub_base,
            "demo_db": str(demo_db),
            "crash_db": str(crash_db),
            "loopback_only": True,
        },
        "checks": {},
    }

    try:
        client_server.start()
        hub_server.start()
        with httpx.Client(timeout=20.0, trust_env=False) as client:
            output["hub_health"] = get(client, hub_base, "/health")
            for client_id, profile, timeout in (
                ("fast", "fast", 1.0),
                ("lagging", "lagging", 1.0),
                ("slow", "slow", 0.20),
            ):
                post(
                    client,
                    hub_base,
                    "/clients",
                    {
                        "client_id": client_id,
                        "config_keys": CONFIG_KEYS,
                        "endpoint_url": f"{client_base}/push/{profile}",
                        "timeout_seconds": timeout,
                        "sample_seed": SAMPLE.seed,
                        "sample_size": SAMPLE.sample_size,
                        "feature_count": SAMPLE.feature_count,
                    },
                )

            post(client, hub_base, "/snapshots", snapshot_body(1, CONFIG_V1, 0))
            round1 = post(client, hub_base, "/pushes", {"version": 1})
            output["round_v1"] = round1

            post(
                client,
                client_base,
                "/admin/delay",
                {"profile": "slow", "delay_seconds": 1.0},
            )
            post(client, hub_base, "/snapshots", snapshot_body(2, CONFIG_V2, 1))
            round2 = post(client, hub_base, "/pushes", {"version": 2})
            output["round_v2"] = round2

            post(
                client,
                client_base,
                "/admin/delay",
                {"profile": "slow", "delay_seconds": 0.0},
            )
            post(client, hub_base, "/snapshots", snapshot_body(3, CONFIG_V3, 2))
            round3 = post(client, hub_base, "/pushes", {"version": 3})
            output["round_v3"] = round3

            stats_before = get(client, client_base, "/stats")
            duplicate_round = post(client, hub_base, "/pushes", {"version": 3})
            stats_after = get(client, client_base, "/stats")
            output["duplicate_push"] = {
                "reused": duplicate_round.get("reused"),
                "batch_id_same": duplicate_round["batch_id"] == round3["batch_id"],
                "request_counts_before": {
                    name: value["request_count"]
                    for name, value in stats_before["profiles"].items()
                },
                "request_counts_after": {
                    name: value["request_count"]
                    for name, value in stats_after["profiles"].items()
                },
            }

            conflict_response = client.post(
                hub_base + "/pushes",
                json={"version": 3, "content_tag": "conflicting-content"},
            )
            output["conflicting_push"] = {
                "status_code": conflict_response.status_code,
                "body": conflict_response.json(),
            }

            duplicate_fast_result = compute_pipeline(SAMPLE, 3, CONFIG_V3)
            duplicate_ack = post(
                client,
                hub_base,
                "/acks",
                {
                    "client_id": "fast",
                    "version": 3,
                    "batch_id": round3["batch_id"],
                    "result": duplicate_fast_result,
                },
            )
            conflicting_ack = client.post(
                hub_base + "/acks",
                json={
                    "client_id": "fast",
                    "version": 3,
                    "batch_id": round3["batch_id"],
                    "result": {**duplicate_fast_result, "result_digest": "0" * 64},
                },
            )
            output["duplicate_and_conflicting_ack"] = {
                "duplicate_ack": duplicate_ack,
                "conflicting_ack_status": conflicting_ack.status_code,
                "conflicting_ack_body": conflicting_ack.json(),
            }

            late_result = compute_pipeline(SAMPLE, 2, CONFIG_V2)
            late_response = client.post(
                hub_base + "/acks",
                json={
                    "client_id": "lagging",
                    "version": 2,
                    "batch_id": round2["batch_id"],
                    "result": late_result,
                },
            )
            output["late_ack"] = {
                "status_code": late_response.status_code,
                "body": late_response.json(),
            }

            clients_final = get(client, hub_base, "/clients")["clients"]
            output["final_clients"] = clients_final
            output["final_consistency_v3"] = get(
                client, hub_base, f"/batches/{round3['batch_id']}/consistency"
            )
            output["ack_history"] = ConfigService(demo_db).storage.query_all(
                """
                SELECT client_id, version, accepted, code, observed_version,
                       substr(result_digest, 1, 12) AS digest_prefix
                FROM ack_records ORDER BY id
                """
            )

        transition_service = ConfigService(demo_db)
        transition_error: ConfigHubError | None = None
        before_transition = transition_service.storage.query_one(
            "SELECT state FROM push_records WHERE client_id='fast' AND version=3"
        )
        try:
            transition_service.transition_push_state("fast", 3, "PENDING", "demo-invalid")
        except ConfigHubError as exc:
            transition_error = exc
        after_transition = transition_service.storage.query_one(
            "SELECT state FROM push_records WHERE client_id='fast' AND version=3"
        )
        output["invalid_transition"] = {
            "code": transition_error.code if transition_error else None,
            "state_before": before_transition["state"] if before_transition else None,
            "state_after": after_transition["state"] if after_transition else None,
        }

        crash_worker = root / "scripts" / "crash_worker.py"
        completed = subprocess.run(
            [
                sys.executable,
                str(crash_worker),
                "--db",
                str(crash_db),
                "--endpoint",
                f"{client_base}/push/crash",
                "--crash-code",
                "37",
            ],
            cwd=root,
            capture_output=True,
            text=True,
            timeout=30,
        )
        output["crash_process"] = {
            "returncode": completed.returncode,
            "stdout": completed.stdout.strip(),
            "stderr": completed.stderr.strip(),
        }
        output["crash_recovery"] = recover_crash_database(crash_db)

        output["checks"] = {
            "v1_all_applied": len(round1["advanced"]) == 3,
            "v2_only_confirmed_client_advanced": [x["client_id"] for x in round2["advanced"]]
            == ["fast"],
            "v2_timeout_is_real": any(
                x["code"] == "E_PUSH_TIMEOUT" for x in round2["not_advanced"]
            ),
            "v2_stale_pick_detected": any(
                x["code"] == "E_STALE_ACK" for x in round2["not_advanced"]
            ),
            "v2_difference_fields_present": bool(
                round2["consistency"]["inconsistent_details"]
                and round2["consistency"]["inconsistent_details"][0]["difference_fields"]
            ),
            "duplicate_push_reused": bool(output["duplicate_push"]["reused"]),
            "duplicate_push_no_network_replay": output["duplicate_push"][
                "request_counts_before"
            ]
            == output["duplicate_push"]["request_counts_after"],
            "conflicting_push_rejected": output["conflicting_push"]["status_code"] == 409
            and output["conflicting_push"]["body"].get("code") == "E_CONFLICTING_PUSH",
            "duplicate_ack_idempotent": duplicate_ack.get("duplicate") is True,
            "conflicting_ack_rejected": output["duplicate_and_conflicting_ack"][
                "conflicting_ack_body"
            ].get("code")
            == "E_CONFLICTING_ACK",
            "late_ack_rejected": output["late_ack"]["body"].get("code") == "E_STALE_ACK",
            "lagging_version_not_overwritten": next(
                c for c in clients_final if c["client_id"] == "lagging"
            )["applied_version"]
            == 1,
            "invalid_transition_rejected_without_change": output["invalid_transition"]["code"]
            == "E_INVALID_TRANSITION"
            and output["invalid_transition"]["state_before"]
            == output["invalid_transition"]["state_after"],
            "crash_was_forced": completed.returncode == 37,
            "recovery_aborted_incomplete_batch": output["crash_recovery"]["crashed_batch"][
                "state"
            ]
            == "ABORTED",
            "recovery_preserved_last_complete_client_version": output["crash_recovery"][
                "client_after_restart"
            ]["applied_version"]
            == 1,
            "no_visibility_violations": not output["crash_recovery"][
                "visibility_violations"
            ],
        }
    finally:
        hub_server.stop()
        client_server.stop()

    report_path = (root / args.report).resolve()
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")

    print("配置中心热更新本地演示")
    print(f"  配置中心: {hub_base}")
    print(f"  客户端端点: {client_base}")
    print(f"  SQLite: {demo_db}")
    print("\n每轮结果")
    for label, key in (("v1", "round_v1"), ("v2", "round_v2"), ("v3", "round_v3")):
        round_data = output[key]
        print(
            f"  {label}: advanced={[x['client_id'] for x in round_data['advanced']]} "
            f"not_advanced={[(x['client_id'], x['code']) for x in round_data['not_advanced']]} "
            f"observed={round_data['consistency']['observed_conclusion']} "
            f"final={round_data['consistency']['conclusion']} "
            f"coverage={round_data['consistency']['coverage_complete']}"
        )
    details = output["round_v2"]["consistency"]["inconsistent_details"]
    if details:
        print("\n一致性差异明细")
        print(
            f"  client={details[0]['client_id']} picked={details[0]['picked_version']} "
            f"eligible={details[0]['eligible_for_final']}"
        )
        print(f"  difference_fields={details[0]['difference_fields']}")
    print("\n幂等与版本保护")
    print(
        f"  duplicate push reused={output['duplicate_push']['reused']} "
        f"network_replayed={output['duplicate_push']['request_counts_before'] != output['duplicate_push']['request_counts_after']}"
    )
    print(
        f"  conflicting push={output['conflicting_push']['body'].get('code')} "
        f"late ack={output['late_ack']['body'].get('code')} "
        f"invalid transition={output['invalid_transition']['code']}"
    )
    print("\n恢复结论")
    print(json.dumps(output["crash_recovery"]["recovery_report"], indent=2, ensure_ascii=False))
    failed = [name for name, passed in output["checks"].items() if not passed]
    print("\n校验汇总")
    print(f"  checks={len(output['checks'])} failed={failed or 'none'}")
    print(f"  report={report_path}")
    return 0 if not failed else 1


if __name__ == "__main__":
    raise SystemExit(run())

