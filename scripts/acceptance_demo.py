from __future__ import annotations

import copy
import json
import subprocess
import sys
import tempfile
from pathlib import Path

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parent.parent
SAMPLES = ROOT / "samples"
ARTIFACT = ROOT / "artifacts" / "acceptance_report.json"
sys.path.insert(0, str(ROOT))

from app.config import Settings  # noqa: E402
from app.main import create_app  # noqa: E402


def state_fingerprint(client: TestClient) -> dict:
    state = client.get("/api/v1/state").json()
    return {
        item["tenant_id"]: {
            key: item["state"][key]
            for key in (
                "status",
                "tokens_units",
                "used_bytes_units",
                "used_request_units",
                "last_refill_ms",
            )
        }
        for item in state["tenants"]
    }


def main() -> int:
    policy_seed = json.loads((SAMPLES / "policy_snapshot.json").read_text(encoding="utf-8"))
    fixed_batch = json.loads((SAMPLES / "events_batch.json").read_text(encoding="utf-8"))
    report: dict = {"samples": {"policy": "samples/policy_snapshot.json", "batch": "samples/events_batch.json"}}

    with tempfile.TemporaryDirectory(prefix="gateway-acceptance-") as temp:
        database = Path(temp) / "gateway.db"
        app = create_app(
            Settings(
                database_path=database,
                samples_dir=SAMPLES,
                auto_seed=True,
                auto_recover=True,
                throttle_rejection_threshold=3,
            )
        )
        with TestClient(app) as client:
            initial_state = state_fingerprint(client)
            batch_response = client.post("/api/v1/batches", json=fixed_batch)
            batch = batch_response.json()
            state_after_batch = state_fingerprint(client)
            report["fixed_batch"] = {
                "http_status": batch_response.status_code,
                "batch_id": batch["batch_id"],
                "summary": batch["summary"],
                "business_codes": [item["code"] for item in batch["events"]],
                "charged": [
                    {
                        "index": item["index"],
                        "tenant_id": item["tenant_id"],
                        "meter_mode": item["meter_mode"],
                        "capacity_bytes": item["charged"]["capacity_bytes"],
                        "request_tokens": item["charged"]["request_tokens"],
                    }
                    for item in batch["events"]
                ],
                "parent_cascade": batch["events"][5]["context"],
                "usage_after": {
                    tenant_id: {
                        "status": value["status"],
                        "used_bytes_units": value["used_bytes_units"],
                        "tokens_units": value["tokens_units"],
                    }
                    for tenant_id, value in state_after_batch.items()
                },
            }

            simulation = client.post(
                "/api/v1/simulations",
                json={
                    "label": "acceptance-before-edit",
                    "events": [
                        {
                            "event_time_ms": 1003000,
                            "tenant_id": "isolated",
                            "meter_mode": "COMPOSITE",
                            "io_direction": "WRITE",
                            "payload_bytes": 100,
                            "request_weight": 1,
                        }
                    ],
                },
            ).json()

            stale_snapshot = client.post("/api/v1/policy", json=copy.deepcopy(policy_seed))
            negative = copy.deepcopy(policy_seed)
            negative["version"] = 2
            negative["generated_at_ms"] = 1002600
            negative["tenants"][0]["capacity_bytes"] = -1
            negative_response = client.post("/api/v1/policy", json=negative)
            cycle = copy.deepcopy(policy_seed)
            cycle["version"] = 2
            cycle["generated_at_ms"] = 1002600
            for tenant in cycle["tenants"]:
                if tenant["tenant_id"] == "child":
                    tenant["parent_id"] = "grand"
                elif tenant["tenant_id"] == "grand":
                    tenant["parent_id"] = "child"
            cycle_response = client.post("/api/v1/policy", json=cycle)

            base_batch = {
                "events": [
                    {
                        "event_time_ms": 1003000,
                        "tenant_id": "isolated",
                        "meter_mode": "BYTES",
                        "io_direction": "WRITE",
                        "payload_bytes": 1,
                        "request_weight": 0,
                    }
                ]
            }
            fault_codes: dict[str, str] = {}
            for name, faults in (
                ("invalid_payload", {"invalid_payload": True}),
                ("unknown_tenant", {"unknown_tenant": True}),
                ("clock_rewind", {"clock_rewind": True}),
            ):
                payload = copy.deepcopy(base_batch)
                payload["batch_id"] = f"acceptance-{name}"
                payload["faults"] = faults
                fault_codes[name] = client.post("/api/v1/batches", json=payload).json()["code"]

            before_abort = state_fingerprint(client)
            abort_payload = copy.deepcopy(base_batch)
            abort_payload["batch_id"] = "acceptance-mid-batch-abort"
            abort_payload["faults"] = {"abort_midway": True}
            abort_response = client.post("/api/v1/batches", json=abort_payload).json()
            fault_codes["mid_batch_abort"] = abort_response["code"]
            abort_did_not_pollute = state_fingerprint(client) == before_abort
            recovery = client.post("/api/v1/recover").json()

            stale_base = {
                "base_version": 999,
                "generated_at_ms": 1002600,
                "tenant_patches": [{"tenant_id": "child", "capacity_bytes": 50000}],
            }
            stale_base_response = client.patch("/api/v1/policy", json=stale_base)

            patch = {
                "base_version": 1,
                "generated_at_ms": 1002600,
                "tenant_patches": [
                    {"tenant_id": "child", "capacity_bytes": 50000},
                    {"tenant_id": "sibling", "capacity_bytes": 60000},
                ],
            }
            patch_response = client.patch("/api/v1/policy", json=patch)
            simulation_stale = client.get(f"/api/v1/simulations/{simulation['run_id']}")

            report["exceptions"] = {
                "E_SPEC_INVALID": {
                    "negative_policy_http": negative_response.status_code,
                    "negative_policy": negative_response.json(),
                },
                "E_STALE_SNAPSHOT": {
                    "http": stale_snapshot.status_code,
                    "body": stale_snapshot.json(),
                },
                "E_TENANT_CYCLE": {
                    "http": cycle_response.status_code,
                    "body": cycle_response.json(),
                },
                "batch_fault_codes": fault_codes,
                "E_BATCH_ABORTED": {
                    "http": 409,
                    "body": abort_response,
                    "state_unchanged": abort_did_not_pollute,
                    "recovery_report": recovery,
                },
                "E_STALE_BASE": {
                    "http": stale_base_response.status_code,
                    "body": stale_base_response.json(),
                },
                "E_SIMULATION_STALE": {
                    "http": simulation_stale.status_code,
                    "body": simulation_stale.json(),
                },
            }
            report["incremental_edit"] = {
                "http": patch_response.status_code,
                "body": patch_response.json(),
                "usage_after": state_fingerprint(client),
            }
            report["simulation_before_edit"] = {
                "run_id": simulation["run_id"],
                "policy_version": simulation["policy_version"],
                "actual_state_mutated": simulation["actual_state_mutated"],
            }
            report["initial_state_units"] = initial_state

        crash_database = Path(temp) / "crash.db"
        crash_run = subprocess.run(
            [
                sys.executable,
                "-m",
                "app.cli",
                "crash-batch",
                "--database",
                str(crash_database),
                "--samples",
                str(SAMPLES),
                "--batch-file",
                str(SAMPLES / "crash_batch.json"),
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=30,
        )
        restarted_app = create_app(
            Settings(
                database_path=crash_database,
                samples_dir=SAMPLES,
                auto_seed=True,
                auto_recover=True,
                throttle_rejection_threshold=3,
            )
        )
        with TestClient(restarted_app) as restarted:
            crash_report = restarted.get("/api/v1/recovery/report").json()
            isolated = restarted.get("/api/v1/tenants/isolated").json()["state"]
            pending = restarted.get("/api/v1/intents", params={"status": "PENDING"}).json()
            report["forced_termination"] = {
                "worker_exit_code": crash_run.returncode,
                "recovery_report": crash_report,
                "pending_intents_after_restart": pending["intents"],
                "isolated_state_after_restart": {
                    "used_bytes_units": isolated["used_bytes_units"],
                    "tokens_units": isolated["tokens_units"],
                },
            }

        rebuild_database = Path(temp) / "rebuild-crash.db"
        rebuild_crash = subprocess.run(
            [
                sys.executable,
                "-m",
                "app.cli",
                "crash-rebuild",
                "--database",
                str(rebuild_database),
                "--samples",
                str(SAMPLES),
                "--patch-file",
                str(SAMPLES / "incremental_patch.json"),
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=30,
        )
        rebuild_app = create_app(
            Settings(
                database_path=rebuild_database,
                samples_dir=SAMPLES,
                auto_seed=True,
                auto_recover=True,
                throttle_rejection_threshold=3,
            )
        )
        with TestClient(rebuild_app) as restarted:
            rebuild_report = restarted.get("/api/v1/recovery/report").json()
            policy_version_after = restarted.get("/api/v1/policy").json()["version"]
            report["forced_termination_during_rebuild"] = {
                "worker_exit_code": rebuild_crash.returncode,
                "recovery_report": rebuild_report,
                "policy_version_after_restart": policy_version_after,
                "pending_intents_after_restart": restarted.get(
                    "/api/v1/intents", params={"status": "PENDING"}
                ).json()["intents"],
            }

    expected_codes = {
        "E_SPEC_INVALID",
        "E_UNKNOWN_TENANT",
        "E_STALE_SNAPSHOT",
        "E_TENANT_CYCLE",
        "E_CLOCK_REWIND",
        "E_BATCH_ABORTED",
        "E_STALE_BASE",
        "E_SIMULATION_STALE",
        "E_QUOTA_EXCEEDED",
        "E_RATE_LIMITED",
        "E_PARENT_QUOTA_EXCEEDED",
    }
    observed = set(report["fixed_batch"]["business_codes"])
    observed.update(report["exceptions"]["batch_fault_codes"].values())
    observed.add(report["exceptions"]["E_SPEC_INVALID"]["negative_policy"]["code"])
    observed.add(report["exceptions"]["E_STALE_SNAPSHOT"]["body"]["code"])
    observed.add(report["exceptions"]["E_TENANT_CYCLE"]["body"]["code"])
    observed.add(report["exceptions"]["E_BATCH_ABORTED"]["body"]["code"])
    observed.add(report["exceptions"]["E_STALE_BASE"]["body"]["code"])
    observed.add(report["exceptions"]["E_SIMULATION_STALE"]["body"]["code"])
    report["observed_business_codes"] = sorted(observed)
    report["acceptance"] = {
        "all_expected_codes_observed": expected_codes <= observed,
        "abort_did_not_pollute_state": report["exceptions"]["E_BATCH_ABORTED"]["state_unchanged"],
        "forced_termination_exit_91": report["forced_termination"]["worker_exit_code"] == 91,
        "recovery_found_pending_intent": bool(
            report["forced_termination"]["recovery_report"]["pending_intents"]
        ),
        "pending_intents_cleared": report["forced_termination"][
            "pending_intents_after_restart"
        ] == [],
        "rebuild_crash_exit_92": report["forced_termination_during_rebuild"][
            "worker_exit_code"
        ]
        == 92,
        "rebuild_recovered_to_version_1": report[
            "forced_termination_during_rebuild"
        ]["policy_version_after_restart"]
        == 1,
        "rebuild_pending_intents_cleared": report[
            "forced_termination_during_rebuild"
        ]["pending_intents_after_restart"]
        == [],
        "incremental_edit_created_version_2": report["incremental_edit"]["body"].get(
            "new_version"
        )
        == 2,
        "simulation_invalidated": report["exceptions"]["E_SIMULATION_STALE"]["body"].get(
            "code"
        )
        == "E_SIMULATION_STALE",
    }
    report["acceptance"]["ok"] = all(
        value for key, value in report["acceptance"].items() if key != "ok"
    )
    ARTIFACT.parent.mkdir(parents=True, exist_ok=True)
    ARTIFACT.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return 0 if report["acceptance"]["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
