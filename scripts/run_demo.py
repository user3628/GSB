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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from mpc.samples import DEFAULT_SEED, generate_sample_workspace


def free_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def http_json(
    method: str,
    url: str,
    body: dict[str, Any] | None = None,
    *,
    timeout: float = 10.0,
) -> tuple[int, dict[str, Any]]:
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body, sort_keys=True).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8")
        try:
            return exc.code, json.loads(raw)
        except json.JSONDecodeError:
            return exc.code, {"raw": raw}


def wait_health(base_url: str, process: subprocess.Popen[Any], timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    last_error: str | None = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stderr = process.stderr.read() if process.stderr else ""
            raise RuntimeError(f"server exited before ready: {stderr[-2000:]}")
        try:
            status, payload = http_json("GET", f"{base_url}/api/health", timeout=1.0)
            if status == 200 and payload.get("ok"):
                return
            last_error = json.dumps(payload, ensure_ascii=False)
        except Exception as exc:  # connection refused while Flask starts
            last_error = str(exc)
        time.sleep(0.1)
    raise RuntimeError(f"server did not become ready: {last_error}")


def start_server(
    *,
    python: str,
    port: int,
    db: Path,
    samples: Path,
    log_path: Path,
) -> subprocess.Popen[Any]:
    env = os.environ.copy()
    for key in ("MPC_CRASH_POINT", "MPC_CRASH_MARKER", "MPC_CRASH_HOLD_SECONDS"):
        env.pop(key, None)
    env["MPC_DB_PATH"] = str(db)
    env["MPC_SAMPLES_DIR"] = str(samples)
    log = log_path.open("ab")
    process = subprocess.Popen(
        [
            python,
            "-m",
            "mpc.server",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--db",
            str(db),
            "--samples",
            str(samples),
        ],
        cwd=str(ROOT),
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    process._demo_log = log  # type: ignore[attr-defined]
    return process


def stop_server(process: subprocess.Popen[Any]) -> None:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)
    log = getattr(process, "_demo_log", None)
    if log is not None:
        log.close()


def success_data(status: int, payload: dict[str, Any], context: str) -> dict[str, Any]:
    if status != 200 or not payload.get("ok"):
        raise RuntimeError(f"{context} failed: HTTP {status} {json.dumps(payload, ensure_ascii=False)}")
    return payload["data"]


def expect_error(
    status: int,
    payload: dict[str, Any],
    expected_code: str,
    context: str,
) -> dict[str, Any]:
    if payload.get("ok") is not False or payload.get("code") != expected_code:
        raise RuntimeError(
            f"{context}: expected {expected_code}, got HTTP {status} {json.dumps(payload, ensure_ascii=False)}"
        )
    return payload


def summary_by_shard(derived: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {shard["shard_id"]: shard for shard in derived["shards"]}


def main() -> int:
    demo_dir = ROOT / "var" / "demo"
    if demo_dir.exists():
        shutil.rmtree(demo_dir)
    demo_dir.mkdir(parents=True)
    samples = ROOT / "data" / "seed_20261122"
    generate_sample_workspace(samples, seed=DEFAULT_SEED, overwrite=True)
    db = demo_dir / "checkpoints.sqlite3"
    port = free_loopback_port()
    base_url = f"http://127.0.0.1:{port}"
    server_log = demo_dir / "server.log"
    server = start_server(
        python=sys.executable,
        port=port,
        db=db,
        samples=samples,
        log_path=server_log,
    )
    report: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "environment": {
            "python": sys.version,
            "platform": sys.platform,
            "loopback_url": base_url,
            "database": str(db),
            "samples": str(samples),
            "seed": DEFAULT_SEED,
        },
        "checks": {},
    }
    try:
        wait_health(base_url, server)

        full_policy_body = {
            "commit_id": "demo-policy-full-1",
            "format_version": 1,
            "version": 1,
            "loss_scale": 64.0,
            "loss_scale_initial": 64.0,
            "growth_factor": 2.0,
            "shards": {"s0": "FP16", "s1": "FP32", "s2": "FP16", "s3": "FP32"},
        }
        conflicting_body = dict(full_policy_body)
        conflicting_body["growth_factor"] = 4.0
        status, payload = http_json("POST", f"{base_url}/api/policies/snapshots", full_policy_body)
        full_policy = success_data(status, payload, "full policy")
        report["full_policy"] = full_policy["policy"]

        status, payload = http_json(
            "POST",
            f"{base_url}/api/checkpoints",
            {
                "commit_id": "demo-checkpoint-round-1",
                "round": 1,
                "policy_version": 1,
                "loss_scale": 64.0,
                "scheduler_position": {"epoch": 0, "step": 12, "lr_index": 12},
                "data_cursor": {"epoch": 0, "batch": 3, "sample_offset": 384},
                "shard_ids": ["s0", "s1", "s2", "s3"],
            },
        )
        full_checkpoint = success_data(status, payload, "full checkpoint")
        report["full_checkpoint"] = {
            key: full_checkpoint[key]
            for key in (
                "commit_id",
                "checkpoint_version",
                "round",
                "policy_version",
                "loss_scale",
                "manifest_digest",
                "formal_manifest_only",
                "partial_shards_excluded",
            )
        }

        status, payload = http_json("GET", f"{base_url}/api/versions/1/derived")
        derived_v1 = success_data(status, payload, "derived v1")
        report["derived_before_increment"] = derived_v1

        # Failure probes prove that invalid submissions do not advance state.
        error_probes: dict[str, Any] = {}
        status, payload = http_json(
            "POST",
            f"{base_url}/api/policies/increments",
            {
                "commit_id": "demo-probe-stale-base",
                "base_version": 99,
                "version": 2,
                "shards": {"s1": "FP16"},
            },
        )
        error_probes["E_STALE_BASE"] = expect_error(status, payload, "E_STALE_BASE", "stale base probe")

        status, payload = http_json(
            "POST",
            f"{base_url}/api/policies/snapshots",
            {
                "commit_id": "demo-probe-stale-snapshot",
                "version": 1,
                "loss_scale": 64.0,
                "shards": {"s0": "FP16", "s1": "FP32", "s2": "FP16", "s3": "FP32"},
            },
        )
        error_probes["E_STALE_SNAPSHOT"] = expect_error(
            status, payload, "E_STALE_SNAPSHOT", "stale snapshot probe"
        )

        status, payload = http_json(
            "POST",
            f"{base_url}/api/checkpoints",
            {
                "commit_id": "demo-probe-checksum",
                "round": 2,
                "scheduler_position": {"epoch": 0, "step": 20},
                "data_cursor": {"epoch": 0, "batch": 5},
                "shards": [{"shard_id": "s0", "fp32_master_digest": "0" * 64}],
            },
        )
        error_probes["E_CHECKSUM_MISMATCH"] = expect_error(
            status, payload, "E_CHECKSUM_MISMATCH", "checksum probe"
        )

        status, payload = http_json(
            "POST",
            f"{base_url}/api/policies/snapshots",
            {
                "commit_id": "demo-probe-precision",
                "version": 90,
                "loss_scale": 64.0,
                "shards": {"s0": "BF16", "s1": "FP32", "s2": "FP16", "s3": "FP32"},
            },
        )
        error_probes["E_INVALID_PRECISION"] = expect_error(
            status, payload, "E_INVALID_PRECISION", "precision probe"
        )

        status, payload = http_json(
            "POST", f"{base_url}/api/policies/snapshots", conflicting_body
        )
        error_probes["E_CONFLICTING_COMMIT"] = expect_error(
            status, payload, "E_CONFLICTING_COMMIT", "conflicting commit probe"
        )
        report["failure_probes"] = {
            key: {"code": value["code"], "message": value["error"]["message"]}
            for key, value in error_probes.items()
        }
        report["checks"]["failure_probes_leave_state_unchanged"] = (
            http_json("GET", f"{base_url}/api/policies/current")[1]["data"]["version"] == 1
        )

        increment_body = {
            "commit_id": "demo-policy-increment-2",
            "base_version": 1,
            "version": 2,
            "shards": {"s1": "FP16"},
        }
        status, payload = http_json("POST", f"{base_url}/api/policies/increments", increment_body)
        increment = success_data(status, payload, "single-shard policy increment")
        report["single_shard_increment"] = {
            "commit_id": increment["commit_id"],
            "base_version": increment["base_version"],
            "policy": increment["policy"],
            "affected_shards": increment["affected_shards"],
            "changed_precisions": increment["changed_precisions"],
            "changed_shards": increment["changed_shards"],
            "loss_scale_changed": increment["loss_scale_changed"],
            "invalidations": increment["derived_rebuild"]["invalidations"],
        }
        status, payload = http_json("GET", f"{base_url}/api/versions/1/derived")
        derived_v2 = success_data(status, payload, "derived v2")
        report["derived_after_increment"] = derived_v2

        before = summary_by_shard(derived_v1)
        after = summary_by_shard(derived_v2)
        changed = sorted(
            shard_id
            for shard_id in before
            if before[shard_id]["derived_summary_digest"]
            != after[shard_id]["derived_summary_digest"]
        )
        report["derived_diff"] = {
            "changed_summary_shards": changed,
            "unchanged_summary_shards": sorted(set(before) - set(changed)),
            "changed_restore_shards": sorted(
                shard_id
                for shard_id in before
                if before[shard_id]["restore_digest"] != after[shard_id]["restore_digest"]
            ),
            "memory_bytes_before": derived_v1["aggregate"]["memory_bytes"],
            "memory_bytes_after": derived_v2["aggregate"]["memory_bytes"],
            "memory_bytes_delta": derived_v2["aggregate"]["memory_bytes"]
            - derived_v1["aggregate"]["memory_bytes"],
            "aggregate_before": derived_v1["aggregate"],
            "aggregate_after": derived_v2["aggregate"],
        }
        report["checks"]["single_shard_increment_changed_only_expected_shard"] = changed == ["s1"]
        report["checks"]["aggregate_rebuilt"] = (
            derived_v1["aggregate"]["derived_summary_digest"]
            != derived_v2["aggregate"]["derived_summary_digest"]
        )
        report["checks"]["memory_estimate_decreased_for_fp16_shard"] = (
            derived_v2["aggregate"]["memory_bytes"] < derived_v1["aggregate"]["memory_bytes"]
        )
        report["checks"]["repeated_query_bitwise_stable"] = (
            http_json("GET", f"{base_url}/api/versions/1/derived")[1]["data"] == derived_v2
        )

        # Stop the API before the deliberately doomed writer starts. The writer
        # is killed after the durable intent and partial transaction writes.
        stop_server(server)
        crash_payload = demo_dir / "crash-payload.json"
        crash_payload.write_text(
            json.dumps(
                {
                    "commit_id": "demo-policy-increment-crash",
                    "base_version": 2,
                    "version": 3,
                    "shards": {"s2": "FP32"},
                    "loss_scale": 32.0,
                }
            ),
            encoding="utf-8",
        )
        marker = demo_dir / "crash-ready.marker"
        env = os.environ.copy()
        env.update(
            {
                "MPC_CRASH_POINT": "mid_rebuild",
                "MPC_CRASH_MARKER": str(marker),
                "MPC_CRASH_HOLD_SECONDS": "60",
            }
        )
        crash_worker = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "mpc.crash_worker",
                "--db",
                str(db),
                "--samples",
                str(samples),
                "--operation",
                "policy-increment",
                "--payload",
                str(crash_payload),
            ],
            cwd=str(ROOT),
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        deadline = time.monotonic() + 20
        while not marker.exists() and time.monotonic() < deadline:
            if crash_worker.poll() is not None:
                break
            time.sleep(0.05)
        if not marker.exists():
            crash_worker.kill()
            crash_worker.wait()
            raise RuntimeError("crash worker never reached the injected mid-rebuild point")
        crash_worker.kill()
        crash_worker.wait(timeout=10)
        report["forced_termination"] = {
            "crash_point": "mid_rebuild",
            "marker_observed": True,
            "worker_returncode": crash_worker.returncode,
            "database_before_restart_policy_version": json.loads(
                subprocess.check_output(
                    [
                        sys.executable,
                        "-c",
                        (
                            "import sqlite3,json,sys;"
                            "c=sqlite3.connect(sys.argv[1]);"
                            "print(json.dumps({"
                            "'policy_version':c.execute('select current_version from policy_state').fetchone()[0],"
                            "'pending_intents':c.execute(\"select count(*) from intents where status='pending'\").fetchone()[0],"
                            "'checkpoint_version':c.execute('select latest_seq from checkpoint_state').fetchone()[0]"
                            "}))"
                        ),
                        str(db),
                    ],
                    text=True,
                )
            ),
        }

        server = start_server(
            python=sys.executable,
            port=port,
            db=db,
            samples=samples,
            log_path=server_log,
        )
        wait_health(base_url, server)
        status, payload = http_json("GET", f"{base_url}/api/recovery/report")
        recovery = success_data(status, payload, "recovery report")
        report["recovery_report"] = recovery
        report["checks"]["recovery_aborted_one_pending_intent"] = (
            recovery["scanned_pending"] == 1
            and recovery["aborted_count"] == 1
            and recovery["completed_count"] == 0
        )
        report["checks"]["recovery_kept_last_complete_checkpoint"] = (
            recovery["last_complete_checkpoint_version"] == 1
        )
        report["checks"]["recovery_kept_previous_policy_version"] = (
            recovery["current_policy_version"] == 2
        )
        report["checks"]["recovery_derived_consistent"] = bool(
            recovery["derived_consistency"]["consistent"]
        )
        report["checks"]["recovery_does_not_claim_complete_exit_report"] = (
            recovery["guarantees_complete_exit_report"] is False
        )
        status, payload = http_json("GET", f"{base_url}/api/versions/1/derived")
        derived_after_recovery = success_data(status, payload, "derived after recovery")
        report["checks"]["derived_identical_after_recovery"] = derived_after_recovery == derived_v2
        report["recovery_derived_summary_digest"] = derived_after_recovery["aggregate"][
            "derived_summary_digest"
        ]
        report["checks"]["all"] = all(
            value for key, value in report["checks"].items() if key != "all"
        )
    finally:
        if server.poll() is None:
            stop_server(server)

    output_path = ROOT / "artifacts" / "demo_report.json"
    output_path.write_text(
        json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print("=== Deterministic mixed-precision checkpoint demo ===")
    print(f"fixed seed: {DEFAULT_SEED}")
    print(f"policy: v1 -> v2, affected={report['single_shard_increment']['affected_shards']}")
    print(f"derived changed shards: {report['derived_diff']['changed_summary_shards']}")
    print(
        "aggregate max_abs: "
        f"{report['derived_diff']['aggregate_before']['max_absolute']:.12g} -> "
        f"{report['derived_diff']['aggregate_after']['max_absolute']:.12g}"
    )
    print(
        "aggregate cumulative rounding error: "
        f"{report['derived_diff']['aggregate_before']['cumulative_rounding_error']:.12g} -> "
        f"{report['derived_diff']['aggregate_after']['cumulative_rounding_error']:.12g}"
    )
    print(
        "memory estimate bytes: "
        f"{report['derived_diff']['memory_bytes_before']} -> "
        f"{report['derived_diff']['memory_bytes_after']}"
    )
    print(
        "forced termination: "
        f"pending={report['forced_termination']['database_before_restart_policy_version']['pending_intents']} "
        f"aborted={report['recovery_report']['aborted_count']} "
        f"policy={report['recovery_report']['current_policy_version']} "
        f"checkpoint={report['recovery_report']['last_complete_checkpoint_version']}"
    )
    print(f"all assertions passed: {report['checks']['all']}")
    print(f"report: {output_path}")
    return 0 if report["checks"]["all"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
