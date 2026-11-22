from __future__ import annotations

import base64
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
from dataclasses import replace
from pathlib import Path
from typing import Any

import requests


ROOT = Path(__file__).resolve().parent.parent
ARTIFACTS = ROOT / "artifacts" / "e2e"
PYTHON = sys.executable
sys.path.insert(0, str(ROOT))

from svc_registry.formats import decode_snapshot, encode_snapshot  # noqa: E402
from svc_registry.models import RegistrationRecord, Snapshot  # noqa: E402


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_http(url: str, timeout: float = 15.0) -> None:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            response = requests.get(url, timeout=0.5)
            if response.status_code == 200:
                return
        except requests.RequestException as exc:
            last_error = exc
        time.sleep(0.1)
    raise RuntimeError(f"local service did not become ready: {url}: {last_error}")


def wait_tcp(host: str, port: int, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.25):
                return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError(f"local TCP target did not become ready: {host}:{port}")


class RegistryProcess:
    def __init__(self, port: int, db_path: Path, log_path: Path, env_extra: dict[str, str] | None = None) -> None:
        self.port = port
        self.db_path = db_path
        self.log_path = log_path
        self.env_extra = env_extra or {}
        self.process: subprocess.Popen[bytes] | None = None
        self.log_file = None

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def start(self) -> None:
        env = dict(os.environ)
        env.update(
            {
                "REGISTRY_DB_PATH": str(self.db_path),
                "REGISTRY_MAX_CONCURRENCY": "4",
                "REGISTRY_QUEUE_SIZE": "4",
                "PYTHONPATH": str(ROOT),
                **self.env_extra,
            }
        )
        env.pop("REGISTRY_FAULT_POINT", None)
        env.update(self.env_extra)
        self.log_file = self.log_path.open("ab")
        self.process = subprocess.Popen(
            [
                PYTHON,
                "-m",
                "uvicorn",
                "svc_registry.api:app",
                "--host",
                "127.0.0.1",
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
        wait_http(f"{self.base_url}/health")

    def stop(self, *, force: bool = False) -> None:
        if self.process is not None and self.process.poll() is None:
            if force:
                self.process.kill()
            else:
                self.process.terminate()
            try:
                self.process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        if self.log_file is not None:
            self.log_file.close()
            self.log_file = None
        self.process = None

    def wait_for_fault_exit(self) -> int:
        assert self.process is not None
        code = self.process.wait(timeout=10)
        if self.log_file is not None:
            self.log_file.close()
            self.log_file = None
        self.process = None
        return code


def api(method: str, base: str, path: str, **kwargs: Any) -> requests.Response:
    return requests.request(method, f"{base}{path}", timeout=15, **kwargs)


def expect_ok(response: requests.Response) -> dict[str, Any]:
    if response.status_code >= 400:
        raise RuntimeError(f"{response.request.method} {response.url} failed: {response.status_code} {response.text}")
    return response.json()


def b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def localize_samples(http_port: int, tcp_port: int) -> tuple[bytes, bytes, dict[str, Any]]:
    http_base = f"http://127.0.0.1:{http_port}"
    tcp_base = f"tcp://127.0.0.1:{tcp_port}"
    legacy_lines: list[dict[str, Any]] = []
    for line in (ROOT / "samples" / "legacy_v1.jsonl").read_text(encoding="utf-8").splitlines():
        item = json.loads(line)
        if item["endpoint"].startswith("http://"):
            item["endpoint"] = http_base + "/health/ok"
        else:
            item["endpoint"] = tcp_base
        legacy_lines.append(item)
    legacy_bytes = (
        "\n".join(json.dumps(item, sort_keys=True, separators=(",", ":")) for item in legacy_lines) + "\n"
    ).encode("utf-8")
    (ARTIFACTS / "legacy_local.jsonl").write_bytes(legacy_bytes)

    modern_source, _, _ = decode_snapshot((ROOT / "samples" / "modern_v2.bin").read_bytes())
    modern_records = []
    for record in modern_source.records:
        assert isinstance(record, RegistrationRecord)
        endpoint = http_base + "/health/ok" if record.endpoint.startswith("http://") else tcp_base
        modern_records.append(replace(record, endpoint=endpoint))
    modern_snapshot = Snapshot("registration", 2, tuple(modern_records), source_format="e2e")
    modern_bytes = encode_snapshot(modern_snapshot, target_format="binary", version=2, endianness="little")
    assert isinstance(modern_bytes, bytes)
    (ARTIFACTS / "modern_local.bin").write_bytes(modern_bytes)
    return legacy_bytes, modern_bytes, {
        "http_base": http_base,
        "tcp_endpoint": tcp_base,
        "note": "sample endpoints translated to dynamically confirmed free loopback ports",
    }


def jsonl_v2(records: list[dict[str, Any]]) -> bytes:
    lines = []
    for sequence, record in enumerate(records):
        lines.append(
            json.dumps(
                {
                    "format_version": 2,
                    "kind": "registration",
                    "sequence": sequence,
                    **record,
                },
                sort_keys=True,
                separators=(",", ":"),
            )
        )
    return ("\n".join(lines) + "\n").encode("utf-8")


def state_fingerprint(base: str) -> list[dict[str, Any]]:
    instances = expect_ok(api("GET", base, "/api/v1/instances"))["instances"]
    fields = (
        "service_name",
        "instance_id",
        "endpoint",
        "weight_permille",
        "lease_version",
        "lease_granted_at_ms",
        "lease_expires_at_ms",
        "last_registered_at_ms",
        "last_heartbeat_at_ms",
        "health_status",
        "health_reason",
        "record_version",
        "updated_at_ms",
    )
    return [{field: item[field] for field in fields} for item in instances]


def main() -> int:
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    for stale in ARTIFACTS.glob("*.db*"):
        stale.unlink()
    report: dict[str, Any] = {"ok": False, "steps": {}}
    http_port = free_port()
    tcp_port = free_port()
    registry_port = free_port()
    db_path = ARTIFACTS / "registry.db"
    test_service_log = (ARTIFACTS / "test_service.log").open("wb")
    test_service = subprocess.Popen(
        [
            PYTHON,
            "-m",
            "svc_registry.test_service",
            "--http-port",
            str(http_port),
            "--tcp-port",
            str(tcp_port),
        ],
        cwd=ROOT,
        stdout=test_service_log,
        stderr=subprocess.STDOUT,
    )
    registry = RegistryProcess(registry_port, db_path, ARTIFACTS / "registry.log")
    try:
        wait_http(f"http://127.0.0.1:{http_port}/health/ok")
        wait_tcp("127.0.0.1", tcp_port)
        registry.start()
        base = registry.base_url
        legacy_bytes, modern_bytes, localized = localize_samples(http_port, tcp_port)
        report["ports"] = {
            "registry": registry_port,
            "http": http_port,
            "tcp": tcp_port,
            "all_loopback": True,
        }
        report["samples"] = localized

        weight_policy = {"source_scale": "ratio", "rounding_rule": "half_up", "declared": True}
        time_policy = {"truncation_rule": "floor", "declared": True}
        negotiate_body = {
            "format": "jsonl",
            "data_base64": b64(legacy_bytes),
            "expected_kind": "registration",
            "weight_policy": weight_policy,
            "time_policy": time_policy,
        }
        negotiation = expect_ok(api("POST", base, "/api/v1/negotiate", json=negotiate_body))
        report["steps"]["negotiation"] = {
            "decision": negotiation["negotiation"]["decision"],
            "detected": negotiation["detected"],
            "degradation_details": negotiation["preview"]["degradation_details"],
            "weight_rounding": negotiation["preview"]["weight_rounding"],
            "time_truncation": negotiation["preview"]["time_truncation"],
        }

        legacy_to_binary = expect_ok(
            api(
                "POST",
                base,
                "/api/v1/convert",
                json={
                    **negotiate_body,
                    "target_format": "binary",
                    "target_version": 2,
                    "endianness": "big",
                },
            )
        )
        binary_to_jsonl = expect_ok(
            api(
                "POST",
                base,
                "/api/v1/convert",
                json={
                    "format": "binary",
                    "data_base64": legacy_to_binary["data_base64"],
                    "target_format": "jsonl",
                    "target_version": 2,
                    "shards": 2,
                    "time_policy": time_policy,
                },
            )
        )
        report["steps"]["conversion_equivalence"] = {
            "legacy_to_big_endian_binary": legacy_to_binary["target"],
            "binary_to_jsonl_shards": len(binary_to_jsonl["shards_base64"]),
            "semantic_hash_forward": legacy_to_binary["semantic_hash"],
            "semantic_hash_reverse": binary_to_jsonl["semantic_hash"],
            "equivalent": legacy_to_binary["semantic_hash"] == binary_to_jsonl["semantic_hash"],
        }
        if not report["steps"]["conversion_equivalence"]["equivalent"]:
            raise RuntimeError("binary/JSONL semantic hashes differ")

        legacy_import = expect_ok(
            api(
                "POST",
                base,
                "/api/v1/import",
                json={**negotiate_body, "batch_id": "legacy-v1"},
            )
        )
        modern_import = expect_ok(
            api(
                "POST",
                base,
                "/api/v1/import",
                json={
                    "batch_id": "modern-v2",
                    "format": "binary",
                    "data_base64": b64(modern_bytes),
                    "expected_kind": "registration",
                },
            )
        )
        replay = expect_ok(
            api(
                "POST",
                base,
                "/api/v1/import",
                json={**negotiate_body, "batch_id": "legacy-v1"},
            )
        )
        report["steps"]["imports"] = {
            "legacy_status": legacy_import["status"],
            "legacy_decision": legacy_import["negotiation"]["decision"],
            "legacy_weight_permille": [item["weight_permille"] for item in legacy_import["weight_rounding"]],
            "legacy_degradations": len(legacy_import["degradation_details"]),
            "modern_status": modern_import["status"],
            "modern_decision": modern_import["negotiation"]["decision"],
            "replay_executed": replay["executed"],
            "replay_reused": replay["replayed"],
        }

        high_version = (ROOT / "samples" / "legacy_v1.jsonl").read_text(encoding="utf-8").splitlines()[0]
        future_item = json.loads(high_version)
        future_item["format_version"] = 3
        future_item["unknown_required_fields"] = ["future_required_field"]
        future_response = api(
            "POST",
            base,
            "/api/v1/negotiate",
            json={"format": "jsonl", "data_base64": b64((json.dumps(future_item) + "\n").encode())},
        )
        corrupt_bytes = bytearray(modern_bytes)
        corrupt_bytes[-1] ^= 0xFF
        corrupt_response = api(
            "POST",
            base,
            "/api/v1/negotiate",
            json={"format": "binary", "data_base64": b64(bytes(corrupt_bytes))},
        )
        report["steps"]["version_and_corruption"] = {
            "future_version_http": future_response.status_code,
            "future_version_code": future_response.json()["error"]["code"],
            "corrupt_http": corrupt_response.status_code,
            "corrupt_code": corrupt_response.json()["error"]["code"],
            "corrupt_record_index": corrupt_response.json()["error"]["details"].get("record_index"),
        }

        extra_instances = [
            {
                "service_name": "probe-extra",
                "instance_id": "slow-http",
                "endpoint": f"http://127.0.0.1:{http_port}/health/slow?delay_ms=1200",
                "probe_method": "http",
                "probe_params": {"timeout_ms": 120, "expected_status": 200, "expected_body_contains": "OK"},
            },
            {
                "service_name": "probe-extra",
                "instance_id": "tcp-refused",
                "endpoint": f"tcp://127.0.0.1:{free_port()}",
                "probe_method": "tcp",
                "probe_params": {"timeout_ms": 150},
            },
        ]
        for item in extra_instances:
            expect_ok(
                api(
                    "POST",
                    base,
                    "/api/v1/register",
                    json={**item, "weight": 500, "lease_ttl_ms": 30_000},
                )
            )
        probe_round = expect_ok(api("POST", base, "/api/v1/probes/run", json={"budget_ms": 4000}))
        by_id = {item["instance_id"]: item for item in probe_round["results"]}
        report["steps"]["probe_round"] = {
            "counts": probe_round["counts"],
            "http_ok": by_id.get("catalog-1", {}).get("outcome"),
            "tcp_ok": by_id.get("catalog-2", {}).get("outcome"),
            "lease": by_id.get("ledger-1", {}).get("outcome"),
            "timeout_code": by_id.get("slow-http", {}).get("error_code"),
            "timeout_outcome": by_id.get("slow-http", {}).get("outcome"),
            "tcp_refused": by_id.get("tcp-refused", {}).get("outcome"),
        }
        if by_id.get("slow-http", {}).get("error_code") != "E_PROBE_TIMEOUT":
            raise RuntimeError("slow HTTP service did not produce E_PROBE_TIMEOUT")

        queue_ids = []
        for index in range(10):
            service_name = "queue"
            instance_id = f"q-{index}"
            expect_ok(
                api(
                    "POST",
                    base,
                    "/api/v1/register",
                    json={
                        "service_name": service_name,
                        "instance_id": instance_id,
                        "endpoint": f"http://127.0.0.1:{http_port}/health/ok",
                        "weight": 500,
                        "lease_ttl_ms": 30_000,
                        "probe_method": "http",
                        "probe_params": {"timeout_ms": 1000, "expected_status": 200, "expected_body_contains": "OK"},
                    },
                )
            )
            queue_ids.append({"service_name": service_name, "instance_id": instance_id})
        queue_round = expect_ok(
            api("POST", base, "/api/v1/probes/run", json={"instances": queue_ids, "budget_ms": 4000})
        )
        queue_full = [item for item in queue_round["results"] if item.get("error_code") == "E_PROBE_QUEUE_FULL"]
        report["steps"]["bounded_queue"] = {
            "requested": queue_round["counts"]["requested"],
            "applied": queue_round["counts"]["applied"],
            "queue_full": len(queue_full),
            "limit": "concurrency=4, queue_size=4",
        }

        existing = expect_ok(api("GET", base, "/api/v1/instances/catalog/catalog-1"))["instance"]
        base_record = {
            "service_name": "catalog",
            "instance_id": "catalog-1",
            "endpoint": existing["endpoint"],
            "weight": existing["weight_permille"],
            "lease_ttl_ms": existing["lease_ttl_ms"],
            "probe_method": existing["probe_method"],
            "probe_params": existing["probe_params"],
            "registered_at_us": existing["last_registered_at_ms"] * 1000,
            "lease_version": existing["lease_version"],
            "probe_window_ms": existing["probe_window_ms"],
        }
        skip_record = {**base_record, "registered_at_us": 1, "weight": 999}
        skip_import = expect_ok(
            api(
                "POST",
                base,
                "/api/v1/import",
                json={
                    "batch_id": "conflict-skip",
                    "format": "jsonl",
                    "data_base64": b64(jsonl_v2([skip_record])),
                    "time_policy": time_policy,
                },
            )
        )
        override_record = {
            **base_record,
            "lease_version": existing["lease_version"] + 1,
            "weight": 800,
        }
        override_import = expect_ok(
            api(
                "POST",
                base,
                "/api/v1/import",
                json={
                    "batch_id": "conflict-override",
                    "format": "jsonl",
                    "data_base64": b64(jsonl_v2([override_record])),
                    "time_policy": time_policy,
                },
            )
        )
        current = expect_ok(api("GET", base, "/api/v1/instances/catalog/catalog-1"))["instance"]
        ambiguous = {
            **base_record,
            "endpoint": f"http://127.0.0.1:{http_port}/health/wrong-marker",
            "weight": current["weight_permille"],
            "lease_version": current["lease_version"],
            "registered_at_us": current["last_registered_at_ms"] * 1000,
        }
        brand_new = {
            **base_record,
            "instance_id": "must-not-appear",
            "weight": 500,
            "lease_version": 1,
            "registered_at_us": 1,
        }
        reject_response = api(
            "POST",
            base,
            "/api/v1/import",
            json={
                "batch_id": "conflict-reject",
                "format": "jsonl",
                "data_base64": b64(jsonl_v2([brand_new, ambiguous])),
                "time_policy": time_policy,
            },
        )
        conflicting_batch = api(
            "POST",
            base,
            "/api/v1/import",
            json={
                "batch_id": "conflict-skip",
                "format": "jsonl",
                "data_base64": b64(jsonl_v2([{**skip_record, "weight": 123}])),
                "time_policy": time_policy,
            },
        )
        new_after_reject = api("GET", base, "/api/v1/instances/catalog/must-not-appear")
        report["steps"]["conflicts"] = {
            "skip_outcome": skip_import["outcomes"][0],
            "override_outcome": override_import["outcomes"][0],
            "reject_http": reject_response.status_code,
            "reject_outcomes": reject_response.json()["error"]["details"]["report"]["outcomes"],
            "atomic_new_record_present": new_after_reject.status_code == 200,
            "conflicting_batch_http": conflicting_batch.status_code,
            "conflicting_batch_code": conflicting_batch.json()["error"]["code"],
        }
        if report["steps"]["conflicts"]["atomic_new_record_present"]:
            raise RuntimeError("rejected import changed instance state")

        # ---- forced termination during import ----
        state_before_import_crash = state_fingerprint(base)
        registry.stop()
        registry = RegistryProcess(
            registry_port,
            db_path,
            ARTIFACTS / "registry_import_crash.log",
            {"REGISTRY_FAULT_POINT": "import_after_stage"},
        )
        registry.start()
        crash_record = {**brand_new, "instance_id": "crash-import", "service_name": "crash"}
        try:
            api(
                "POST",
                registry.base_url,
                "/api/v1/import",
                json={
                    "batch_id": "crash-import",
                    "format": "jsonl",
                    "data_base64": b64(jsonl_v2([crash_record])),
                    "time_policy": time_policy,
                },
            )
        except requests.RequestException:
            pass
        import_crash_exit = registry.wait_for_fault_exit()
        registry = RegistryProcess(registry_port, db_path, ARTIFACTS / "registry.log")
        registry.start()
        import_recovery = expect_ok(api("GET", registry.base_url, "/api/v1/recovery/latest"))["report"]
        state_after_import_recovery = state_fingerprint(registry.base_url)
        report["steps"]["recovery_import"] = {
            "forced_exit_code": import_crash_exit,
            "scanned_intents": import_recovery["scanned_intents"],
            "aborted_batches": import_recovery["imports"]["aborted_batches"],
            "state_diff": [] if state_before_import_crash == state_after_import_recovery else {
                "before": state_before_import_crash,
                "after": state_after_import_recovery,
            },
            "committed_facts_preserved": import_recovery["committed_facts_preserved"],
        }
        if state_before_import_crash != state_after_import_recovery:
            raise RuntimeError("recovery changed committed instance facts after import crash")

        # ---- forced termination during probe round ----
        state_before_probe_crash = state_fingerprint(registry.base_url)
        history_before = expect_ok(api("GET", registry.base_url, "/api/v1/probes/history?limit=1000"))["history"]
        registry.stop()
        registry = RegistryProcess(
            registry_port,
            db_path,
            ARTIFACTS / "registry_probe_crash.log",
            {"REGISTRY_FAULT_POINT": "probe_after_attempts"},
        )
        registry.start()
        try:
            api(
                "POST",
                registry.base_url,
                "/api/v1/probes/run",
                json={"instances": queue_ids[:3], "budget_ms": 3000},
            )
        except requests.RequestException:
            pass
        probe_crash_exit = registry.wait_for_fault_exit()
        registry = RegistryProcess(registry_port, db_path, ARTIFACTS / "registry.log")
        registry.start()
        probe_recovery = expect_ok(api("GET", registry.base_url, "/api/v1/recovery/latest"))["report"]
        state_after_probe_recovery = state_fingerprint(registry.base_url)
        history_after = expect_ok(api("GET", registry.base_url, "/api/v1/probes/history?limit=1000"))["history"]
        report["steps"]["recovery_probe"] = {
            "forced_exit_code": probe_crash_exit,
            "scanned_intents": probe_recovery["scanned_intents"],
            "aborted_rounds": probe_recovery["probe"]["aborted_rounds"],
            "rolled_back_attempts": len(probe_recovery["probe"]["rolled_back_attempts"]),
            "history_rows_before": len(history_before),
            "history_rows_after": len(history_after),
            "state_diff": [] if state_before_probe_crash == state_after_probe_recovery else {
                "before": state_before_probe_crash,
                "after": state_after_probe_recovery,
            },
            "uncommitted_probe_results": probe_recovery["probe"]["uncommitted_probe_results"],
        }
        if state_before_probe_crash != state_after_probe_recovery:
            raise RuntimeError("recovery changed committed instance facts after probe crash")
        if len(history_before) != len(history_after):
            raise RuntimeError("uncommitted probe history became visible after crash")

        report["ok"] = True
        report["verified_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        return 0
    finally:
        registry.stop()
        if test_service.poll() is None:
            test_service.terminate()
            try:
                test_service.wait(timeout=5)
            except subprocess.TimeoutExpired:
                test_service.kill()
                test_service.wait(timeout=5)
        test_service_log.close()
        (ARTIFACTS / "e2e_report.json").write_text(
            json.dumps(report, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        print(json.dumps(report, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    raise SystemExit(main())
