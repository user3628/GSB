from __future__ import annotations

import argparse
import hashlib
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Iterator

import httpx

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from msmq.formats import decode_auto  # noqa: E402

SAMPLES = ROOT / "artifacts" / "samples"
VALIDATION_OUT = ROOT / "artifacts" / "validation"
LOG_OUT = ROOT / "artifacts" / "logs"


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class ServiceProcess:
    def __init__(self, db_path: Path, *, failpoints: bool = False) -> None:
        self.db_path = db_path
        self.failpoints = failpoints
        self.port = free_port()
        self.base_url = f"http://127.0.0.1:{self.port}"
        self.process: subprocess.Popen[bytes] | None = None
        self.log_path: Path | None = None
        self.log_handle = None

    def start(self) -> "ServiceProcess":
        LOG_OUT.mkdir(parents=True, exist_ok=True)
        self.log_path = LOG_OUT / f"service-{self.port}-{time.time_ns()}.log"
        self.log_handle = self.log_path.open("wb")
        command = [
            sys.executable,
            str(ROOT / "scripts" / "start_service.py"),
            "--host",
            "127.0.0.1",
            "--port",
            str(self.port),
            "--db",
            str(self.db_path),
        ]
        if self.failpoints:
            command.append("--enable-failpoints")
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        self.process = subprocess.Popen(
            command,
            cwd=ROOT,
            stdout=self.log_handle,
            stderr=subprocess.STDOUT,
            env=env,
        )
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                self._raise_start_failure()
            try:
                with self.client(timeout=1.0) as client:
                    response = client.get("/health")
                    if response.status_code == 200:
                        return self
            except Exception:
                pass
            time.sleep(0.1)
        self._raise_start_failure("timed out waiting for /health")
        raise RuntimeError("unreachable")

    def _raise_start_failure(self, reason: str = "server exited before health check") -> None:
        output = ""
        if self.log_path and self.log_path.exists():
            output = self.log_path.read_text(encoding="utf-8", errors="replace")[-5000:]
        raise RuntimeError(f"{reason}. Exit={self.process.poll() if self.process else None}\n{output}")

    def client(self, *, timeout: float = 10.0) -> httpx.Client:
        return httpx.Client(
            base_url=self.base_url,
            timeout=timeout,
            trust_env=False,
            follow_redirects=False,
        )

    def wait_for_crash(self, timeout: float = 10.0) -> int:
        if self.process is None:
            raise RuntimeError("service has not been started")
        try:
            return self.process.wait(timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError("expected failpoint process termination did not occur") from exc

    def stop(self) -> None:
        if self.process is not None and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        if self.log_handle is not None:
            self.log_handle.flush()
            self.log_handle.close()
            self.log_handle = None

    def __enter__(self) -> "ServiceProcess":
        return self.start()

    def __exit__(self, exc_type, exc, tb) -> None:
        self.stop()


def expect_ok(response: httpx.Response, expected: int = 200) -> dict[str, Any]:
    if response.status_code != expected:
        raise AssertionError(
            f"expected HTTP {expected}, got {response.status_code}: {response.text[:3000]}"
        )
    return response.json()


def expect_error(response: httpx.Response, code: str, status: int | None = None) -> dict[str, Any]:
    if status is not None and response.status_code != status:
        raise AssertionError(
            f"expected HTTP {status} for {code}, got {response.status_code}: {response.text[:3000]}"
        )
    body = response.json()
    actual = body.get("error", {}).get("code")
    if actual != code:
        raise AssertionError(f"expected {code}, got {actual}: {response.text[:3000]}")
    return body


def canonical_message(message: dict[str, Any]) -> dict[str, Any]:
    return {
        "message_id": message["message_id"],
        "queue": message["queue"],
        "body": message["body"],
        "content_type": message["content_type"],
        "produced_at_us": message["produced_at_us"],
        "expires_at_us": message["expires_at_us"],
        "state": message["state"],
        "record_version": message["record_version"],
        "last_write_at_us": message["last_write_at_us"],
        "delivery_count": message["delivery_count"],
        "extensions": message["extensions"],
    }


def canonical_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    return {
        "semantics": snapshot["queue"]["semantics"],
        "visibility_timeout_ms": snapshot["queue"]["visibility_timeout_ms"],
        "queue_extensions": snapshot["queue"]["extensions"],
        "messages": [canonical_message(message) for message in snapshot["messages"]],
    }


def load_sample_messages() -> tuple[dict[str, Any], list[dict[str, Any]]]:
    data = json.loads((SAMPLES / "messages.json").read_text(encoding="utf-8"))
    return data["queue"], data["messages"]


def seed_source_queue(client: httpx.Client) -> dict[str, Any]:
    queue_meta, messages = load_sample_messages()
    queue_meta = dict(queue_meta)
    queue_meta.pop("version", None)
    expect_ok(
        client.post("/v1/queues", json={**queue_meta, "visibility_timeout_ms": 500}),
        expected=201,
    )
    for message in messages:
        response = client.post(
            f"/v1/queues/{queue_meta['name']}/messages",
            json={
                "message_id": message["message_id"],
                "body": message["body"],
                "content_type": message["content_type"],
                "ttl_ms": (message["expires_at_us"] - message["produced_at_us"]) // 1000,
                "produced_at_ms": message["produced_at_us"] // 1000,
                "extensions": message["extensions"],
            },
        )
        expect_ok(response, expected=201)
    return expect_ok(client.get(f"/v1/queues/{queue_meta['name']}/snapshot"))


def check_roundtrip() -> dict[str, Any]:
    VALIDATION_OUT.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="msmq-roundtrip-source-") as temp:
        with ServiceProcess(Path(temp) / "source.sqlite3") as source:
            with source.client() as client:
                baseline = seed_source_queue(client)
                compact_response = client.get("/v1/export/compact?queue=demo-orders")
                jsonl_response = client.get("/v1/export/jsonl?queue=demo-orders")
                if compact_response.status_code != 200 or jsonl_response.status_code != 200:
                    raise AssertionError("export endpoint failed")
                binary_data = compact_response.content
                jsonl_data = jsonl_response.content
    if compact_response.headers.get("x-msmq-format") != "compact":
        raise AssertionError("compact export is missing format header")
    if jsonl_response.headers.get("x-msmq-format") != "jsonl":
        raise AssertionError("JSONL export is missing format header")
    (VALIDATION_OUT / "source-v2.bin").write_bytes(binary_data)
    (VALIDATION_OUT / "source-v1.jsonl").write_bytes(jsonl_data)
    baseline_canonical = canonical_snapshot(baseline)
    imported_snapshots: dict[str, dict[str, Any]] = {}
    reports: dict[str, dict[str, Any]] = {}
    for format_name, endpoint, data in [
        ("compact", "/v1/import/compact", binary_data),
        ("jsonl", "/v1/import/jsonl", jsonl_data),
    ]:
        with tempfile.TemporaryDirectory(prefix=f"msmq-roundtrip-{format_name}-") as temp:
            with ServiceProcess(Path(temp) / "target.sqlite3") as target:
                with target.client() as client:
                    response = client.post(
                        endpoint,
                        content=data,
                        headers={"content-type": "application/octet-stream"},
                    )
                    report = expect_ok(response)["import_report"]
                    snapshot = expect_ok(client.get("/v1/queues/demo-orders/snapshot"))
        reports[format_name] = report
        imported_snapshots[format_name] = snapshot
    compact_canonical = canonical_snapshot(imported_snapshots["compact"])
    jsonl_canonical = canonical_snapshot(imported_snapshots["jsonl"])
    if compact_canonical != baseline_canonical:
        raise AssertionError("compact export/import changed message semantics")
    if jsonl_canonical != baseline_canonical:
        raise AssertionError("JSONL export/import changed message semantics")
    if compact_canonical != jsonl_canonical:
        raise AssertionError("the two persistence formats are not semantically interchangeable")
    for format_name, report in reports.items():
        if report["decision"] != "accept" or report["inserted"] != 6 or not report["committed"]:
            raise AssertionError(f"{format_name} import report is not a clean accepted commit: {report}")
    return {
        "messages": baseline["message_count"],
        "compact_inserted": reports["compact"]["inserted"],
        "jsonl_inserted": reports["jsonl"]["inserted"],
        "semantic_digest": hashlib.sha256(
            json.dumps(baseline_canonical, sort_keys=True).encode("utf-8")
        ).hexdigest(),
    }


def check_compatibility_and_capacity() -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="msmq-compat-") as temp:
        with ServiceProcess(Path(temp) / "compat.sqlite3") as service:
            with service.client() as client:
                legacy = expect_ok(
                    client.post(
                        "/v1/import/compact",
                        content=(SAMPLES / "legacy_v1.bin").read_bytes(),
                        headers={"content-type": "application/octet-stream"},
                    )
                )["import_report"]
                if legacy["decision"] != "degrade_accept":
                    raise AssertionError("legacy v1 was not degraded-accepted")
                if not any(
                    item["field"] == "expires_at_us" and item["reason"] == "missing_in_v1"
                    for item in legacy["degradations"]
                ):
                    raise AssertionError("legacy expiry default was not reported")
                if not legacy["sub_ms_normalizations"]:
                    raise AssertionError("legacy sub-millisecond timestamp was not normalized")
                legacy_snapshot = expect_ok(client.get("/v1/queues/demo-orders/snapshot"))
                legacy_message = next(
                    item for item in legacy_snapshot["messages"] if item["message_id"] == "legacy-0001"
                )
                if legacy_message["produced_at_us"] % 1000 != 0:
                    raise AssertionError("normalized timestamp is not on the millisecond grid")
                if legacy_message["expires_at_us"] - legacy_message["produced_at_us"] != 60_000_000:
                    raise AssertionError("legacy default TTL was not applied")

                optional = expect_ok(
                    client.post(
                        "/v1/import/compact",
                        content=(SAMPLES / "optional_v2.bin").read_bytes(),
                        headers={"content-type": "application/octet-stream"},
                    )
                )["import_report"]
                if optional["decision"] != "accept":
                    raise AssertionError("current v2 optional-extension sample was not accepted")
                if not any("vendor_trace_id" in key for key in optional["preserved_optional_fields"]):
                    raise AssertionError("unknown optional key was not reported as preserved")
                exported = client.get("/v1/export/compact?queue=demo-orders")
                if exported.status_code != 200:
                    raise AssertionError("re-export after optional import failed")
                decoded = decode_auto(exported.content)
                vendor = next(message for message in decoded.messages if message.message_id == "msg-0001")
                if vendor.extensions.get("vendor_trace_id") != "vendor-0001":
                    raise AssertionError("unknown optional field did not survive compact re-export")

                before = expect_ok(client.get("/v1/queues/demo-orders/snapshot"))
                future = client.post(
                    "/v1/import/compact",
                    content=(SAMPLES / "future_required_v3.bin").read_bytes(),
                    headers={"content-type": "application/octet-stream"},
                )
                future_error = expect_error(future, "E_FORMAT_INCOMPATIBLE", 422)
                after = expect_ok(client.get("/v1/queues/demo-orders/snapshot"))
                if canonical_snapshot(before) != canonical_snapshot(after):
                    raise AssertionError("rejected future-version import changed committed state")
                if future_error["error"]["details"].get("state_changed") is not False:
                    raise AssertionError("future-version rejection did not declare state_changed=false")

                oversize_batch = client.post(
                    "/v1/import/jsonl",
                    content=(SAMPLES / "oversize_batch.jsonl").read_bytes(),
                    headers={"content-type": "application/x-ndjson"},
                )
                expect_error(oversize_batch, "E_BATCH_SPLIT_REQUIRED", 413)
                oversize_body = client.post(
                    "/v1/import/jsonl",
                    content=(SAMPLES / "oversize_body.jsonl").read_bytes(),
                    headers={"content-type": "application/x-ndjson"},
                )
                expect_error(oversize_body, "E_RECORD_TOO_LARGE", 413)
                after_capacity = expect_ok(client.get("/v1/queues/demo-orders/snapshot"))
                if canonical_snapshot(before) != canonical_snapshot(after_capacity):
                    raise AssertionError("capacity rejection changed committed state")

                expect_ok(
                    client.post(
                        "/v1/import/jsonl",
                        content=(SAMPLES / "conflict_seed.jsonl").read_bytes(),
                        headers={"content-type": "application/x-ndjson"},
                    )
                )
                conflict_before = expect_ok(client.get("/v1/queues/conflict-queue/snapshot"))
                conflict_response = client.post(
                    "/v1/import/jsonl",
                    content=(SAMPLES / "conflict_atomic_reject.jsonl").read_bytes(),
                    headers={"content-type": "application/x-ndjson"},
                )
                conflict_error = expect_error(conflict_response, "E_CONFLICT_AMBIGUOUS", 409)
                if conflict_error["error"]["details"].get("rejected_index") != 0:
                    raise AssertionError("ambiguous conflict did not report rejected_index=0")
                conflict_after = expect_ok(client.get("/v1/queues/conflict-queue/snapshot"))
                if canonical_snapshot(conflict_before) != canonical_snapshot(conflict_after):
                    raise AssertionError("atomic conflict rejection changed committed state")
                if any(item["message_id"] == "conflict-2" for item in conflict_after["messages"]):
                    raise AssertionError("later record from rejected batch was partially committed")
    return {
        "legacy_decision": legacy["decision"],
        "legacy_fields_filled": len(legacy["fields_filled"]),
        "legacy_time_normalizations": len(legacy["sub_ms_normalizations"]),
        "optional_fields_preserved": len(optional["preserved_optional_fields"]),
        "future_rejection_code": future_error["error"]["code"],
        "oversize_batch_code": oversize_batch.json()["error"]["code"],
        "oversize_body_code": oversize_body.json()["error"]["code"],
        "conflict_rejected_index": conflict_error["error"]["details"]["rejected_index"],
        "atomic_conflict_inserted_after": conflict_after["message_count"] - conflict_before["message_count"],
    }


def check_delivery_semantics() -> dict[str, Any]:
    produced_at_ms = 4_102_444_800_000
    with tempfile.TemporaryDirectory(prefix="msmq-delivery-") as temp:
        with ServiceProcess(Path(temp) / "delivery.sqlite3") as service:
            with service.client() as client:
                expect_ok(
                    client.post(
                        "/v1/queues",
                        json={
                            "name": "alo-local",
                            "semantics": "at_least_once",
                            "visibility_timeout_ms": 150,
                        },
                    ),
                    expected=201,
                )
                expect_ok(
                    client.post(
                        "/v1/queues/alo-local/messages",
                        json={
                            "message_id": "alo-1",
                            "body": "redeliver-me",
                            "produced_at_ms": produced_at_ms,
                            "ttl_ms": 60_000,
                        },
                    ),
                    expected=201,
                )
                first = expect_ok(
                    client.post("/v1/queues/alo-local/claim", json={"consumer_id": "alo-consumer"})
                )["message"]
                time.sleep(0.25)
                second = expect_ok(
                    client.post("/v1/queues/alo-local/claim", json={"consumer_id": "alo-consumer"})
                )["message"]
                if first["message_id"] != second["message_id"] or second["delivery_count"] != 2:
                    raise AssertionError("at-least-once redelivery did not increment delivery_count")
                ack_second = expect_ok(
                    client.post(
                        "/v1/queues/alo-local/acks",
                        json={
                            "consumer_id": "alo-consumer",
                            "acks": [
                                {"message_id": "alo-1", "lease_token": second["lease_token"]}
                            ],
                        },
                    )
                )
                if ack_second["succeeded"] != 1:
                    raise AssertionError("redelivered message could not be acknowledged")

                expect_ok(
                    client.post(
                        "/v1/queues",
                        json={
                            "name": "exact-local",
                            "semantics": "exactly_once",
                            "visibility_timeout_ms": 1_000,
                        },
                    ),
                    expected=201,
                )
                expect_ok(
                    client.post(
                        "/v1/queues/exact-local/messages",
                        json={
                            "message_id": "exact-1",
                            "body": "exactly-once",
                            "produced_at_ms": produced_at_ms,
                            "ttl_ms": 60_000,
                        },
                    ),
                    expected=201,
                )
                exact_message = expect_ok(
                    client.post("/v1/queues/exact-local/claim", json={"consumer_id": "exact-consumer"})
                )["message"]
                exact_ack_body = {
                    "consumer_id": "exact-consumer",
                    "acks": [
                        {"message_id": "exact-1", "lease_token": exact_message["lease_token"]}
                    ],
                }
                first_exact = expect_ok(
                    client.post("/v1/queues/exact-local/acks", json=exact_ack_body)
                )
                duplicate_exact = expect_ok(
                    client.post("/v1/queues/exact-local/acks", json=exact_ack_body)
                )
                duplicate_code = duplicate_exact["results"][0]["error"]["code"]
                if first_exact["succeeded"] != 1 or duplicate_code != "E_DUPLICATE_ACK":
                    raise AssertionError("exactly-once duplicate acknowledgement was not rejected")
                exact_snapshot = expect_ok(client.get("/v1/queues/exact-local/snapshot"))
                if exact_snapshot["ack_count"] != 1 or exact_snapshot["counts"]["pending"] != 0:
                    raise AssertionError("duplicate exactly-once ack changed pending or ack state")

                expect_ok(
                    client.post(
                        "/v1/queues",
                        json={
                            "name": "batch-local",
                            "semantics": "at_least_once",
                            "visibility_timeout_ms": 1_000,
                        },
                    ),
                    expected=201,
                )
                for index in (1, 2):
                    expect_ok(
                        client.post(
                            "/v1/queues/batch-local/messages",
                            json={
                                "message_id": f"batch-{index}",
                                "body": f"batch-body-{index}",
                                "produced_at_ms": produced_at_ms,
                                "ttl_ms": 60_000,
                            },
                        ),
                        expected=201,
                    )
                batch_first = expect_ok(
                    client.post("/v1/queues/batch-local/claim", json={"consumer_id": "batch-consumer"})
                )["message"]
                batch_second = expect_ok(
                    client.post("/v1/queues/batch-local/claim", json={"consumer_id": "batch-consumer"})
                )["message"]
                partial = expect_ok(
                    client.post(
                        "/v1/queues/batch-local/acks",
                        json={
                            "consumer_id": "batch-consumer",
                            "acks": [
                                {
                                    "message_id": batch_first["message_id"],
                                    "lease_token": batch_first["lease_token"],
                                },
                                {
                                    "message_id": batch_second["message_id"],
                                    "lease_token": "wrong-token",
                                },
                            ],
                        },
                    )
                )
                if (
                    partial["succeeded"] != 1
                    or partial["failed"] != 1
                    or not partial["partial_success"]
                ):
                    raise AssertionError("batch acknowledgement did not report partial success")
                batch_snapshot = expect_ok(client.get("/v1/queues/batch-local/snapshot"))
                if batch_snapshot["counts"]["acked"] != 1 or batch_snapshot["counts"]["leased"] != 1:
                    raise AssertionError("batch partial success was not committed per item")

                expect_ok(
                    client.post(
                        "/v1/queues",
                        json={
                            "name": "race-local",
                            "semantics": "exactly_once",
                            "visibility_timeout_ms": 10_000,
                        },
                    ),
                    expected=201,
                )
                expect_ok(
                    client.post(
                        "/v1/queues/race-local/messages",
                        json={
                            "message_id": "race-1",
                            "body": "single-winner",
                            "produced_at_ms": produced_at_ms,
                            "ttl_ms": 60_000,
                        },
                    ),
                    expected=201,
                )

                def concurrent_claim(index: int) -> str | None:
                    with httpx.Client(
                        base_url=service.base_url, timeout=10, trust_env=False
                    ) as contender:
                        payload = expect_ok(
                            contender.post(
                                "/v1/queues/race-local/claim",
                                json={"consumer_id": f"racer-{index}"},
                            )
                        )
                        message = payload["message"]
                        return message["message_id"] if message else None

                with ThreadPoolExecutor(max_workers=8) as executor:
                    winners = list(executor.map(concurrent_claim, range(8)))
                winner_count = sum(1 for item in winners if item is not None)
                if winner_count != 1:
                    raise AssertionError(f"concurrent claim produced {winner_count} winners")
                race_snapshot = expect_ok(client.get("/v1/queues/race-local/snapshot"))
                race_message = race_snapshot["messages"][0]
                if race_message["state"] != "leased" or race_message["delivery_count"] != 1:
                    raise AssertionError("snapshot exposed an inconsistent lease/delivery count")
    return {
        "at_least_once_delivery_count": second["delivery_count"],
        "at_least_once_ack_succeeded": ack_second["succeeded"],
        "exactly_once_duplicate_code": duplicate_code,
        "exactly_once_ack_count": exact_snapshot["ack_count"],
        "batch_succeeded": partial["succeeded"],
        "batch_failed": partial["failed"],
        "concurrent_claims": len(winners),
        "concurrent_winners": winner_count,
        "race_delivery_count": race_message["delivery_count"],
    }


def _crash_request(client: httpx.Client, method: str, url: str, **kwargs: Any) -> None:
    try:
        client.request(method, url, **kwargs)
    except Exception:
        return
    raise AssertionError("failpoint request unexpectedly returned instead of terminating the process")


def check_forced_termination_recovery() -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="msmq-recovery-") as temp:
        db_path = Path(temp) / "recovery.sqlite3"
        service = ServiceProcess(db_path, failpoints=True).start()
        try:
            with service.client() as client:
                expect_ok(
                    client.post(
                        "/v1/queues",
                        json={
                            "name": "crash-local",
                            "semantics": "at_least_once",
                            "visibility_timeout_ms": 500,
                        },
                    ),
                    expected=201,
                )
                expect_ok(
                    client.post(
                        "/v1/queues/crash-local/messages",
                        json={
                            "message_id": "crash-1",
                            "body": "survive-the-kill",
                            "produced_at_ms": 4_102_444_800_000,
                            "ttl_ms": 60_000,
                        },
                    ),
                    expected=201,
                )
                _crash_request(
                    client,
                    "POST",
                    "/v1/queues/crash-local/claim",
                    json={"consumer_id": "crash-consumer"},
                    headers={"X-MSMQ-Failpoint": "claim_mid_transaction"},
                )
        finally:
            exit_code_claim = service.wait_for_crash()
            service.stop()
        if exit_code_claim == 0:
            raise AssertionError("claim failpoint did not force a non-zero process exit")

        service = ServiceProcess(db_path, failpoints=True).start()
        try:
            with service.client() as client:
                claim_recovery = expect_ok(client.get("/v1/recovery/report"))["recovery_report"]
                if claim_recovery["rolled_back_claims"] < 1:
                    raise AssertionError("claim intent was not reported as rolled back")
                if claim_recovery["evidence"]["runtime_memory_used"] is not False:
                    raise AssertionError("recovery report claims runtime-memory evidence")
                if claim_recovery["evidence"]["crash_exit_report_available"] is not False:
                    raise AssertionError("recovery report incorrectly promises a crash exit report")
                after_claim = expect_ok(client.get("/v1/queues/crash-local/snapshot"))
                message = after_claim["messages"][0]
                if (
                    message["stored_state"] != "pending"
                    or message["delivery_count"] != 0
                    or message["lease_token"] is not None
                ):
                    raise AssertionError("uncommitted claim was not rolled back to deliverable state")
                claimed = expect_ok(
                    client.post("/v1/queues/crash-local/claim", json={"consumer_id": "crash-consumer"})
                )["message"]
                if claimed["delivery_count"] != 1:
                    raise AssertionError("post-recovery claim did not start from delivery_count=1")
                _crash_request(
                    client,
                    "POST",
                    "/v1/queues/crash-local/acks",
                    json={
                        "consumer_id": "crash-consumer",
                        "acks": [
                            {"message_id": "crash-1", "lease_token": claimed["lease_token"]}
                        ],
                    },
                    headers={"X-MSMQ-Failpoint": "ack_mid_transaction"},
                )
        finally:
            exit_code_ack = service.wait_for_crash()
            service.stop()
        if exit_code_ack == 0:
            raise AssertionError("ack failpoint did not force a non-zero process exit")

        service = ServiceProcess(db_path, failpoints=True).start()
        try:
            with service.client() as client:
                ack_recovery = expect_ok(client.get("/v1/recovery/report"))["recovery_report"]
                if ack_recovery["rolled_back_acks"] < 1:
                    raise AssertionError("ack intent was not reported as rolled back")
                after_ack = expect_ok(client.get("/v1/queues/crash-local/snapshot"))
                message = after_ack["messages"][0]
                if message["stored_state"] != "leased" or after_ack["ack_count"] != 0:
                    raise AssertionError("uncommitted acknowledgement changed durable message state")
                final_ack = expect_ok(
                    client.post(
                        "/v1/queues/crash-local/acks",
                        json={
                            "consumer_id": "crash-consumer",
                            "acks": [
                                {"message_id": "crash-1", "lease_token": claimed["lease_token"]}
                            ],
                        },
                    )
                )
                if final_ack["succeeded"] != 1:
                    raise AssertionError("durable lease could not acknowledge after recovery")
                final_snapshot = expect_ok(client.get("/v1/queues/crash-local/snapshot"))
                if final_snapshot["counts"]["acked"] != 1 or final_snapshot["ack_count"] != 1:
                    raise AssertionError("post-recovery acknowledgement was not committed exactly once")
        finally:
            service.stop()
    return {
        "claim_crash_exit_code": exit_code_claim,
        "ack_crash_exit_code": exit_code_ack,
        "rolled_back_claims": claim_recovery["rolled_back_claims"],
        "rolled_back_acks": ack_recovery["rolled_back_acks"],
        "recovery_evidence_source": claim_recovery["evidence"]["source"],
        "runtime_memory_used": claim_recovery["evidence"]["runtime_memory_used"],
        "post_recovery_ack_succeeded": final_ack["succeeded"],
        "final_ack_count": final_snapshot["ack_count"],
    }


CHECKS: list[tuple[str, str, Callable[[], dict[str, Any]]]] = [
    (
        "format_roundtrip_equivalence",
        "Binary and JSONL export/import preserve identifier, queue, body and absolute expiry",
        check_roundtrip,
    ),
    (
        "compatibility_capacity_atomicity",
        "Version matrix, time normalization, unknown optional fields, capacity and atomic conflicts",
        check_compatibility_and_capacity,
    ),
    (
        "delivery_semantics_concurrency",
        "At-least-once redelivery, exactly-once duplicate ack, partial ack and single-winner claim",
        check_delivery_semantics,
    ),
    (
        "forced_termination_recovery",
        "Crash mid-claim and mid-ack, restart recovery from durable SQLite facts only",
        check_forced_termination_recovery,
    ),
]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the local deterministic acceptance validation")
    parser.add_argument("--output", default=str(VALIDATION_OUT / "validation_report.json"))
    args = parser.parse_args()
    if not (SAMPLES / "manifest.json").exists():
        raise SystemExit("samples are missing; run scripts/generate_samples.py first")
    results: list[dict[str, Any]] = []
    for check_id, description, function in CHECKS:
        started = time.monotonic()
        try:
            details = function()
            result = {
                "id": check_id,
                "description": description,
                "status": "passed",
                "elapsed_seconds": round(time.monotonic() - started, 3),
                "details": details,
            }
        except Exception as exc:
            result = {
                "id": check_id,
                "description": description,
                "status": "failed",
                "elapsed_seconds": round(time.monotonic() - started, 3),
                "error": str(exc),
                "traceback": traceback.format_exc(),
            }
        results.append(result)
        marker = "PASS" if result["status"] == "passed" else "FAIL"
        print(f"[{marker}] {check_id} ({result['elapsed_seconds']}s)")
        if result["status"] == "passed":
            print(f"       {json.dumps(result['details'], ensure_ascii=False, sort_keys=True)}")
        else:
            print(f"       {result['error']}")
    overall = "passed" if all(item["status"] == "passed" for item in results) else "failed"
    report = {
        "schema_version": 1,
        "overall": overall,
        "checks_total": len(results),
        "checks_passed": sum(1 for item in results if item["status"] == "passed"),
        "checks_failed": sum(1 for item in results if item["status"] == "failed"),
        "checks": results,
    }
    output = Path(args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"\noverall={overall} report={output}")
    if overall != "passed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()

