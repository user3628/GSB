#!/usr/bin/env python3
"""Deterministic end-to-end experiment for seqtxn.

The script uses only loopback HTTP services and one local SQLite file.  Two
crashes are induced with os._exit: one after a lease effect is persisted, and
one after the coordinator answers but before the transaction decision is
committed.  Recovery data comes exclusively from SQLite after process exit.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import socket
import sqlite3
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests


ROOT = Path(__file__).resolve().parents[1]
ARTIFACT_DIR = ROOT / "artifacts" / "experiment"


def canonical(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sanitize_for_replay(value, coordinator_url: str):
    """Remove run-specific ports, UUIDs, and elapsed timings from the report."""
    if isinstance(value, dict):
        cleaned = {}
        for key, item in value.items():
            if key in {"run_id", "observation_id", "elapsed_ms", "url"}:
                continue
            cleaned[key] = sanitize_for_replay(item, coordinator_url)
        return cleaned
    if isinstance(value, list):
        return [sanitize_for_replay(item, coordinator_url) for item in value]
    if isinstance(value, str):
        return value.replace(coordinator_url, "http://127.0.0.1:<coordinator>")
    return value


def free_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def wait_http(url: str, timeout: float = 15.0, session: requests.Session | None = None) -> None:
    client = session or requests.Session()
    client.trust_env = False
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            response = client.get(url, timeout=0.5)
            if response.status_code == 200:
                return
        except requests.RequestException as exc:
            last_error = exc
        time.sleep(0.04)
    raise RuntimeError(f"service did not become healthy: {url}: {last_error}")


class ManagedProcess:
    def __init__(self, args: list[str], env: dict, log_path: Path) -> None:
        self.log_path = log_path
        self.log = log_path.open("a", encoding="utf-8")
        self.process = subprocess.Popen(
            args,
            cwd=ROOT,
            env=env,
            stdout=self.log,
            stderr=subprocess.STDOUT,
        )

    def stop(self) -> int | None:
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=8)
        self.log.flush()
        self.log.close()
        return self.process.returncode


class Experiment:
    def __init__(self, seed: int) -> None:
        self.seed = seed
        self.run_dir = ARTIFACT_DIR
        self.db_path = self.run_dir / "middleware.db"
        self.log_dir = self.run_dir / "logs"
        self.coordinator: ManagedProcess | None = None
        self.component: ManagedProcess | None = None
        self.component_port: int | None = None
        self.coordinator_url = ""
        self.component_url = ""
        self.unreachable_port = free_port()
        self.fault_codes: list[dict] = []
        self.delivery: dict[str, list[int]] = {}
        self.produced: dict[str, dict[str, int]] = {}
        self.validation: dict = {}
        self.log_counter = 0
        self.session = requests.Session()
        self.session.trust_env = False

    # ------------------------------------------------------------------
    def reset(self) -> None:
        if self.run_dir.exists():
            shutil.rmtree(self.run_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def record_code(self, step: str, response: requests.Response | None, code: str | None = None) -> None:
        self.fault_codes.append(
            {
                "step": step,
                "http_status": None if response is None else response.status_code,
                "code": code
                if code is not None
                else (response.json().get("code") if response is not None else None),
            }
        )

    def request(
        self,
        step: str,
        method: str,
        path: str,
        *,
        json_body=None,
        expected_code: str | None = "OK",
        timeout: float = 10.0,
    ) -> dict:
        response = self.session.request(
            method,
            self.component_url + path,
            json=json_body,
            timeout=timeout,
        )
        try:
            payload = response.json()
        except ValueError:
            payload = {"code": "NON_JSON", "text": response.text}
        self.record_code(step, response)
        if expected_code is not None and payload.get("code") != expected_code:
            raise AssertionError(
                f"{step}: expected {expected_code}, got HTTP {response.status_code} {payload}"
            )
        return payload

    # ------------------------------------------------------------------
    def start_coordinator(self) -> None:
        self.coordinator_port = free_port()
        self.coordinator_url = f"http://127.0.0.1:{self.coordinator_port}"
        env = os.environ.copy()
        env["PYTHONPATH"] = str(ROOT)
        args = [
            sys.executable,
            "-m",
            "seqtxn.coordinator_cli",
            "--config",
            str(ROOT / "samples" / "coordinator_responses.json"),
            "--host",
            "127.0.0.1",
            "--port",
            str(self.coordinator_port),
        ]
        self.coordinator = ManagedProcess(
            args, env, self.log_dir / "coordinator.log"
        )
        wait_http(self.coordinator_url + "/health", session=self.session)

    def start_component(self, extra_env: dict | None = None) -> None:
        self.log_counter += 1
        self.component_port = free_port()
        self.component_url = f"http://127.0.0.1:{self.component_port}"
        env = os.environ.copy()
        env["PYTHONPATH"] = str(ROOT)
        env.update(extra_env or {})
        args = [
            sys.executable,
            "-m",
            "seqtxn.cli",
            "--db",
            str(self.db_path),
            "--coordinator-url",
            self.coordinator_url,
            "--unreachable-url",
            f"http://127.0.0.1:{self.unreachable_port}",
            "--query-timeout",
            "0.2",
            "--host",
            "127.0.0.1",
            "--port",
            str(self.component_port),
        ]
        self.component = ManagedProcess(
            args, env, self.log_dir / f"component-{self.log_counter}.log"
        )
        wait_http(self.component_url + "/health", session=self.session)

    def stop_component(self) -> int | None:
        if not self.component:
            return None
        code = self.component.stop()
        self.component = None
        return code

    def wait_crash(self) -> int:
        if not self.component:
            raise AssertionError("component is not running")
        try:
            self.component.process.wait(timeout=8)
        except subprocess.TimeoutExpired as exc:
            raise AssertionError("crash injection did not terminate the process") from exc
        code = self.component.stop()
        self.component = None
        if code != 97:
            raise AssertionError(f"expected crash exit code 97, got {code}")
        return code

    # ------------------------------------------------------------------
    def db_snapshot(self) -> dict:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            active_leases = conn.execute(
                "SELECT topic, partition_no, sequence, consumer_id, state FROM leases "
                "WHERE state='ACTIVE' ORDER BY topic, partition_no, sequence"
            ).fetchall()
            started = conn.execute(
                "SELECT kind, scope_key, state FROM intents "
                "WHERE state='STARTED' ORDER BY rowid"
            ).fetchall()
            pending_txns = conn.execute(
                "SELECT txn_id, state FROM transactions WHERE state='PENDING' ORDER BY txn_id"
            ).fetchall()
            pending_count = conn.execute(
                "SELECT COUNT(*) AS n FROM pending_messages"
            ).fetchone()["n"]
            return {
                "active_leases": [dict(row) for row in active_leases],
                "started_intents": [dict(row) for row in started],
                "pending_transactions": [dict(row) for row in pending_txns],
                "pending_message_count": int(pending_count),
            }
        finally:
            conn.close()

    def coordinator_observations(self) -> list[dict]:
        response = self.session.get(
            self.coordinator_url + "/admin/observations", timeout=2
        )
        return response.json()["data"]

    def wait_disconnect_observation(self, txn_id: str) -> dict:
        deadline = time.monotonic() + 3.5
        while time.monotonic() < deadline:
            observations = [
                item
                for item in self.coordinator_observations()
                if item["transaction_id"] == txn_id
            ]
            for item in observations:
                if item.get("client_disconnected"):
                    return item
            time.sleep(0.05)
        raise AssertionError(f"coordinator did not observe a closed connection for {txn_id}")

    # ------------------------------------------------------------------
    def setup_topics_and_messages(self) -> tuple[list[dict], list[dict]]:
        topics = json.loads((ROOT / "samples" / "topics.json").read_text(encoding="utf-8"))
        messages = json.loads((ROOT / "samples" / "messages.json").read_text(encoding="utf-8"))
        created = []
        for topic in topics["topics"]:
            payload = self.request("create-topic", "POST", "/topics", json_body=topic)
            created.append(payload["data"])

        produced = []
        for message in messages["messages"]:
            body = {
                "producer_id": message["producer_id"],
                "client_sequence": message["client_sequence"],
                "body": message["body"],
            }
            if "partition" in message:
                body["partition"] = message["partition"]
            if "partition_key" in message:
                body["partition_key"] = message["partition_key"]
            if "txn_id" in message:
                body["txn_id"] = message["txn_id"]
            response = self.request(
                f"produce:{message['case']}",
                "POST",
                f"/topics/{message['topic']}/messages",
                json_body=body,
            )["data"]
            produced.append({"case": message["case"], **response})
            key = f"{response['topic']}/{response['partition']}"
            self.produced.setdefault(key, {})[message["client_sequence"]] = response["sequence"]

        duplicate = self.request(
            "duplicate-publish",
            "POST",
            "/topics/orders/messages",
            json_body={
                "producer_id": "producer-A",
                "client_sequence": "A-1",
                "partition": 0,
                "body": {"kind": "order", "id": 1},
            },
        )["data"]
        if not duplicate["duplicate"] or duplicate["sequence"] != 1:
            raise AssertionError("idempotent publish did not return the original message")

        invalid_route = self.request(
            "invalid-partition",
            "POST",
            "/topics/orders/messages",
            json_body={
                "producer_id": "producer-invalid",
                "client_sequence": "invalid-1",
                "partition": 99,
                "body": {"must_not_allocate": True},
            },
            expected_code="E_PARTITION_INVALID",
        )
        self.validation = {
            "invalid_partition_code": invalid_route["code"],
            "duplicate_publish_sequence": duplicate["sequence"],
            "duplicate_publish_reused": duplicate["duplicate"],
        }

        partition_sequences = {}
        for topic_name in ("orders", "events"):
            detail = self.request(
                f"topic-detail:{topic_name}", "GET", f"/topics/{topic_name}"
            )["data"]
            for partition in detail["partitions"]:
                rows = self.request(
                    f"messages:{topic_name}:{partition['partition_no']}",
                    "GET",
                    f"/topics/{topic_name}/partitions/{partition['partition_no']}/messages",
                )["data"]
                sequences = [row["sequence"] for row in rows]
                if sequences != list(range(1, len(sequences) + 1)):
                    raise AssertionError(
                        f"sequence gap in {topic_name}/{partition['partition_no']}: {sequences}"
                    )
                partition_sequences[f"{topic_name}/{partition['partition_no']}"] = sequences

        event_partitions = {
            item["case"]: item["partition"] for item in produced if item["topic"] == "events"
        }
        if event_partitions["hash-route-1"] != event_partitions["hash-route-2"]:
            raise AssertionError("same hash key was routed to different partitions")
        return produced, partition_sequences

    def claim(self, partition_no: int, consumer: str, timeout_ms: int, step: str) -> requests.Response:
        return self.session.post(
            self.component_url + f"/topics/orders/partitions/{partition_no}/claim",
            json={"consumer_id": consumer, "visibility_timeout_ms": timeout_ms},
            timeout=10,
        )

    def claim_and_ack(
        self,
        partition_no: int,
        consumer: str,
        expected_sequence: int,
        *,
        timeout_ms: int = 5_000,
    ) -> tuple[dict, str]:
        step = f"claim:{partition_no}:{consumer}"
        payload = self.request(
            step,
            "POST",
            f"/topics/orders/partitions/{partition_no}/claim",
            json_body={"consumer_id": consumer, "visibility_timeout_ms": timeout_ms},
        )["data"]
        if payload.get("message") is None:
            raise AssertionError(f"expected sequence {expected_sequence}, got {payload}")
        if payload["message"]["sequence"] != expected_sequence:
            raise AssertionError(
                f"expected sequence {expected_sequence}, got {payload['message']['sequence']}"
            )
        key = f"orders/{partition_no}"
        self.delivery.setdefault(key, []).append(expected_sequence)
        token = payload["lease_token"]
        self.request(
            f"ack:{partition_no}:{expected_sequence}",
            "POST",
            "/acks",
            json_body={"lease_token": token, "sequence": expected_sequence},
        )
        return payload, token

    def blocked_claim(self, partition_no: int, consumer: str) -> dict:
        response = self.claim(partition_no, consumer, 5_000, f"blocked-claim:{partition_no}")
        payload = response.json()
        self.record_code(f"blocked-claim:{partition_no}", response)
        if response.status_code != 200 or payload["data"].get("reason") != "HEAD_PENDING_TRANSACTION":
            raise AssertionError(f"expected pending-head block, got {payload}")
        return payload["data"]

    def transaction_trace(self) -> list[dict]:
        txn_ids = [
            "txn-commit-001",
            "txn-rollback-001",
            "txn-timeout-001",
            "txn-conflict-001",
            "txn-crash-001",
            "txn-unknown-001",
            "txn-unreachable-001",
            "txn-duplicate-001",
        ]
        result = []
        for txn_id in txn_ids:
            trace = self.request(f"txn-trace:{txn_id}", "GET", f"/transactions/{txn_id}")["data"]
            result.append(
                {
                    "txn_id": txn_id,
                    "state": trace["state"],
                    "outcome": trace["outcome"],
                    "decision_source": trace["decision_source"],
                    "query_count": trace["query_count"],
                    "message_sequences": [
                        {
                            "topic": message["topic"],
                            "partition": message["partition"],
                            "sequence": message["sequence"],
                            "state": message["state"],
                        }
                        for message in trace["messages"]
                    ],
                    "pending_count": trace["pending_count"],
                    "conflict_count": len(trace["conflicts"]),
                    "conflicts": [
                        {
                            "existing": conflict["existing_outcome"],
                            "incoming": conflict["incoming_outcome"],
                            "source": conflict["source"],
                        }
                        for conflict in trace["conflicts"]
                    ],
                    "query_results": [
                        {
                            "attempt": query["attempt"],
                            "outcome": query["outcome"],
                            "result_code": query["result_code"],
                            "source": query["source"],
                        }
                        for query in trace["queries"]
                    ],
                }
            )
        return result

    # ------------------------------------------------------------------
    def run_delivery_scenarios(self) -> dict:
        # p0: normal, pending tx block, commit, rollback placeholder, timeout,
        # conflicting duplicate conclusion.
        self.claim_and_ack(0, "consumer-p0", 1)
        blocked_commit = self.blocked_claim(0, "consumer-p0")
        commit = self.request(
            "resolve:txn-commit", "POST", "/transactions/txn-commit-001/resolve"
        )["data"]
        if commit["state"] != "COMMITTED" or commit["pending_count"] != 0:
            raise AssertionError("transaction commit did not atomically clear pending rows")
        for expected in (2, 3, 4):
            self.claim_and_ack(0, "consumer-p0", expected)

        rollback = self.request(
            "resolve:txn-rollback",
            "POST",
            "/transactions/txn-rollback-001/resolve",
            json_body={"injection": "rollback"},
        )["data"]
        if rollback["state"] != "ROLLED_BACK":
            raise AssertionError("rollback decision was not persisted")
        blocked_timeout = self.blocked_claim(0, "consumer-p0")

        timeout_payload = self.request(
            "fault:timeout",
            "POST",
            "/transactions/txn-timeout-001/resolve",
            json_body={"injection": "hang", "timeout_ms": 150},
            expected_code="E_TXN_TIMEOUT",
        )
        if self.request("txn-timeout-pending", "GET", "/transactions/txn-timeout-001")["data"]["state"] != "PENDING":
            raise AssertionError("timeout must leave the transaction pending")
        disconnect = self.wait_disconnect_observation("txn-timeout-001")
        retried = self.request(
            "retry:txn-timeout",
            "POST",
            "/transactions/txn-timeout-001/resolve",
            json_body={"injection": "commit"},
        )["data"]
        if retried["state"] != "COMMITTED":
            raise AssertionError("timeout retry did not commit")
        for expected in (6, 7):
            self.claim_and_ack(0, "consumer-p0", expected)

        first_conflict = self.request(
            "resolve:txn-conflict-first",
            "POST",
            "/transactions/txn-conflict-001/resolve",
            json_body={"injection": "commit"},
        )["data"]
        if first_conflict["state"] != "COMMITTED":
            raise AssertionError("first conflict transaction conclusion was not committed")
        conflict = self.request(
            "fault:conflict",
            "POST",
            "/transactions/txn-conflict-001/resolve",
            json_body={"injection": "duplicate_conflict", "force": True},
            expected_code="E_TXN_CONFLICT",
        )
        conflict_detail = self.request(
            "txn-conflict-detail", "GET", "/transactions/txn-conflict-001"
        )["data"]
        if conflict_detail["state"] != "COMMITTED" or len(conflict_detail["conflicts"]) != 1:
            raise AssertionError("conflict did not retain the first conclusion")
        self.claim_and_ack(0, "consumer-p0", 8)
        last, last_token = self.claim_and_ack(0, "consumer-p0", 9)
        duplicate_ack = self.request(
            "fault:consumer-duplicate-ack",
            "POST",
            "/acks",
            json_body={"lease_token": last_token, "sequence": 9},
            expected_code="E_DUPLICATE_ACK",
        )

        # p1: crash after the coordinator replied but before decision commit.
        self.claim_and_ack(1, "consumer-p1", 1)
        self.claim_and_ack(1, "consumer-p1", 2)
        blocked_crash_txn = self.blocked_claim(1, "consumer-p1")
        self.stop_component()
        self.start_component({"SEQ_TXN_CRASH_AT": "txn_after_remote"})
        try:
            self.session.post(
                self.component_url + "/transactions/txn-crash-001/resolve",
                json={},
                timeout=5,
            )
        except requests.RequestException:
            pass
        crash_code = self.wait_crash()
        txn_crash_before = self.db_snapshot()
        if not any(
            item["kind"] == "TXN_RESOLVE" for item in txn_crash_before["started_intents"]
        ):
            raise AssertionError("crashed transaction intent was not persisted")
        self.start_component()
        txn_crash_after = self.request(
            "txn-crash-after-recovery", "GET", "/transactions/txn-crash-001"
        )["data"]
        if txn_crash_after["state"] != "COMMITTED" or txn_crash_after["pending_count"] != 0:
            raise AssertionError("transaction was not renegotiated on restart")
        txn_recovery_report = self.request(
            "txn-recovery-report", "GET", "/admin/recovery-report"
        )["data"]
        self.claim_and_ack(1, "consumer-p1", 3)

        # p2: expiry, concurrent claim, unfinished lease crash, unknown txn.
        expired_first = self.claim(2, "lease-owner", 120, "claim:expiry:first").json()
        old_token = expired_first["data"]["lease_token"]
        time.sleep(0.18)
        expired_second = self.claim(2, "lease-next", 5_000, "claim:expiry:retry").json()
        if (
            expired_second["data"]["message"]["sequence"] != 1
            or expired_second["data"]["attempt"] != 2
        ):
            raise AssertionError("expired lease did not redeliver the original sequence")
        stale_ack = self.request(
            "fault:stale-ack",
            "POST",
            "/acks",
            json_body={"lease_token": old_token, "sequence": 1},
            expected_code="E_LEASE_EXPIRED",
        )
        self.request(
            "ack:expiry:fresh",
            "POST",
            "/acks",
            json_body={"lease_token": expired_second["data"]["lease_token"], "sequence": 1},
        )
        self.delivery.setdefault("orders/2", []).append(1)

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [
                pool.submit(self.claim, 2, f"concurrent-{i}", 5_000, f"claim:concurrent:{i}")
                for i in range(2)
            ]
            responses = [future.result() for future in futures]
        winners = [response for response in responses if response.json()["code"] == "OK"]
        losers = [response for response in responses if response.json()["code"] == "E_LEASE_HELD"]
        if len(winners) != 1 or len(losers) != 1:
            raise AssertionError(f"concurrent claim did not have one winner: {[r.json() for r in responses]}")
        for code in sorted({response.json()["code"] for response in responses}):
            self.record_code(f"fault:concurrent-claim:{code}", None, code)
        winner = winners[0].json()["data"]
        if winner["message"]["sequence"] != 2:
            raise AssertionError("concurrent winner received an unexpected sequence")
        self.delivery.setdefault("orders/2", []).append(2)
        self.request(
            "ack:concurrent-winner",
            "POST",
            "/acks",
            json_body={"lease_token": winner["lease_token"], "sequence": 2},
        )

        self.stop_component()
        self.start_component({"SEQ_TXN_CRASH_AT": "lease_after_effect"})
        try:
            self.session.post(
                self.component_url + "/topics/orders/partitions/2/claim",
                json={"consumer_id": "crash-consumer", "visibility_timeout_ms": 5_000},
                timeout=5,
            )
        except requests.RequestException:
            pass
        lease_crash_code = self.wait_crash()
        lease_crash_before = self.db_snapshot()
        if not any(
            lease["sequence"] == 3 for lease in lease_crash_before["active_leases"]
        ):
            raise AssertionError("crashed lease effect was not persisted")
        if not any(
            intent["kind"] == "LEASE_ACQUIRE"
            for intent in lease_crash_before["started_intents"]
        ):
            raise AssertionError("crashed lease intent was not persisted")
        self.start_component()
        lease_crash_after_partition = self.request(
            "lease-crash-after-recovery", "GET", "/topics/orders/partitions/2"
        )["data"]
        if lease_crash_after_partition["active_lease"] is not None:
            raise AssertionError("recovery did not release the unfinished lease")
        lease_recovery_report = self.request(
            "lease-recovery-report", "GET", "/admin/recovery-report"
        )["data"]
        lease_claim = self.request(
            "claim:after-lease-recovery",
            "POST",
            "/topics/orders/partitions/2/claim",
            json_body={"consumer_id": "after-recovery", "visibility_timeout_ms": 5_000},
        )["data"]
        if lease_claim["message"]["sequence"] != 3 or lease_claim["attempt"] != 2:
            raise AssertionError("recovered lease was not redelivered at its original sequence")
        self.delivery.setdefault("orders/2", []).append(3)
        self.request(
            "ack:after-lease-recovery",
            "POST",
            "/acks",
            json_body={"lease_token": lease_claim["lease_token"], "sequence": 3},
        )

        blocked_unknown = self.blocked_claim(2, "consumer-p2")
        unknown = self.request(
            "fault:unknown",
            "POST",
            "/transactions/txn-unknown-001/resolve",
            json_body={"injection": "unknown"},
            expected_code="E_TXN_UNKNOWN",
        )
        if self.request("txn-unknown-pending", "GET", "/transactions/txn-unknown-001")["data"]["state"] != "PENDING":
            raise AssertionError("unknown conclusion must remain pending")
        self.request(
            "resolve:txn-unknown-retry",
            "POST",
            "/transactions/txn-unknown-001/resolve",
            json_body={"injection": "commit"},
        )
        self.claim_and_ack(2, "consumer-p2", 4)
        self.claim_and_ack(2, "consumer-p2", 5)

        return {
            "blocked_by_pending": {
                "orders/0": blocked_commit,
                "orders/0_after_rollback": blocked_timeout,
                "orders/1": blocked_crash_txn,
                "orders/2_unknown": blocked_unknown,
            },
            "timeout": {
                "request_code": timeout_payload["code"],
                "coordinator_observation": disconnect,
                "retry_state": retried["state"],
            },
            "conflict": {
                "request_code": conflict["code"],
                "retained_state": conflict_detail["state"],
                "conflict_count": len(conflict_detail["conflicts"]),
            },
            "duplicate_consumer_ack": duplicate_ack["code"],
            "stale_ack": stale_ack["code"],
            "concurrent_claim": {
                "winner": winner["message"]["sequence"],
                "loser_code": losers[0].json()["code"],
            },
            "crash_exit_codes": {"txn": crash_code, "lease": lease_crash_code},
            "txn_crash_before": txn_crash_before,
            "txn_crash_after": {
                "state": txn_crash_after["state"],
                "pending_count": txn_crash_after["pending_count"],
            },
            "txn_crash_recovery": {
                "actions": [event["action"] for event in txn_recovery_report["events"]],
                "facts": txn_recovery_report["facts"],
                "complete_exit_report_possible_after_kill": txn_recovery_report[
                    "complete_exit_report_possible_after_kill"
                ],
            },
            "lease_crash_before": lease_crash_before,
            "lease_crash_after": {
                "active_lease": lease_crash_after_partition["active_lease"],
                "redelivery_sequence": lease_claim["message"]["sequence"],
                "redelivery_attempt": lease_claim["attempt"],
            },
            "lease_crash_recovery": {
                "actions": [event["action"] for event in lease_recovery_report["events"]],
                "facts": lease_recovery_report["facts"],
                "complete_exit_report_possible_after_kill": lease_recovery_report[
                    "complete_exit_report_possible_after_kill"
                ],
            },
        }

    def run_events_transactions(self) -> dict:
        produced = self.request(
            "events-detail", "GET", "/topics/events"
        )["data"]
        # The transaction ids are global, so resolving them only changes their
        # own messages and keeps unrelated partitions blocked at their own heads.
        refused = self.request(
            "fault:unreachable",
            "POST",
            "/transactions/txn-unreachable-001/resolve",
            json_body={"injection": "refuse"},
            expected_code="E_TXN_UNREACHABLE",
        )
        pending = self.request(
            "txn-unreachable-pending", "GET", "/transactions/txn-unreachable-001"
        )["data"]
        if pending["state"] != "PENDING":
            raise AssertionError("unreachable coordinator must leave transaction pending")
        committed = self.request(
            "resolve:txn-unreachable-retry",
            "POST",
            "/transactions/txn-unreachable-001/resolve",
            json_body={"injection": "commit"},
        )["data"]
        first_duplicate = self.request(
            "resolve:txn-duplicate-first",
            "POST",
            "/transactions/txn-duplicate-001/resolve",
        )["data"]
        duplicate = self.request(
            "fault:duplicate-conclusion",
            "POST",
            "/transactions/txn-duplicate-001/resolve",
            json_body={"injection": "duplicate_same", "force": True},
            expected_code="E_DUPLICATE_ACK",
        )
        return {
            "event_partition_count": len(produced["partitions"]),
            "unreachable_code": refused["code"],
            "unreachable_retry_state": committed["state"],
            "duplicate_conclusion_code": duplicate["code"],
            "duplicate_transaction_state": first_duplicate["state"],
        }

    # ------------------------------------------------------------------
    def build_report(self) -> dict:
        produced, partition_sequences = self.setup_topics_and_messages()
        scenarios = self.run_delivery_scenarios()
        events = self.run_events_transactions()
        fault_events = self.request("fault-events", "GET", "/admin/fault-events")["data"]
        txn_trace = self.transaction_trace()
        latest_recovery = self.request(
            "latest-recovery-report", "GET", "/admin/recovery-report"
        )["data"]
        delivery = {key: self.delivery.get(key, []) for key in partition_sequences}
        expected_delivery = {key: [] for key in partition_sequences}
        expected_delivery.update(
            {
                "orders/0": [1, 2, 3, 4, 6, 7, 8, 9],
                "orders/1": [1, 2, 3],
                "orders/2": [1, 2, 3, 4, 5],
            }
        )
        if delivery != expected_delivery:
            raise AssertionError(f"unexpected delivery sequences: {delivery}")

        report = {
            "seed": self.seed,
            "environment": {
                "python": sys.version.split()[0],
                "bind_scope": "127.0.0.1",
                "storage": "sqlite",
                "container": False,
                "external_network_dependency": False,
            },
            "samples": {
                "topics": json.loads((ROOT / "samples" / "topics.json").read_text(encoding="utf-8")),
                "message_count": len(produced),
                "coordinator_script": json.loads(
                    (ROOT / "samples" / "coordinator_responses.json").read_text(encoding="utf-8")
                ),
            },
            "validation": self.validation,
            "partition_sequences_after_produce": partition_sequences,
            "delivery_sequences": delivery,
            "expected_delivery_sequences": expected_delivery,
            "transaction_trace": txn_trace,
            "scenario_observations": scenarios,
            "events_transaction_observations": events,
            "business_code_trace": self.fault_codes,
            "fault_events": [
                {
                    "txn_id": event["txn_id"],
                    "injection": event["injection"],
                    "conditions": event["conditions"],
                    "observed_code": event["observed_code"],
                    "observed": event["observed"],
                }
                for event in fault_events
            ],
            "latest_recovery_report": {
                "run_id": latest_recovery.get("run_id"),
                "facts": latest_recovery.get("facts"),
                "actions": [event["action"] for event in latest_recovery.get("events", [])],
                "report_basis": latest_recovery.get("report_basis"),
                "complete_exit_report_possible_after_kill": latest_recovery.get(
                    "complete_exit_report_possible_after_kill"
                ),
            },
            "required_codes_observed": sorted(
                {
                    item["code"]
                    for item in self.fault_codes
                    if item["code"] and item["code"].startswith("E_")
                }
            ),
        }
        return report

    def run(self) -> dict:
        self.reset()
        self.start_coordinator()
        self.start_component()
        try:
            report = self.build_report()
        finally:
            self.stop_component()
            if self.coordinator:
                self.coordinator.stop()
                self.coordinator = None
        report = sanitize_for_replay(report, self.coordinator_url)
        report["content_sha256"] = hashlib.sha256(canonical(report).encode("utf-8")).hexdigest()
        (self.run_dir / "experiment_result.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return report


def print_summary(report: dict) -> None:
    print("=" * 72)
    print(f"seqtxn deterministic experiment (seed={report['seed']})")
    print("=" * 72)
    print("delivery sequences:")
    for key, value in report["delivery_sequences"].items():
        print(f"  {key}: {value}")
    print("transaction conclusions:")
    for trace in report["transaction_trace"]:
        print(
            f"  {trace['txn_id']}: {trace['state']} source={trace['decision_source']} "
            f"conflicts={trace['conflict_count']}"
        )
    codes = sorted(
        {
            item["code"]
            for item in report["business_code_trace"]
            if item["code"] and item["code"].startswith("E_")
        }
    )
    print("business error codes:")
    print("  " + ", ".join(codes))
    for key in report["delivery_sequences"]:
        print(f"  {key}: {report['delivery_sequences'][key]}")
    print("recovery before/after:")
    observations = report["scenario_observations"]
    print(
        "  txn crash: "
        f"before={observations['txn_crash_before']} -> "
        f"after={observations['txn_crash_after']}"
    )
    print(
        "  lease crash: "
        f"before={observations['lease_crash_before']} -> "
        f"after={observations['lease_crash_after']}"
    )
    print(f"report: {ARTIFACT_DIR / 'experiment_result.json'}")
    print(f"sha256: {report['content_sha256']}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    report = Experiment(args.seed).run()
    print_summary(report)


if __name__ == "__main__":
    main()

