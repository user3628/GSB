from __future__ import annotations

import hashlib
import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.sample_data import expected_object_digest, load_samples, sample, verify_part_hashes


ROOT = Path(__file__).resolve().parent.parent
ARTIFACTS = ROOT / "artifacts"
LOGS = ARTIFACTS / "demo-logs"
DB_PATH = ARTIFACTS / "demo.sqlite3"
CONFIG_PATH = ROOT / "samples" / "backends.json"
GATEWAY_PORT = 18100
GATEWAY_URL = f"http://127.0.0.1:{GATEWAY_PORT}"


class DemoFailure(RuntimeError):
    pass


def log(message: str = "") -> None:
    print(message, flush=True)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def sha(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


class Harness:
    def __init__(self) -> None:
        self.processes: list[subprocess.Popen] = []
        self.client = httpx.Client(timeout=20.0)
        self.codes_seen: set[str] = set()
        self.report: dict[str, Any] = {
            "started_at": now_iso(),
            "checks": [],
            "traces": {},
            "quota_snapshots": {},
            "business_codes": [],
            "object_digests": {},
        }

    def check(self, name: str, condition: bool, detail: Any = None) -> None:
        if not condition:
            raise DemoFailure(f"check failed: {name}; detail={detail}")
        self.report["checks"].append({"name": name, "ok": True, "detail": detail})
        log(f"  CHECK OK  {name}")

    def call(
        self,
        method: str,
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
        request_id: str | None = None,
        expected_status: int | None = None,
        base_url: str = GATEWAY_URL,
    ) -> tuple[int, dict[str, Any]]:
        headers = {"Idempotency-Key": request_id} if request_id else None
        response = self.client.request(method, f"{base_url}{path}", json=json_body, headers=headers)
        try:
            body = response.json()
        except ValueError:
            body = {"raw": response.text}
        if expected_status is not None and response.status_code != expected_status:
            raise DemoFailure(
                f"{method} {path} expected {expected_status}, got {response.status_code}: {body}"
            )
        if isinstance(body, dict) and body.get("code", "").startswith("E_"):
            self.codes_seen.add(body["code"])
        return response.status_code, body

    def record_quota(self, label: str) -> dict[str, Any]:
        _, body = self.call("GET", "/v1/backends", expected_status=200)
        snapshot = {item["backend_id"]: item for item in body["backends"]}
        self.report["quota_snapshots"][label] = snapshot
        return snapshot

    def start_process(self, name: str, args: list[str], env_updates: dict[str, str]) -> subprocess.Popen:
        LOGS.mkdir(parents=True, exist_ok=True)
        log_path = LOGS / f"{name}.log"
        log_handle = log_path.open("a", encoding="utf-8")
        env = os.environ.copy()
        env.update(env_updates)
        env["PYTHONUNBUFFERED"] = "1"
        process = subprocess.Popen(
            args,
            cwd=ROOT,
            env=env,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            text=True,
        )
        self.processes.append(process)
        self.report.setdefault("processes", []).append(
            {"name": name, "pid": process.pid, "log": str(log_path)}
        )
        return process

    def wait_http(self, url: str, label: str, timeout: float = 15.0) -> None:
        deadline = time.monotonic() + timeout
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            try:
                response = self.client.get(url)
                if response.status_code == 200:
                    log(f"  READY     {label} -> {url}")
                    return
            except httpx.HTTPError as error:
                last_error = error
            time.sleep(0.05)
        raise DemoFailure(f"{label} did not become ready: {last_error}")

    def stop_process(self, process: subprocess.Popen, timeout: float = 5.0) -> None:
        if process.poll() is not None:
            return
        process.terminate()
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=timeout)
        if process in self.processes:
            self.processes.remove(process)

    def cleanup(self) -> None:
        for process in reversed(list(self.processes)):
            try:
                self.stop_process(process)
            except Exception:
                pass
        self.client.close()


def load_config() -> dict[str, Any]:
    with CONFIG_PATH.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def reset_database(path: Path) -> None:
    for suffix in ("", "-wal", "-shm"):
        candidate = Path(str(path) + suffix)
        if candidate.exists():
            candidate.unlink()


def create_and_submit(
    harness: Harness,
    sam: dict[str, Any],
    *,
    submit_order: list[int] | None = None,
) -> None:
    session_id = sam["session_id"]
    status, body = harness.call(
        "POST",
        "/v1/sessions",
        json_body={
            "object_key": sam["object_key"],
            "backend_id": sam["backend_id"],
            "max_parts": sam["max_parts"],
            "total_parts": sam["total_parts"],
            "priority": sam["priority"],
            "session_id": session_id,
            "metadata": {"scenario": sam["scenario"]},
        },
        request_id=f"create-{session_id}",
        expected_status=201,
    )
    harness.check(f"create {session_id}", body["version"] >= 1, body["session"])
    parts = {part["seq"]: part for part in sam["parts"]}
    order = submit_order or sorted(parts)
    for seq in order:
        part = parts[seq]
        status, body = harness.call(
            "POST",
            f"/v1/sessions/{session_id}/parts",
            json_body={
                "seq": seq,
                "size_bytes": part["size_bytes"],
                "sha256": part["sha256"],
                "arrived_at": f"2026-01-01T00:00:{seq:02d}.000+00:00",
            },
            request_id=f"submit-{session_id}-{seq}",
            expected_status=200,
        )
        stored = next(item for item in body["parts"] if item["seq"] == seq)
        harness.check(
            f"submit {session_id} seq={seq}",
            stored["sha256"] == part["sha256"],
            {"version": body["version"]},
        )


def schedule(
    harness: Harness,
    request_id: str,
    *,
    session_ids: list[str] | None = None,
    budget_ms: int = 2000,
    queue_limit: int = 8,
    write_timeout_ms: int | None = None,
    read_timeout_ms: int | None = None,
    expected_status: int = 200,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "request_id": request_id,
        "time_budget_ms": budget_ms,
        "queue_limit": queue_limit,
        "session_ids": session_ids,
    }
    if write_timeout_ms is not None:
        payload["write_timeout_ms"] = write_timeout_ms
    if read_timeout_ms is not None:
        payload["read_timeout_ms"] = read_timeout_ms
    status, body = harness.call(
        "POST",
        "/v1/scheduler/rounds",
        json_body=payload,
        expected_status=expected_status,
    )
    harness.report["traces"][request_id] = body
    return body


def print_trace(body: dict[str, Any]) -> None:
    trace = body.get("trace") or body.get("context", {}).get("round", {}).get("trace") or []
    for task in trace:
        log(
            "    "
            f"{task['order_index']:>2} p={task['priority']:<3} {task['session_id']}:{task['seq']} "
            f"{task['backend_id']:<10} {task['state']:<10} {task['business_code'] or '-'}"
        )


def quota_summary(snapshot: dict[str, Any]) -> str:
    return ", ".join(
        f"{backend}: committed={row['committed_bytes']} reserved={row['reserved_bytes']} "
        f"remaining={row['remaining_bytes']}"
        for backend, row in sorted(snapshot.items())
    )


def set_backend_mode(harness: Harness, cfg: dict[str, Any], backend_id: str, mode: str, hang: float = 0.0) -> None:
    backend = next(item for item in cfg["backends"] if item["id"] == backend_id)
    payload: dict[str, Any] = {"mode": mode}
    if hang:
        payload["hang_seconds"] = hang
    response = harness.client.post(f"{backend['url']}/admin/mode", json=payload)
    response.raise_for_status()
    log(f"  MODE      {backend_id} -> {mode}" + (f" ({hang}s)" if hang else ""))


def backend_state(harness: Harness, cfg: dict[str, Any], backend_id: str) -> dict[str, Any]:
    backend = next(item for item in cfg["backends"] if item["id"] == backend_id)
    response = harness.client.get(f"{backend['url']}/admin/state")
    response.raise_for_status()
    return response.json()


def poll_crash_progress(round_started_after: str, timeout: float = 8.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        try:
            connection = sqlite3.connect(DB_PATH, timeout=1.0)
            connection.row_factory = sqlite3.Row
            round_row = connection.execute(
                "SELECT * FROM scheduling_rounds WHERE status='RUNNING' ORDER BY created_at DESC LIMIT 1"
            ).fetchone()
            if round_row is not None:
                states = {
                    row["state"]: row["count"]
                    for row in connection.execute(
                        "SELECT state, COUNT(*) AS count FROM round_tasks WHERE round_id=? GROUP BY state",
                        (round_row["id"],),
                    ).fetchall()
                }
                reserved = connection.execute(
                    "SELECT COALESCE(SUM(reserved_bytes), 0) AS total FROM backend_quota"
                ).fetchone()["total"]
                last = {
                    "round_id": round_row["id"],
                    "started_at": round_row["created_at"],
                    "states": states,
                    "reserved_bytes": reserved,
                }
                if round_row["created_at"] >= round_started_after and reserved > 0 and states.get("COMMITTED", 0) >= 2:
                    connection.close()
                    return last
            connection.close()
        except sqlite3.Error:
            pass
        time.sleep(0.03)
    raise DemoFailure(f"did not observe a partially committed, quota-reserved round: {last}")


def main() -> int:
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    LOGS.mkdir(parents=True, exist_ok=True)
    for file in LOGS.glob("*.log"):
        file.unlink()
    reset_database(DB_PATH)
    config = load_config()
    harness = Harness()
    gateway: subprocess.Popen | None = None
    worker: subprocess.Popen | None = None
    try:
        log("=" * 88)
        log("LOCAL OBJECT GATEWAY / SHARDED UPLOAD COORDINATION DEMO")
        log("=" * 88)
        integrity_failures = verify_part_hashes()
        harness.check("fixed sample content recomputes to declared SHA-256", not integrity_failures, integrity_failures)

        log("\n[1] Start local backend simulators and gateway")
        for backend in config["backends"]:
            port = int(backend["url"].rsplit(":", 1)[1])
            harness.start_process(
                backend["id"],
                [
                    sys.executable,
                    "-m",
                    "uvicorn",
                    "app.backend_sim:app",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(port),
                    "--log-level",
                    "warning",
                ],
                {
                    "BACKEND_CONFIG_FILE": str(CONFIG_PATH),
                    "BACKEND_ID": backend["id"],
                },
            )
        for backend in config["backends"]:
            harness.wait_http(f"{backend['url']}/health", backend["id"])

        gateway_env = {
            "GATEWAY_DB": str(DB_PATH),
            "GATEWAY_BACKENDS_FILE": str(CONFIG_PATH),
        }
        gateway = harness.start_process(
            "gateway",
            [
                sys.executable,
                "-m",
                "uvicorn",
                "app.main:app",
                "--host",
                "127.0.0.1",
                "--port",
                str(GATEWAY_PORT),
                "--log-level",
                "warning",
            ],
            gateway_env,
        )
        harness.wait_http(f"{GATEWAY_URL}/health", "gateway")

        log("\n[2] Validation, conflict and idempotency business codes")
        main_samples = [sample(name) for name in ("sess-alpha", "sess-beta", "sess-gamma")]
        create_and_submit(harness, main_samples[0], submit_order=[3, 1, 5, 2, 4])
        create_and_submit(harness, main_samples[1], submit_order=[2, 4, 1, 3])
        create_and_submit(harness, main_samples[2], submit_order=[3, 2, 1])

        _, conflict = harness.call(
            "POST",
            "/v1/sessions",
            json_body={
                "object_key": "drive/alpha.dat",
                "backend_id": "backend-a",
                "max_parts": 2,
                "total_parts": 2,
                "session_id": "sess-conflict",
            },
            request_id="conflict-session",
            expected_status=409,
        )
        harness.check("same-key active session rejected", conflict["code"] == "E_SESSION_CONFLICT" and conflict["context"]["existing_session_id"] == "sess-alpha", conflict)

        before_invalid = harness.call("GET", "/v1/sessions/sess-alpha", expected_status=200)[1]
        _, invalid_seq = harness.call(
            "POST",
            "/v1/sessions/sess-alpha/parts",
            json_body={"seq": 6, "size_bytes": 10, "sha256": sha("out-of-range")},
            expected_status=422,
        )
        after_invalid = harness.call("GET", "/v1/sessions/sess-alpha", expected_status=200)[1]
        harness.check("invalid seq returns E_PART_SEQ_INVALID without session version change", invalid_seq["code"] == "E_PART_SEQ_INVALID" and before_invalid["version"] == after_invalid["version"], invalid_seq)

        alpha_part = main_samples[0]["parts"][0]
        _, duplicate = harness.call(
            "POST",
            "/v1/sessions/sess-alpha/parts",
            json_body={"seq": 1, "size_bytes": alpha_part["size_bytes"], "sha256": alpha_part["sha256"]},
            request_id="duplicate-alpha-1",
            expected_status=200,
        )
        harness.check("identical duplicate submit is idempotent", duplicate["duplicate"] is True, {"version": duplicate["version"]})
        _, mismatch = harness.call(
            "POST",
            "/v1/sessions/sess-alpha/parts",
            json_body={"seq": 1, "size_bytes": alpha_part["size_bytes"], "sha256": sha("different-content")},
            expected_status=409,
        )
        harness.check("same seq with different digest returns E_PART_MISMATCH", mismatch["code"] == "E_PART_MISMATCH" and mismatch["context"]["existing_sha256"] == alpha_part["sha256"], mismatch)

        _, conflicting_request = harness.call(
            "POST",
            "/v1/sessions",
            json_body={
                "object_key": "drive/some-other.dat",
                "backend_id": "backend-a",
                "max_parts": 1,
                "total_parts": 1,
                "session_id": "sess-different-body",
            },
            request_id="create-sess-alpha",
            expected_status=409,
        )
        harness.check("reused request id with different body returns E_CONFLICTING_REQUEST", conflicting_request["code"] == "E_CONFLICTING_REQUEST", conflicting_request)

        log("\n[3] Bounded queue guard")
        queue_body = schedule(
            harness,
            "queue-full-demo",
            session_ids=[sam["session_id"] for sam in main_samples],
            budget_ms=2000,
            queue_limit=0,
            expected_status=429,
        )
        harness.check("queue overflow returns E_SCHED_QUEUE_FULL", queue_body["code"] == "E_SCHED_QUEUE_FULL", queue_body)
        quota_before_parallel = harness.record_quota("before-parallel")
        harness.check("queue rejection reserves zero quota", all(row["reserved_bytes"] == 0 for row in quota_before_parallel.values()))

        log("\n[4] Multi-session parallel scheduling round")
        main_round = schedule(
            harness,
            "parallel-main-round",
            session_ids=[sam["session_id"] for sam in main_samples],
            budget_ms=2000,
            queue_limit=8,
        )
        print_trace(main_round)
        quota_after_parallel = harness.record_quota("after-parallel")
        log("  QUOTA     " + quota_summary(quota_after_parallel))
        harness.check("all 12 parallel parts verified", main_round["summary"]["verified"] == 12, main_round["summary"])
        harness.check("global concurrency never exceeded", main_round["summary"]["scheduler_metrics"]["global_peak_active"] <= config["global_concurrency"], main_round["summary"]["scheduler_metrics"])
        harness.check("per-backend concurrency respected by simulator", all(backend_state(harness, config, backend["id"])["peak_active_writes"] <= backend["max_concurrency"] for backend in config["backends"]))
        for sam in main_samples:
            detail = harness.call("GET", f"/v1/sessions/{sam['session_id']}", expected_status=200)[1]
            expected_digest = expected_object_digest(sam)
            harness.check(
                f"{sam['session_id']} deterministic object digest",
                detail["session"]["status"] == "COMPLETED" and detail["session"]["object_digest"] == expected_digest,
                {"actual": detail["session"]["object_digest"], "expected": expected_digest},
            )
            harness.report["object_digests"][sam["session_id"]] = expected_digest

        cached_round = schedule(
            harness,
            "parallel-main-round",
            session_ids=[sam["session_id"] for sam in main_samples],
            budget_ms=2000,
            queue_limit=8,
        )
        quota_after_cached = harness.record_quota("after-idempotent-replay")
        harness.check("idempotent round replay returns cached round", cached_round["round_id"] == main_round["round_id"], {"round_id": cached_round["round_id"]})
        harness.check("idempotent round replay changes no quota", quota_after_cached == quota_after_parallel)

        log("\n[5] Backend quota exhaustion")
        quota_sample = sample("sess-quota-probe")
        create_and_submit(harness, quota_sample)
        quota_failure = schedule(
            harness,
            "quota-exhaustion-round",
            session_ids=[quota_sample["session_id"]],
            budget_ms=2000,
            expected_status=409,
        )
        print_trace(quota_failure)
        quota_after_reject = harness.record_quota("after-quota-reject")
        harness.check(
            "quota exhaustion returns E_QUOTA_EXCEEDED with remaining bytes",
            quota_failure["code"] == "E_QUOTA_EXCEEDED" and quota_failure["context"]["remaining_bytes"] == 24576,
            quota_failure,
        )
        harness.check("failed reservation does not alter committed or reserved quota", quota_after_reject["backend-b"]["reserved_bytes"] == 0 and quota_after_reject["backend-b"]["committed_bytes"] == 16384, quota_after_reject["backend-b"])
        log("  QUOTA     " + quota_summary(quota_after_reject))

        log("\n[6] Readback mismatch then deterministic retry")
        mismatch_sample = sample("sess-mismatch")
        create_and_submit(harness, mismatch_sample)
        set_backend_mode(harness, config, "backend-b", "mismatch")
        mismatch_failure = schedule(
            harness,
            "readback-mismatch-round",
            session_ids=[mismatch_sample["session_id"]],
            expected_status=502,
        )
        print_trace(mismatch_failure)
        quota_after_mismatch = harness.record_quota("after-readback-mismatch")
        harness.check("readback mismatch returns E_READBACK_MISMATCH and rolls back reservation", mismatch_failure["code"] == "E_READBACK_MISMATCH" and quota_after_mismatch["backend-b"]["reserved_bytes"] == 0, mismatch_failure)
        set_backend_mode(harness, config, "backend-b", "normal")
        mismatch_retry = schedule(
            harness,
            "readback-mismatch-retry",
            session_ids=[mismatch_sample["session_id"]],
        )
        print_trace(mismatch_retry)
        mismatch_detail = harness.call("GET", f"/v1/sessions/{mismatch_sample['session_id']}", expected_status=200)[1]
        harness.check("mismatched part is absent from completed state before retry and verified after retry", mismatch_retry["summary"]["verified"] == 1 and mismatch_detail["session"]["status"] == "COMPLETED", mismatch_detail["session"])
        harness.report["object_digests"][mismatch_sample["session_id"]] = expected_object_digest(mismatch_sample)

        log("\n[7] Timeout release, degraded retry and quota restoration")
        timeout_sample = sample("sess-timeout")
        create_and_submit(harness, timeout_sample)
        set_backend_mode(harness, config, "backend-c", "hang", 0.6)
        started = time.monotonic()
        timeout_failure = schedule(
            harness,
            "timeout-round",
            session_ids=[timeout_sample["session_id"]],
            write_timeout_ms=80,
            read_timeout_ms=80,
            expected_status=504,
        )
        elapsed = time.monotonic() - started
        print_trace(timeout_failure)
        quota_after_timeout = harness.record_quota("after-timeout")
        timeout_detail = harness.call("GET", f"/v1/sessions/{timeout_sample['session_id']}", expected_status=200)[1]
        harness.check("write timeout returns E_BACKEND_TIMEOUT without waiting for hung handler", timeout_failure["code"] == "E_BACKEND_TIMEOUT" and elapsed < 1.5, {"elapsed_seconds": round(elapsed, 3)})
        harness.check("timeout rollback restores quota reservation", quota_after_timeout["backend-c"]["reserved_bytes"] == 0 and quota_after_timeout["backend-c"]["committed_bytes"] == 12288, quota_after_timeout["backend-c"])
        harness.check("timed-out parts are deferred, not completed", {part["status"] for part in timeout_detail["parts"]} == {"DEFERRED"}, timeout_detail["parts"])
        time.sleep(0.8)
        set_backend_mode(harness, config, "backend-c", "normal")
        timeout_retry = schedule(
            harness,
            "timeout-retry-round",
            session_ids=[timeout_sample["session_id"]],
            write_timeout_ms=800,
            read_timeout_ms=800,
        )
        print_trace(timeout_retry)
        timeout_detail = harness.call("GET", f"/v1/sessions/{timeout_sample['session_id']}", expected_status=200)[1]
        harness.check("deferred timeout parts succeed on retry", timeout_retry["summary"]["verified"] == 2 and timeout_detail["session"]["status"] == "COMPLETED", timeout_detail["session"])
        harness.report["object_digests"][timeout_sample["session_id"]] = expected_object_digest(timeout_sample)

        log("\n[8] Forced termination during scheduling and startup recovery")
        crash_samples = [sample("sess-crash-ok"), sample("sess-crash-hang")]
        for sam in crash_samples:
            create_and_submit(harness, sam)
        set_backend_mode(harness, config, "backend-c", "hang", 30.0)
        worker_started_at = now_iso()
        worker = harness.start_process(
            "sigkill-worker",
            [
                sys.executable,
                "-m",
                "scripts.sigkill_worker",
                "--db",
                str(DB_PATH),
                "--config",
                str(CONFIG_PATH),
                "--request-id",
                "force-kill-round",
                "--session-ids",
                "sess-crash-ok",
                "sess-crash-hang",
            ],
            {"PYTHONPATH": str(ROOT)},
        )
        progress = poll_crash_progress(worker_started_at)
        log(f"  OBSERVED  round={progress['round_id']} states={progress['states']} reserved={progress['reserved_bytes']}")
        os.kill(worker.pid, signal.SIGKILL)
        worker.wait(timeout=5)
        harness.processes.remove(worker)
        worker = None
        harness.check("worker was force-killed after durable quota reservation", progress["reserved_bytes"] > 0)

        log("  Restarting gateway to execute startup intent recovery...")
        assert gateway is not None
        harness.stop_process(gateway)
        gateway = harness.start_process(
            "gateway",
            [
                sys.executable,
                "-m",
                "uvicorn",
                "app.main:app",
                "--host",
                "127.0.0.1",
                "--port",
                str(GATEWAY_PORT),
                "--log-level",
                "warning",
            ],
            gateway_env,
        )
        harness.wait_http(f"{GATEWAY_URL}/health", "gateway after restart")
        recovery = harness.call("GET", "/v1/recovery/report", expected_status=200)[1]
        report = recovery["report"]
        log("  RECOVERY  " + json.dumps({
            "report_id": recovery["report_id"],
            "scope": report["scope"],
            "aborted_rounds": report["aborted_rounds"],
            "rolled_back": len(report["rolled_back_intents"]),
            "preserved_committed": len(report["committed_intents_preserved"]),
        }, separators=(",", ": ")))
        quota_recovered = harness.record_quota("after-sigkill-recovery")
        crash_ok = harness.call("GET", "/v1/sessions/sess-crash-ok", expected_status=200)[1]
        crash_hang = harness.call("GET", "/v1/sessions/sess-crash-hang", expected_status=200)[1]
        harness.check("restart report is explicitly startup-recovery-only", report["scope"] == "startup_recovery_only")
        harness.check("terminated round is marked aborted", progress["round_id"] in report["aborted_rounds"], report["aborted_rounds"])
        harness.check("uncommitted reservations are rolled back", all(row["reserved_bytes"] == 0 for row in quota_recovered.values()), quota_recovered)
        harness.check("some committed facts survive the kill", len(report["committed_intents_preserved"]) >= 1, report["committed_intents_preserved"])
        harness.check("hung parts are recovered to pre-dispatch state", all(part["status"] in {"PENDING", "DEFERRED"} for part in crash_hang["parts"]), crash_hang["parts"])
        log(f"  CRASH-OK  status={crash_ok['session']['status']} completed={crash_ok['session']['completed_parts']}/{crash_ok['session']['total_parts']} digest={crash_ok['session']['object_digest']}")
        log(f"  CRASH-HANG status={crash_hang['session']['status']} parts={[part['status'] for part in crash_hang['parts']]}")
        log("  QUOTA     " + quota_summary(quota_recovered))
        harness.report["recovery"] = recovery

        set_backend_mode(harness, config, "backend-c", "normal")
        harness.report["business_codes"] = sorted(harness.codes_seen)
        required_codes = {
            "E_SESSION_CONFLICT",
            "E_PART_SEQ_INVALID",
            "E_PART_MISMATCH",
            "E_SCHED_QUEUE_FULL",
            "E_BACKEND_TIMEOUT",
            "E_READBACK_MISMATCH",
            "E_QUOTA_EXCEEDED",
            "E_CONFLICTING_REQUEST",
        }
        harness.check("demo exercised all required business codes", required_codes.issubset(harness.codes_seen), sorted(required_codes - harness.codes_seen))
        harness.report["completed_at"] = now_iso()
        harness.report["final_quota"] = quota_recovered
        report_path = ARTIFACTS / "demo-report.json"
        report_path.write_text(json.dumps(harness.report, indent=2, sort_keys=True), encoding="utf-8")

        log("\n" + "=" * 88)
        log("FINAL OBJECT DIGESTS")
        for session_id, digest in harness.report["object_digests"].items():
            log(f"  {session_id:<18} {digest}")
        log(f"  sess-crash-ok      {crash_ok['session']['object_digest']}")
        log("BUSINESS CODES OBSERVED: " + ", ".join(sorted(harness.codes_seen)))
        log(f"FINAL QUOTA: {quota_summary(quota_recovered)}")
        log(f"REPORT: {report_path}")
        log("=" * 88)
        return 0
    except Exception as error:
        harness.report["failed_at"] = now_iso()
        harness.report["failure"] = f"{type(error).__name__}: {error}"
        (ARTIFACTS / "demo-report.json").write_text(
            json.dumps(harness.report, indent=2, sort_keys=True), encoding="utf-8"
        )
        log(f"\nDEMO FAILED: {type(error).__name__}: {error}")
        for process in harness.processes:
            log(f"  process {process.args}: poll={process.poll()}")
        return 1
    finally:
        harness.cleanup()


if __name__ == "__main__":
    raise SystemExit(main())
