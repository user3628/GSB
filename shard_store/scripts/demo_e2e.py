"""End-to-end demonstration against a real local uvicorn process.

Scenarios required by the task:
  1. out-of-order upload then merge (fast path + full path equivalence)
  2. duplicate part submission (idempotent success + mismatch rejection)
  3. gap merge failure (E_PART_MISSING, no visible object)
  4. session cancellation
  5. forced-termination recovery and re-merge with identical digest

The script starts uvicorn as a subprocess on an ephemeral port, talks plain
HTTP over localhost, and uses only the fixed files under samples/.
"""
from __future__ import annotations

import base64
import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _wait_ready(base_url: str, timeout: float = 15.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = httpx.get(f"{base_url}/api/v1/health", timeout=1.0)
            if r.status_code == 200:
                return
        except httpx.TransportError:
            time.sleep(0.2)
    raise RuntimeError("uvicorn did not become ready in time")


def _short(value: str, keep: int = 16) -> str:
    return value[:keep] + "..." if len(value) > keep else value


class DemoClient:
    def __init__(self, base_url: str):
        self.http = httpx.Client(base_url=base_url, timeout=10.0)
        self.step_no = 0

    def step(self, title: str) -> None:
        self.step_no += 1
        print(f"\n{'=' * 72}\nSTEP {self.step_no}: {title}\n{'=' * 72}")

    def call(self, method: str, path: str, **kwargs) -> httpx.Response:
        r = self.http.request(method, path, **kwargs)
        status = r.status_code
        try:
            body = r.json()
        except ValueError:
            body = r.text
        code = body.get("error", {}).get("code") if isinstance(body, dict) else None
        print(f"  {method} {path} -> HTTP {status}" + (f" [{code}]" if code else " [ok]"))
        return r

    def create_session(self, object_key: str, **extra) -> dict:
        payload = {"object_key": object_key, "min_part_size": 1,
                   "max_part_size": 10 * 1024 * 1024, **extra}
        r = self.call("POST", "/api/v1/sessions", json=payload)
        r.raise_for_status()
        return r.json()["data"]

    def upload(self, session_id: str, part_number: int, content: bytes) -> httpx.Response:
        return self.call(
            "POST", f"/api/v1/sessions/{session_id}/parts/{part_number}",
            json={"content_base64": base64.b64encode(content).decode()},
        )

    def close(self) -> None:
        self.http.close()


def _load_samples() -> dict:
    return json.loads((ROOT / "samples" / "manifest.json").read_text())


def _part_bytes(manifest_entry: dict) -> bytes:
    return (ROOT / manifest_entry["file"]).read_bytes()


def main() -> int:
    port = _free_port()
    db_path = ROOT / "data" / "demo.db"
    env = {**os.environ, "SHARD_STORE_DB": str(db_path),
           "PYTHONPATH": str(ROOT)}
    # Fresh deterministic database for every demo run.
    for suffix in ("", "-wal", "-shm"):
        sidecar = Path(str(db_path) + suffix)
        if sidecar.exists():
            sidecar.unlink()

    server = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app",
         "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning"],
        cwd=ROOT, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    base_url = f"http://127.0.0.1:{port}"
    manifest = _load_samples()
    alpha = manifest["objects"][0]
    bravo = manifest["objects"][1]

    try:
        _wait_ready(base_url)
        api = DemoClient(base_url)
        print(f"uvicorn listening on {base_url} (pid={server.pid})")
        print(f"database: {db_path}")

        # --- Scenario 1: out-of-order upload + merge, both strategies -----
        api.step("out-of-order upload then fast/full merge equivalence")
        sess = api.create_session(
            alpha["object_key"], expected_total_size=alpha["expected_total_size"],
            conflict_policy="allow",
        )
        order = [3, 1, 4, 2]
        print(f"  arrival order deliberately shuffled: {order}")
        for part_number in order:
            entry = alpha["parts"][part_number - 1]
            r = api.upload(sess["session_id"], part_number, _part_bytes(entry))
            assert r.status_code == 200
        listing = api.call("GET", f"/api/v1/sessions/{sess['session_id']}/parts")
        stored_order = [p["part_number"] for p in listing.json()["data"]["parts"]]
        print(f"  stored manifest order: {stored_order}")
        assert stored_order == [1, 2, 3, 4]

        fast = api.call("POST", f"/api/v1/sessions/{sess['session_id']}/merge",
                        json={"strategy": "fast"})
        fast_data = fast.json()["data"]
        print(f"  fast digest : {_short(fast_data['object_sha256'])}  "
              f"version={fast_data['version']}  etag={fast_data['etag']}")
        print(f"  fast evidence: {fast_data['strategy_report']['source_table']} "
              f"({len(fast_data['strategy_report']['parts'])} persisted part digests)")
        assert fast_data["object_sha256"] == alpha["expected_object_sha256"]

        # Coexisting session, full path, same key -> version 2, same digest.
        sess_b = api.create_session(
            alpha["object_key"], expected_total_size=alpha["expected_total_size"],
            conflict_policy="allow",
        )
        for entry in alpha["parts"]:
            api.upload(sess_b["session_id"], entry["part_number"], _part_bytes(entry))
        full = api.call("POST", f"/api/v1/sessions/{sess_b['session_id']}/merge",
                        json={"strategy": "full"})
        full_data = full.json()["data"]
        timings = [p["verify_elapsed_ms"] for p in
                   full_data["strategy_report"]["per_part_verification"]]
        print(f"  full digest : {_short(full_data['object_sha256'])}  "
              f"version={full_data['version']}  "
              f"per-part ms={[round(t, 4) for t in timings]}  "
              f"total={full_data['strategy_report']['total_verify_elapsed_ms']}ms")
        assert full_data["object_sha256"] == fast_data["object_sha256"]
        assert full_data["version"] == fast_data["version"] + 1
        obj = api.call("GET", f"/api/v1/objects/{alpha['object_key']}")
        print(f"  current object version: {obj.json()['data']['version']}")

        # --- Scenario 2: duplicate part submission ------------------------
        api.step("duplicate part: idempotent success then mismatch rejection")
        dup_sess = api.create_session("demo/dup.obj")
        content = _part_bytes(bravo["parts"][0])
        first = api.upload(dup_sess["session_id"], 1, content)
        again = api.upload(dup_sess["session_id"], 1, content)
        print(f"  repeat same content -> {again.json()['data']['status']}")
        assert again.json()["data"]["status"] == "idempotent"
        changed = api.upload(dup_sess["session_id"], 1, b"tampered-content-bytes")
        assert changed.json()["error"]["code"] == "E_PART_MISMATCH"
        kept = api.call("GET", f"/api/v1/sessions/{dup_sess['session_id']}/parts")
        kept_sha = kept.json()["data"]["parts"][0]["sha256"]
        print(f"  first submission retained: {_short(kept_sha)} (part_count stays "
              f"{kept.json()['data']['part_count']})")
        assert kept_sha == bravo["parts"][0]["sha256"]

        # --- Scenario 3: gap merge failure --------------------------------
        api.step("gap merge failure: E_PART_MISSING with no visible object")
        gap_sess = api.create_session(bravo["object_key"] + ".gap")
        api.upload(gap_sess["session_id"], 1, _part_bytes(bravo["parts"][0]))
        api.upload(gap_sess["session_id"], 3, _part_bytes(bravo["parts"][2]))
        gap = api.call("POST", f"/api/v1/sessions/{gap_sess['session_id']}/merge",
                       json={})
        assert gap.json()["error"]["code"] == "E_PART_MISSING"
        print(f"  missing numbers: "
              f"{gap.json()['error']['details']['missing_part_numbers']}")
        invisible = api.call("GET", f"/api/v1/objects/{bravo['object_key']}.gap")
        assert invisible.json()["error"]["code"] == "E_OBJECT_NOT_FOUND"
        state = api.call("GET", f"/api/v1/sessions/{gap_sess['session_id']}")
        print(f"  session still '{state.json()['data']['status']}', object not visible")

        # --- Scenario 4: cancellation --------------------------------------
        api.step("cancel session and verify rejection of further writes")
        cancel_sess = api.create_session("demo/cancel.obj")
        api.upload(cancel_sess["session_id"], 1, _part_bytes(bravo["parts"][0]))
        cancelled = api.call("POST",
                             f"/api/v1/sessions/{cancel_sess['session_id']}/cancel",
                             json={})
        print(f"  status -> {cancelled.json()['data']['status']}")
        rejected = api.upload(cancel_sess["session_id"], 2,
                              _part_bytes(bravo["parts"][1]))
        assert rejected.json()["error"]["code"] == "E_SESSION_CANCELLED"
        print("  further part submission rejected with E_SESSION_CANCELLED")

        # --- Scenario 5: forced termination, recovery, re-merge ------------
        api.step("simulated forced termination -> recovery -> identical re-merge")
        crash_sess = api.create_session(
            "demo/crash.obj", expected_total_size=alpha["expected_total_size"],
        )
        for entry in alpha["parts"]:
            api.upload(crash_sess["session_id"], entry["part_number"],
                       _part_bytes(entry))
        before = api.call("GET", f"/api/v1/sessions/{crash_sess['session_id']}")
        before_status = before.json()["data"]["status"]
        before_parts = before.json()["data"]["part_count"]
        crash = api.call("POST", f"/api/v1/sessions/{crash_sess['session_id']}/merge",
                         json={"simulate_crash_after_intent": True})
        assert crash.json()["error"]["code"] == "E_SIMULATED_CRASH"
        intent_id = crash.json()["error"]["details"]["intent_id"]
        mid = api.call("GET", f"/api/v1/sessions/{crash_sess['session_id']}")
        print(f"  before crash: status={before_status}, parts={before_parts}")
        print(f"  after crash : status={mid.json()['data']['status']}, "
              f"intent={intent_id}, object visible="
              f"{api.call('GET', '/api/v1/objects/demo/crash.obj').status_code == 200}")

        report = api.call("POST", "/api/v1/recovery/run", json={})
        report_data = report.json()["data"]
        print(f"  recovery scanned={report_data['scanned_pending_intents']} "
              f"reopened={report_data['reopened_for_retry']} "
              f"aborted={report_data['aborted']}")
        action = report_data["actions"][0]
        print(f"  resolution={action['resolution']}, "
              f"facts={action['facts']}")
        after = api.call("GET", f"/api/v1/sessions/{crash_sess['session_id']}")
        print(f"  session status after recovery: {after.json()['data']['status']} "
              f"(parts retained={after.json()['data']['part_count']})")

        remerge = api.call("POST",
                           f"/api/v1/sessions/{crash_sess['session_id']}/merge",
                           json={"strategy": "full"})
        remerge_data = remerge.json()["data"]
        print(f"  re-merge digest: {_short(remerge_data['object_sha256'])} "
              f"version={remerge_data['version']}")
        assert remerge_data["object_sha256"] == alpha["expected_object_sha256"]
        print("  digest identical to uninterrupted fast/full merges: True")

        api.call("GET", "/api/v1/recovery/reports")
        api.close()
        print(f"\n{'=' * 72}\nALL DEMO SCENARIOS PASSED\n{'=' * 72}")
        return 0
    finally:
        server.terminate()
        try:
            server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            server.kill()


if __name__ == "__main__":
    raise SystemExit(main())
