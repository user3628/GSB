from __future__ import annotations

import argparse
import hashlib
import json
import os
import queue
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from alert_sieve.domain import topology_increment_checksum  # noqa: E402
from alert_sieve.samples import load_alerts_jsonl, load_json  # noqa: E402
from alert_sieve.util import canonical_json  # noqa: E402


class ApiError(RuntimeError):
    def __init__(self, status: int, body: dict[str, Any]) -> None:
        self.status = status
        self.body = body
        self.code = body.get("error", {}).get("code", "E_UNKNOWN")
        super().__init__(f"{status} {self.code}: {body.get('error', {}).get('message')}")


class Client:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

    def request(
        self,
        method: str,
        path: str,
        body: Any | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        data = None
        request_headers = dict(headers or {})
        if body is not None:
            data = json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            request_headers["Content-Type"] = "application/json; charset=utf-8"
        req = urllib.request.Request(self.base_url + path, data=data, headers=request_headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=20) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            payload = json.loads(exc.read().decode("utf-8"))
            raise ApiError(exc.code, payload) from exc

    def get(self, path: str) -> dict[str, Any]:
        return self.request("GET", path)["data"]

    def post(self, path: str, body: Any, headers: dict[str, str] | None = None) -> dict[str, Any]:
        return self.request("POST", path, body, headers)["data"]


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _read_ready(proc: subprocess.Popen[str], timeout_s: float = 10.0) -> dict[str, Any]:
    q: queue.Queue[str | None] = queue.Queue()

    def reader() -> None:
        assert proc.stdout is not None
        q.put(proc.stdout.readline())
        q.put(None)

    threading.Thread(target=reader, daemon=True).start()
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            line = q.get(timeout=0.1)
        except queue.Empty:
            continue
        if line is None:
            break
        if line.strip():
            return json.loads(line)
    stderr = ""
    if proc.stderr is not None:
        try:
            stderr = proc.stderr.read()
        except Exception:
            pass
    raise RuntimeError(f"server did not become ready; stderr={stderr[:4000]}")


def start_server(db_path: Path, *, fault_injection: bool) -> tuple[subprocess.Popen[str], Client]:
    env = os.environ.copy()
    pythonpath = str(ROOT / "src")
    env["PYTHONPATH"] = pythonpath + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    port = _free_port()
    args = [
        sys.executable,
        "-m",
        "alert_sieve.server",
        "--db",
        str(db_path),
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
    ]
    if fault_injection:
        args.append("--enable-fault-injection")
    proc = subprocess.Popen(
        args,
        cwd=str(ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    ready = _read_ready(proc)
    if not ready.get("ready"):
        proc.kill()
        raise RuntimeError(f"server readiness payload was invalid: {ready}")
    return proc, Client(f"http://127.0.0.1:{ready['port']}")


def stop_server(proc: subprocess.Popen[str] | None) -> None:
    if proc is None or proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def product_digest(client: Client) -> str:
    state = client.get("/v1/state")
    # Counts include recovery bookkeeping. Product comparisons must only use the
    # committed versions and derived product views.
    committed_state = {key: value for key, value in state.items() if key != "counts"}
    views = {
        "state": committed_state,
        "reduction": client.get("/v1/reduction?include_alerts=true"),
        "suppression": client.get("/v1/suppression-index"),
        "root_causes": client.get("/v1/root-causes?limit=10000"),
    }
    digest = hashlib.sha256(canonical_json(views).encode("utf-8")).hexdigest()
    return "sha256:" + digest


def error_code(callable_) -> str:
    try:
        callable_()
    except ApiError as exc:
        return exc.code
    return "NO_ERROR"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="End-to-end Alert Sieve local HTTP demo")
    parser.add_argument("--db", default=str(ROOT / "artifacts" / "e2e.db"))
    parser.add_argument("--report", default=str(ROOT / "artifacts" / "e2e_result.json"))
    args = parser.parse_args(argv)

    db_path = Path(args.db)
    report_path = Path(args.report)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    for suffix in ("", "-wal", "-shm"):
        candidate = Path(str(db_path) + suffix)
        if candidate.exists():
            candidate.unlink()

    server: subprocess.Popen[str] | None = None
    report: dict[str, Any] = {"sample_dir": str(ROOT / "samples"), "steps": []}
    try:
        server, client = start_server(db_path, fault_injection=True)
        report["steps"].append({"step": "server_started", "health": client.get("/v1/health")})

        alerts_body = load_alerts_jsonl(ROOT / "samples" / "alerts_v1.jsonl")
        report["steps"].append(
            {
                "step": "full_alert_snapshot",
                "result": client.post("/v1/alerts/snapshot", alerts_body),
            }
        )
        report["steps"].append(
            {
                "step": "full_topology_snapshot",
                "result": client.post("/v1/topology/snapshot", load_json(ROOT / "samples" / "topology_v1.json")),
            }
        )
        report["steps"].append(
            {
                "step": "full_rules_snapshot",
                "result": client.post("/v1/rules/snapshot", load_json(ROOT / "samples" / "rules_v1.json")),
            }
        )

        reduction_loaded = client.get("/v1/reduction")
        roots_loaded = client.get("/v1/root-causes?service=edge-gateway&limit=100")
        report["steps"].append(
            {
                "step": "baseline_views",
                "reduction_totals": reduction_loaded["totals"],
                "gateway_root_candidates": roots_loaded["alerts"],
            }
        )

        report["steps"].append(
            {
                "step": "alert_increment_v2",
                "result": client.post(
                    "/v1/alerts/increments", load_json(ROOT / "samples" / "alerts_increment_v2.json")
                ),
            }
        )
        report["steps"].append(
            {
                "step": "topology_increment_v2",
                "result": client.post(
                    "/v1/topology/increments", load_json(ROOT / "samples" / "topology_increment_v2.json")
                ),
            }
        )
        report["steps"].append(
            {
                "step": "rules_increment_v2",
                "result": client.post(
                    "/v1/rules/increments", load_json(ROOT / "samples" / "rules_increment_v2.json")
                ),
            }
        )

        reduction_final = client.get("/v1/reduction")
        roots_final = client.get("/v1/root-causes?limit=10000")
        invalidations = client.get("/v1/invalidations?limit=20")
        latest_event = client.get(f"/v1/invalidations/{invalidations['events'][0]['event_id']}")
        before_crash_digest = product_digest(client)
        report["steps"].append(
            {
                "step": "pre_crash_committed_state",
                "state": client.get("/v1/state"),
                "reduction_totals": reduction_final["totals"],
                "root_cause_rows": roots_final["row_count"],
                "invalidation_events": invalidations["events"],
                "latest_invalidation": latest_event,
                "product_digest": before_crash_digest,
            }
        )

        crash_error = None
        try:
            client.post(
                "/v1/topology/increments",
                load_json(ROOT / "samples" / "topology_increment_v3_crash.json"),
                headers={"X-Debug-Crash-After-Products": "1"},
            )
        except Exception as exc:  # connection reset is expected
            crash_error = type(exc).__name__
        try:
            crash_returncode = server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            server.kill()
            crash_returncode = server.wait(timeout=5)
        report["steps"].append(
            {
                "step": "forced_termination",
                "observed_transport_error": crash_error,
                "server_returncode": crash_returncode,
            }
        )
        server = None

        server, client = start_server(db_path, fault_injection=False)
        recovery = client.get("/v1/recovery/report")
        after_crash_digest = product_digest(client)
        state_after = client.get("/v1/state")
        recovery_state_ok = (
            state_after["alert_version"] == 2
            and state_after["topology_version"] == 2
            and state_after["rules_version"] == 2
        )
        aborted = [
            item
            for item in recovery["recovered_intents"]
            if item["outcome"] == "aborted_after_uncommitted_rebuild"
        ]

        unknown_batch_code = error_code(lambda: client.get("/v1/batches/not-a-batch"))
        stale_payload = {
            "kind": "topology_increment",
            "base_version": 1,
            "version": 9,
            "checksum": topology_increment_checksum(1, 9, [("checkout", "ledger")], [], [], []),
            "upsert_edges": [{"source": "checkout", "target": "ledger"}],
            "remove_edges": [],
            "upsert_nodes": [],
            "remove_nodes": [],
        }
        stale_code = error_code(lambda: client.post("/v1/topology/increments", stale_payload))
        codes = {"E_ORDER_NOT_FOUND": unknown_batch_code, "E_STALE_BASE": stale_code}
        report["steps"].append(
            {
                "step": "recovery_and_business_codes",
                "recovery": recovery,
                "state_after_recovery": state_after,
                "product_digest_after_recovery": after_crash_digest,
                "recovery_matches_pre_crash": after_crash_digest == before_crash_digest,
                "aborted_intents": aborted,
                "business_codes": codes,
            }
        )

        checks = {
            "recovery_has_aborted_intent": bool(aborted),
            "recovery_state_is_v2": recovery_state_ok,
            "product_digest_unchanged": after_crash_digest == before_crash_digest,
            "unknown_batch_code": unknown_batch_code == "E_ORDER_NOT_FOUND",
            "stale_base_code": stale_code == "E_STALE_BASE",
        }
        report["verification"] = checks
        report["summary"] = {
            "alerts_before_reduction": reduction_final["totals"]["raw_count"],
            "alerts_after_reduction": reduction_final["totals"]["retained_count"],
            "deduped_count": reduction_final["totals"]["deduped_count"],
            "root_cause_rows": roots_final["row_count"],
            "latest_invalidation_event": invalidations["events"][0]["event_id"],
            "recovered_intents": recovery["recovered_count"],
        }
        report_path.write_text(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        targets_by_artifact = {}
        for target in latest_event["targets"]:
            summary = targets_by_artifact.setdefault(
                target["artifact"], {"services": [], "fingerprints": [], "generation": target["generation"]}
            )
            if target["object_kind"] == "service":
                summary["services"].append(target["object_key"])
            else:
                summary["fingerprints"].append(target["object_key"])
        compact = {
            "verification": checks,
            "summary": {
                **report["summary"],
                "reduction_before_changes": reduction_loaded["totals"],
                "reduction_after_changes": reduction_final["totals"],
            },
            "business_codes": codes,
            "root_cause_candidates": next(
                (
                    view["candidates"]
                    for view in roots_final["alerts"]
                    if view["source"]["service"] == "edge-gateway"
                ),
                [],
            ),
            "latest_invalidation_trace": {
                "event": latest_event["event"],
                "targets_by_artifact": targets_by_artifact,
            },
            "recovery": {
                "state": recovery["state"],
                "recovered_intents": recovery["recovered_intents"],
                "disclaimer": recovery["disclaimer"],
            },
            "report_path": str(report_path),
        }
        print(json.dumps(compact, ensure_ascii=False, sort_keys=True, indent=2))
        return 0 if all(checks.values()) else 1
    finally:
        stop_server(server)


if __name__ == "__main__":
    raise SystemExit(main())
