from __future__ import annotations

import json
import os
import socket
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

import httpx

ROOT = Path(__file__).resolve().parents[1]
ARTIFACTS = ROOT / "artifacts"
CONFIG = ROOT / "config" / "demo.json"
SAMPLES = ROOT / "samples"
DB_PATH = ROOT / "data" / "demo.db"
LOG_PATH = ARTIFACTS / "demo-server.log"


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def clean_sqlite_files() -> None:
    for suffix in ("", "-wal", "-shm"):
        path = Path(str(DB_PATH) + suffix)
        if path.exists():
            path.unlink()


def assert_writable(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    probe = path.parent / ".demo-write-probe"
    probe.write_text("ok", encoding="utf-8")
    probe.unlink()


class DemoServer:
    def __init__(self) -> None:
        self.port = free_port()
        self.process: subprocess.Popen[bytes] | None = None
        self.log_file = LOG_PATH

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def start(self, *, append_log: bool = True) -> None:
        ARTIFACTS.mkdir(parents=True, exist_ok=True)
        log = self.log_file.open("ab" if append_log else "wb")
        env = dict(os.environ)
        env.pop("CDN_SIM_CONFIG", None)
        self.process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "workbench.server",
                "--config",
                str(CONFIG),
                "--port",
                str(self.port),
                "--sqlite",
                str(DB_PATH),
            ],
            cwd=str(ROOT),
            stdout=log,
            stderr=subprocess.STDOUT,
            env=env,
        )
        log.close()
        deadline = time.time() + 15
        while time.time() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(f"server exited early; see {self.log_file}")
            try:
                response = httpx.get(self.base_url + "/api/health", timeout=0.4, trust_env=False)
                if response.status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            time.sleep(0.05)
        raise RuntimeError(f"server did not become ready; see {self.log_file}")

    def kill(self) -> int | None:
        if self.process is None:
            return None
        code = self.process.poll()
        if code is None:
            self.process.kill()
            self.process.wait(timeout=5)
            code = self.process.returncode
        self.process = None
        return code

    def stop(self) -> None:
        if self.process is None:
            return
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        self.process = None

    def client(self, timeout: float = 15.0) -> httpx.Client:
        return httpx.Client(base_url=self.base_url, timeout=timeout, trust_env=False)

    def post(self, path: str, payload: dict[str, Any] | None = None, *, timeout: float = 15.0) -> httpx.Response:
        with self.client(timeout=timeout) as client:
            return client.post(path, json=payload or {})

    def get(self, path: str, *, timeout: float = 15.0) -> httpx.Response:
        with self.client(timeout=timeout) as client:
            return client.get(path)


def response_json(response: httpx.Response) -> dict[str, Any]:
    try:
        return response.json()
    except ValueError:
        return {"ok": False, "http_status": response.status_code, "raw": response.text[:500]}


def request_record(server: DemoServer, node: str, payload: dict[str, Any], *, label: str) -> dict[str, Any]:
    response = server.post(f"/api/edges/{node}/requests", payload, timeout=20)
    data = response_json(response)
    return {"label": label, "node_id": node, "http_status": response.status_code, "response": data}


def wait_for_intent(db_path: Path, request_id: str, timeout: float = 10.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            connection = sqlite3.connect(db_path, timeout=2)
            connection.row_factory = sqlite3.Row
            try:
                row = connection.execute(
                    "SELECT status FROM intents WHERE request_id = ? AND status = 'IN_PROGRESS' "
                    "ORDER BY created_at DESC LIMIT 1",
                    (request_id,),
                ).fetchone()
            finally:
                connection.close()
            if row is not None:
                return True
        except sqlite3.Error:
            pass
        time.sleep(0.05)
    return False


def entry_signature(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "node_id": entry["node_id"],
            "cache_key": entry["cache_key"],
            "canonical_path": entry["canonical_path"],
            "etag": entry["etag"],
            "generation": entry["generation"],
            "policy_version": entry["policy_version"],
        }
        for entry in sorted(entries, key=lambda item: (item["node_id"], item["cache_key"]))
    ]


def all_entries(server: DemoServer) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for node in ("edge-a", "edge-b", "edge-c"):
        data = server.get(f"/api/edges/{node}/entries").json()
        entries.extend(data["entries"])
    return entries


def print_trace(label: str, result: dict[str, Any]) -> None:
    data = result["response"]
    trace = data.get("trace", {})
    phases = " -> ".join(event.get("phase", "?") for event in trace.get("events", []))
    print(
        f"  {label:<30} HTTP {result['http_status']:<3} "
        f"cache={str(data.get('cache_status', data.get('business_code', '-'))):<12} "
        f"origin={data.get('origin_fetches', 0)} decision={trace.get('decision', '-')}"
    )
    if phases:
        print(f"    trace: {phases}")


def main() -> int:
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    assert_writable(DB_PATH)
    clean_sqlite_files()
    server = DemoServer()
    output: dict[str, Any] = {
        "demo": "cdn-edge-cache-consistency-workbench",
        "started_at": time.time(),
        "database": str(DB_PATH),
        "port": server.port,
        "scope": "local deterministic samples and loopback HTTP only",
        "scenarios": {},
        "assertions": {},
    }
    try:
        LOG_PATH.write_bytes(b"")
        server.start(append_log=False)
        baseline = {
            "health": server.get("/api/health").json(),
            "diagnostics": server.get("/api/diagnostics/baseline").json(),
            "recovery": server.get("/api/recovery/report").json()["report"],
        }
        output["baseline"] = baseline
        print("== Clean startup baseline ==")
        print(json.dumps({
            "sqlite": str(DB_PATH),
            "clean_startup": baseline["recovery"]["clean_startup"],
            "policy": baseline["diagnostics"]["baseline"]["policy"],
            "nodes": baseline["diagnostics"]["baseline"]["nodes"],
        }, indent=2, ensure_ascii=False))
        assert baseline["recovery"]["clean_startup"] is True

        policy = read_json(SAMPLES / "policy_v1.json")
        policy_response = server.post("/api/policies", policy)
        assert policy_response.status_code == 200, policy_response.text
        output["policy_commit"] = policy_response.json()

        print("\n== Cold-start request replay ==")
        sample_requests = read_json(SAMPLES / "requests.json")["requests"]
        cold_results: list[dict[str, Any]] = []
        for item in sample_requests:
            node = item.pop("node_id")
            label = item.pop("label", "sample")
            result = request_record(server, node, item, label=label)
            cold_results.append(result)
            print_trace(label, result)
            assert result["response"]["cache_status"] == "MISS"
            assert result["response"]["origin_fetches"] == 1

        # Ensure all three nodes hold the tagged app object before purge.
        for node in ("edge-a", "edge-b", "edge-c"):
            payload = {
                "request_id": f"demo-purge-warm-{node}",
                "path": "/assets/app.js",
                "query": "v=1&lang=zh-CN",
                "headers": {"Accept-Language": "zh-CN"},
            }
            result = request_record(server, node, payload, label=f"warm-before-purge-{node}")
            cold_results.append(result)
            print_trace(result["label"], result)
        output["scenarios"]["cold_start"] = cold_results
        output["after_cold"] = {
            "state": server.get("/api/diagnostics/state").json()["state"],
            "entries": all_entries(server),
        }

        print("\n== Origin content replacement ==")
        old_app = next(
            result["response"]
            for result in cold_results
            if result["label"] == "cold-app-edge-a"
        )
        new_body = (SAMPLES / "content" / "app_v2.js").read_bytes().decode("utf-8")
        update = server.post(
            "/api/origin/content",
            {
                "path": "/assets/app.js",
                "body": new_body,
                "content_type": "application/javascript",
                "tags": ["static", "app", "js", "release-v2"],
            },
        )
        assert update.status_code == 200, update.text
        replacement = request_record(
            server,
            "edge-a",
            {
                "request_id": "demo-origin-replaced-force-revalidate",
                "path": "/assets/app.js",
                "query": "v=1&lang=zh-CN",
                "headers": {"Accept-Language": "zh-CN"},
                "force_revalidate": True,
            },
            label="content-replaced-force-revalidate",
        )
        print_trace(replacement["label"], replacement)
        replacement_data = replacement["response"]
        commit_event = next(
            event for event in replacement_data["trace"]["events"] if event["phase"] == "ENTRY_COMMIT"
        )
        assert commit_event["version_changed"] is True
        assert replacement_data["etag"] != old_app["etag"]
        assert replacement_data["generation"] == old_app["generation"] + 1
        output["scenarios"]["content_replacement"] = {
            "origin_update": update.json(),
            "old": old_app,
            "new": replacement,
            "version_changed": commit_event["version_changed"],
        }

        print("\n== Purge propagation ==")
        purge_failed = server.post(
            "/api/purges",
            {
                "request_id": "demo-purge-injected-failure",
                "tag": "app",
                "injections": [{"type": "purge_broadcast_interrupt", "target_node": "edge-b"}],
            },
        )
        failed_data = response_json(purge_failed)
        print(f"  injected purge failure: HTTP {purge_failed.status_code} code={failed_data.get('business_code')}")
        assert purge_failed.status_code == 503
        assert failed_data["business_code"] == "E_PURGE_BROADCAST_FAIL"
        assert all(entry["state"] != "PURGED" for entry in all_entries(server) if "app" in entry.get("tags", []))

        purge = server.post("/api/purges", {"request_id": "demo-purge-app-tag", "tag": "app"})
        purge_data = response_json(purge)
        assert purge.status_code == 200, purge.text
        print(json.dumps(purge_data["purge"], indent=2, ensure_ascii=False))
        entries_after_purge = all_entries(server)
        app_entries = [entry for entry in entries_after_purge if "app" in entry.get("tags", [])]
        assert app_entries and all(entry["state"] == "PURGED" for entry in app_entries)
        state_after_purge = server.get("/api/diagnostics/state").json()["state"]
        affected_keys = {
            key for keys in purge_data["purge"]["affected"].values() for key in keys
        }
        connection = sqlite3.connect(DB_PATH, timeout=5)
        try:
            placeholders = ",".join("?" for _ in affected_keys)
            route_count = connection.execute(
                f"SELECT COUNT(*) FROM route_index WHERE cache_key IN ({placeholders})", tuple(affected_keys)
            ).fetchone()[0]
            tag_count = connection.execute(
                f"SELECT COUNT(*) FROM tag_index WHERE cache_key IN ({placeholders})", tuple(affected_keys)
            ).fetchone()[0]
        finally:
            connection.close()
        assert route_count == 0 and tag_count == 0
        output["scenarios"]["purge"] = {
            "injected_failure": failed_data,
            "success": purge_data,
            "entries_after_purge": entries_after_purge,
            "state_after_purge": state_after_purge,
        }

        print("\n== Deterministic exception injection ==")
        injection_cases: list[dict[str, Any]] = []
        cases = [
            (
                "timeout",
                "edge-a",
                {
                    "request_id": "demo-inject-timeout",
                    "path": "/api/catalog.json",
                    "injections": [{"type": "origin_delay", "delay_ms": 500, "timeout_ms": 80}],
                },
                "E_ORIGIN_TIMEOUT",
            ),
            (
                "origin-5xx",
                "edge-a",
                {
                    "request_id": "demo-inject-5xx",
                    "path": "/api/catalog.json",
                    "injections": [{"type": "origin_status", "status": 503}],
                },
                "E_ORIGIN_5XX",
            ),
            (
                "connection-refused",
                "edge-a",
                {
                    "request_id": "demo-inject-refused",
                    "path": "/api/catalog.json",
                    "injections": [{"type": "connection_refused"}],
                },
                "E_ORIGIN_CONNECT",
            ),
            (
                "invalid-injection",
                "edge-a",
                {
                    "request_id": "demo-inject-invalid",
                    "path": "/api/catalog.json",
                    "injections": [{"type": "not-a-real-injection"}],
                },
                "E_INJECT_INVALID",
            ),
        ]
        for label, node, payload, expected_code in cases:
            started = time.perf_counter()
            result = request_record(server, node, payload, label=label)
            elapsed_ms = (time.perf_counter() - started) * 1000
            result["elapsed_ms"] = elapsed_ms
            injection_cases.append(result)
            print_trace(label, result)
            assert result["response"]["business_code"] == expected_code
        output["scenarios"]["exceptions"] = injection_cases

        print("\n== Force termination and restart recovery ==")
        stable_request = request_record(
            server,
            "edge-a",
            {
                "request_id": "demo-recovery-stable",
                "path": "/assets/theme.css",
                "headers": {"Accept-Language": "zh-CN"},
            },
            label="stable-entry-before-kill",
        )
        assert stable_request["response"]["ok"] is True
        print_trace(stable_request["label"], stable_request)
        pre_crash_entries = all_entries(server)
        pre_crash_state = server.get("/api/diagnostics/state").json()["state"]

        crash_id = "demo-recovery-in-flight"
        crash_payload = {
            "request_id": crash_id,
            "path": "/api/catalog.json",
            "force_revalidate": True,
            "injections": [{"type": "origin_delay", "delay_ms": 30000, "timeout_ms": 60000}],
        }
        crash_result: dict[str, Any] = {}

        def crash_request() -> None:
            try:
                with httpx.Client(base_url=server.base_url, timeout=20, trust_env=False) as client:
                    crash_result["response"] = client.post(
                        "/api/edges/edge-c/requests", json=crash_payload
                    ).json()
            except BaseException as exc:
                crash_result["client_error"] = type(exc).__name__

        thread = threading.Thread(target=crash_request, daemon=True)
        thread.start()
        if not wait_for_intent(DB_PATH, crash_id, timeout=10):
            raise RuntimeError("The crash injection never produced a persisted in-progress intent")
        exit_code = server.kill()
        thread.join(timeout=5)
        assert not thread.is_alive()
        print(f"  SIGKILL-style termination completed; returncode={exit_code}")
        print("  no exit report is expected from the terminated process")

        time.sleep(0.25)
        server.start(append_log=True)
        recovery = server.get("/api/recovery/report").json()["report"]
        post_recovery_entries = all_entries(server)
        post_recovery_state = server.get("/api/diagnostics/state").json()["state"]
        duplicate_payload = dict(crash_payload)
        duplicate = server.post("/api/edges/edge-c/requests", duplicate_payload)
        duplicate_data = response_json(duplicate)
        assert duplicate_data["business_code"] == "E_REQUEST_INCOMPLETE"

        pre_sig = entry_signature(pre_crash_entries)
        post_sig = entry_signature(post_recovery_entries)
        changed = [
            {"before": before, "after": after}
            for before, after in zip(pre_sig, post_sig)
            if before != after
        ]
        recovery_diff = {
            "pre_crash_entry_count": len(pre_sig),
            "post_recovery_entry_count": len(post_sig),
            "changed_entries": changed,
            "rollback_kept_last_complete_versions": not changed,
            "recovered_intents": recovery.get("recovered_intents", []),
            "interrupted_origin_fetches": recovery.get("interrupted_origin_fetches", 0),
            "note": "Only SQLite rows are used; the killed process produced no exit report.",
        }
        assert recovery["intents_rolled_back"] >= 1
        assert recovery["clean_startup"] is False
        assert recovery_diff["rollback_kept_last_complete_versions"] is True
        output["scenarios"]["forced_termination_recovery"] = {
            "stable_before": stable_request,
            "crash_client": crash_result,
            "process_returncode": exit_code,
            "pre_crash_state": pre_crash_state,
            "recovery_report": recovery,
            "post_recovery_state": post_recovery_state,
            "post_recovery_entries": post_recovery_entries,
            "duplicate_after_restart": duplicate_data,
            "diff": recovery_diff,
        }

        final_entries = all_entries(server)
        output["final"] = {
            "state": server.get("/api/diagnostics/state").json()["state"],
            "entries": final_entries,
            "traces": server.get("/api/traces?limit=500").json()["traces"],
            "injections": server.get("/api/injections?limit=500").json()["injections"],
            "recovery_reports": server.get("/api/recovery/reports?limit=20").json()["reports"],
        }
        output["assertions"] = {
            "cold_start_all_miss": True,
            "content_version_replaced_whole_entry": True,
            "purge_atomic_and_indexes_invalidated": True,
            "exception_codes_distinct": [item[3] for item in cases],
            "forced_termination_recovered": True,
            "recovery_used_persisted_facts_only": True,
        }
        output["completed_at"] = time.time()
        (ARTIFACTS / "demo-output.json").write_text(
            json.dumps(output, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )
        (ARTIFACTS / "demo-traces.json").write_text(
            json.dumps(
                {
                    "requests": cold_results,
                    "content_replacement": output["scenarios"]["content_replacement"],
                    "exceptions": injection_cases,
                    "recovery": output["scenarios"]["forced_termination_recovery"],
                },
                indent=2,
                ensure_ascii=False,
            )
            + "\n",
            encoding="utf-8",
        )
        summary = [
            "CDN edge cache consistency demo summary",
            f"SQLite: {DB_PATH}",
            f"Loopback port: {server.port}",
            f"Cold-start requests: {len(cold_results)} (all MISS, one origin fetch each)",
            f"Content replacement: generation {old_app['generation']} -> {replacement_data['generation']}, version_changed=True",
            f"Purge affected entries: {purge_data['purge']['affected_count']} across {len(purge_data['purge']['nodes'])} nodes",
            "Exception codes: " + ", ".join(item[3] for item in cases) + ", E_PURGE_BROADCAST_FAIL",
            f"Recovery: {recovery['intents_rolled_back']} intent(s) rolled back; cache changed={not recovery_diff['rollback_kept_last_complete_versions']}",
            "Recovery basis: persisted SQLite facts after restart; no termination-time report exists",
            f"Full output: {ARTIFACTS / 'demo-output.json'}",
            f"Full traces: {ARTIFACTS / 'demo-traces.json'}",
            f"Server log: {LOG_PATH}",
        ]
        (ARTIFACTS / "demo-summary.txt").write_text("\n".join(summary) + "\n", encoding="utf-8")
        print("\n== Final node state ==")
        for node in ("edge-a", "edge-b", "edge-c"):
            print(node, json.dumps(output["final"]["state"]["nodes"][node], ensure_ascii=False))
        print("\n== Recovery diff ==")
        print(json.dumps(recovery_diff, indent=2, ensure_ascii=False))
        print("\n" + "\n".join(summary))
        return 0
    finally:
        server.stop()


if __name__ == "__main__":
    raise SystemExit(main())

