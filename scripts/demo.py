from __future__ import annotations

import argparse
import json
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from werkzeug.serving import make_server

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.build_fixtures import build_all  # noqa: E402
from slowquery.api import create_app  # noqa: E402
from slowquery.config import WorkbenchConfig  # noqa: E402


DATA_DIR = PROJECT_ROOT / "data"
FAST_SAMPLE = "sq-fast"
FAILURE_SAMPLE = "sq-full-scan"
PRIMARY_SAMPLES = [
    "sq-missing-index",
    "sq-selectivity",
    "sq-lock-wait",
    "sq-or-split",
    "sq-many-rows",
    "sq-full-scan",
    "sq-fast",
]


class DemoOutput:
    def __init__(self) -> None:
        self.lines: list[str] = []

    def write(self, message: str = "") -> None:
        print(message, flush=True)
        self.lines.append(message)

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("\n".join(self.lines) + "\n", encoding="utf-8")


def _fixture(name: str) -> Any:
    return json.loads((DATA_DIR / name).read_text(encoding="utf-8"))


class LocalHttp:
    def __init__(self, config: WorkbenchConfig) -> None:
        self.app = create_app(config)
        self.server = make_server("127.0.0.1", 0, self.app, threaded=True)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_port}"

    def call(self, method: str, path: str, body: Any = None) -> tuple[int, Any]:
        data = None
        headers = {"Accept": "application/json"}
        if body is not None:
            data = json.dumps(body, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = Request(self.base + path, data=data, headers=headers, method=method)
        try:
            with urlopen(request, timeout=15) as response:
                return response.status, json.loads(response.read().decode("utf-8"))
        except HTTPError as error:
            return error.code, json.loads(error.read().decode("utf-8"))

    def stop(self) -> None:
        self.server.shutdown()
        self.thread.join(timeout=5)


def _reset_workdir(workdir: Path) -> None:
    workdir.mkdir(parents=True, exist_ok=True)
    for suffix in ("", "-wal", "-shm"):
        candidate = Path(str(workdir / "workbench.sqlite3") + suffix)
        if candidate.exists():
            candidate.unlink()


def _trace_text(session: dict[str, Any]) -> str:
    states = []
    for event in session.get("trace", []):
        if not states:
            states.append(event["to_status"])
        elif states[-1] != event["to_status"]:
            states.append(event["to_status"])
    return " -> ".join(states)


def _crash(config: WorkbenchConfig, session_id: str, operation: str, point: str) -> int:
    command = [
        sys.executable,
        str(PROJECT_ROOT / "scripts" / "crash_worker.py"),
        "--database",
        str(config.database_path),
        "--sample-db",
        str(config.sample_database_path),
        "--session-id",
        session_id,
        "--operation",
        operation,
        "--crash-point",
        point,
    ]
    completed = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
        timeout=30,
    )
    if completed.stderr.strip():
        print(completed.stderr.strip(), file=sys.stderr)
    return completed.returncode


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the complete local slow-query workbench demo")
    parser.add_argument("--workdir", default=str(PROJECT_ROOT / "artifacts" / "demo"))
    parser.add_argument("--output", default=None)
    args = parser.parse_args(argv)
    workdir = Path(args.workdir).resolve()
    output_path = Path(args.output).resolve() if args.output else workdir / "demo_output.txt"
    output = DemoOutput()
    _reset_workdir(workdir)
    if not (DATA_DIR / "slowquery_sample.sqlite3").exists():
        build_all(DATA_DIR)
    config = WorkbenchConfig(
        database_path=workdir / "workbench.sqlite3",
        sample_database_path=DATA_DIR / "slowquery_sample.sqlite3",
    )

    output.write("SQLite slow-query diagnosis workbench - deterministic local demo")
    output.write(f"database={config.database_path}")
    output.write(f"sample_database={config.sample_database_path}")

    http = LocalHttp(config)
    try:
        status, health = http.call("GET", "/api/health")
        output.write(f"\n[health] HTTP {status} status={health['status']}")
        assert status == 200 and health["status"] == "ok"

        status, samples = http.call("POST", "/api/samples", _fixture("samples.json"))
        output.write(
            f"[samples] HTTP {status} accepted={samples['accepted']} inserted={samples['inserted']}"
        )
        assert status == 201 and samples["inserted"] == 7

        status, snapshot = http.call("POST", "/api/snapshots", _fixture("snapshot_v1.json"))
        output.write(
            f"[snapshot] HTTP {status} active={snapshot['active_version']} "
            f"summary={snapshot['summary']}"
        )
        assert status == 201 and snapshot["active_version"] == 1

        for sample_id in PRIMARY_SAMPLES:
            status, created = http.call(
                "POST",
                "/api/sessions",
                {"session_id": sample_id, "sample_id": sample_id},
            )
            assert status == 201
            status, diagnosed = http.call("POST", f"/api/sessions/{sample_id}/diagnose", {})
            assert status == 200
            session = diagnosed["session"]
            reason_codes = [reason["code"] for reason in session["analysis"]["slow_reasons"]]
            output.write(
                f"\n[session {sample_id}] trace={_trace_text(session)} "
                f"slow={session['is_slow']} snapshot={session['snapshot_version']}"
            )
            output.write(f"  slow_reasons={reason_codes}")
            for suggestion in session["suggestions"]:
                ddl = f" setup={suggestion['setup_sql']}" if suggestion["setup_sql"] else ""
                output.write(
                    f"  suggestion {suggestion['rule_id']} [{suggestion['kind']}] "
                    f"snapshot={suggestion['snapshot_version']}{ddl}"
                )
                output.write(f"    sql={suggestion['rewritten_sql']}")
                output.write(f"    benefit={suggestion['expected_benefit']}")

            if session["suggestions"]:
                status, verification = http.call(
                    "POST", f"/api/sessions/{sample_id}/verify", {}
                )
                if status == 200:
                    record = verification["verification"]
                    output.write(
                        f"  verify=VERIFIED rows={record['original_row_count']}->"
                        f"{record['rewritten_row_count']} "
                        f"rows_equal={record['original_result_digest'] == record['rewritten_result_digest']} "
                        f"cost={record['original_cost']['score']}->{record['rewritten_cost']['score']} "
                        f"cost_improved={record['detail']['cost_improved']}"
                    )
                    output.write(f"  resources={record['detail']['resources']}")
                else:
                    error = verification["error"]
                    output.write(
                        f"  verify=FAILED code={error['code']} "
                        f"rows={error['details']['original_row_count']}->"
                        f"{error['details']['rewritten_row_count']} "
                        f"cost={error['details']['original_cost']['score']}->"
                        f"{error['details']['rewritten_cost']['score']}"
                    )
                    assert sample_id == FAILURE_SAMPLE and error["code"] == "E_VERIFY_MISMATCH"

        status, final_failure = http.call("GET", f"/api/sessions/{FAILURE_SAMPLE}")
        assert status == 200
        output.write(
            f"\n[failed-session] {FAILURE_SAMPLE} trace={_trace_text(final_failure)} "
            f"verification={final_failure['verifications'][0]['outcome']}"
        )
        assert final_failure["status"] == "FAILED"

        http.call("POST", "/api/sessions", {"session_id": "crash-analysis", "sample_id": "sq-missing-index"})
        http.call("POST", "/api/sessions", {"session_id": "crash-verify", "sample_id": "sq-missing-index"})
        status, _ = http.call("POST", "/api/sessions/crash-verify/diagnose", {})
        assert status == 200

        status, retired = http.call("POST", "/api/snapshots", _fixture("snapshot_v2.json"))
        output.write(f"\n[snapshot replacement] HTTP {status} active={retired['active_version']}")
        assert status == 201
        http.call("POST", "/api/sessions", {"session_id": "retired-check", "sample_id": "sq-many-rows"})
        status, retired_error = http.call(
            "POST", "/api/sessions/retired-check/diagnose", {"snapshot_version": 1}
        )
        output.write(
            f"[snapshot retirement] HTTP {status} code={retired_error['error']['code']} "
            f"active={retired_error['error']['details']['active_snapshot_version']}"
        )
        assert status == 409 and retired_error["error"]["code"] == "E_SNAPSHOT_RETIRED"

        status, old_session = http.call("GET", "/api/sessions/sq-missing-index")
        output.write(
            f"[bound-session] status={old_session['status']} "
            f"snapshot={old_session['snapshot_version']} "
            f"suggestion_snapshot={old_session['suggestions'][0]['snapshot_version']}"
        )
        assert old_session["snapshot_version"] == 1
        assert old_session["suggestions"][0]["snapshot_version"] == 1
    finally:
        http.stop()

    output.write("\n[forced termination] stopping HTTP server, then killing two worker processes...")
    analysis_exit = _crash(config, "crash-analysis", "diagnose", "ANALYZE_AFTER_ANALYZING")
    verify_exit = _crash(config, "crash-verify", "verify", "VERIFY_AFTER_ANALYZING")
    output.write(f"  analyze worker exit={analysis_exit} (86 means deterministic os._exit)")
    output.write(f"  verify  worker exit={verify_exit} (86 means deterministic os._exit)")
    assert analysis_exit == 86 and verify_exit == 86

    restarted_http = LocalHttp(config)
    try:
        status, analysis_session = restarted_http.call("GET", "/api/sessions/crash-analysis")
        assert status == 200
        output.write(
            f"[recovery] crash-analysis status={analysis_session['status']} "
            f"error={analysis_session['last_error_code']} suggestions={len(analysis_session['suggestions'])}"
        )
        status, verify_session = restarted_http.call("GET", "/api/sessions/crash-verify")
        assert status == 200
        output.write(
            f"[recovery] crash-verify status={verify_session['status']} "
            f"error={verify_session['last_error_code']} verifications={len(verify_session['verifications'])}"
        )
        status, report = restarted_http.call("GET", "/api/recovery/report")
        assert status == 200
        payload = report["payload"]
        output.write(f"[recovery-report] report_id={report['report_id']} facts_only={payload['facts_only']}")
        for recovery in payload["recoveries"]:
            output.write(
                f"  {recovery['kind']} previous={recovery['previous_status']} "
                f"action={recovery['action']} final={recovery['final_status']}"
            )
        output.write(f"  guarantees={payload['guarantees']}")
        output.write(f"  invariants={payload['facts']['invariants']}")

        assert analysis_session["status"] == "CREATED"
        assert analysis_session["suggestions"] == []
        assert verify_session["status"] == "FAILED"
        assert verify_session["verifications"] == []
        assert payload["guarantees"]["interrupted_verification_marked_verified"] is False
        assert payload["facts"]["invariants"]["suggested_without_suggestions"] == 0
        assert payload["facts"]["invariants"]["verified_without_verification_record"] == 0
        assert payload["facts"]["invariants"]["pending_intents_remaining"] == 0
    finally:
        restarted_http.stop()

    output.write("\nDEMO PASS")
    output.write(f"output={output_path}")
    output.save(output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
