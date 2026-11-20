"""Deterministic end-to-end demo, including a real forced child-process exit."""

from __future__ import annotations

import json
import socket
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

from .expressions import canonical_json
from .sample import PROJECT_ROOT, generate_sample_database, load_manifest


DEFAULT_OUTPUT = PROJECT_ROOT / "artifacts" / "demo_report.json"
DEFAULT_WORK_DIR = PROJECT_ROOT / "runtime" / "demo"


def run_demo(
    *,
    output: str | Path | None = None,
    work_dir: str | Path = DEFAULT_WORK_DIR,
) -> Path:
    output_path = Path(output) if output is not None else DEFAULT_OUTPUT
    work = Path(work_dir)
    work.mkdir(parents=True, exist_ok=True)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    sample_db = work / "sample.sqlite3"
    store_db = work / "plans.sqlite3"
    exit_summary_path = work / "exit-summary.json"
    for suffix in ("", "-wal", "-shm"):
        target = Path(str(store_db) + suffix)
        if target.exists():
            target.unlink()
    if exit_summary_path.exists():
        exit_summary_path.unlink()
    generate_sample_database(sample_db)

    server_process: subprocess.Popen[str] | None = None
    port = _free_port()
    try:
        server_process = _start_server(port, sample_db, store_db, exit_summary_path)
        base_url = f"http://127.0.0.1:{port}"
        _wait_until_healthy(server_process, base_url)

        manifest = load_manifest()
        normal_entries = [entry for entry in manifest if entry["expected_code"] == "OK"]
        normal_cases: list[dict[str, Any]] = []
        committed_by_id: dict[str, dict[str, Any]] = {}
        for entry in normal_entries:
            status, payload = _request_json(
                "POST",
                base_url + "/v1/plans",
                {"sql": entry["sql"], "request_id": "demo_" + entry["id"]},
            )
            if status != 200:
                raise RuntimeError(f"normal case {entry['id']} failed: {status} {payload}")
            committed_by_id[entry["id"]] = payload
            normal_cases.append(
                {
                    "id": entry["id"],
                    "description": entry["description"],
                    "request_id": payload["request_id"],
                    "business_code": payload["business_code"],
                    "version_id": payload["version_id"],
                    "initial_node_count": payload["initial_node_count"],
                    "node_count": payload["node_count"],
                    "normalized_plan": payload["normalized_plan"],
                    "plan_text": payload["plan_text"],
                    "node_summary": payload["node_summary"],
                    "rewrite_log": payload["rewrite_log"],
                    "rejections": payload["rejections"],
                    "node_mapping": payload["node_mapping"],
                    "rule_applications": payload["rule_applications"],
                    "rule_rejections": payload["rule_rejections"],
                    "semantic_equivalence": payload["semantic_equivalence"],
                    "query_result": payload["query_result"],
                    "after_fingerprint": payload["after_fingerprint"],
                }
            )

        injection_specs = [
            ("malformed_sql", {"kind": "malformed_sql"}),
            ("plan_too_large", {"kind": "plan_too_large", "limit": 1}),
            ("invalid_aggregate", {"kind": "invalid_aggregate"}),
            ("rewrite_aborted", {"kind": "rewrite_aborted", "abort_after": 0}),
        ]
        injection_cases: list[dict[str, Any]] = []
        for kind, injection in injection_specs:
            status, payload = _request_json(
                "POST",
                base_url + "/v1/plans",
                {
                    "sql": normal_entries[0]["sql"],
                    "request_id": "demo_inject_" + kind,
                    "inject": injection,
                },
            )
            injection_cases.append(
                {
                    "kind": kind,
                    "http_status": status,
                    "business_code": payload.get("business_code"),
                    "message": payload.get("message"),
                    "context": payload.get("context"),
                    "request_id": payload.get("request_id"),
                }
            )

        invalid_entry = next(entry for entry in manifest if entry["expected_code"] == "E_INVALID_AGGREGATE")
        invalid_status, invalid_payload = _request_json(
            "POST",
            base_url + "/v1/plans",
            {"sql": invalid_entry["sql"], "request_id": "demo_natural_invalid_aggregate"},
        )
        natural_aggregate_case = {
            "id": invalid_entry["id"],
            "http_status": invalid_status,
            "business_code": invalid_payload.get("business_code"),
            "context": invalid_payload.get("context"),
        }

        concurrent_sql = (
            "SELECT id, name FROM employees WHERE active = 1 "
            "ORDER BY id LIMIT 3"
        )
        concurrent_ids = [f"demo_concurrent_{index}" for index in range(6)]
        with ThreadPoolExecutor(max_workers=6) as pool:
            futures = [
                pool.submit(
                    _request_json,
                    "POST",
                    base_url + "/v1/plans",
                    {"sql": concurrent_sql, "request_id": request_id},
                )
                for request_id in concurrent_ids
            ]
            concurrent_responses = [future.result() for future in futures]
        concurrent_payloads = [payload for status, payload in concurrent_responses if status == 200]
        if len(concurrent_payloads) != len(concurrent_ids):
            raise RuntimeError(f"concurrent submissions failed: {concurrent_responses}")
        concurrency_case = {
            "sql": concurrent_sql,
            "request_ids": sorted(concurrent_ids),
            "all_http_200": True,
            "executed_count": sum(1 for payload in concurrent_payloads if not payload["reused"]),
            "reused_count": sum(1 for payload in concurrent_payloads if payload["reused"]),
            "reuse_kinds": sorted(str(payload["reuse_kind"]) for payload in concurrent_payloads),
            "version_ids": sorted({int(payload["version_id"]) for payload in concurrent_payloads}),
            "all_plans_equal": len({payload["normalized_plan"] for payload in concurrent_payloads}) == 1,
        }

        repeat_sql = "SELECT name FROM employees WHERE active = 1 ORDER BY name LIMIT 2"
        repeat_first_status, repeat_first = _request_json(
            "POST",
            base_url + "/v1/plans",
            {"sql": repeat_sql, "request_id": "demo_repeat_request"},
        )
        repeat_second_status, repeat_second = _request_json(
            "POST",
            base_url + "/v1/plans",
            {"sql": repeat_sql, "request_id": "demo_repeat_request"},
        )
        repeat_case = {
            "request_id": "demo_repeat_request",
            "first_status": repeat_first_status,
            "second_status": repeat_second_status,
            "same_version": repeat_first.get("version_id") == repeat_second.get("version_id"),
            "second_reused": repeat_second.get("reused"),
            "second_reuse_kind": repeat_second.get("reuse_kind"),
            "same_normalized_plan": repeat_first.get("normalized_plan") == repeat_second.get("normalized_plan"),
        }

        versions_before_crash = _store_counts(store_db)["versions"]
        crash_request_id = "demo_forced_crash"
        crash_status, crash_payload = _request_json(
            "POST",
            base_url + "/v1/plans",
            {
                "sql": "SELECT COUNT(*) AS active_count FROM employees WHERE active = 1",
                "request_id": crash_request_id,
                "inject": {"kind": "crash_after_intent"},
            },
            timeout=5.0,
        )
        crash_exit_code = _wait_process(server_process, timeout=10.0)
        server_process = None
        if crash_exit_code != 70:
            raise RuntimeError(f"forced crash produced exit code {crash_exit_code}, expected 70")

        before_facts = _crash_facts(store_db, crash_request_id, exit_summary_path)
        if before_facts["intent_status"] != "pending":
            raise RuntimeError(f"crash intent was not left pending: {before_facts}")
        if before_facts["request_row_exists"]:
            raise RuntimeError("crash request became visible before recovery")
        if before_facts["exit_summary_exists"]:
            raise RuntimeError("forced termination unexpectedly produced a complete exit report")

        server_process = _start_server(port, sample_db, store_db, exit_summary_path)
        base_url = f"http://127.0.0.1:{port}"
        _wait_until_healthy(server_process, base_url)
        recovery_status, recovery_payload = _request_json(
            "GET", base_url + "/v1/recovery-report"
        )
        if recovery_status != 200:
            raise RuntimeError(f"recovery report unavailable: {recovery_status} {recovery_payload}")
        recovery_report = recovery_payload["recovery_report"]
        recovery_report.pop("persisted_at", None)
        intent_status, intent_payload = _request_json(
            "GET", base_url + "/v1/intents?request_id=" + crash_request_id
        )
        deterministic_intents = [
            {
                key: value
                for key, value in intent.items()
                if key not in {"created_at", "updated_at"}
            }
            for intent in intent_payload.get("intents", [])
        ]
        crash_lookup_status, crash_lookup_payload = _request_json(
            "GET", base_url + "/v1/plans/" + crash_request_id
        )
        prior_request_id = committed_by_id[normal_entries[0]["id"]]["request_id"]
        prior_status, prior_payload = _request_json(
            "GET", base_url + "/v1/plans/" + prior_request_id
        )
        versions_after_recovery = _store_counts(store_db)["versions"]
        recovery_case = {
            "crash_request_id": crash_request_id,
            "crash_transport_status": crash_status,
            "crash_transport_payload": crash_payload,
            "forced_exit_code": crash_exit_code,
            "before_recovery": before_facts,
            "after_recovery_report": recovery_report,
            "intent_after_recovery": deterministic_intents,
            "crash_request_lookup_status": crash_lookup_status,
            "crash_request_lookup_business_code": crash_lookup_payload.get("business_code"),
            "versions_before_crash": versions_before_crash,
            "versions_after_recovery": versions_after_recovery,
            "versions_unchanged": versions_before_crash == versions_after_recovery,
            "previously_committed_plan_status": prior_status,
            "previously_committed_plan_unchanged": (
                prior_status == 200
                and prior_payload.get("after_fingerprint")
                == committed_by_id[normal_entries[0]["id"]]["after_fingerprint"]
            ),
            "complete_exit_report_available_after_forced_exit": before_facts["exit_summary_exists"],
        }

        shutdown_status, shutdown_payload = _request_json(
            "POST", base_url + "/v1/admin/shutdown", {"confirm": True}
        )
        safe_exit_code = _wait_process(server_process, timeout=10.0)
        server_process = None
        safe_exit_summary = None
        if exit_summary_path.exists():
            safe_exit_summary = json.loads(exit_summary_path.read_text(encoding="utf-8"))
        recovery_case["safe_shutdown"] = {
            "http_status": shutdown_status,
            "response": shutdown_payload,
            "process_exit_code": safe_exit_code,
            "exit_summary_written": safe_exit_summary is not None,
            "exit_summary": safe_exit_summary,
        }

        observed_codes = {"OK"}
        observed_codes.update(case["business_code"] for case in injection_cases)
        observed_codes.add(natural_aggregate_case["business_code"])
        observed_codes.add(crash_lookup_payload.get("business_code"))
        report = {
            "schema_version": 1,
            "reproducible": True,
            "environment": {
                "python": sys.version.split()[0],
                "core_dependencies": "Python standard library only",
                "sqlite3": sqlite3.sqlite_version,
                "http_server": "http.server.ThreadingHTTPServer",
                "sample_db": _relative(sample_db),
                "store_db": _relative(store_db),
            },
            "manifest": manifest,
            "normal_cases": normal_cases,
            "injection_cases": injection_cases,
            "natural_invalid_aggregate_case": natural_aggregate_case,
            "concurrency": concurrency_case,
            "same_request_repeat": repeat_case,
            "recovery": recovery_case,
            "business_codes": sorted(code for code in observed_codes if code),
            "forced_termination_policy": {
                "complete_exit_report_promised": False,
                "startup_recovery_source": "persisted intent/version facts",
                "graceful_exit_summary_only": True,
            },
        }
        temp_output = output_path.with_suffix(output_path.suffix + ".tmp")
        temp_output.write_text(
            json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temp_output.replace(output_path)
        _print_demo_summary(report, output_path)
        return output_path
    finally:
        if server_process is not None and server_process.poll() is None:
            server_process.terminate()
            try:
                server_process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                server_process.kill()
                server_process.wait(timeout=3)


def _start_server(
    port: int, sample_db: Path, store_db: Path, exit_summary_path: Path
) -> subprocess.Popen[str]:
    command = [
        sys.executable,
        "-m",
        "sqlplan_engine",
        "serve",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--sample-db",
        str(sample_db),
        "--store-db",
        str(store_db),
        "--exit-summary",
        str(exit_summary_path),
    ]
    return subprocess.Popen(
        command,
        cwd=str(PROJECT_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _wait_until_healthy(process: subprocess.Popen[str], base_url: str) -> None:
    deadline = time.monotonic() + 12.0
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stdout, stderr = process.communicate(timeout=2)
            raise RuntimeError(
                f"server exited during startup ({process.returncode})\nstdout={stdout}\nstderr={stderr}"
            )
        try:
            with urllib.request.urlopen(base_url + "/health", timeout=0.5) as response:
                if response.status == 200:
                    return
        except (urllib.error.URLError, ConnectionError, TimeoutError):
            time.sleep(0.1)
    raise RuntimeError("server did not become healthy within 12 seconds")


def _request_json(
    method: str,
    url: str,
    body: dict[str, Any] | None = None,
    *,
    timeout: float = 8.0,
) -> tuple[int, dict[str, Any]]:
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        try:
            raw = exc.read().decode("utf-8", errors="replace")
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                payload = {"business_code": "E_TRANSPORT", "raw": raw}
            return exc.code, payload
        finally:
            exc.close()
    except (urllib.error.URLError, ConnectionError, TimeoutError, OSError) as exc:
        return 0, {"business_code": "E_TRANSPORT", "detail": type(exc).__name__}


def _wait_process(process: subprocess.Popen[str], *, timeout: float) -> int:
    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError("child process did not exit in time") from exc
    try:
        process.communicate(timeout=1)
    except (subprocess.TimeoutExpired, ValueError):
        pass
    return int(process.returncode)


def _store_counts(store_db: Path) -> dict[str, int]:
    connection = sqlite3.connect(store_db)
    try:
        tables = ("intents", "versions", "requests", "statement_runs", "recovery_reports")
        return {
            table: int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            for table in tables
        }
    finally:
        connection.close()


def _crash_facts(store_db: Path, request_id: str, exit_summary: Path) -> dict[str, Any]:
    connection = sqlite3.connect(store_db)
    try:
        intent = connection.execute(
            "SELECT intent_id, status FROM intents WHERE request_id=? ORDER BY intent_seq DESC LIMIT 1",
            (request_id,),
        ).fetchone()
        request_row = connection.execute(
            "SELECT request_id, version_id FROM requests WHERE request_id=?", (request_id,)
        ).fetchone()
        version_count = int(connection.execute("SELECT COUNT(*) FROM versions").fetchone()[0])
        return {
            "intent_id": intent[0] if intent else None,
            "intent_status": intent[1] if intent else None,
            "request_row_exists": request_row is not None,
            "last_complete_version": request_row[1] if request_row else None,
            "version_count": version_count,
            "exit_summary_exists": exit_summary.exists(),
        }
    finally:
        connection.close()


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _relative(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path)


def _print_demo_summary(report: dict[str, Any], output: Path) -> None:
    print("本地 SQL 逻辑计划引擎端到端演练")
    print(f"- 固定语句: {len(report['normal_cases'])} 条正常计划")
    for case in report["normal_cases"]:
        print(
            f"  [{case['id']}] {case['business_code']} nodes={case['node_count']} "
            f"rules={case['rule_applications']} rejected={case['rule_rejections']} "
            f"等价={case['semantic_equivalence']['original_plan_equals_normalized_plan_result']}"
        )
        print(case["plan_text"])
    print("- 注入业务码:")
    for case in report["injection_cases"]:
        print(
            f"  {case['kind']}: HTTP {case['http_status']} {case['business_code']} "
            f"kind={case['context'].get('injected') if case['context'] else None}"
        )
    concurrency = report["concurrency"]
    print(
        f"- 并发同语句: executed={concurrency['executed_count']} "
        f"reused={concurrency['reused_count']} versions={concurrency['version_ids']}"
    )
    recovery = report["recovery"]
    print(
        f"- 强制终止: exit={recovery['forced_exit_code']} "
        f"intent={recovery['before_recovery']['intent_status']}→"
        f"{recovery['intent_after_recovery'][-1]['status']} "
        f"versions {recovery['versions_before_crash']}→{recovery['versions_after_recovery']}"
    )
    print(f"- 安全退出摘要已生成: {recovery['safe_shutdown']['exit_summary_written']}")
    print(f"- 完整报告: {output}")


if __name__ == "__main__":
    run_demo()

