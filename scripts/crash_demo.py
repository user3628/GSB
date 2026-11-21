#!/usr/bin/env python3
from __future__ import annotations

import argparse
import copy
import json
import os
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from replica_repair.http_client import LocalHttpClient  # noqa: E402
from replica_repair.numerics import canonical_json  # noqa: E402


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_report(name: str, round_id: str) -> dict[str, Any]:
    report = copy.deepcopy(load_json(ROOT / "samples" / "reports" / name))
    report["report_id"] = f"{round_id}:{report['report_id']}"
    return report


def remove_database(path: Path) -> None:
    for suffix in ("", "-wal", "-shm"):
        candidate = Path(str(path) + suffix)
        if candidate.exists():
            candidate.unlink()


def start_server(
    db_path: Path,
    log_path: Path,
    *,
    port: int,
    delay_ms: int,
) -> tuple[subprocess.Popen[bytes], Any]:
    env = os.environ.copy()
    env["REPAIR_AGGREGATION_DELAY_MS"] = str(delay_ms)
    env["REPAIR_CLOCK_BASE_ISO"] = "2000-01-01T00:00:03Z"
    handle = log_path.open("ab")
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "replica_repair.server",
            "--db",
            str(db_path),
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ],
        cwd=ROOT,
        env=env,
        stdout=handle,
        stderr=subprocess.STDOUT,
    )
    return process, handle


def stop_server(process: subprocess.Popen[bytes], handle: Any) -> None:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
    handle.close()


def db_facts(db_path: Path, round_id: str) -> dict[str, Any]:
    with sqlite3.connect(db_path, timeout=5) as connection:
        intent = connection.execute(
            "SELECT state, attempt FROM round_intents WHERE round_id = ?",
            (round_id,),
        ).fetchone()
        round_row = connection.execute(
            "SELECT status, sealed_at FROM repair_rounds WHERE round_id = ?",
            (round_id,),
        ).fetchone()
        conclusion_count = connection.execute(
            "SELECT COUNT(*) FROM round_conclusions WHERE round_id = ?",
            (round_id,),
        ).fetchone()[0]
    return {
        "intent_state": intent[0] if intent else None,
        "intent_attempt": intent[1] if intent else None,
        "round_status": round_row[0] if round_row else None,
        "sealed_at": round_row[1] if round_row else None,
        "conclusion_count": int(conclusion_count),
    }


def wait_for_prepared_intent(
    db_path: Path,
    round_id: str,
    *,
    timeout_seconds: float = 8.0,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        try:
            last = db_facts(db_path, round_id)
            if (
                last["intent_state"] == "PREPARED"
                and last["round_status"] == "AGGREGATING"
            ):
                return last
        except sqlite3.Error:
            pass
        time.sleep(0.05)
    raise RuntimeError(f"未观察到已持久化的 PREPARED 意图: {last}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="SIGKILL aggregation recovery demo")
    parser.add_argument("--port", type=int, default=18765)
    parser.add_argument(
        "--json-output",
        default=str(ROOT / "artifacts" / "crash_recovery_output.json"),
    )
    args = parser.parse_args(argv)

    db_path = ROOT / "data" / "repair_crash.sqlite3"
    log_path = ROOT / "data" / "crash_server.log"
    remove_database(db_path)
    if log_path.exists():
        log_path.unlink()

    client = LocalHttpClient(f"http://127.0.0.1:{args.port}")
    group = load_json(ROOT / "samples" / "groups.json")["groups"][0]
    round_id = "round-crash-recovery"
    deadline = load_json(ROOT / "samples" / "rounds.json")["round_defaults"][
        "deadline"
    ]

    process, handle = start_server(
        db_path,
        log_path,
        port=args.port,
        delay_ms=5000,
    )
    aggregate_result: dict[str, Any] = {}
    aggregate_error: list[str] = []
    try:
        client.wait_until_healthy()
        status, payload = client.post("/v1/groups", group)
        if status != 200 or not payload.get("ok"):
            raise RuntimeError(f"创建副本组失败: {status} {payload}")
        status, payload = client.post(
            "/v1/rounds",
            {
                "round_id": round_id,
                "group_id": group["group_id"],
                "group_version": group["version"],
                "deadline": deadline,
                "target_progress": 0.25,
                "relative_tolerance": 1e-8,
            },
        )
        if status != 200 or not payload.get("ok"):
            raise RuntimeError(f"创建轮次失败: {status} {payload}")
        status, payload = client.post(
            f"/v1/rounds/{round_id}/reports",
            {
                "group_id": group["group_id"],
                "group_version": group["version"],
                "reports": [
                    load_report("r0_ontime.json", round_id),
                    load_report("r2_ontime.json", round_id),
                ],
            },
        )
        if status != 200 or not payload.get("ok"):
            raise RuntimeError(f"提交上报失败: {status} {payload}")

        def call_aggregate() -> None:
            try:
                code, response = client.post(
                    f"/v1/rounds/{round_id}/aggregate",
                    {"normalization": "REPORTED_RATIO", "strict_wait": False},
                )
                aggregate_result.update({"http_status": code, "response": response})
            except Exception as exc:  # expected when the server is SIGKILLed
                aggregate_error.append(f"{type(exc).__name__}: {exc}")

        thread = threading.Thread(target=call_aggregate, daemon=True)
        thread.start()
        before_kill = wait_for_prepared_intent(db_path, round_id)
        process.kill()
        process.wait(timeout=5)
        thread.join(timeout=5)
        after_kill = db_facts(db_path, round_id)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        handle.close()

    restart_process, restart_handle = start_server(
        db_path,
        log_path,
        port=args.port,
        delay_ms=0,
    )
    try:
        client.wait_until_healthy()
        recovery_status, recovery_payload = client.get(
            "/v1/recovery/report/latest"
        )
        if recovery_status != 200 or not recovery_payload.get("ok"):
            raise RuntimeError(
                f"读取恢复报告失败: {recovery_status} {recovery_payload}"
            )
        recovery = recovery_payload["report"]
        recovered_intent = next(
            (
                item
                for item in recovery["intents"]
                if item["round_id"] == round_id
            ),
            None,
        )
        if recovered_intent is None:
            raise RuntimeError("恢复报告不包含被终止轮次")
        if recovered_intent["action"] != "MARKED_ABORTED":
            raise RuntimeError(f"恢复动作异常: {recovered_intent}")

        retry_status, retry_payload = client.post(
            f"/v1/rounds/{round_id}/aggregate",
            {"normalization": "REPORTED_RATIO", "strict_wait": False},
        )
        if retry_status != 200 or not retry_payload.get("ok"):
            raise RuntimeError(f"恢复后重试聚合失败: {retry_status} {retry_payload}")
        query_status, query_payload = client.get(
            f"/v1/rounds/{round_id}/conclusion"
        )
        if query_status != 200 or not query_payload.get("ok"):
            raise RuntimeError(f"恢复后查询失败: {query_status} {query_payload}")
        repeat_status, repeat_payload = client.post(
            f"/v1/rounds/{round_id}/aggregate",
            {"normalization": "REPORTED_RATIO", "strict_wait": False},
        )
        if repeat_status != 200 or not repeat_payload.get("ok"):
            raise RuntimeError(f"恢复后重复聚合失败: {repeat_status} {repeat_payload}")

        summary = {
            "service_url": f"http://127.0.0.1:{args.port}",
            "database": str(db_path.relative_to(ROOT)),
            "before_kill": before_kill,
            "after_kill": after_kill,
            "recovery_report": {
                "report_digest": recovery["report_digest"],
                "incomplete_intent_count": recovery["incomplete_intent_count"],
                "aborted_count": recovery["aborted_count"],
                "intent": recovered_intent,
                "source": recovery["source"],
            },
            "retry_aggregation": {
                "http_status": retry_status,
                "attempt": retry_payload["conclusion"]["aggregation"][
                    "execution_attempt"
                ],
                "conclusion_digest": retry_payload["conclusion_digest"],
                "converged": retry_payload["conclusion"]["convergence"][
                    "converged"
                ],
                "progress": retry_payload["conclusion"]["estimates"]["progress"],
            },
            "query_after_recovery_digest": query_payload["conclusion_digest"],
            "repeat_after_recovery": {
                "reused": repeat_payload["reused"],
                "digest_identical": (
                    repeat_payload["conclusion_digest"]
                    == retry_payload["conclusion_digest"]
                ),
                "json_identical": (
                    canonical_json(repeat_payload["conclusion"])
                    == canonical_json(retry_payload["conclusion"])
                ),
            },
            "killed_inflight_http_error": aggregate_error,
        }
        output_path = Path(args.json_output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        print(f"\n强制终止恢复摘要已写入: {output_path}")
        return 0
    finally:
        stop_server(restart_process, restart_handle)


if __name__ == "__main__":
    raise SystemExit(main())

