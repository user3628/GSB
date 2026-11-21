from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from chunk_engine.samples import generate_suite  # noqa: E402
from chunk_engine.utils import atomic_write_json, canonical_json_bytes, sha256_hex  # noqa: E402


def pick_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def http_json(
    base: str,
    method: str,
    path: str,
    body: dict[str, Any] | None = None,
    *,
    timeout: float = 60.0,
) -> tuple[int, dict[str, Any]]:
    data = None if body is None else json.dumps(body, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        base + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        raw = error.read().decode("utf-8")
        return error.code, json.loads(raw) if raw else {"ok": False, "error": {"code": "HTTP_ERROR"}}


def wait_for_health(base: str, proc: subprocess.Popen[Any], log_path: Path, timeout: float = 20.0) -> None:
    deadline = time.time() + timeout
    last_error: Exception | None = None
    while time.time() < deadline:
        if proc.poll() is not None:
            tail = ""
            try:
                tail = log_path.read_text(encoding="utf-8", errors="replace")[-4000:]
            except OSError:
                pass
            raise RuntimeError(f"server exited with {proc.returncode}\n{tail}")
        try:
            status, payload = http_json(base, "GET", "/health", timeout=1.0)
            if status == 200 and payload.get("ok"):
                return
        except Exception as error:  # connection is expected to fail while uvicorn starts
            last_error = error
        time.sleep(0.1)
    raise RuntimeError(f"server did not become healthy: {last_error}")


def start_server(data_dir: Path, port: int, *, allow_debug_kill: bool) -> tuple[subprocess.Popen[Any], str, Path]:
    log_path = data_dir / f"server-{port}.log"
    command = [
        sys.executable,
        "-m",
        "chunk_engine.server",
        "--data-dir",
        str(data_dir),
        "--port",
        str(port),
        "--host",
        "127.0.0.1",
        "--memory-budget",
        str(4 * 1024 * 1024),
        "--single-task-budget",
        str(64 * 1024),
        "--max-parallel-blocks",
        "8",
        "--max-keys",
        "20000",
        "--max-page-size",
        "200",
        "--log-level",
        "warning",
    ]
    if allow_debug_kill:
        command.append("--allow-debug-kill")
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(ROOT) + os.pathsep + environment.get("PYTHONPATH", "")
    environment["CHUNK_ENGINE_ALLOW_DEBUG_KILL"] = "1" if allow_debug_kill else "0"
    with log_path.open("ab") as log_handle:
        proc = subprocess.Popen(
            command,
            cwd=ROOT,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            env=environment,
        )
    base = f"http://127.0.0.1:{port}"
    wait_for_health(base, proc, log_path)
    return proc, base, log_path


def hard_stop(proc: subprocess.Popen[Any]) -> None:
    if proc.poll() is not None:
        return
    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/F", "/T", "/PID", str(proc.pid)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
    else:
        proc.send_signal(signal.SIGKILL)
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=10)


def collect_pages(
    base: str,
    handle: str,
    first_page: dict[str, Any],
    prefix: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows = list(first_page["rows"])
    summaries = [
        {
            "page_no": first_page["page_no"],
            "row_count": len(first_page["rows"]),
            "first_key": first_page["rows"][0]["key"] if first_page["rows"] else None,
            "last_key": first_page["rows"][-1]["key"] if first_page["rows"] else None,
        }
    ]
    for page_no in range(1, int(first_page["total_pages"])):
        status, payload = http_json(
            base,
            "GET",
            f"/results/{handle}/pages/{page_no}?consumer_id={prefix}-{page_no}",
        )
        if status != 200:
            raise RuntimeError(f"page fetch failed: {status} {payload}")
        page = payload["page"]
        rows.extend(page["rows"])
        summaries.append(
            {
                "page_no": page_no,
                "row_count": len(page["rows"]),
                "first_key": page["rows"][0]["key"] if page["rows"] else None,
                "last_key": page["rows"][-1]["key"] if page["rows"] else None,
            }
        )
    return rows, summaries


def query_summary(
    base: str,
    submitted: dict[str, Any],
    expected_keys: list[int],
    *,
    page_prefix: str,
) -> dict[str, Any]:
    batch = submitted["batch"]
    handle = submitted["result_handle"]
    rows, pages = collect_pages(base, handle, submitted["page"], page_prefix)
    status, verification = http_json(base, "GET", f"/results/{handle}/verify-pagination")
    if status != 200:
        raise RuntimeError(f"pagination verification failed: {status} {verification}")
    keys = [int(row["key"]) for row in rows]
    expected = sorted(set(expected_keys))
    page_digest = sha256_hex(canonical_json_bytes(rows))
    return {
        "batch_id": batch["batch_id"],
        "result_handle": handle,
        "selected_path": batch["plan"]["selected_path"],
        "parallel_blocks": batch["plan"]["parallel_blocks"],
        "peak_memory_estimate_bytes": batch["plan"]["peak_memory_estimate_bytes"],
        "split_record_count": len(batch["plan"]["split_records"]),
        "reason": batch["plan"]["reason"],
        "row_count": len(rows),
        "page_count": len(pages),
        "pages": pages,
        "first_key": keys[0] if keys else None,
        "last_key": keys[-1] if keys else None,
        "strict_key_order": all(keys[index] < keys[index + 1] for index in range(len(keys) - 1)),
        "matches_expected_key_set": keys == expected,
        "page_digest": page_digest,
        "pagination_verification": verification,
    }


def inspect_sqlite_after_kill(data_dir: Path, batch_id: str) -> dict[str, Any]:
    connection = sqlite3.connect(data_dir / "engine.sqlite3", timeout=10)
    connection.row_factory = sqlite3.Row
    try:
        batch = connection.execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
        if batch is None:
            raise RuntimeError("forced-kill batch was not persisted")
        tasks = connection.execute(
            "SELECT block_no,state,reusable FROM block_tasks WHERE batch_id=? ORDER BY block_no",
            (batch_id,),
        ).fetchall()
        reserved = connection.execute(
            "SELECT COALESCE(SUM(bytes),0) AS bytes, COUNT(*) AS count FROM memory_ledger WHERE state='reserved'"
        ).fetchone()
        pending = connection.execute("SELECT COUNT(*) AS count FROM intents WHERE state='pending'").fetchone()
        return {
            "batch_state": batch["state"],
            "batch_result_handle": batch["result_handle"],
            "tasks": [dict(row) for row in tasks],
            "committed_tasks": sum(1 for row in tasks if row["state"] == "committed"),
            "reserved_bytes": int(reserved["bytes"]),
            "reserved_count": int(reserved["count"]),
            "pending_intents": int(pending["count"]),
        }
    finally:
        connection.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Reproducible chunk-engine experiment")
    parser.add_argument("--data-dir", default=str(ROOT / "data"))
    parser.add_argument("--large-rows", type=int, default=150000)
    parser.add_argument("--small-rows", type=int, default=5000)
    parser.add_argument("--corrupt-rows", type=int, default=5000)
    parser.add_argument("--reuse-data", action="store_true", help="keep existing generated data directory")
    args = parser.parse_args(argv)
    data_dir = Path(args.data_dir).resolve()
    if data_dir.exists() and not args.reuse_data:
        shutil.rmtree(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    if not (data_dir / "snapshots" / "v0002" / "manifest.json").exists() or not args.reuse_data:
        print("Generating deterministic snapshots...", flush=True)
        suite = generate_suite(
            data_dir,
            small_rows=args.small_rows,
            large_rows=args.large_rows,
            corrupt_rows=args.corrupt_rows,
            small_block_size=512,
            large_block_size=16384,
            corrupt_block_size=512,
            seed=20261122,
            replace=True,
        )
    else:
        suite = {
            "v1_small": {"root": str(data_dir / "snapshots" / "v0001")},
            "v2_large": {"root": str(data_dir / "snapshots" / "v0002")},
            "v3_corrupt": {"root": str(data_dir / "snapshots" / "v0003")},
            "queries": {
                "directory": str(data_dir / "queries"),
                "corrupt_key": 512 + 256,
            },
        }
    query_dir = Path(suite["queries"]["directory"])
    small_query = json.loads((query_dir / "keys_small.json").read_text(encoding="utf-8"))
    large_query = json.loads((query_dir / "keys_large.json").read_text(encoding="utf-8"))
    corrupt_query = json.loads((query_dir / "keys_corrupt.json").read_text(encoding="utf-8"))

    report: dict[str, Any] = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "data_dir": str(data_dir),
        "sample_versions": {
            "small": {
                "version": 1,
                "rows": args.small_rows,
                "block_size": 512,
                "path": suite["v1_small"]["root"],
            },
            "large": {
                "version": 2,
                "rows": args.large_rows,
                "block_size": 16384,
                "path": suite["v2_large"]["root"],
            },
            "corrupt": {
                "version": 3,
                "rows": args.corrupt_rows,
                "block_size": 512,
                "corrupt_block": 1,
                "path": suite["v3_corrupt"]["root"],
            },
        },
    }
    server: subprocess.Popen[Any] | None = None
    try:
        port = pick_port()
        server, base, _ = start_server(data_dir, port, allow_debug_kill=True)
        report["first_server"] = {"port": port, "base": base}
        status, payload = http_json(base, "POST", "/snapshots/load", {"path": suite["v1_small"]["root"]})
        if status != 200:
            raise RuntimeError(f"v1 load failed: {status} {payload}")

        status, payload = http_json(
            base,
            "POST",
            "/batches",
            {
                "keys": small_query["keys"],
                "columns": small_query["columns"],
                "page_size": small_query["page_size"],
                "consumer_id": "experiment-small-submit",
            },
        )
        if status != 200:
            raise RuntimeError(f"small query failed: {status} {payload}")
        report["small_stream"] = query_summary(
            base,
            payload,
            small_query["keys"],
            page_prefix="small-page",
        )

        status, payload = http_json(base, "POST", "/snapshots/load", {"path": suite["v2_large"]["root"]})
        if status != 200:
            raise RuntimeError(f"v2 load failed: {status} {payload}")
        status, payload = http_json(
            base,
            "POST",
            "/batches",
            {
                "keys": large_query["keys"],
                "columns": large_query["columns"],
                "page_size": large_query["page_size"],
                "consumer_id": "experiment-large-submit",
            },
        )
        if status != 200:
            raise RuntimeError(f"large query failed: {status} {payload}")
        report["large_batch"] = query_summary(
            base,
            payload,
            large_query["keys"],
            page_prefix="large-page",
        )

        status, payload = http_json(
            base,
            "POST",
            "/compare",
            {
                "keys": large_query["keys"],
                "columns": large_query["columns"],
                "page_size": large_query["page_size"],
                "max_page_size": 200,
            },
        )
        if status != 200:
            raise RuntimeError(f"comparison failed: {status} {payload}")
        report["path_compare"] = payload

        kill_keys = [1, 20000, 40000, 60000]
        kill_batch_id = "b_forced_kill_demo"
        kill_request: dict[str, Any] = {
            "keys": kill_keys,
            "columns": ["id", "region", "shard", "score", "label"],
            "page_size": 2,
            "execution_mode": "stream",
            "batch_id": kill_batch_id,
            "consumer_id": "forced-kill-submit",
            "debug_kill_after_committed_blocks": 2,
        }
        try:
            http_json(base, "POST", "/batches", kill_request, timeout=30.0)
        except Exception:
            # The debug hook exits the process before an HTTP response can be written.
            pass
        try:
            kill_return_code = server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            hard_stop(server)
            kill_return_code = server.returncode
        pre_kill = inspect_sqlite_after_kill(data_dir, kill_batch_id)
        report["forced_kill"] = {
            "batch_id": kill_batch_id,
            "server_return_code": kill_return_code,
            "facts_before_restart": pre_kill,
        }
        server = None

        port2 = pick_port()
        server, base2, _ = start_server(data_dir, port2, allow_debug_kill=False)
        report["second_server"] = {"port": port2, "base": base2}
        status, recovery_payload = http_json(base2, "GET", "/admin/recovery")
        if status != 200:
            raise RuntimeError(f"recovery report failed: {status} {recovery_payload}")
        recovery_envelope = recovery_payload["report"]
        if isinstance(recovery_envelope, dict) and isinstance(recovery_envelope.get("report"), dict):
            recovery_report = dict(recovery_envelope["report"])
            recovery_report["report_created_at"] = recovery_envelope.get("created_at")
        else:
            recovery_report = recovery_envelope
        status, batch_payload = http_json(base2, "GET", f"/batches/{kill_batch_id}")
        if status != 200:
            raise RuntimeError(f"recovered batch status failed: {status} {batch_payload}")
        batch_before_resume = batch_payload["batch"]
        handle = batch_before_resume["result_handle"]
        status, page0_payload = http_json(
            base2,
            "GET",
            f"/results/{handle}/pages/0?consumer_id=recovery-page-0",
            timeout=60.0,
        )
        if status != 200:
            raise RuntimeError(f"recovered page 0 failed: {status} {page0_payload}")
        recovered_rows, recovered_pages = collect_pages(
            base2,
            handle,
            page0_payload["page"],
            "recovery-page",
        )
        status, verification = http_json(base2, "GET", f"/results/{handle}/verify-pagination")
        status_after, batch_after = http_json(base2, "GET", f"/batches/{kill_batch_id}")
        report["forced_kill"].update(
            {
                "recovery_report": recovery_report,
                "batch_before_resume": {
                    "state": batch_before_resume["state"],
                    "next_ready_page": batch_before_resume["next_ready_page"],
                    "block_task_counts": batch_before_resume["block_task_counts"],
                },
                "resumed": {
                    "state": batch_after["batch"]["state"] if status_after == 200 else None,
                    "result_handle": handle,
                    "row_count": len(recovered_rows),
                    "page_count": len(recovered_pages),
                    "keys": [int(row["key"]) for row in recovered_rows],
                    "strict_key_order": all(
                        int(recovered_rows[index]["key"]) < int(recovered_rows[index + 1]["key"])
                        for index in range(len(recovered_rows) - 1)
                    ),
                    "expected_keys": kill_keys,
                    "matches_expected_keys": [int(row["key"]) for row in recovered_rows] == kill_keys,
                    "execution": batch_after["batch"]["execution"] if status_after == 200 else None,
                    "pagination_verification": verification,
                },
            }
        )

        status, loaded = http_json(base2, "POST", "/snapshots/load", {"path": suite["v3_corrupt"]["root"]})
        if status != 200:
            raise RuntimeError(f"v3 load failed: {status} {loaded}")
        corrupt_key = int(suite["queries"]["corrupt_key"])
        status, corrupt_payload = http_json(
            base2,
            "POST",
            "/batches",
            {
                "keys": [corrupt_key],
                "columns": ["id", "label"],
                "page_size": corrupt_query["page_size"],
                "consumer_id": "corrupt-demo",
                "batch_id": "b_corrupt_demo",
            },
        )
        report["corrupt_block"] = {
            "http_status": status,
            "key": corrupt_key,
            "response": corrupt_payload,
        }

        small = report["small_stream"]
        large = report["large_batch"]
        comparison = report["path_compare"]
        forced = report["forced_kill"]
        verdict = {
            "small_query_used_stream": small["selected_path"] == "stream",
            "large_query_used_batch": large["selected_path"] == "batch",
            "both_queries_pagination_equal": bool(
                small["pagination_verification"].get("equal")
                and large["pagination_verification"].get("equal")
            ),
            "paths_bitwise_equal": bool(comparison.get("bitwise_equal")),
            "paths_within_tolerance": bool(comparison.get("within_tolerance")),
            "stream_and_batch_digest_equal": (
                comparison.get("stream", {}).get("result_digest")
                == comparison.get("batch", {}).get("result_digest")
            ),
            "forced_kill_persisted_two_blocks": forced["facts_before_restart"]["committed_tasks"] == 2,
            "restart_reused_committed_blocks": bool(
                len(forced["recovery_report"].get("tasks_reused", [])) >= 2
                and forced["resumed"]["execution"].get("blocks_reused", 0) >= 2
            ),
            "restart_resumed_pages_in_order": bool(
                forced["resumed"]["matches_expected_keys"]
                and forced["resumed"]["strict_key_order"]
                and forced["resumed"]["pagination_verification"].get("equal")
            ),
            "corrupt_block_rejected": (
                report["corrupt_block"]["http_status"] == 400
                and report["corrupt_block"]["response"].get("error", {}).get("code") == "E_BLOCK_CORRUPT"
            ),
        }
        report["verdict"] = verdict
        report["all_checks_passed"] = all(verdict.values())
        report_path = data_dir / "experiment_report.json"
        atomic_write_json(report_path, report)
        print("\n=== Reproducible experiment report ===")
        print(f"small: path={small['selected_path']} peak={small['peak_memory_estimate_bytes']} rows={small['row_count']} pages={small['page_count']}")
        print(f"large: path={large['selected_path']} peak={large['peak_memory_estimate_bytes']} rows={large['row_count']} pages={large['page_count']} splits={large['split_record_count']}")
        print(
            "compare: bitwise_equal={} max_abs={} max_rel={} digest_equal={}".format(
                comparison["bitwise_equal"],
                comparison["max_absolute_deviation"],
                comparison["max_relative_deviation"],
                verdict["stream_and_batch_digest_equal"],
            )
        )
        print(
            "forced kill: committed_before={} reused_after={} reclaimed_budget={} resumed_keys={}".format(
                forced["facts_before_restart"]["committed_tasks"],
                len(forced["recovery_report"].get("tasks_reused", [])),
                forced["recovery_report"].get("budget_reservations_reclaimed"),
                forced["resumed"]["keys"],
            )
        )
        print(
            "corrupt block: http={} code={} block={}".format(
                report["corrupt_block"]["http_status"],
                report["corrupt_block"]["response"].get("error", {}).get("code"),
                report["corrupt_block"]["response"].get("error", {}).get("context", {}).get("block_no"),
            )
        )
        print(f"verdict: {json.dumps(verdict, ensure_ascii=False)}")
        print(f"report: {report_path}")
        return 0 if report["all_checks_passed"] else 1
    finally:
        if server is not None:
            hard_stop(server)


if __name__ == "__main__":
    raise SystemExit(main())
