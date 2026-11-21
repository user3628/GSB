from __future__ import annotations

import argparse
import base64
import copy
import json
import shutil
import socket
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

from colscan.reference import reference_scan_file
from colscan.snapshot import snapshot_checksum


COLUMNS = ["id", "name", "status", "score", "active"]
PREDICATE = {
    "op": "and",
    "predicates": [
        {"column": "score", "op": "ge", "value": 35.0},
        {"column": "status", "op": "ne", "value": "archived"},
    ],
}


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class ServerProcess:
    def __init__(self, *, state_db: Path, delay_ms: int, log_path: Path) -> None:
        self.port = free_port()
        self.base_url = f"http://127.0.0.1:{self.port}"
        self.log_file = log_path.open("w", encoding="utf-8")
        self.process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "colscan.server",
                "--host",
                "127.0.0.1",
                "--port",
                str(self.port),
                "--state-db",
                str(state_db),
                "--task-delay-ms",
                str(delay_ms),
            ],
            cwd=str(ROOT),
            stdout=self.log_file,
            stderr=subprocess.STDOUT,
        )
        self._wait_for_health()

    def _wait_for_health(self) -> None:
        deadline = time.monotonic() + 15.0
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                self.log_file.flush()
                raise RuntimeError(f"server exited early with code {self.process.returncode}")
            try:
                status, payload = http_call(self.base_url, "GET", "/health", timeout=0.5)
                if status == 200 and payload.get("ok"):
                    return
            except OSError:
                pass
            time.sleep(0.05)
        raise RuntimeError("server did not become healthy")

    def terminate(self) -> int:
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        self.log_file.close()
        return int(self.process.returncode)

    def force_kill(self) -> int:
        self.process.kill()
        self.process.wait(timeout=10)
        self.log_file.close()
        return int(self.process.returncode)


def http_call(
    base_url: str,
    method: str,
    path: str,
    body: dict[str, Any] | None = None,
    *,
    timeout: float = 15.0,
) -> tuple[int, dict[str, Any]]:
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    data = json.dumps(body).encode("utf-8") if body is not None else None
    request = urllib.request.Request(
        base_url + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with opener.open(request, timeout=timeout) as response:
            return int(response.status), json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return int(exc.code), json.loads(exc.read().decode("utf-8"))


def expect_ok(base_url: str, method: str, path: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
    status, payload = http_call(base_url, method, path, body)
    if status < 200 or status >= 300 or not payload.get("ok"):
        raise AssertionError(f"HTTP {method} {path} failed: {status} {payload}")
    return payload["data"]


def expect_error(
    base_url: str,
    method: str,
    path: str,
    body: dict[str, Any] | None = None,
    *,
    code: str,
) -> dict[str, Any]:
    status, payload = http_call(base_url, method, path, body)
    if payload.get("ok") or payload.get("error", {}).get("code") != code:
        raise AssertionError(f"expected {code}, got HTTP {status}: {payload}")
    return {"http_status": status, **payload["error"]}


def submit_and_wait(base_url: str, request: dict[str, Any], timeout: float = 20.0) -> dict[str, Any]:
    status, payload = http_call(base_url, "POST", "/v1/queries", request)
    if status not in (200, 202) or not payload.get("ok"):
        raise AssertionError(f"query submission failed: {status} {payload}")
    query_id = request["query_id"]
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        query = expect_ok(base_url, "GET", f"/v1/queries/{query_id}")
        if query["status"] in ("COMPLETE", "FAILED"):
            return query
        time.sleep(0.03)
    raise AssertionError(f"query {query_id} timed out")


def fetch_rows(base_url: str, query_id: str, *, offset: int | None = None, limit: int | None = None) -> dict[str, Any]:
    params = []
    if offset is not None:
        params.append(f"offset={offset}")
    if limit is not None:
        params.append(f"limit={limit}")
    suffix = "?" + "&".join(params) if params else ""
    return expect_ok(base_url, "GET", f"/v1/queries/{query_id}/results{suffix}")


def base_query(query_id: str, *, memory: int = 1_048_576, concurrency: int = 3, queue_capacity: int = 256) -> dict[str, Any]:
    return {
        "query_id": query_id,
        "columns": COLUMNS,
        "predicate": PREDICATE,
        "block_start": 0,
        "block_end": 6,
        "offset": 0,
        "limit": None,
        "budget": {
            "max_concurrent_tasks": concurrency,
            "per_task_memory_bytes": memory,
            "round_time_budget_ms": 8.0,
            "queue_capacity": queue_capacity,
            "degrade_mode": "split",
        },
    }


def make_invalid_snapshot(source: Path, target: Path) -> None:
    document = json.loads(source.read_text(encoding="utf-8"))
    document["generated_at"] = "tampered-without-updating-checksum"
    target.write_text(json.dumps(document, indent=2), encoding="utf-8")


def make_corrupt_snapshot(source: Path, target: Path) -> str:
    document = copy.deepcopy(json.loads(source.read_text(encoding="utf-8")))
    document["version"] = 2
    document["generated_at"] = "2026-01-01T00:00:02Z"
    payload = bytearray(base64.b64decode(document["columns"][1]["blocks"][0]["payload_b64"]))
    payload[-1] ^= 0xFF
    document["columns"][1]["blocks"][0]["payload_b64"] = base64.b64encode(bytes(payload)).decode("ascii")
    document.pop("checksum", None)
    document["checksum"] = snapshot_checksum(document)
    target.write_text(json.dumps(document, indent=2), encoding="utf-8")
    return str(document["checksum"])


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the complete local verification suite")
    parser.add_argument("--work-dir", default="artifacts/e2e_work")
    parser.add_argument("--keep-work-dir", action="store_true")
    args = parser.parse_args()
    work_dir = (ROOT / args.work_dir).resolve()
    if work_dir.exists() and not args.keep_work_dir:
        shutil.rmtree(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)
    snapshot_dir = work_dir / "snapshots"
    subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "generate_sample.py"),
            "--out",
            str(snapshot_dir),
            "--versions",
            "1",
            "3",
            "--rows",
            "96",
            "--block-size",
            "16",
        ],
        cwd=str(ROOT),
        check=True,
        stdout=subprocess.DEVNULL,
    )
    v1_path = snapshot_dir / "snapshot_v1.csnap"
    v3_path = snapshot_dir / "snapshot_v3.csnap"
    invalid_path = snapshot_dir / "snapshot_invalid.csnap"
    corrupt_path = snapshot_dir / "snapshot_v2_corrupt.csnap"
    make_invalid_snapshot(v1_path, invalid_path)
    corrupt_checksum = make_corrupt_snapshot(v1_path, corrupt_path)
    state_db = work_dir / "state.db"
    report: dict[str, Any] = {
        "work_dir": str(work_dir),
        "snapshot_versions": [1, 3],
        "checks": {},
    }
    print("PHASE prepare deterministic sample and clean SQLite state")

    server = ServerProcess(state_db=state_db, delay_ms=0, log_path=work_dir / "server_initial.log")
    try:
        invalid = expect_error(
            server.base_url,
            "POST",
            "/v1/snapshots/load",
            {"path": str(invalid_path)},
            code="E_SNAPSHOT_INVALID",
        )
        print(f"CHECK E_SNAPSHOT_INVALID http={invalid['http_status']} reason={invalid['details']['reason']}")
        loaded_v1 = expect_ok(server.base_url, "POST", "/v1/snapshots/load", {"path": str(v1_path)})
        block_stats = expect_ok(server.base_url, "GET", "/v1/snapshots/blocks")
        block_count = sum(len(column["blocks"]) for column in block_stats["columns"])
        first_block = block_stats["columns"][0]["blocks"][0]
        if block_count != 36 or first_block["compressed_size"] < 1 or first_block["raw_size"] < 1:
            raise AssertionError("snapshot block statistics are incomplete")
        print(
            f"PHASE block stats columns={len(block_stats['columns'])} blocks={block_count} "
            f"sample={first_block['block_id']} raw={first_block['raw_size']} "
            f"compressed={first_block['compressed_size']} ratio={first_block['compression_ratio']}"
        )
        retired = expect_error(
            server.base_url,
            "POST",
            "/v1/snapshots/load",
            {"path": str(v1_path)},
            code="E_VERSION_RETIRED",
        )
        current = expect_ok(server.base_url, "GET", "/v1/snapshots/current")
        if current["version"] != 1:
            raise AssertionError("retired load changed the active snapshot")
        queue_full = expect_error(
            server.base_url,
            "POST",
            "/v1/queries",
            {
                **base_query("queue_full", concurrency=1, queue_capacity=1),
            },
            code="E_SCAN_QUEUE_FULL",
        )
        print(
            f"CHECK E_VERSION_RETIRED active={current['version']} | "
            f"E_SCAN_QUEUE_FULL http={queue_full['http_status']} limit={queue_full['details']['admission_limit']}"
        )

        baseline_status = submit_and_wait(server.base_url, base_query("baseline_full"))
        if baseline_status["status"] != "COMPLETE":
            raise AssertionError(f"baseline failed: {baseline_status}")
        baseline = fetch_rows(server.base_url, "baseline_full")
        reference_v1 = reference_scan_file(
            v1_path,
            columns=COLUMNS,
            predicate=PREDICATE,
            block_start=0,
            block_end=6,
        )
        if baseline["rows"] != reference_v1["all_rows"]:
            raise AssertionError("baseline rows differ from one-shot reference scan")
        duplicate_status, duplicate_payload = http_call(
            server.base_url,
            "POST",
            "/v1/queries",
            base_query("baseline_full"),
        )
        if duplicate_status != 200 or duplicate_payload["data"]["created"]:
            raise AssertionError("duplicate query was not reused")
        if duplicate_payload["data"]["query"]["commit_generation"] != baseline_status["commit_generation"]:
            raise AssertionError("duplicate query produced a new commit generation")
        print(
            f"PHASE full scan complete rows={len(baseline['rows'])} generation={baseline['commit_generation']} "
            f"snapshot=v{baseline['snapshot_version']}"
        )

        low_status = submit_and_wait(
            server.base_url,
            base_query("low_budget", memory=1200, concurrency=2, queue_capacity=256),
        )
        if low_status["status"] != "COMPLETE":
            raise AssertionError(f"low budget query failed: {low_status}")
        low = fetch_rows(server.base_url, "low_budget")
        if low["rows"] != baseline["rows"]:
            raise AssertionError("low-budget rows differ from baseline")
        decisions = expect_ok(server.base_url, "GET", "/v1/queries/low_budget/decisions")
        if decisions["action_counts"].get("split", 0) < 1:
            raise AssertionError("low-budget trace did not contain split decisions")
        split_example = next(item for item in decisions["decisions"] if item["action"] == "split")
        print(
            f"PHASE low budget complete rows={len(low['rows'])} equal=true "
            f"actions={json.dumps(decisions['action_counts'], sort_keys=True)} "
            f"first_split={split_example['block_id']}:{split_example['split_ranges']}"
        )

        page_a = fetch_rows(server.base_url, "baseline_full", offset=0, limit=7)
        page_b = fetch_rows(server.base_url, "baseline_full", offset=7, limit=9)
        if page_a["rows"] + page_b["rows"] != baseline["rows"][:16]:
            raise AssertionError("pagination stitching is not exact")
        if len({tuple(row) for row in page_a["rows"] + page_b["rows"]}) != 16:
            raise AssertionError("pagination produced duplicate rows")
        print(f"PHASE pagination pages={len(page_a['rows'])}+{len(page_b['rows'])} no_gap_no_overlap=true")

        expect_ok(server.base_url, "POST", "/v1/snapshots/load", {"path": str(corrupt_path)})
        corrupt_request = base_query("corrupt_probe", concurrency=1, queue_capacity=16)
        corrupt_request["columns"] = ["id", "name"]
        corrupt_request["predicate"] = None
        corrupt_request["block_end"] = 1
        corrupt_status = submit_and_wait(server.base_url, corrupt_request)
        if corrupt_status["status"] != "FAILED" or corrupt_status["error"]["code"] != "E_BLOCK_CORRUPT":
            raise AssertionError(f"corrupt block was not surfaced: {corrupt_status}")
        corrupt_error = expect_error(
            server.base_url,
            "GET",
            "/v1/queries/corrupt_probe/results",
            code="E_BLOCK_CORRUPT",
        )
        if corrupt_error["details"]["block_id"] != "name:b0000":
            raise AssertionError("corrupt block identifier was not preserved")
        print(
            f"CHECK E_BLOCK_CORRUPT http={corrupt_error['http_status']} "
            f"block_id={corrupt_error['details']['block_id']} checksum={corrupt_checksum[:12]}"
        )

        loaded_v3 = expect_ok(server.base_url, "POST", "/v1/snapshots/load", {"path": str(v3_path)})
        if loaded_v3["version"] != 3:
            raise AssertionError("valid v3 snapshot was not installed")
        print(f"PHASE loaded clean snapshot v3 generation={loaded_v3['generation']}")
    finally:
        initial_exit = server.terminate()

    crash_server = ServerProcess(state_db=state_db, delay_ms=250, log_path=work_dir / "server_crash.log")
    try:
        crash_request = base_query("crash_resume", concurrency=2, queue_capacity=256)
        status, payload = http_call(crash_server.base_url, "POST", "/v1/queries", crash_request)
        if status != 202 or not payload.get("ok"):
            raise AssertionError(f"crash query submission failed: {status} {payload}")
        committed_before_kill = 0
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            query = expect_ok(crash_server.base_url, "GET", "/v1/queries/crash_resume")
            committed_before_kill = query["committed_blocks"]
            if 0 < committed_before_kill < query["total_blocks"]:
                break
            time.sleep(0.02)
        if not (0 < committed_before_kill < 6):
            raise AssertionError("could not observe a partially committed query before kill")
    finally:
        killed_exit = crash_server.force_kill()
    print(f"PHASE forced termination committed_before_kill={committed_before_kill} exit_code={killed_exit}")

    recover_server = ServerProcess(state_db=state_db, delay_ms=10, log_path=work_dir / "server_recovery.log")
    try:
        recovered = submit_and_wait(recover_server.base_url, crash_request, timeout=30.0)
        if recovered["status"] != "COMPLETE":
            raise AssertionError(f"recovery query failed: {recovered}")
        recovered_rows = fetch_rows(recover_server.base_url, "crash_resume")
        reference_v3 = reference_scan_file(
            v3_path,
            columns=COLUMNS,
            predicate=PREDICATE,
            block_start=0,
            block_end=6,
        )
        if recovered_rows["rows"] != reference_v3["all_rows"]:
            raise AssertionError("recovered rows differ from one-shot v3 reference scan")
        shards = expect_ok(recover_server.base_url, "GET", "/v1/queries/crash_resume/shards")["shards"]
        reused = sum(1 for shard in shards if shard["reused"])
        resumed = sum(1 for shard in shards if shard["recovery_count"] > 0)
        recovery = expect_ok(recover_server.base_url, "GET", "/v1/recovery/report?query_id=crash_resume")
        if not recovery["reports"]:
            raise AssertionError("recovery report was not persisted")
        latest = recovery["reports"][-1]
        if latest["reused_shards"] < 1 or latest["resumed_shards"] < 1:
            raise AssertionError(f"recovery report lacks reuse/resume facts: {latest}")
        if recovery["complete_exit_report_available"]:
            raise AssertionError("recovery report falsely claims a clean exit report")
        print(
            f"PHASE recovery complete rows={len(recovered_rows['rows'])} equal=true reused={reused} "
            f"resumed={resumed} generation={recovered['commit_generation']}"
        )
        print(
            f"PHASE recovery report report_id={latest['report_id']} source={recovery['source']} "
            f"clean_exit_report=false persisted_facts=true"
        )
    finally:
        recovery_exit = recover_server.terminate()

    report["checks"] = {
        "snapshot_invalid": True,
        "snapshot_block_stats": {
            "columns": len(block_stats["columns"]),
            "blocks": block_count,
            "sample_block_id": first_block["block_id"],
            "sample_raw_size": first_block["raw_size"],
            "sample_compressed_size": first_block["compressed_size"],
            "sample_compression_ratio": first_block["compression_ratio"],
        },
        "version_retired": True,
        "scan_queue_full": True,
        "block_corrupt": True,
        "full_scan_rows": len(baseline["rows"]),
        "full_scan_equals_reference": True,
        "low_budget_rows": len(low["rows"]),
        "low_budget_equals_full": True,
        "low_budget_action_counts": decisions["action_counts"],
        "low_budget_first_split": {
            "block_id": split_example["block_id"],
            "row_range": split_example["row_range"],
            "split_ranges": split_example["split_ranges"],
            "estimated_memory_bytes": split_example["estimated_memory_bytes"],
            "budget": split_example["budget"],
        },
        "pagination": {"first_page": 7, "second_page": 9, "exact": True},
        "duplicate_query_reused": True,
        "committed_before_kill": committed_before_kill,
        "killed_exit_code": killed_exit,
        "recovery_rows": len(recovered_rows["rows"]),
        "recovery_equals_v3_reference": True,
        "recovery_reused_shards": reused,
        "recovery_resumed_shards": resumed,
        "recovery_report_id": latest["report_id"],
        "initial_process_exit": initial_exit,
        "recovery_process_exit": recovery_exit,
    }
    report["passed"] = True
    report_path = work_dir / "e2e_report.json"
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"ok": True, "report": str(report_path), "summary": report["checks"]}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

