from __future__ import annotations

import argparse
import hashlib
import json
import os
import socket
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class Api:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def call(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        *,
        timeout: float = 10.0,
        expected_codes: set[str] | None = None,
    ) -> dict[str, Any]:
        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        request = urllib.request.Request(
            self.base_url + path,
            data=data,
            method=method,
            headers={"Content-Type": "application/json"},
        )
        try:
            with self.opener.open(request, timeout=timeout) as response:
                body = response.read().decode("utf-8")
                parsed = json.loads(body)
        except urllib.error.HTTPError as exc:
            parsed = json.loads(exc.read().decode("utf-8"))
            if expected_codes is None or parsed.get("code") not in expected_codes:
                raise RuntimeError(f"HTTP {exc.code}: {parsed}") from exc
            return parsed
        if expected_codes is not None and parsed.get("code") not in expected_codes:
            raise RuntimeError(f"Unexpected response: {parsed}")
        return parsed

    def get(self, path: str) -> dict[str, Any]:
        return self.call("GET", path)

    def post(self, path: str, payload: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
        return self.call("POST", path, payload, **kwargs)


class Server:
    def __init__(self, db_path: Path, block_count: int, log_path: Path):
        self.db_path = db_path
        self.block_count = block_count
        self.log_path = log_path
        self.port = free_port()
        self.process: subprocess.Popen[bytes] | None = None
        self.log_handle = None
        self.api = Api(f"http://127.0.0.1:{self.port}")

    def start(self) -> None:
        env = os.environ.copy()
        env.update(
            {
                "COW_DB_PATH": str(self.db_path),
                "COW_VOLUME_ID": "vol0",
                "COW_BLOCK_COUNT": str(self.block_count),
                "COW_FAULT_INJECTION": "1",
                "PYTHONPATH": str(ROOT),
            }
        )
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self.log_handle = self.log_path.open("ab")
        self.process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "app.main:app",
                "--host",
                "127.0.0.1",
                "--port",
                str(self.port),
                "--log-level",
                "warning",
            ],
            cwd=ROOT,
            env=env,
            stdout=self.log_handle,
            stderr=subprocess.STDOUT,
        )
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(
                    f"Server exited during startup with {self.process.returncode}; see {self.log_path}"
                )
            try:
                health = self.api.get("/health")
                if health.get("ok"):
                    return
            except (urllib.error.URLError, ConnectionError, TimeoutError, json.JSONDecodeError):
                time.sleep(0.1)
        raise RuntimeError(f"Server did not become ready; see {self.log_path}")

    def wait_for_crash(self, timeout: float = 20.0) -> int:
        if self.process is None:
            raise RuntimeError("Server was not started")
        code = self.process.wait(timeout=timeout)
        if self.log_handle is not None:
            self.log_handle.close()
            self.log_handle = None
        return int(code)

    def stop(self) -> None:
        if self.process is not None and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=10)
        if self.log_handle is not None:
            self.log_handle.close()
            self.log_handle = None


def read_db_facts(db_path: Path) -> dict[str, Any]:
    conn = sqlite3.connect(str(db_path), timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        states = {
            row["snapshot_id"]: row["state"]
            for row in conn.execute("SELECT snapshot_id, state FROM snapshots ORDER BY snapshot_id")
        }
        pending_intents = [
            dict(row)
            for row in conn.execute(
                "SELECT intent_id, operation, state FROM intents WHERE state = 'PENDING'"
            )
        ]
        in_progress_requests = [
            row["request_id"]
            for row in conn.execute("SELECT request_id FROM requests WHERE state = 'IN_PROGRESS'")
        ]
        return {
            "snapshot_states": states,
            "pending_intents": pending_intents,
            "in_progress_requests": in_progress_requests,
        }
    finally:
        conn.close()


def remove_database(path: Path) -> None:
    for candidate in (path, Path(str(path) + "-wal"), Path(str(path) + "-shm")):
        if candidate.exists():
            candidate.unlink()


def content_for(seed: int, phase: str, block_no: int, revision: int) -> str:
    return f"seed={seed}|phase={phase}|block={block_no:02d}|revision={revision}"


def assert_true(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def write_phase(api: Api, seed: int, phase: str, entries: list[dict[str, int]]) -> list[dict[str, Any]]:
    responses = []
    for entry in entries:
        block_no = int(entry["block_no"])
        revision = int(entry["revision"])
        content = content_for(seed, phase, block_no, revision)
        response = api.post(
            "/api/volumes/vol0/blocks/write",
            {
                "block_no": block_no,
                "content": content,
                "sha256": sha256_text(content),
                "request_id": f"write-{phase}-{block_no}",
            },
        )
        responses.append(response)
    return responses


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the local COW snapshot end-to-end demo.")
    parser.add_argument(
        "--artifacts-dir",
        type=Path,
        default=ROOT / "artifacts" / "e2e",
        help="Directory for the SQLite database, logs, and JSON result.",
    )
    args = parser.parse_args()

    sample = json.loads((ROOT / "samples" / "blocks.json").read_text(encoding="utf-8"))
    seed = int((ROOT / sample["seed_file"]).read_text(encoding="utf-8").strip())
    volume_id = str(sample["volume_id"])
    block_count = int(sample["block_count"])
    snapshots = sample["snapshots"]

    artifacts_dir = args.artifacts_dir.resolve()
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    db_path = artifacts_dir / "cow-demo.sqlite3"
    remove_database(db_path)

    server = Server(db_path, block_count, artifacts_dir / "uvicorn.log")
    result: dict[str, Any] = {
        "seed": seed,
        "volume_id": volume_id,
        "block_count": block_count,
        "database": str(db_path),
    }
    try:
        server.start()
        api = server.api
        initial = api.get(f"/api/volumes/{volume_id}/blocks")
        assert_true(initial["consistency_seq"] == 1, "Initial consistency sequence should be 1")

        write_phase(api, seed, "before_full", sample["phases"]["before_full"])
        full = api.post(
            f"/api/volumes/{volume_id}/snapshots/full",
            {"snapshot_id": snapshots["full"], "request_id": "create-full"},
        )
        assert_true(full["snapshot"]["ref_count"] == block_count, "Full snapshot should freeze all blocks")

        cow_writes = write_phase(api, seed, "after_full", sample["phases"]["after_full"])
        assert_true(all(item["cow_performed"] for item in cow_writes), "Post-snapshot writes must perform COW")
        inc1 = api.post(
            f"/api/volumes/{volume_id}/snapshots/incremental",
            {
                "snapshot_id": snapshots["incremental_1"],
                "parent_id": snapshots["full"],
                "request_id": "create-inc-1",
            },
        )
        assert_true(inc1["snapshot"]["ref_count"] == 2, "First incremental snapshot should contain two deltas")

        write_phase(api, seed, "after_incremental_1", sample["phases"]["after_incremental_1"])
        inc2 = api.post(
            f"/api/volumes/{volume_id}/snapshots/incremental",
            {
                "snapshot_id": snapshots["incremental_2"],
                "parent_id": snapshots["incremental_1"],
                "request_id": "create-inc-2",
            },
        )
        assert_true(inc2["snapshot"]["ref_count"] == 1, "Second incremental snapshot should contain one delta")

        chain_before = api.get(f"/api/volumes/{volume_id}/chain")
        merged = api.post(
            f"/api/volumes/{volume_id}/snapshots/{snapshots['incremental_1']}/merge",
            {"target_snapshot_id": snapshots["full"], "request_id": "merge-adjacent"},
        )
        assert_true(merged["source_state_after"] == "MERGED", "Merge source must remain as MERGED")
        assert_true(merged["chain_depth_after"] == merged["chain_depth_before"] - 1, "Merge must reduce chain depth")

        diff = api.get(f"/api/volumes/{volume_id}/snapshots/{snapshots['full']}/diff")
        assert_true(diff["write_back_blocks"] == 1, "Rollback diff should contain one changed block")
        rollback = api.post(
            f"/api/volumes/{volume_id}/snapshots/{snapshots['full']}/rollback",
            {"request_id": "rollback-to-merged-target"},
        )
        assert_true(rollback["write_back_blocks"] == 1, "Rollback should write back one block")
        assert_true(rollback["unchanged_blocks"] == block_count - 1, "All other blocks should be unaffected")

        pre_crash_facts = read_db_facts(db_path)
        crash_request_error: str | None = None
        try:
            api.post(
                f"/api/volumes/vol0/snapshots/{snapshots['incremental_2']}/merge",
                {
                    "target_snapshot_id": snapshots["full"],
                    "request_id": "crash-merge",
                    "crash_after_begin": True,
                },
                timeout=3.0,
            )
        except (urllib.error.URLError, ConnectionError, TimeoutError, OSError) as exc:
            crash_request_error = type(exc).__name__
        exit_code = server.wait_for_crash()
        assert_true(exit_code == 86, f"Expected forced exit code 86, got {exit_code}")
        after_crash_facts = read_db_facts(db_path)
        assert_true(after_crash_facts["snapshot_states"][snapshots["incremental_2"]] == "MERGING", "Crash must leave a durable MERGING fact")
        assert_true(len(after_crash_facts["pending_intents"]) == 1, "Crash must leave a pending intent")
        assert_true(len(after_crash_facts["in_progress_requests"]) == 1, "Crash must leave an in-progress request")

        restarted = Server(db_path, block_count, artifacts_dir / "uvicorn-restart.log")
        server = restarted
        restarted.start()
        report = restarted.api.get(f"/api/volumes/{volume_id}/recovery-report")
        assert_true(report["status"] == "RECOVERED", "Recovery report should report RECOVERED")
        assert_true(report["durable_only"] is True, "Recovery report must be durable-only")
        assert_true(report["summary"]["intents_aborted"] == 1, "One intent should be aborted")
        assert_true(report["summary"]["requests_aborted"] == 1, "One request should be aborted")
        assert_true(report["consistency"]["ok"] is True, "Recovery consistency checks should pass")
        recovered_chain = restarted.api.get(f"/api/volumes/{volume_id}/chain")
        recovered_source = next(
            item for item in recovered_chain["snapshots"]
            if item["snapshot_id"] == snapshots["incremental_2"]
        )
        assert_true(recovered_source["state"] == "COMMITTED", "MERGING source should recover to COMMITTED")

        retried_merge = restarted.api.post(
            f"/api/volumes/{volume_id}/snapshots/{snapshots['incremental_2']}/merge",
            {"target_snapshot_id": snapshots["full"], "request_id": "merge-after-recovery"},
        )
        assert_true(retried_merge["source_state_after"] == "MERGED", "Recovered chain should accept a new merge")
        final_chain = restarted.api.get(f"/api/volumes/{volume_id}/chain")

        result.update(
            {
                "chain_before_merge": {
                    "chain_depth": chain_before["chain_depth"],
                    "reference_block_count": chain_before["reference_block_count"],
                    "active_snapshot_count": chain_before["active_snapshot_count"],
                },
                "merge_result": {
                    "source_state_after": merged["source_state_after"],
                    "chain_depth_before": merged["chain_depth_before"],
                    "chain_depth_after": merged["chain_depth_after"],
                    "reference_block_count_before": merged["reference_block_count_before"],
                    "reference_block_count_after": merged["reference_block_count_after"],
                    "refs_rewritten": merged["refs_rewritten"],
                    "children_reparented": merged["children_reparented"],
                },
                "rollback_diff": {
                    "write_back_blocks": diff["write_back_blocks"],
                    "unchanged_blocks": diff["unchanged_blocks"],
                    "overwritten_current_versions": diff["overwritten_current_versions"],
                },
                "rollback_result": {
                    "write_back_blocks": rollback["write_back_blocks"],
                    "unchanged_blocks": rollback["unchanged_blocks"],
                    "consistency_seq_before": rollback["current_consistency_seq_before"],
                    "consistency_seq_after": rollback["current_consistency_seq_after"],
                },
                "forced_termination": {
                    "http_client_error": crash_request_error,
                    "server_exit_code": exit_code,
                    "pre_crash_facts": pre_crash_facts,
                    "durable_facts_after_crash": after_crash_facts,
                },
                "recovery_report": {
                    "status": report["status"],
                    "durable_only": report["durable_only"],
                    "summary": report["summary"],
                    "snapshot_repairs": report["snapshot_repairs"],
                    "consistency_ok": report["consistency"]["ok"],
                    "note": report["note"],
                },
                "post_recovery_merge": {
                    "source_state_after": retried_merge["source_state_after"],
                    "chain_depth_before": retried_merge["chain_depth_before"],
                    "chain_depth_after": retried_merge["chain_depth_after"],
                    "reference_block_count_after": retried_merge["reference_block_count_after"],
                },
                "final_chain": {
                    "chain_depth": final_chain["chain_depth"],
                    "active_snapshot_count": final_chain["active_snapshot_count"],
                    "reference_block_count": final_chain["reference_block_count"],
                    "state_counts": final_chain["state_counts"],
                },
                "conclusion": "PASS: snapshot chain, incremental merge, rollback diff, and forced-termination recovery all verified from durable SQLite state.",
            }
        )
        result_path = artifacts_dir / "e2e_demo_result.json"
        result_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
        print(json.dumps(result, indent=2, ensure_ascii=False))
        print(f"\nResult written to {result_path}")
        return 0
    finally:
        server.stop()


if __name__ == "__main__":
    raise SystemExit(main())

