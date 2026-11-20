from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import pytest

from conftest import ROOT, SAMPLES
from shardroute.datastore import SQLiteDataStore
from shardroute.engine import ShardRouteService
from shardroute.errors import BusinessError
from shardroute.util import fingerprint as make_fingerprint


def http_json(base_url: str, method: str, path: str, payload=None, timeout=10):
    body = None
    headers = {}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = Request(base_url + path, data=body, headers=headers, method=method)
    try:
        with urlopen(request, timeout=timeout) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


def test_startup_recovery_uses_only_persisted_facts(tmp_path, seed_document):
    data_dir = tmp_path / "data"
    metadata_db = tmp_path / "metadata.sqlite3"
    SQLiteDataStore(data_dir).initialize(seed_document)
    first = ShardRouteService(
        metadata_db=metadata_db,
        data_dir=data_dir,
        snapshot_file=SAMPLES / "snapshot_v1.json",
    )
    running_request = {
        "logical_table": "orders",
        "query_type": "scan",
        "limit": 100,
        "routing_version": 1,
    }
    fingerprint = make_fingerprint(running_request)
    first.store.create_query_batch(
        request_id="crashed-direct-query",
        fingerprint=fingerprint,
        routing_version=1,
        intent_id="query:crashed-direct-query",
        request_detail=running_request,
    )
    first.store.create_intent(
        intent_id="snapshot:interrupted-direct",
        kind="SNAPSHOT_COMMIT",
        routing_version=2,
        detail={"simulated": "process died after intent, before metadata commit"},
    )
    first.close(graceful=False)

    second = ShardRouteService(metadata_db=metadata_db, data_dir=data_dir)
    try:
        report = second.latest_recovery_report()
        assert report["report_basis"] == "persisted_sqlite_metadata_only"
        assert report["persisted_facts_only"] is True
        assert report["crash_window_detected"] is True
        assert report["complete_exit_report_available"] is False
        assert report["active_version_before_recovery"] == 1
        assert report["active_version_after_recovery"] == 1
        assert report["last_complete_snapshot_version"] == 1
        assert report["rolled_back_to_last_complete_snapshot"] is False
        assert [item["request_id"] for item in report["aborted_query_batches"]] == [
            "crashed-direct-query"
        ]
        assert any(
            item["intent_id"] == "snapshot:interrupted-direct"
            for item in report["aborted_intents"]
        )
        assert report["cache_rebuild"]["routing_version"] == 1
        assert report["cache_rebuild"]["tables"] == [
            "catalog",
            "events",
            "order_items",
            "orders",
            "orders_view",
        ]
        batch = second.store.get_query_batch("crashed-direct-query")
        assert batch["status"] == "ABORTED"
        stored_error = json.loads(batch["error_json"])
        assert stored_error["code"] == "E_EXECUTION_ABORTED"
        assert second.current_state().version == 1
        counts = second.store.counts()
        assert counts["running_query_batches"] == 0

        retried = dict(running_request, request_id="crashed-direct-query")
        with pytest.raises(BusinessError) as caught:
            second.execute_query(retried)
        assert caught.value.code == "E_EXECUTION_ABORTED"
        assert caught.value.context["recovered"] is True
    finally:
        second.close(graceful=True)

    third = ShardRouteService(metadata_db=metadata_db, data_dir=data_dir)
    try:
        clean_report = third.latest_recovery_report()
        assert clean_report["crash_window_detected"] is False
        assert clean_report["complete_exit_report_available"] is True
    finally:
        third.close(graceful=True)


def test_startup_rolls_back_to_last_complete_snapshot_when_active_row_is_corrupt(
    tmp_path, seed_document, patch_document
):
    data_dir = tmp_path / "data"
    metadata_db = tmp_path / "metadata.sqlite3"
    SQLiteDataStore(data_dir).initialize(seed_document)
    first = ShardRouteService(
        metadata_db=metadata_db,
        data_dir=data_dir,
        snapshot_file=SAMPLES / "snapshot_v1.json",
    )
    first.apply_patch(patch_document)
    assert first.current_state().version == 2
    first.close(graceful=True)

    connection = sqlite3.connect(metadata_db)
    try:
        connection.execute(
            "UPDATE snapshots SET checksum = ? WHERE version = 2",
            ("0" * 64,),
        )
        connection.commit()
    finally:
        connection.close()

    recovered = ShardRouteService(metadata_db=metadata_db, data_dir=data_dir)
    try:
        report = recovered.latest_recovery_report()
        assert report["active_version_before_recovery"] == 2
        assert report["active_version_after_recovery"] == 1
        assert report["last_complete_snapshot_version"] == 1
        assert report["rolled_back_to_last_complete_snapshot"] is True
        assert report["rejected_snapshot_rows"][0]["version"] == 2
        assert recovered.current_state().version == 1
        with pytest.raises(BusinessError) as caught:
            recovered.execute_query(
                {
                    "request_id": "after-rollback-v2",
                    "logical_table": "orders",
                    "query_type": "eq",
                    "shard_key_value": 7,
                    "routing_version": 2,
                }
            )
        assert caught.value.code == "E_VERSION_RETIRED"
        ok = recovered.execute_query(
            {
                "request_id": "after-rollback-v1",
                "logical_table": "orders",
                "query_type": "eq",
                "shard_key_value": 7,
                "routing_version": 1,
            }
        )
        assert ok["physical_tables"][0]["table"] == "orders_0003"
    finally:
        recovered.close(graceful=True)


def wait_for_server(port_file: Path, process: subprocess.Popen, timeout: float = 20.0) -> str:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stdout = process.stdout.read() if process.stdout else ""
            stderr = process.stderr.read() if process.stderr else ""
            raise AssertionError(
                f"server exited before readiness: code={process.returncode} stdout={stdout} stderr={stderr}"
            )
        if port_file.exists():
            try:
                descriptor = json.loads(port_file.read_text(encoding="utf-8"))
                base_url = f"http://{descriptor['host']}:{descriptor['port']}"
                status, _ = http_json(base_url, "GET", "/health", timeout=2)
                if status == 200:
                    return base_url
            except Exception as exc:  # readiness polling intentionally retries
                last_error = exc
        time.sleep(0.05)
    raise AssertionError(f"server did not become ready: {last_error}")


def start_server(tmp_path: Path, data_dir: Path, metadata_db: Path, port_name: str):
    port_file = tmp_path / port_name
    port_file.unlink(missing_ok=True)
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(ROOT) + os.pathsep + environment.get("PYTHONPATH", "")
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "shardroute",
            "serve",
            "--metadata-db",
            str(metadata_db),
            "--data-dir",
            str(data_dir),
            "--host",
            "127.0.0.1",
            "--port",
            "0",
            "--port-file",
            str(port_file),
        ],
        cwd=str(ROOT),
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return process, wait_for_server(port_file, process)


def test_real_forced_termination_during_cross_table_merge(tmp_path, seed_document, snapshot_document):
    data_dir = tmp_path / "data"
    metadata_db = tmp_path / "metadata.sqlite3"
    SQLiteDataStore(data_dir).initialize(seed_document)

    first_process, base_url = start_server(tmp_path, data_dir, metadata_db, "port1.json")
    try:
        status, loaded = http_json(base_url, "POST", "/snapshot", snapshot_document)
        assert status == 200
        assert loaded["active_version"] == 1
        before_state = http_json(base_url, "GET", "/state")[1]
        assert before_state["active_version"] == 1

        crash_request = {
            "request_id": "crash-mid-merge",
            "logical_table": "orders",
            "query_type": "scan",
            "routing_version": 1,
            "limit": 100,
            "inject": "crash_mid_merge",
        }
        with pytest.raises((URLError, ConnectionError, TimeoutError)):
            http_json(base_url, "POST", "/query", crash_request, timeout=10)
        first_process.wait(timeout=15)
        assert first_process.returncode == 86
    finally:
        if first_process.poll() is None:
            first_process.kill()
            first_process.wait(timeout=10)

    second_process, second_url = start_server(
        tmp_path, data_dir, metadata_db, "port2.json"
    )
    try:
        status, recovery = http_json(second_url, "GET", "/recovery/report")
        assert status == 200
        assert recovery["facts_available"] is True
        report = recovery["report"]
        assert report["report_basis"] == "persisted_sqlite_metadata_only"
        assert report["persisted_facts_only"] is True
        assert report["crash_window_detected"] is True
        assert report["complete_exit_report_available"] is False
        assert "forced process termination" in report["report_limitation"]
        assert report["active_version_before_recovery"] == 1
        assert report["active_version_after_recovery"] == 1
        assert report["rolled_back_to_last_complete_snapshot"] is False
        assert [item["request_id"] for item in report["aborted_query_batches"]] == [
            "crash-mid-merge"
        ]

        status, state = http_json(second_url, "GET", "/state")
        assert status == 200
        assert state["active_version"] == 1
        assert state["metadata_counts"]["running_query_batches"] == 0
        batch = next(
            item
            for item in state["recent_query_batches"]
            if item["request_id"] == "crash-mid-merge"
        )
        assert batch["status"] == "ABORTED"

        status, retry_error = http_json(second_url, "POST", "/query", crash_request)
        assert status == 503
        assert retry_error["error"]["code"] == "E_EXECUTION_ABORTED"
        assert retry_error["error"]["context"]["recovered"] is True

        status, shutdown = http_json(second_url, "POST", "/shutdown", {})
        assert status == 200
        assert shutdown["ok"] is True
        second_process.wait(timeout=15)
        assert second_process.returncode == 0
    finally:
        if second_process.poll() is None:
            second_process.kill()
            second_process.wait(timeout=10)


def test_forced_termination_after_patch_and_cache_intents_keeps_previous_snapshot(
    tmp_path, seed_document, snapshot_document, patch_document
):
    data_dir = tmp_path / "data"
    metadata_db = tmp_path / "metadata.sqlite3"
    SQLiteDataStore(data_dir).initialize(seed_document)

    first_process, base_url = start_server(tmp_path, data_dir, metadata_db, "patch-port1.json")
    try:
        status, loaded = http_json(base_url, "POST", "/snapshot", snapshot_document)
        assert status == 200
        assert loaded["active_version"] == 1
        with pytest.raises((URLError, ConnectionError, TimeoutError)):
            http_json(
                base_url,
                "POST",
                "/snapshot/patch?crash_after_prepare=1",
                patch_document,
                timeout=10,
            )
        first_process.wait(timeout=15)
        assert first_process.returncode == 86
    finally:
        if first_process.poll() is None:
            first_process.kill()
            first_process.wait(timeout=10)

    second_process, second_url = start_server(
        tmp_path, data_dir, metadata_db, "patch-port2.json"
    )
    try:
        status, recovery = http_json(second_url, "GET", "/recovery/report")
        assert status == 200
        report = recovery["report"]
        assert report["crash_window_detected"] is True
        assert report["complete_exit_report_available"] is False
        assert report["active_version_before_recovery"] == 1
        assert report["active_version_after_recovery"] == 1
        aborted_kinds = {item["kind"] for item in report["aborted_intents"]}
        assert {"SNAPSHOT_PATCH", "CACHE_REBUILD"}.issubset(aborted_kinds)
        status, state = http_json(second_url, "GET", "/state")
        assert status == 200
        assert state["active_version"] == 1
        assert [item["version"] for item in state["snapshots"]] == [1]

        status, query = http_json(
            second_url,
            "POST",
            "/query",
            {
                "request_id": "after-patch-crash",
                "logical_table": "orders",
                "query_type": "eq",
                "shard_key_value": 7,
                "routing_version": 1,
            },
        )
        assert status == 200
        assert query["physical_tables"][0]["table"] == "orders_0003"
        status, _ = http_json(second_url, "POST", "/shutdown", {})
        assert status == 200
        second_process.wait(timeout=15)
        assert second_process.returncode == 0
    finally:
        if second_process.poll() is None:
            second_process.kill()
            second_process.wait(timeout=10)

