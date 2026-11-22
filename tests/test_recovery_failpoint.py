from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path

import httpx

from tests.support import (
    current_version,
    free_port,
    install_full_snapshot,
    reverse_route,
    start_workbench,
    stop_process,
    wait_port_free,
)


def test_forced_termination_rolls_back_orphan_intent(
    tmp_path: Path, local_services
) -> None:
    host = local_services.host
    port = free_port(host)
    db_path = tmp_path / "crash.sqlite3"
    log_a = tmp_path / "app-a.log"
    log_b = tmp_path / "app-b.log"
    base_url = f"http://{host}:{port}"
    process = start_workbench(
        host=host,
        port=port,
        db_path=db_path,
        enable_failpoints=True,
        log_path=log_a,
    )
    restarted = None
    thread: threading.Thread | None = None
    errors: list[str] = []
    try:
        install_full_snapshot(
            base_url, [reverse_route("alpha", "/alpha", local_services.backend_port)]
        )
        base = current_version(base_url)
        payload = {
            "base_version": base,
            "upserts": [
                reverse_route(
                    "alpha",
                    "/alpha",
                    local_services.alt_backend_port,
                    timeout_ms=777,
                )
            ],
        }

        def blocked() -> None:
            try:
                httpx.post(
                    base_url + "/workbench/api/config/incremental",
                    json=payload,
                    headers={"X-Workbench-Failpoint": "crash-after-intent"},
                    timeout=30.0,
                )
            except Exception as exc:
                errors.append(type(exc).__name__)

        thread = threading.Thread(target=blocked, daemon=True)
        thread.start()
        orphan_id = None
        deadline = time.monotonic() + 8.0
        while time.monotonic() < deadline:
            with sqlite3.connect(str(db_path), timeout=2.0) as connection:
                row = connection.execute(
                    "SELECT id, target_version FROM intents WHERE state='PREPARED' ORDER BY id DESC LIMIT 1"
                ).fetchone()
            if row:
                orphan_id, target_version = row
                break
            time.sleep(0.05)
        assert orphan_id is not None
        assert target_version == base + 1

        process.kill()
        process.wait(timeout=5.0)
        thread.join(timeout=5.0)
        assert errors
        wait_port_free(host, port)

        restarted = start_workbench(
            host=host,
            port=port,
            db_path=db_path,
            enable_failpoints=True,
            log_path=log_b,
        )
        health = httpx.get(base_url + "/workbench/api/health", timeout=5.0).json()
        assert health["current_config_version"] == base
        recovery = httpx.get(base_url + "/workbench/api/recovery", timeout=5.0).json()
        startup = recovery["startup_recovery"]
        assert startup["last_complete_version"] == base
        assert startup["orphan_intent_ids"] == [orphan_id]
        assert startup["rolled_back_intent_count"] == 1
        versions = httpx.get(base_url + "/workbench/api/versions", timeout=5.0).json()
        assert [item["version"] for item in versions["versions"]] == [base]
    finally:
        if thread is not None and thread.is_alive():
            thread.join(timeout=1.0)
        stop_process(restarted, force=True)
        stop_process(process, force=True)
        for started in (process, restarted):
            if started is not None:
                handle = getattr(started, "_test_log_handle", None)
                if handle is not None:
                    handle.close()
