from __future__ import annotations

from pathlib import Path

import pytest

from delay_queue.config import Settings
from delay_queue.mock_endpoint import start_server
from delay_queue.service import DelayQueueService


@pytest.fixture
def endpoint():
    server, thread = start_server(port=0)
    port = server.server_address[1]
    try:
        yield f"http://127.0.0.1:{port}/dispatch", server.state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=1)


@pytest.fixture
def settings(tmp_path: Path, endpoint):
    base_url, _ = endpoint
    return Settings(
        db_path=tmp_path / "queue.db",
        intent_log_dir=tmp_path / "intent_logs",
        default_endpoint=base_url,
        global_concurrency=16,
        default_max_concurrency=4,
        default_time_budget_ms=1_000,
        default_max_task_resource=100,
        default_waiting_queue_capacity=100,
        default_dispatch_timeout_ms=500,
        retry_backoff_base_ms=40,
        round_wait_timeout_ms=2_000,
    )


@pytest.fixture
def service(settings):
    instance = DelayQueueService(settings)
    instance.startup()
    yield instance


def task(
    task_id: str,
    *,
    mode: str = "success",
    due_at_ms: int = 0,
    priority: int = 5,
    retries: int = 0,
    resource: int = 1,
    **payload,
):
    return {
        "task_id": task_id,
        "due_at_ms": due_at_ms,
        "priority": priority,
        "remaining_retries": retries,
        "resource_requirement": resource,
        "payload": {"mode": mode, **payload},
    }

