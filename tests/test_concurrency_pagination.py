from __future__ import annotations

import json
import threading

import pytest

from app.errors import AppError, ErrorCode
from tests.conftest import T0


def app(request_id: str, tenant_id: str = "starter", arrival: int = T0 + 500, cpu: int = 10) -> dict:
    return {
        "request_id": request_id,
        "tenant_id": tenant_id,
        "namespace": "shared-prod",
        "arrival_ms": arrival,
        "cpu_millicores": cpu,
        "memory_bytes": 1024,
    }


def test_concurrent_duplicate_request_is_adjudicated_once(loaded_service):
    svc = loaded_service
    barrier = threading.Barrier(8)
    results: list[tuple[str, int | None, str, object]] = []
    lock = threading.Lock()

    def worker():
        barrier.wait()
        execution = svc.submit_decision(app("same-request", cpu=100))
        with lock:
            results.append(
                (
                    "ok",
                    execution.decision["decision_id"],
                    execution.decision["decision"],
                    execution.replayed,
                )
            )

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(results) == 8
    assert len({item[1] for item in results}) == 1
    assert all(item[2] == "ACCEPTED" for item in results)
    assert sum(1 for item in results if item[3] is False) == 1
    assert svc.list_decisions(page_size=100)["total"] == 1


def test_same_request_id_with_different_payload_conflicts(loaded_service):
    svc = loaded_service
    svc.submit_decision(app("conflict-id", cpu=10))
    with pytest.raises(AppError) as exc:
        svc.submit_decision(app("conflict-id", cpu=11))
    assert exc.value.code == ErrorCode.E_CONFLICTING_REQUEST
    assert svc.list_decisions(page_size=100)["total"] == 1


def test_pagination_pinned_revision_is_exact(loaded_service, batch):
    svc = loaded_service
    svc.submit_batch(batch)
    pinned = svc.meta()["revision"]
    full = svc.list_decisions(page=1, page_size=1000, revision=pinned)
    assert full["total"] == 11
    concatenated = []
    first_chunk = svc.list_decisions(page=1, page_size=3, revision=pinned)
    for page in range(1, first_chunk["pages"] + 1):
        chunk = svc.list_decisions(page=page, page_size=3, revision=pinned)
        assert chunk["revision"] == pinned
        assert chunk["total"] == full["total"]
        concatenated.extend(chunk["items"])
    assert concatenated == full["items"]
    assert len({item["request_id"] for item in concatenated}) == 11


def test_pagination_stays_stable_while_writer_commits_new_revision(loaded_service, batch):
    svc = loaded_service
    svc.submit_batch(batch)
    pinned = svc.meta()["revision"]
    full = svc.list_decisions(page=1, page_size=1000, revision=pinned)

    writer_started = threading.Event()
    writer_done = threading.Event()
    captured = {}

    def writer():
        writer_started.set()
        svc.submit_decision(app("new-after-pin", tenant_id="bob", arrival=T0 + 5000, cpu=10))
        writer_done.set()

    def reader():
        writer_started.wait()
        items = []
        page = 1
        while True:
            part = svc.list_decisions(page=page, page_size=2, revision=pinned)
            items.extend(part["items"])
            if page >= part["pages"]:
                break
            page += 1
        captured["items"] = items

    writer_thread = threading.Thread(target=writer)
    reader_thread = threading.Thread(target=reader)
    writer_thread.start()
    reader_thread.start()
    writer_thread.join()
    reader_thread.join()
    assert writer_done.is_set()
    assert captured["items"] == full["items"]
    assert svc.meta()["revision"] > pinned
    current = svc.list_decisions(page_size=1000)
    assert current["total"] == full["total"] + 1
    assert current["items"][-1]["request_id"] == "new-after-pin"
