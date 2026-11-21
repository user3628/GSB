from __future__ import annotations

import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor

from obs_sampler.models import BatchRequest
from obs_sampler.service import SamplerService

from .conftest import make_log, policy_document


def _init_service(service, *, record_count: int = 20) -> None:
    service.submit_policy(policy_document(rates={"checkout": {"INFO": 1.0}}))
    service.submit_batch(
        BatchRequest(
            batch_id="initial",
            logs=[make_log(index, trace_id=f"init-{index}") for index in range(record_count)],
        )
    )


def test_concurrent_duplicate_batch_executes_once_and_others_replay(service):
    _init_service(service, record_count=5)
    request = BatchRequest(
        batch_id="concurrent-duplicate",
        logs=[make_log(index, trace_id=f"new-{index}") for index in range(80)],
    )

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(lambda _index: service.submit_batch(request), range(8)))

    assert sum(result["execution"] == "COMMITTED_ONCE" for result in results) == 1
    assert sum(result["execution"] == "REPLAYED_COMMITTED_RESULT" for result in results) == 7
    assert {result["record_count"] for result in results} == {80}
    assert {tuple(sorted(result["decision_states"].items())) for result in results} == {
        tuple(sorted(results[0]["decision_states"].items()))
    }

    connection = sqlite3.connect(service.storage.path)
    try:
        intent_count = connection.execute(
            "SELECT COUNT(*) FROM intents WHERE kind = 'BATCH_COMMIT' AND batch_id = ?",
            ("concurrent-duplicate",),
        ).fetchone()[0]
        batch_count = connection.execute(
            "SELECT COUNT(*) FROM batches WHERE batch_id = ?", ("concurrent-duplicate",)
        ).fetchone()[0]
    finally:
        connection.close()
    assert intent_count == 1
    assert batch_count == 1


def test_readers_only_see_last_complete_snapshot_during_uncommitted_write(
    service, monkeypatch
):
    _init_service(service, record_count=30)
    old_state = service.get_policy_state(1)
    assert old_state["commit"]["record_count"] == 30

    reached_uncommitted_state = threading.Event()
    release_writer = threading.Event()
    original_replace = service.storage.replace_derived_state

    def paused_replace(*args, **kwargs):
        result = original_replace(*args, **kwargs)
        reached_uncommitted_state.set()
        if not release_writer.wait(timeout=10):
            raise TimeoutError("test writer was not released")
        return result

    monkeypatch.setattr(service.storage, "replace_derived_state", paused_replace)
    request = BatchRequest(
        batch_id="isolation-batch",
        logs=[make_log(index, trace_id=f"iso-{index}") for index in range(50)],
    )
    writer = threading.Thread(target=service.submit_batch, args=(request,), daemon=True)
    writer.start()
    assert reached_uncommitted_state.wait(timeout=10)

    with ThreadPoolExecutor(max_workers=8) as executor:
        snapshots = list(
            executor.map(lambda _index: service.get_policy_state(1), range(8))
        )
    for snapshot in snapshots:
        assert snapshot["commit"]["record_count"] == 30
        assert sum(snapshot["decision_states"].values()) == 30
        assert all(row["population_count"] <= 30 for row in snapshot["counts"])

    release_writer.set()
    writer.join(timeout=10)
    assert not writer.is_alive()
    new_state = service.get_policy_state(1)
    assert new_state["commit"]["record_count"] == 80
    assert sum(new_state["decision_states"].values()) == 80


def test_duplicate_batch_across_two_service_instances_reuses_committed_result(service):
    _init_service(service, record_count=5)
    other = SamplerService(service.storage.path)
    request = BatchRequest(
        batch_id="cross-instance-duplicate",
        logs=[make_log(index, trace_id=f"cross-{index}") for index in range(120)],
    )

    with ThreadPoolExecutor(max_workers=2) as executor:
        first_future = executor.submit(service.submit_batch, request)
        second_future = executor.submit(other.submit_batch, request)
        results = [first_future.result(), second_future.result()]

    assert sum(result["execution"] == "COMMITTED_ONCE" for result in results) == 1
    assert sum(result["execution"] == "REPLAYED_COMMITTED_RESULT" for result in results) == 1
    assert {result["record_count"] for result in results} == {120}

    connection = sqlite3.connect(service.storage.path)
    try:
        batch_count = connection.execute(
            "SELECT COUNT(*) FROM batches WHERE batch_id = ?",
            ("cross-instance-duplicate",),
        ).fetchone()[0]
    finally:
        connection.close()
    assert batch_count == 1
