from __future__ import annotations

import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from gateway_middleware.errors import E_CONFLICTING_REQUEST
from gateway_middleware.models import BatchInput
from gateway_middleware.sample_data import generate_small, seed_dataset
from gateway_middleware.service import GatewayService

from .conftest import event_for_token, seeded_tiny_service


def test_batch_replay_and_cross_batch_request_reuse(small_service):
    service, dataset = small_service
    first = service.submit_batch(dataset.batch("batch-a"))
    first_window_count = _window_event_count(service)

    replay = service.submit_batch(dataset.batch("batch-a"))
    assert replay.replayed is True
    assert replay.commit_version == first.commit_version
    assert _window_event_count(service) == first_window_count

    reused = service.submit_batch(dataset.batch("batch-b"))
    assert reused.reused_event_count == len(dataset.events)
    assert reused.decided_event_count == 0
    assert all(decision.reused for decision in reused.decisions)
    assert _window_event_count(service) == first_window_count


def test_conflicting_batch_and_request_content(small_service):
    service, dataset = small_service
    service.submit_batch(dataset.batch("batch-a"))

    changed_batch = dataset.batch("batch-a")
    changed_batch.events[0].timestamp_ms += 1
    with pytest.raises(Exception) as batch_error:
        service.submit_batch(changed_batch)
    assert getattr(batch_error.value, "code", None) == E_CONFLICTING_REQUEST

    changed_request = dataset.batch("batch-c")
    changed_request.events[0].request_id = dataset.events[0].request_id
    changed_request.events[0].timestamp_ms += 10
    with pytest.raises(Exception) as request_error:
        service.submit_batch(changed_request)
    assert getattr(request_error.value, "code", None) == E_CONFLICTING_REQUEST


def test_duplicate_request_alias_in_same_batch_is_decided_once(tmp_path):
    service, token, seeded = seeded_tiny_service(tmp_path)
    base = 1_700_000_000_000
    primary = event_for_token(token, seq=0, timestamp_ms=base)
    primary.request_id = "same-request"
    alias = primary.model_copy(update={"event_seq": 1})
    result = service.submit_batch(BatchInput(batch_id="aliases", events=[primary, alias]))

    assert result.event_count == 2
    assert result.decided_event_count == 1
    assert result.reused_event_count == 1
    assert len(result.decisions) == 2
    assert sorted(decision.reused for decision in result.decisions) == [False, True]
    assert _window_event_count(service) == 1
    with sqlite3.connect(service.store.db_path) as connection:
        decision_count = connection.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
    assert decision_count == 1


def test_concurrent_batches_are_serialized_without_cross_contamination(tmp_path):
    service, token, seeded = seeded_tiny_service(tmp_path, short_limit=100, long_limit=1_000)
    base = 1_700_000_000_000
    batches = []
    for batch_index in range(4):
        events = [
            event_for_token(
                token,
                seq=0,
                timestamp_ms=base + event_index,
                route=f"/route-{batch_index}",
            ).model_copy(
                update={
                    "event_seq": event_index,
                    "request_id": f"b{batch_index}-r{event_index}",
                }
            )
            for event_index in range(5 + batch_index)
        ]
        batches.append(BatchInput(batch_id=f"concurrent-{batch_index}", events=events))

    barrier = threading.Barrier(len(batches))
    def submit(batch):
        barrier.wait()
        return service.submit_batch(batch)

    with ThreadPoolExecutor(max_workers=len(batches)) as executor:
        results = list(executor.map(submit, batches))

    versions = sorted(result.commit_version for result in results)
    assert versions == list(range(seeded + 1, seeded + 1 + len(batches)))
    assert _window_event_count(service) == sum(result.event_count for result in results)
    for batch_index in range(len(batches)):
        windows = service.query_windows(
            route_key=f"/route-{batch_index}",
            client_id="c1",
            at_ms=base + 10,
        )
        assert windows["short"]["event_count"] == 5 + batch_index
    with sqlite3.connect(service.store.db_path) as connection:
        pending = connection.execute(
            "SELECT COUNT(*) FROM intents WHERE status != 'COMMITTED'"
        ).fetchone()[0]
    assert pending == 0


def test_concurrent_duplicate_request_is_decided_once(tmp_path):
    service, token, seeded = seeded_tiny_service(tmp_path, short_limit=100, long_limit=1_000)
    base = 1_700_000_000_000
    payload = event_for_token(token, seq=0, timestamp_ms=base)
    payload.request_id = "shared-request"
    payload.route_key = "/shared"
    batches = [
        BatchInput(batch_id=f"shared-batch-{index}", events=[payload.model_copy()])
        for index in range(2)
    ]
    barrier = threading.Barrier(2)

    def submit(batch):
        barrier.wait()
        return service.submit_batch(batch)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(submit, batches))

    assert _window_event_count(service) == 1
    assert sum(result.decided_event_count for result in results) == 1
    assert sum(result.reused_event_count for result in results) == 1
    with sqlite3.connect(service.store.db_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM decisions").fetchone()[0] == 1


def _window_event_count(service: GatewayService) -> int:
    with sqlite3.connect(service.store.db_path) as connection:
        return int(connection.execute("SELECT COUNT(*) FROM window_events").fetchone()[0])


def test_many_concurrent_readers_observe_one_complete_snapshot(small_service):
    service, dataset = small_service
    result = service.submit_batch(dataset.batch("read-snapshot-batch"))
    request_id = next(
        decision.request_id for decision in result.decisions if decision.decision_code == "ALLOW"
    )
    barrier = threading.Barrier(12)

    def read_once():
        barrier.wait()
        decision = service.get_decision(request_id)
        windows = service.query_windows(
            route_key=decision.route_key,
            client_id=decision.client_id,
            at_ms=decision.timestamp_ms,
        )
        return decision.commit_version, windows["snapshot_version"]

    with ThreadPoolExecutor(max_workers=12) as executor:
        observations = list(executor.map(lambda _: read_once(), range(12)))

    assert all(commit == snapshot for commit, snapshot in observations)
    assert {snapshot for _, snapshot in observations} == {result.commit_version}
