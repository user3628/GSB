from __future__ import annotations

import sqlite3

import pytest

from obs_sampler.errors import BusinessError
from obs_sampler.models import BatchRequest

from .conftest import make_log, make_span


def _row_count(service, table: str) -> int:
    connection = sqlite3.connect(service.storage.path)
    try:
        return int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
    finally:
        connection.close()


def test_non_monotonic_logs_reject_entire_batch(service, sample_policy_v1):
    service.submit_policy(sample_policy_v1)
    request = BatchRequest(
        batch_id="bad-log-order",
        logs=[
            make_log(0, timestamp_ms=2000),
            make_log(1, timestamp_ms=1999),
        ],
        spans=[make_span(2, timestamp_ms=3000)],
    )
    with pytest.raises(BusinessError) as raised:
        service.submit_batch(request)
    assert raised.value.code == "E_RECORD_INVALID"
    assert raised.value.details["reason"] == "TIMESTAMP_NOT_MONOTONIC"
    assert raised.value.details["stream"] == "logs"
    assert _row_count(service, "batches") == 0
    assert _row_count(service, "records") == 0
    assert _row_count(service, "decisions") == 0


def test_non_monotonic_spans_reject_entire_batch(service, sample_policy_v1):
    service.submit_policy(sample_policy_v1)
    request = BatchRequest(
        batch_id="bad-span-order",
        logs=[make_log(0, timestamp_ms=1000)],
        spans=[
            make_span(1, timestamp_ms=5000),
            make_span(2, timestamp_ms=4000),
        ],
    )
    with pytest.raises(BusinessError) as raised:
        service.submit_batch(request)
    assert raised.value.code == "E_RECORD_INVALID"
    assert raised.value.details["stream"] == "spans"
    assert _row_count(service, "batches") == 0


def test_duplicate_record_identity_is_invalid(service, sample_policy_v1):
    service.submit_policy(sample_policy_v1)
    duplicate = make_span(0, trace_id="same-trace", span_id="same-span")
    request = BatchRequest(
        batch_id="duplicate-identity",
        spans=[duplicate, {**duplicate, "timestamp_ms": 1001}],
    )
    with pytest.raises(BusinessError) as raised:
        service.submit_batch(request)
    assert raised.value.code == "E_RECORD_INVALID"
    assert raised.value.details["reason"] == "DUPLICATE_RECORD_IDENTITY"
    assert _row_count(service, "records") == 0


def test_batch_idempotency_and_conflict(service, sample_policy_v1):
    service.submit_policy(sample_policy_v1)
    request = BatchRequest(
        batch_id="idem-batch",
        logs=[make_log(0), make_log(1)],
        spans=[make_span(2)],
    )
    first = service.submit_batch(request)
    assert first["execution"] == "COMMITTED_ONCE"
    assert first["record_count"] == 3
    assert _row_count(service, "records") == 3

    replay = service.submit_batch(request)
    assert replay["execution"] == "REPLAYED_COMMITTED_RESULT"
    assert replay["decision_states"] == first["decision_states"]
    assert _row_count(service, "records") == 3
    assert _row_count(service, "batches") == 1

    conflicting = BatchRequest(
        batch_id="idem-batch",
        logs=[make_log(0, sample_weight=3.0), make_log(1)],
        spans=[make_span(2)],
    )
    with pytest.raises(BusinessError) as raised:
        service.submit_batch(conflicting)
    assert raised.value.code == "E_CONFLICTING_RECORD"
    assert _row_count(service, "records") == 3


def test_policy_requires_digest_and_strictly_increasing_version(
    service, sample_policy_v1, sample_policy_v2
):
    tampered = dict(sample_policy_v1)
    tampered["strata"] = {"checkout": {"INFO": 0.01}}
    with pytest.raises(BusinessError) as invalid:
        service.submit_policy(tampered)
    assert invalid.value.code == "E_POLICY_INVALID"

    service.submit_policy(sample_policy_v1)
    with pytest.raises(BusinessError) as stale:
        service.submit_policy(sample_policy_v1)
    assert stale.value.code == "E_STALE_SNAPSHOT"
    assert stale.value.details["current_version"] == 1

    service.submit_policy(sample_policy_v2)
    assert service.get_policy()["policy_version"] == 2


def test_batch_requires_current_policy_version_if_supplied(service, sample_policy_v1):
    service.submit_policy(sample_policy_v1)
    request = BatchRequest(batch_id="wrong-version", policy_version=9, logs=[make_log(0)])
    with pytest.raises(BusinessError) as raised:
        service.submit_batch(request)
    assert raised.value.code == "E_STALE_SNAPSHOT"
    assert _row_count(service, "batches") == 0


def test_committed_facts_survive_new_service_instance(
    seeded_service, sample_policy_v1
):
    from obs_sampler.service import SamplerService

    restarted = SamplerService(seeded_service.storage.path)
    assert restarted.get_policy()["policy_version"] == 1
    state = restarted.get_policy_state(1)
    assert state["commit"]["record_count"] == 1416
    assert state["decision_states"] == seeded_service.get_policy_state(1)["decision_states"]
