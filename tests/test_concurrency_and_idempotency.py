from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

from app.errors import DomainError
from app.models import IncrementalSnapshotInput

from conftest import increment_payload, submit_increment, submit_v1, submit_v2


def test_same_base_concurrent_increments_only_one_wins(store):
    submit_v1(store)
    submit_v2(store)
    barrier = threading.Barrier(2)

    def attempt(payload):
        barrier.wait()
        try:
            return "ok", submit_increment(store, payload)
        except DomainError as exc:
            return "error", exc.code

    payloads = [
        increment_payload("concurrent-a", digest_char="a"),
        increment_payload("concurrent-b", digest_char="b"),
    ]
    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(attempt, payloads))

    assert sorted(outcome[0] for outcome in outcomes) == ["error", "ok"]
    winner = next(outcome for outcome in outcomes if outcome[0] == "ok")
    loser = next(outcome for outcome in outcomes if outcome[0] == "error")
    assert winner[1]["snapshot_version"] == 3
    assert loser[1] == "E_STALE_BASE"
    assert store.current_version() == 3


def test_concurrent_duplicate_commit_id_executes_once(store):
    submit_v1(store)
    submit_v2(store)
    payload = increment_payload("same-commit", digest_char="c")
    barrier = threading.Barrier(2)

    def attempt():
        barrier.wait()
        return submit_increment(store, payload)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: attempt(), range(2)))

    assert [result["snapshot_version"] for result in results] == [3, 3]
    assert sum(1 for result in results if result.get("idempotent_replay")) == 1
    assert store.current_version() == 3


def test_conflicting_reuse_of_commit_id_does_not_mutate_state(store):
    submit_v1(store)
    submit_v2(store)
    original = increment_payload("conflict-id", digest_char="d")
    first = submit_increment(store, original)
    assert first["snapshot_version"] == 3

    conflicting = increment_payload("conflict-id", digest_char="e")
    try:
        submit_increment(store, conflicting)
    except DomainError as exc:
        assert exc.code == "E_CONFLICTING_COMMIT"
    else:
        raise AssertionError("expected E_CONFLICTING_COMMIT")
    assert store.current_version() == 3


def test_bound_readers_never_observe_partial_rebuild(store):
    submit_v1(store)
    submit_v2(store)
    baseline = store.environment_bundle("prod", version=2)
    barrier = threading.Barrier(5)

    def reader():
        barrier.wait()
        observed = []
        for _ in range(25):
            bundle = store.environment_bundle("prod", version=2)
            observed.append(
                (
                    bundle["snapshot_version"],
                    len(bundle["plan"]),
                    tuple(bundle["topology"]["layers"][-1]),
                    bundle["artifact_hashes"]["topology:environment"],
                )
            )
        return observed

    def writer():
        barrier.wait()
        return submit_increment(store, increment_payload("reader-writer", digest_char="f"))

    with ThreadPoolExecutor(max_workers=5) as executor:
        reader_futures = [executor.submit(reader) for _ in range(4)]
        writer_future = executor.submit(writer)
        writer_result = writer_future.result()
        observations = [item for future in reader_futures for item in future.result()]

    expected = (
        2,
        7,
        tuple(baseline["topology"]["layers"][-1]),
        baseline["artifact_hashes"]["topology:environment"],
    )
    assert all(item == expected for item in observations)
    assert writer_result["snapshot_version"] == 3


def test_same_base_after_success_is_stale(store):
    submit_v1(store)
    submit_v2(store)
    submit_increment(store, increment_payload("first", digest_char="1"))
    try:
        submit_increment(store, increment_payload("second", digest_char="2"))
    except DomainError as exc:
        assert exc.code == "E_STALE_BASE"
        assert exc.details == {"current_version": 3, "base_version": 2}
    else:
        raise AssertionError("expected E_STALE_BASE")
    assert store.current_version() == 3


def test_increment_model_requires_unique_units(sample):
    payload = sample("increment_image")
    payload["units"].append(payload["units"][0])
    try:
        IncrementalSnapshotInput.model_validate(payload)
    except Exception as exc:
        assert "unique" in str(exc)
    else:
        raise AssertionError("duplicate increment units must be rejected")
