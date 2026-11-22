import threading

import pytest

from graphstore.errors import ConflictingRequestError


def test_duplicate_request_runs_once(engine):
    body = {"request_id": "dup-1", "query": "multi_hop", "version": 1,
            "mode": "exact", "params": {"start": "p1", "max_hops": 2}}
    calls = []
    original = engine._execute_query

    def counting(body):
        calls.append(1)
        return original(body)

    engine._execute_query = counting
    results = []
    errors = []

    def submit():
        try:
            results.append(engine.query(dict(body)))
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=submit) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert not errors
    assert len(calls) == 1
    assert len(results) == 4
    assert all(item["version"] == 1 for item in results)
    reused = {item["dedup"]["reused"] for item in results}
    assert reused == {"fresh"}


def test_conflicting_request_body(engine):
    engine.query({"request_id": "dup-2", "query": "multi_hop", "version": 1,
                  "mode": "exact", "params": {"start": "p1", "max_hops": 1}})
    with pytest.raises(ConflictingRequestError) as exc:
        engine.query({"request_id": "dup-2", "query": "multi_hop", "version": 1,
                      "mode": "exact", "params": {"start": "p2", "max_hops": 1}})
    assert exc.value.code == "E_CONFLICTING_REQUEST"


def test_persisted_request_replay(engine):
    body = {"request_id": "persist-1", "query": "multi_hop", "version": 1,
            "mode": "exact", "params": {"start": "p1", "max_hops": 1}}
    first = engine.query(dict(body))
    second = engine.query(dict(body))
    assert first["dedup"]["reused"] == "fresh"
    assert second["dedup"]["reused"] == "persisted"
    assert first["nodes"] == second["nodes"]


def test_single_writer_serializes_updates(engine):
    outcomes = []

    def update(node_id):
        outcomes.append(engine.apply_delta({
            "add_nodes": [{"id": node_id, "label": "Person", "properties": {}}],
            "generated_at": "2026-11-23T13:00:00+08:00",
        })["version"])

    threads = [threading.Thread(target=update, args=(f"n{i}",)) for i in range(5)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sorted(outcomes) == [2, 3, 4, 5, 6]
    assert engine.current_version == 6
