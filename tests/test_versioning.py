import threading

import pytest

from graphstore.errors import VersionRetiredError


def test_query_bound_to_requested_version(engine, sample_path):
    v1 = engine.query({"query": "multi_hop", "version": 1, "mode": "exact",
                       "params": {"start": "p1", "max_hops": 1}})
    engine.load_snapshot(str(sample_path / "snapshot_v2.json"))
    v1_again = engine.query({"query": "multi_hop", "version": 1, "mode": "exact",
                             "params": {"start": "p1", "max_hops": 1}})
    v2 = engine.query({"query": "multi_hop", "version": 2, "mode": "exact",
                       "params": {"start": "p5", "max_hops": 1}})
    assert v1["version"] == v1_again["version"] == 1
    assert v1["nodes"] == v1_again["nodes"]
    assert v2["version"] == 2 and "p1" in v2["nodes"]


def test_retired_version_rejected(engine, sample_path):
    engine.load_snapshot(str(sample_path / "snapshot_v2.json"))
    with pytest.raises(VersionRetiredError) as exc:
        engine.query({"query": "multi_hop", "version": 99, "mode": "exact",
                      "params": {"start": "p1", "max_hops": 1}})
    assert exc.value.code == "E_VERSION_RETIRED"


def test_default_version_resolves_to_current(engine, sample_path):
    engine.load_snapshot(str(sample_path / "snapshot_v2.json"))
    result = engine.query({"query": "multi_hop", "mode": "exact",
                           "params": {"start": "p5", "max_hops": 1}})
    assert result["version"] == 2


def test_concurrent_query_stays_on_old_version(engine, sample_path):
    hold = threading.Event()
    proceed = threading.Event()
    observed = {}

    def reader():
        view = engine.registry.acquire(1)
        observed["held"] = view.version
        hold.set()
        proceed.wait(timeout=5)
        from graphstore.query import FastBudget, multi_hop
        observed["result"] = multi_hop(
            view, {"start": "p1", "max_hops": 1}, "exact", FastBudget()
        )
        engine.registry.release(view)

    thread = threading.Thread(target=reader)
    thread.start()
    hold.wait(timeout=5)
    engine.load_snapshot(str(sample_path / "snapshot_v2.json"))
    proceed.set()
    thread.join()
    assert observed["held"] == 1
    assert observed["result"]["version"] == 1
    assert engine.current_version == 2
