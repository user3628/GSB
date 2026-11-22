from __future__ import annotations

import threading

from rca_pipeline.errors import ConflictingBatch, StaleBase
from tests.conftest import submit_topology_v1


def test_single_writer_serializes_and_replays_duplicate(pipeline, topo_v1, dataset):
    submit_topology_v1(pipeline, topo_v1)
    outcomes = []
    barrier = threading.Barrier(3)

    def submit():
        barrier.wait()
        try:
            outcomes.append(("ok", pipeline.submit_samples(dataset[0])))
        except ConflictingBatch as exc:
            outcomes.append(("conflict", exc.code))
        except Exception as exc:  # pragma: no cover - defensive
            outcomes.append(("error", str(exc)))

    threads = [threading.Thread(target=submit) for _ in range(3)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(outcomes) == 3
    assert all(kind in ("ok", "conflict") for kind, _ in outcomes)
    # identical concurrent payloads: exactly one executes, others replay
    ok_results = [value for kind, value in outcomes if kind == "ok"]
    assert len(ok_results) == 3
    assert {value["new_version"] for value in ok_results} == {1}
    assert sum(1 for value in ok_results if value.get("replayed")) == 2


def test_concurrent_stale_increments_only_one_wins(pipeline, topo_v1, topo_v2):
    submit_topology_v1(pipeline, topo_v1)
    results = []
    barrier = threading.Barrier(2)

    def submit(payload):
        barrier.wait()
        try:
            results.append(("ok", pipeline.submit_topology(payload)))
        except StaleBase as exc:
            results.append(("stale", exc.code))

    # Distinct commit ids sharing base_version=1: only one can land at v2.
    payload_a = dict(topo_v2)
    payload_b = dict(topo_v2, commit_id="topo-v2-competing")
    t1 = threading.Thread(target=submit, args=(payload_a,))
    t2 = threading.Thread(target=submit, args=(payload_b,))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    kinds = sorted(kind for kind, _ in results)
    assert kinds == ["ok", "stale"]
