from __future__ import annotations

import concurrent.futures
import sqlite3
import time

from fastapi.testclient import TestClient

from tcp_sim.api import create_app

from .conftest import api_batch


def test_full_split_and_concurrent_waits_are_bitwise_identical(client, trace):
    full_payload = api_batch(
        trace,
        run_id="full",
        batch_id="full-all",
        events=trace["events"],
        final_batch=True,
    )
    response = client.post("/v1/simulations/batches", json=full_payload)
    assert response.status_code == 200, response.text

    split_run = "split"
    base = None
    offset = 0
    for index, end in enumerate(trace["suggested_splits"]):
        final = index == len(trace["suggested_splits"]) - 1
        if final:
            # Let a reader block while this final batch commits.
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(
                    client.get,
                    f"/v1/runs/{split_run}/state",
                    params={"at_ms": 220, "conn_id": "c1", "wait_ms": 4000},
                )
                time.sleep(0.05)
                response = client.post(
                    "/v1/simulations/batches",
                    json=api_batch(
                        trace,
                        run_id=split_run,
                        batch_id=f"split-{index}",
                        events=trace["events"][offset:end],
                        final_batch=True,
                        expected_base=base,
                    ),
                )
                waited = future.result(timeout=5)
            assert response.status_code == 200, response.text
            assert waited.status_code == 200, waited.text
            assert waited.json()["data"]["sim_time_ms"] == 220
            assert waited.json()["data"]["snapshot_sha256"] == client.get(
                f"/v1/runs/{split_run}/state",
                params={"at_ms": 220, "conn_id": "c1"},
            ).json()["data"]["snapshot_sha256"]
        else:
            response = client.post(
                "/v1/simulations/batches",
                json=api_batch(
                    trace,
                    run_id=split_run,
                    batch_id=f"split-{index}",
                    events=trace["events"][offset:end],
                    final_batch=False,
                    expected_base=base,
                ),
            )
            assert response.status_code == 200, response.text
        base = response.json()["data"]["committed_checkpoint_id"]
        offset = end

    compare = client.post(
        "/v1/runs/compare",
        json={"left_run_id": "full", "right_run_id": split_run},
    )
    assert compare.status_code == 200, compare.text
    assert compare.json()["data"]["identical"] is True
    assert compare.json()["data"]["left_snapshot_digest"] == compare.json()["data"]["right_snapshot_digest"]

    for at_ms in (0, 10, 110, 118, 200, 220, 310, 320):
        left = client.get("/v1/runs/full/state", params={"at_ms": at_ms, "conn_id": "c1"})
        right = client.get(f"/v1/runs/{split_run}/state", params={"at_ms": at_ms, "conn_id": "c1"})
        assert left.status_code == right.status_code == 200
        assert left.json()["data"]["snapshot_sha256"] == right.json()["data"]["snapshot_sha256"]


def test_errors_have_stable_codes_and_do_not_advance(client, trace):
    first = client.post(
        "/v1/simulations/batches",
        json=api_batch(
            trace,
            run_id="stable",
            batch_id="stable-1",
            events=trace["events"][:6],
            final_batch=False,
        ),
    )
    assert first.status_code == 200
    base = first.json()["data"]["committed_checkpoint_id"]
    before = client.get("/v1/runs/stable").json()["data"]

    invalid_trace = client.post(
        "/v1/simulations/batches",
        json=api_batch(
            trace,
            run_id="stable",
            batch_id="bad-trace",
            events=[{"ts_ms": 61, "conn_id": "c1", "event_type": "bad", "payload_bytes": 1}],
            final_batch=True,
            expected_base=base,
        ),
    )
    assert invalid_trace.status_code == 400
    assert invalid_trace.json()["error"]["code"] == "E_TRACE_INVALID"
    after = client.get("/v1/runs/stable").json()["data"]
    assert after == before

    # The first event is a legal FIN transition, the second is illegal from
    # FIN_WAIT. Whole-batch preflight must discard both.
    illegal_from_existing = client.post(
        "/v1/simulations/batches",
        json=api_batch(
            trace,
            run_id="stable",
            batch_id="bad-transition-existing",
            events=[
                {"ts_ms": 61, "conn_id": "c1", "event_type": "send", "payload_bytes": 0},
                {"ts_ms": 62, "conn_id": "c1", "event_type": "send", "payload_bytes": 0},
            ],
            final_batch=True,
            expected_base=base,
        ),
    )
    assert illegal_from_existing.status_code == 409
    assert illegal_from_existing.json()["error"]["code"] == "E_INVALID_TRANSITION"
    assert client.get("/v1/runs/stable").json()["data"] == before

    invalid_transition = client.post(
        "/v1/simulations/batches",
        json=api_batch(
            trace,
            run_id="closed-new",
            batch_id="bad-transition",
            events=[{"ts_ms": 1, "conn_id": "x", "event_type": "ack", "payload_bytes": 0, "ack_seq": 0}],
            final_batch=True,
        ),
    )
    assert invalid_transition.status_code == 409
    assert invalid_transition.json()["error"]["code"] == "E_INVALID_TRANSITION"
    assert client.get("/v1/runs/closed-new").status_code == 404

    stale = client.post(
        "/v1/simulations/batches",
        json=api_batch(
            trace,
            run_id="stable",
            batch_id="stale",
            events=trace["events"][6:7],
            final_batch=False,
            expected_base="not-the-head",
        ),
    )
    assert stale.status_code == 409
    assert stale.json()["error"]["code"] == "E_STALE_BASE"

    missing = client.get("/v1/batches/no-such-batch")
    assert missing.status_code == 404
    assert missing.json()["error"]["code"] == "E_BATCH_NOT_FOUND"

    # A rejected batch identifier reuses the same error and commits no state.
    duplicate_error = client.post(
        "/v1/simulations/batches",
        json=api_batch(
            trace,
            run_id="stable",
            batch_id="bad-trace",
            events=[{"ts_ms": 61, "conn_id": "c1", "event_type": "bad", "payload_bytes": 1}],
            final_batch=True,
            expected_base=base,
        ),
    )
    assert duplicate_error.status_code == 400
    assert duplicate_error.json()["error"]["code"] == "E_TRACE_INVALID"
    assert client.get("/v1/runs/stable").json()["data"] == before


def test_non_monotonic_timestamp_is_invalid(client, trace):
    response = client.post(
        "/v1/simulations/batches",
        json=api_batch(
            trace,
            run_id="non-monotonic",
            batch_id="non-monotonic-1",
            events=[
                {"ts_ms": 10, "conn_id": "c1", "event_type": "send", "payload_bytes": 0},
                {"ts_ms": 9, "conn_id": "c1", "event_type": "ack", "payload_bytes": 0, "ack_seq": 0},
            ],
            final_batch=True,
        ),
    )
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "E_TRACE_INVALID"
    assert "previous_ts_ms" in response.json()["error"]["context"]


def test_query_refuses_unstable_future_point_but_serves_committed_past(client, trace):
    response = client.post(
        "/v1/simulations/batches",
        json=api_batch(
            trace,
            run_id="unstable-query",
            batch_id="unstable-query-1",
            events=trace["events"][:6],
            final_batch=False,
        ),
    )
    assert response.status_code == 200

    future = client.get(
        "/v1/runs/unstable-query/state",
        params={"at_ms": 100, "conn_id": "c1", "wait_ms": 0},
    )
    assert future.status_code == 409
    assert future.json()["error"]["code"] == "E_SNAPSHOT_NOT_READY"
    assert future.json()["error"]["context"]["committed_sim_time_ms"] == 20

    past = client.get(
        "/v1/runs/unstable-query/state",
        params={"at_ms": 19, "conn_id": "c1"},
    )
    assert past.status_code == 200
    assert past.json()["data"]["event_index"] == 1
    assert past.json()["data"]["run_status"] == "ACTIVE"


def test_checkpoint_corruption_is_detected_without_repair(tmp_path, trace):
    app = create_app(tmp_path / "corrupt.db")
    with TestClient(app) as client:
        first = client.post(
            "/v1/simulations/batches",
            json=api_batch(
                trace,
                run_id="corrupt-run",
                batch_id="corrupt-1",
                events=trace["events"][:6],
                final_batch=False,
            ),
        )
        checkpoint_id = first.json()["data"]["committed_checkpoint_id"]
        db_path = tmp_path / "corrupt.db"
        conn = sqlite3.connect(db_path)
        try:
            conn.execute(
                "UPDATE checkpoints SET state_json = state_json || ' ' WHERE checkpoint_id = ?",
                (checkpoint_id,),
            )
            conn.commit()
        finally:
            conn.close()

        response = client.get(f"/v1/checkpoints/{checkpoint_id}")
        assert response.status_code == 500
        assert response.json()["error"]["code"] == "E_CHECKPOINT_CORRUPT"

        submit_more = client.post(
            "/v1/simulations/batches",
            json=api_batch(
                trace,
                run_id="corrupt-run",
                batch_id="corrupt-2",
                events=trace["events"][6:12],
                final_batch=True,
                expected_base=checkpoint_id,
            ),
        )
        assert submit_more.status_code == 500
        assert submit_more.json()["error"]["code"] == "E_CHECKPOINT_CORRUPT"
