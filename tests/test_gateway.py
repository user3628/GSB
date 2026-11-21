from __future__ import annotations

import asyncio
import base64
import copy
from pathlib import Path
from typing import Any

import pytest

from vgw.errors import GatewayError
from vgw.gateway import Gateway
from vgw.models import FrameSubmission
from vgw.store import Store
from vgw.transport import LocalTestNetwork

from .conftest import make_patch, make_snapshot, make_topology, running_gateway


def frame(
    frame_id: str,
    session_id: str,
    sequence: int,
    *,
    delay_ms: int = 0,
    timeout_ms: int | None = None,
    **options: Any,
) -> FrameSubmission:
    payload: dict[str, Any] = {
        "op": "echo",
        "body": f"{session_id}-{sequence}",
        "options": {"delay_ms": delay_ms, **options},
    }
    return FrameSubmission(
        frame_id=frame_id,
        session_id=session_id,
        sequence=sequence,
        payload=payload,
        timeout_ms=timeout_ms,
    )


@pytest.mark.asyncio
async def test_snapshot_failure_preserves_state(tmp_path: Path) -> None:
    async with running_gateway(tmp_path) as (gateway, _store, _network, _topology, snapshot):
        await gateway.submit_frame(frame("a1", "alpha", 1))
        before = copy.deepcopy(gateway.public_state())
        tampered = copy.deepcopy(snapshot)
        tampered["version"] = 2
        tampered["digest"] = "sha256:" + "f" * 64
        with pytest.raises(GatewayError) as caught:
            await gateway.load_snapshot(tampered)
        assert caught.value.code == "E_SNAPSHOT_INVALID"
        after = gateway.public_state()
        assert after["version"] == before["version"]
        assert after["snapshot"] == before["snapshot"]
        assert after["sessions"]["alpha"]["last_acked_seq"] == 1

        stale = copy.deepcopy(snapshot)
        with pytest.raises(GatewayError) as caught:
            await gateway.load_snapshot(stale)
        assert caught.value.code == "E_SNAPSHOT_INVALID"
        assert gateway.public_state()["sessions"]["alpha"]["last_acked_seq"] == 1


@pytest.mark.asyncio
async def test_parallel_channels_and_session_order(tmp_path: Path) -> None:
    async with running_gateway(tmp_path) as (gateway, _store, _network, _topology, _snapshot):
        first_round = await asyncio.gather(
            gateway.submit_frame(frame("a1", "alpha", 1, delay_ms=100)),
            gateway.submit_frame(frame("b1", "beta", 1, delay_ms=100)),
            gateway.submit_frame(frame("g1", "gamma", 1, delay_ms=100)),
        )
        assert {item["channel_id"] for item in first_round} == {0, 1, 2}
        second_round = await asyncio.gather(
            gateway.submit_frame(frame("a2", "alpha", 2)),
            gateway.submit_frame(frame("b2", "beta", 2)),
            gateway.submit_frame(frame("g2", "gamma", 2)),
        )
        assert all(item["sequence"] == 2 for item in second_round)

        traces = gateway.list_traces(limit=500)
        acquired = [item for item in traces if item["kind"] == "channel_acquired"]
        acked = [item for item in traces if item["kind"] == "frame_acked"]
        assert len(acquired) == 6
        assert len(acked) == 6
        for session_id in ("alpha", "beta", "gamma"):
            sequences = [
                item["sequence"] for item in acked if item["session_id"] == session_id
            ]
            assert sequences == [1, 2]
        assert _max_concurrency(traces) == 3


@pytest.mark.asyncio
async def test_same_session_never_occupies_two_channels(tmp_path: Path) -> None:
    async with running_gateway(tmp_path) as (gateway, _store, _network, _topology, _snapshot):
        first = asyncio.create_task(
            gateway.submit_frame(frame("a1", "alpha", 1, delay_ms=130))
        )
        await asyncio.sleep(0.02)
        second = asyncio.create_task(gateway.submit_frame(frame("a2", "alpha", 2)))
        results = await asyncio.gather(first, second)
        assert [item["sequence"] for item in results] == [1, 2]
        events = gateway.list_traces(limit=200)
        alpha_events = [
            item
            for item in events
            if item["session_id"] == "alpha"
            and item["kind"] in {"channel_acquired", "frame_acked"}
        ]
        assert [item["kind"] for item in alpha_events] == [
            "channel_acquired",
            "frame_acked",
            "channel_acquired",
            "frame_acked",
        ]


@pytest.mark.asyncio
async def test_priority_and_arrival_order_in_waiting_queue(tmp_path: Path) -> None:
    topology = make_topology()
    bindings = {
        "block": {"priority": 50, "channel": 0, "egress": "east", "max_payload": 4096},
        "low": {"priority": 1, "channel": 0, "egress": "east", "max_payload": 4096},
        "high": {"priority": 99, "channel": 0, "egress": "east", "max_payload": 4096},
    }
    async with running_gateway(
        tmp_path,
        topology=topology,
        channel_count=1,
        queue_capacity=4,
        bindings=bindings,
    ) as (gateway, _store, _network, _topology, _snapshot):
        blocker = asyncio.create_task(
            gateway.submit_frame(frame("block1", "block", 1, delay_ms=180))
        )
        await asyncio.sleep(0.03)
        low = asyncio.create_task(gateway.submit_frame(frame("low1", "low", 1)))
        await asyncio.sleep(0.005)
        high = asyncio.create_task(gateway.submit_frame(frame("high1", "high", 1)))
        await asyncio.gather(blocker, low, high)
        acked = [
            item
            for item in gateway.list_traces(limit=200)
            if item["kind"] == "frame_acked"
        ]
        assert [item["session_id"] for item in acked] == ["block", "high", "low"]


@pytest.mark.asyncio
async def test_bounded_queue_returns_business_error(tmp_path: Path) -> None:
    topology = make_topology()
    bindings = {
        name: {"priority": 10, "channel": 0, "egress": "east", "max_payload": 4096}
        for name in ("a", "b", "c", "d")
    }
    async with running_gateway(
        tmp_path,
        topology=topology,
        channel_count=1,
        queue_capacity=2,
        bindings=bindings,
    ) as (gateway, _store, _network, _topology, _snapshot):
        active = asyncio.create_task(gateway.submit_frame(frame("a1", "a", 1, delay_ms=350)))
        await asyncio.sleep(0.03)
        queued_one = asyncio.create_task(gateway.submit_frame(frame("b1", "b", 1)))
        await asyncio.sleep(0.01)
        queued_two = asyncio.create_task(gateway.submit_frame(frame("c1", "c", 1)))
        await asyncio.sleep(0.01)
        with pytest.raises(GatewayError) as caught:
            await gateway.submit_frame(frame("d1", "d", 1))
        assert caught.value.code == "E_CHANNEL_QUEUE_FULL"
        assert caught.value.context["queue_capacity"] == 2
        assert caught.value.context["queue_depth"] == 2
        await asyncio.gather(active, queued_one, queued_two)
        assert gateway.public_state()["queue_depth"] == 0


@pytest.mark.asyncio
async def test_timeout_closes_socket_and_reuses_channel(tmp_path: Path) -> None:
    async with running_gateway(tmp_path) as (gateway, _store, _network, _topology, _snapshot):
        with pytest.raises(GatewayError) as caught:
            await gateway.submit_frame(
                frame("slow1", "alpha", 1, delay_ms=500, timeout_ms=80)
            )
        assert caught.value.code == "E_TUNNEL_TIMEOUT"
        assert caught.value.context["socket_closed_after_timeout"] is True
        assert caught.value.context["wait_cancelled"] is True
        assert gateway.public_state()["active"] == []
        assert gateway.public_state()["channels"][0]["state"] == "free"

        retry = await gateway.submit_frame(frame("retry1", "alpha", 1))
        assert retry["sequence"] == 1
        assert retry["response"]["endpoint"] == "decap-east"


@pytest.mark.asyncio
async def test_corrupt_response_is_rejected_and_channel_released(tmp_path: Path) -> None:
    async with running_gateway(tmp_path) as (gateway, _store, _network, _topology, _snapshot):
        with pytest.raises(GatewayError) as caught:
            await gateway.submit_frame(
                frame("corrupt1", "alpha", 1, corrupt_response=True)
            )
        assert caught.value.code == "E_FRAME_INVALID"
        assert gateway.public_state()["channels"][0]["state"] == "free"


@pytest.mark.asyncio
async def test_duplicate_frame_id_dispatches_once_and_conflict_is_rejected(
    tmp_path: Path,
) -> None:
    async with running_gateway(tmp_path) as (gateway, _store, _network, _topology, _snapshot):
        first = asyncio.create_task(
            gateway.submit_frame(frame("same1", "alpha", 1, delay_ms=100))
        )
        await asyncio.sleep(0.015)
        second = asyncio.create_task(
            gateway.submit_frame(frame("same1", "alpha", 1, delay_ms=100))
        )
        one, two = await asyncio.gather(first, second)
        assert one == two
        acquired = [
            item
            for item in gateway.list_traces(limit=200)
            if item["kind"] == "channel_acquired" and item["frame_id"] == "same1"
        ]
        assert len(acquired) == 1
        with pytest.raises(GatewayError) as caught:
            await gateway.submit_frame(
                FrameSubmission(
                    frame_id="same1",
                    session_id="alpha",
                    sequence=1,
                    payload={"different": True},
                )
            )
        assert caught.value.code == "E_CONFLICTING_FRAME"


@pytest.mark.asyncio
async def test_incremental_patch_is_atomic_and_rebuilds_stats(tmp_path: Path) -> None:
    async with running_gateway(tmp_path) as (gateway, _store, _network, topology, snapshot):
        await gateway.submit_frame(frame("a1", "alpha", 1))
        await gateway.submit_frame(frame("b1", "beta", 1))
        before_beta_stats = copy.deepcopy(gateway.public_state()["stats"]["beta"])
        stale = make_patch(
            base_version=2,
            bindings={
                "beta": {
                    "priority": 20,
                    "channel": 1,
                    "egress": "east",
                    "max_payload": 4096,
                }
            },
            version=3,
        )
        with pytest.raises(GatewayError) as caught:
            await gateway.apply_patch(stale)
        assert caught.value.code == "E_STALE_BASE"
        assert gateway.public_state()["version"] == 1
        assert gateway.public_state()["stats"]["beta"] == before_beta_stats

        patch = make_patch(
            base_version=1,
            bindings={
                "beta": {
                    "priority": 20,
                    "channel": 1,
                    "egress": "east",
                    "max_payload": 4096,
                }
            },
        )
        result = await gateway.apply_patch(patch)
        assert result["affected_sessions"] == ["beta"]
        state = gateway.public_state()
        assert state["version"] == 2
        assert state["sessions"]["beta"]["egress_label"] == "east"
        assert state["stats"]["beta"]["invalidations"] == before_beta_stats["invalidations"] + 1
        assert state["stats"]["beta"]["frames_acked"] == 1
        probe = await gateway.submit_frame(frame("b2", "beta", 2))
        assert probe["response"]["endpoint"] == "decap-east"


@pytest.mark.asyncio
async def test_patch_waits_for_inflight_frame_on_old_binding(tmp_path: Path) -> None:
    async with running_gateway(tmp_path) as (gateway, _store, _network, topology, snapshot):
        inflight = asyncio.create_task(
            gateway.submit_frame(frame("inflight1", "alpha", 1, delay_ms=160))
        )
        await asyncio.sleep(0.03)
        patch = make_patch(
            base_version=1,
            bindings={
                "alpha": {
                    "priority": 30,
                    "channel": 0,
                    "egress": "west",
                    "max_payload": 4096,
                }
            },
        )
        patch_result = await gateway.apply_patch(patch)
        frame_result = await inflight
        assert patch_result["quiesced_before_switch"] is True
        assert frame_result["response"]["endpoint"] == "decap-east"
        assert frame_result["snapshot_version"] == 1
        state = gateway.public_state()
        assert state["active"] == []
        assert state["version"] == 2
        assert state["sessions"]["alpha"]["egress_label"] == "west"


@pytest.mark.asyncio
async def test_payload_limit_rejected_without_send(tmp_path: Path) -> None:
    async with running_gateway(tmp_path) as (gateway, _store, _network, _topology, _snapshot):
        too_large = base64.b64encode(b"x" * 5000).decode("ascii")
        with pytest.raises(GatewayError) as caught:
            await gateway.submit_frame(
                FrameSubmission(
                    frame_id="too-large",
                    session_id="alpha",
                    sequence=1,
                    payload_b64=too_large,
                )
            )
        assert caught.value.code == "E_FRAME_INVALID"
        assert all(
            row["frame_id"] != "too-large" for row in gateway.list_frames(limit=100)
        )
        assert gateway.public_state()["queue_depth"] == 0


@pytest.mark.asyncio
async def test_persisted_pending_intent_recovers_as_retransmit(tmp_path: Path) -> None:
    topology = make_topology()
    snapshot = make_snapshot(topology)
    network = LocalTestNetwork(topology)
    await network.start()
    store = Store(tmp_path / "recovery.db")
    gateway = Gateway(store)
    await gateway.start()
    await gateway.load_snapshot(snapshot)
    try:
        store.commit_queued_frame(
            frame_id="pending1",
            session_id="alpha",
            sequence=1,
            payload_hash="sha256:" + "a" * 64,
            payload_len=12,
            snapshot_version=1,
        )
        store.commit_dispatch(
            intent_id="intent-pending1",
            frame_id="pending1",
            session_id="alpha",
            sequence=1,
            channel_id=0,
            egress=topology.carriers[0].listen_address,
            snapshot_version=1,
            payload_len=12,
        )
    finally:
        await gateway.stop()
        store.close()
        await network.stop()

    recovered_store = Store(tmp_path / "recovery.db")
    report = recovered_store.recover_incomplete_intents()
    frame_row = recovered_store.get_frame("pending1")
    lease = recovered_store.load_leases()["alpha"]
    recovered_store.close()
    assert report["facts_only"] is True
    assert report["graceful_exit_report_available"] is False
    assert report["retransmit_required"][0]["frame_id"] == "pending1"
    assert frame_row is not None and frame_row["status"] == "retransmit_required"
    assert lease["channel_id"] is None
    assert lease["sent_unacked_seq"] is None
    assert lease["status"] == "recovered_idle"


def _max_concurrency(traces: list[dict[str, Any]]) -> int:
    current = maximum = 0
    for item in traces:
        if item["kind"] == "channel_acquired":
            current += 1
            maximum = max(maximum, current)
        elif item["kind"] == "channel_released":
            current -= 1
    return maximum

