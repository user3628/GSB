from __future__ import annotations

import threading

from eventbus.service import EventBus


def test_read_snapshots_never_see_ack_or_offset_without_aggregate(tmp_path):
    db_path = tmp_path / "snapshot.db"
    bus = EventBus(db_path)
    bus.create_topic("snapshot", partitions=1, group_id="readers")
    events = [
        {
            "event_id": f"s-{index}",
            "partition_key": "same",
            "event_time": f"2026-04-01T00:00:{index:02d}Z",
            "key": "all",
            "measure": 1.0,
        }
        for index in range(30)
    ]
    bus.publish("snapshot", "batch", events)
    failures: list[str] = []
    finished = threading.Event()

    def writer():
        try:
            while True:
                polled = bus.poll("snapshot", "readers", 0, "writer", lease_seconds=30)
                if polled["event"] is None:
                    break
                bus.ack(
                    "snapshot",
                    "readers",
                    0,
                    "writer",
                    polled["event"]["event_id"],
                    polled["lease"]["lease_token"],
                )
        except Exception as exc:  # pragma: no cover - surfaced below
            failures.append(f"writer: {exc}")
        finally:
            finished.set()

    thread = threading.Thread(target=writer)
    thread.start()
    while not finished.is_set():
        snapshot = bus.snapshot("snapshot", "readers")
        acked = sum(1 for event in snapshot["events"] if event["state"] == "ACKED")
        aggregate_count = sum(item["count"] for item in snapshot["aggregates"])
        offset = snapshot["offsets"][0]["offset"]
        if aggregate_count != acked:
            failures.append(
                f"snapshot saw acked={acked}, aggregate_count={aggregate_count}"
            )
            break
        if offset != acked + 1:
            failures.append(f"snapshot saw acked={acked}, offset={offset}")
            break
    thread.join(timeout=10)
    assert not failures
    assert bus.snapshot("snapshot", "readers")["offsets"][0]["offset"] == 31
