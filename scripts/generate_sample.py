from __future__ import annotations

import argparse
import json
import random
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from eventbus.aggregation import NeumaierSum
from eventbus.hashing import partition_for


SEED = 20261122
TOPIC = "orders"
GROUP = "analytics"
PARTITIONS = 3


def build_sample() -> dict:
    rng = random.Random(SEED)
    partition_keys = [
        "customer-001",
        "customer-002",
        "customer-003",
        "customer-004",
        "customer-005",
        "customer-006",
    ]
    event_keys = ["alpha", "beta", "gamma", "delta"]
    magnitudes = [
        1_000_000_000_000.0,
        0.000001,
        -1_000_000_000_000.0,
        3.5,
        0.1,
        2.0,
        0.000003,
        42.75,
        None,
        "missing",
        1_000_000_000.0,
        -0.000002,
    ]
    base = datetime(2026, 1, 15, 8, 0, 0, tzinfo=timezone.utc)
    events = []
    for index in range(24):
        measure = magnitudes[index % len(magnitudes)]
        event = {
            "event_id": f"evt-{index + 1:03d}",
            "partition_key": partition_keys[index % len(partition_keys)],
            "event_time": (base + timedelta(seconds=index)).isoformat(),
            "key": event_keys[index % len(event_keys)],
            "batch_id": f"sample-batch-{index // 4 + 1:02d}",
            "payload": {
                "source": "local-deterministic-sample",
                "ordinal": index,
                "sample_noise": round(rng.uniform(-0.5, 0.5), 6),
                "note": "null and missing measures are both declared as count+1, sum+0",
            },
        }
        if measure == "missing":
            pass
        elif measure is None:
            event["measure"] = None
        else:
            event["measure"] = measure
        # Keep at least one deliberately nested body to exercise aliases while
        # still using the short form for the rest of the sample.
        if index == 23:
            event["body"] = {"key": event.pop("key"), "measure": event.pop("measure")}
        events.append(event)

    partition_assignments: dict[int, list[str]] = {p: [] for p in range(PARTITIONS)}
    sequences: dict[str, int] = {str(p): 0 for p in range(PARTITIONS)}
    sequence_by_id: dict[str, int] = {}
    routed: dict[str, int] = {}
    for event in events:
        partition = partition_for(TOPIC, event["partition_key"], PARTITIONS)
        sequences[str(partition)] += 1
        sequence_by_id[event["event_id"]] = sequences[str(partition)]
        routed[event["event_id"]] = partition
        partition_assignments[partition].append(event["event_id"])

    expected: dict[str, dict[str, float | int]] = {}
    for event in sorted(
        events,
        key=lambda item: (
            event_key(item),
            routed[item["event_id"]],
            sequence_by_id[item["event_id"]],
        ),
    ):
        key = event_key(event)
        if key not in expected:
            expected[key] = {"count": 0, "total": 0.0, "compensation": 0.0}
        total = float(expected[key]["total"])
        compensation = float(expected[key]["compensation"])
        accumulator = NeumaierSum(total, compensation)
        accumulator.add(event_measure(event))
        expected[key] = {
            "count": int(expected[key]["count"]) + 1,
            "total": accumulator.total,
            "compensation": accumulator.compensation,
        }

    partition_counts = Counter(routed.values())
    if set(partition_counts) != set(range(PARTITIONS)):
        raise RuntimeError(f"sample does not cover all partitions: {partition_counts}")

    return {
        "seed": SEED,
        "generator": "scripts/generate_sample.py",
        "topic": {
            "topic": TOPIC,
            "partitions": PARTITIONS,
            "group_id": GROUP,
            "min_retained_seq": 1,
        },
        "batch_id": "sample-batch-001",
        "events": events,
        "expected": {
            "partition_counts": {str(partition): partition_counts[partition] for partition in range(PARTITIONS)},
            "partition_assignments": {
                str(partition): event_ids
                for partition, event_ids in partition_assignments.items()
            },
            "sequence_by_event_id": sequence_by_id,
            "aggregate_by_key": {
                key: {
                    "key": key,
                    "count": value["count"],
                    "sum": float(value["total"] + value["compensation"]),
                    "compensation": value["compensation"],
                }
                for key, value in sorted(expected.items())
            },
        },
    }


def event_key(event: dict) -> str:
    return event.get("key") or event.get("event_key") or event["body"]["key"]


def event_measure(event: dict) -> float | None:
    if "measure" in event:
        return event["measure"]
    if "metric" in event:
        return event["metric"]
    if "body" in event and "measure" in event["body"]:
        return event["body"]["measure"]
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate the deterministic local sample.")
    parser.add_argument("--output", default="data/sample_events.json")
    args = parser.parse_args()
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    sample = build_sample()
    output.write_text(
        json.dumps(sample, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "output": str(output),
                "events": len(sample["events"]),
                "partition_counts": sample["expected"]["partition_counts"],
                "aggregate_keys": list(sample["expected"]["aggregate_by_key"]),
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
