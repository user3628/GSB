"""Generate the fixed, deterministic local data set used by every test run.

Outputs:
  data/sli_samples.json        - three incremental SLI batches
  data/topology_snapshot.json  - full topology snapshot at version 1
  data/topology_increment.json - edge increment at version 2
  data/topology_snapshot_v3.json - full replacement snapshot at version 3

The data is intentionally deterministic: no randomness, no clocks, so repeated
runs produce byte-identical files and identical alert/RCA outcomes.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from rca_pipeline.topology import canonical_checksum

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
BASE_TS = 1_700_000_000_000
STEP_MS = 120

SERVICES_V1 = ["gateway", "auth", "order", "payment", "inventory", "notify", "cache"]
EDGES_V1 = [
    ["gateway", "auth"],
    ["gateway", "order"],
    ["order", "payment"],
    ["order", "inventory"],
    ["payment", "notify"],
    ["auth", "cache"],
]

# Per-service baseline latency (us) per metric.
BASELINES = {
    "gateway": 12_000,
    "auth": 8_000,
    "order": 18_000,
    "payment": 22_000,
    "inventory": 10_000,
    "notify": 6_000,
    "cache": 2_000,
}
METRICS = ["request_latency_us", "queue_latency_us"]


def _record(ts_ms, service, metric, is_good, latency_us):
    return {
        "ts_ms": ts_ms,
        "service": service,
        "metric": metric,
        "is_good": is_good,
        "latency_us": latency_us,
    }


def healthy_record(index, ts_ms, service, metric):
    base = BASELINES[service] + (500 if metric == "queue_latency_us" else 0)
    # Deterministic small oscillation, always well under thresholds.
    latency = base + (index % 5) * 200
    return _record(ts_ms, service, metric, True, latency)


def degraded_payment_record(index, ts_ms, metric):
    # 25% bad events -> 75% good rate, below the 0.85 payment floor, while
    # latencies also breach the 45_000 p99 cap.
    is_good = index % 4 != 3
    latency = 46_000 + (index % 4) * 3_000  # 46k..55k, deterministic
    return _record(ts_ms, "payment", metric, bool(is_good), latency)


def build_batches():
    batch1, batch2, batch3, batch4 = [], [], [], []
    for service in SERVICES_V1:
        for metric in METRICS:
            for index in range(42):
                ts_ms = BASE_TS + index * STEP_MS
                batch1.append(healthy_record(index, ts_ms, service, metric))
            for index in range(42):
                ts_ms = BASE_TS + (42 + index) * STEP_MS
                if service == "payment":
                    batch2.append(degraded_payment_record(index, ts_ms, metric))
                else:
                    batch2.append(healthy_record(42 + index, ts_ms, service, metric))
            for index in range(42):
                ts_ms = BASE_TS + (84 + index) * STEP_MS
                if service == "payment":
                    batch3.append(degraded_payment_record(42 + index, ts_ms, metric))
                else:
                    batch3.append(healthy_record(84 + index, ts_ms, service, metric))
            for index in range(42):
                ts_ms = BASE_TS + (126 + index) * STEP_MS
                batch4.append(healthy_record(126 + index, ts_ms, service, metric))

    # Deterministic in-batch duplicate (first record repeated) for batch 1.
    batch1_with_dup = list(batch1)
    batch1_with_dup.append(dict(batch1[0]))
    return batch1_with_dup, batch2, batch3, batch4


def write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    batch1, batch2, batch3, batch4 = build_batches()

    write_json(DATA_DIR / "sli_samples.json", {
        "description": "Deterministic SLI batches: healthy, degraded, sustained-degraded, recovered",
        "batches": [
            {"commit_id": "samples-b1", "base_version": 0, "records": batch1},
            {"commit_id": "samples-b2", "base_version": 1, "records": batch2},
            {"commit_id": "samples-b3", "base_version": 2, "records": batch3},
            {"commit_id": "samples-b4", "base_version": 3, "records": batch4},
        ],
    })

    checksum_v1 = canonical_checksum(SERVICES_V1, [tuple(e) for e in EDGES_V1])
    write_json(DATA_DIR / "topology_snapshot.json", {
        "commit_id": "topo-v1",
        "kind": "snapshot",
        "version": 1,
        "base_version": 0,
        "services": SERVICES_V1,
        "edges": EDGES_V1,
        "checksum": checksum_v1,
    })

    services_v2 = SERVICES_V1 + ["risk"]
    edges_v2 = EDGES_V1 + [["payment", "risk"]]
    affected = ["payment", "risk"]
    # Increment checksums cover exactly the payload's affected subset.
    checksum_v2 = canonical_checksum(affected, [("payment", "risk")])
    write_json(DATA_DIR / "topology_increment.json", {
        "commit_id": "topo-v2",
        "kind": "increment",
        "version": 2,
        "base_version": 1,
        "services": affected,
        "edges": [["payment", "risk"]],
        "checksum": checksum_v2,
    })

    # Full replacement snapshot: drops 'cache' and its edge; must not mix content.
    services_v3 = [name for name in services_v2 if name != "cache"]
    edges_v3 = [edge for edge in edges_v2 if edge != ["auth", "cache"]]
    checksum_v3 = canonical_checksum(services_v3, [tuple(e) for e in edges_v3])
    write_json(DATA_DIR / "topology_snapshot_v3.json", {
        "commit_id": "topo-v3",
        "kind": "snapshot",
        "version": 3,
        "base_version": 2,
        "services": services_v3,
        "edges": edges_v3,
        "checksum": checksum_v3,
    })

    print(f"wrote deterministic data to {DATA_DIR}")
    print(f"batch1 records (incl. duplicate): {len(batch1)}")
    print(f"batch2 records: {len(batch2)}  batch3 records: {len(batch3)}")
    print(f"topology checksums: v1={checksum_v1[:12]} v2={checksum_v2[:12]} v3={checksum_v3[:12]}")


if __name__ == "__main__":
    main()
