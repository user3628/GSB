from __future__ import annotations

import base64
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
sys.path.insert(0, str(ROOT))

from app.gf import encode_shards  # noqa: E402

ENGINE = {
    "rebuild_concurrency": 2,
    "rebuild_queue_capacity": 4,
    "round_byte_budget": 2048,
    "round_time_budget_us": 20000,
    "read_base_latency_us": 1200,
    "read_per_shard_latency_us": 80,
    "replica_read_latency_us": 350,
    "rebuild_setup_us": 700,
    "rebuild_per_shard_latency_us": 250,
    "rebuild_bandwidth_bytes_per_us": 2,
    "merge_per_shard_latency_us": 40,
}

OBJECTS = {
    "obj-alpha": {"k": 4, "m": 2, "size": 512},
    "obj-beta": {"k": 4, "m": 2, "size": 512},
    "obj-gamma": {"k": 4, "m": 2, "size": 512},
    "obj-delta": {"k": 4, "m": 2, "size": 512},
    "obj-epsilon": {"k": 4, "m": 2, "size": 512},
    "obj-zeta": {"k": 4, "m": 2, "size": 512},
    "obj-eta": {"k": 4, "m": 2, "size": 512},
    "obj-theta": {"k": 4, "m": 2, "size": 512},
    "obj-iota": {"k": 4, "m": 2, "size": 512},
    "obj-kappa": {"k": 3, "m": 2, "size": 384},
}


def digest(content: bytes) -> str:
    return "sha256:" + hashlib.sha256(content).hexdigest()


def deterministic_bytes(namespace: str, size: int) -> bytes:
    return hashlib.shake_256(namespace.encode("utf-8")).digest(size)


def b64(content: bytes) -> str:
    return base64.b64encode(content).decode("ascii")


def build() -> dict:
    nodes: dict[str, bool] = {}
    disks: list[dict[str, str]] = []
    objects: list[dict] = []

    for object_id, spec in OBJECTS.items():
        k = spec["k"]
        m = spec["m"]
        size = spec["size"]
        data = [deterministic_bytes(f"{object_id}:data:{index}", size) for index in range(k)]
        parity = encode_shards(data, m)
        contents = data + parity
        shards = []
        for index, content in enumerate(contents):
            node_id = f"node-{object_id}-{index}"
            disk_id = f"disk-{object_id}-{index}"
            nodes[node_id] = True
            disks.append({"node_id": node_id, "disk_id": disk_id, "status": "online"})
            shard = {
                "shard_index": index,
                "role": "data" if index < k else "parity",
                "size_bytes": size,
                "digest": digest(content),
                "node_id": node_id,
                "disk_id": disk_id,
                "present": True,
                "content_b64": b64(content),
            }
            shards.append(shard)

        if object_id == "obj-alpha":
            nodes["node-obj-alpha-4"] = False
            disks = [
                entry
                if not (entry["node_id"] == "node-obj-alpha-2" and entry["disk_id"] == "disk-obj-alpha-2")
                else {**entry, "status": "offline"}
                for entry in disks
            ]
        elif object_id == "obj-beta":
            primary = shards[1]
            secondary_node = "node-obj-beta-1-replica"
            secondary_disk = "disk-obj-beta-1-replica"
            nodes[secondary_node] = True
            disks.append({"node_id": secondary_node, "disk_id": secondary_disk, "status": "online"})
            shards[1] = {
                "shard_index": 1,
                "role": "data",
                "size_bytes": size,
                "digest": primary["digest"],
                "replicas": [
                    {
                        "replica_id": "obj-beta-s1-primary",
                        "node_id": primary["node_id"],
                        "disk_id": primary["disk_id"],
                        "present": False,
                    },
                    {
                        "replica_id": "obj-beta-s1-replica",
                        "node_id": secondary_node,
                        "disk_id": secondary_disk,
                        "present": True,
                        "content_b64": primary["content_b64"],
                    },
                ],
            }
        elif object_id == "obj-gamma":
            shards[0]["content_b64"] = b64(deterministic_bytes("obj-gamma:corrupt:0", size))
            disks = [
                entry
                if not (entry["node_id"] == "node-obj-gamma-3" and entry["disk_id"] == "disk-obj-gamma-3")
                else {**entry, "status": "offline"}
                for entry in disks
            ]
            shards[5]["present"] = False
            shards[5].pop("content_b64", None)
        elif object_id == "obj-epsilon":
            disks = [
                entry
                if not (entry["node_id"] == "node-obj-epsilon-3" and entry["disk_id"] == "disk-obj-epsilon-3")
                else {**entry, "status": "offline"}
                for entry in disks
            ]
        elif object_id == "obj-zeta":
            nodes["node-obj-zeta-4"] = False
        elif object_id == "obj-eta":
            shards[2]["present"] = False
            shards[2].pop("content_b64", None)
        elif object_id == "obj-theta":
            disks = [
                entry
                if not (entry["node_id"] == "node-obj-theta-5" and entry["disk_id"] == "disk-obj-theta-5")
                else {**entry, "status": "offline"}
                for entry in disks
            ]
        elif object_id == "obj-iota":
            shards[2]["content_b64"] = b64(deterministic_bytes("obj-iota:corrupt:2", size))
        elif object_id == "obj-kappa":
            shards[1]["present"] = False
            shards[1].pop("content_b64", None)
            shards[1]["digest"] = "sha256:" + "0" * 64

        objects.append(
            {
                "object_id": object_id,
                "k": k,
                "m": m,
                "shards": shards,
            }
        )

    return {
        "schema_version": 1,
        "layout_version": "layout-2026-11-22-v1",
        "description": "Deterministic local read-repair and EC rebuild fixture",
        "max_k_plus_m": 8,
        "matrix_id": "cauchy-gf256-0x11d-v1",
        "engine": ENGINE,
        "nodes": nodes,
        "disks": disks,
        "objects": objects,
    }


MAIN_TRACE = {
    "trace_id": "main-budget-shed-demo",
    "layout_version": "layout-2026-11-22-v1",
    "requests": [
        {"request_id": "req-gamma-unrecoverable", "arrival_ms": 1000, "object_id": "obj-gamma", "shard_bitmap": "0x3f", "priority": 10, "tolerable_latency_us": 5000},
        {"request_id": "req-alpha-ec", "arrival_ms": 1001, "object_id": "obj-alpha", "shard_bitmap": "0x3f", "priority": 9, "tolerable_latency_us": 10000},
        {"request_id": "req-beta-replica", "arrival_ms": 1002, "object_id": "obj-beta", "shard_bitmap": "0x3f", "priority": 8, "tolerable_latency_us": 8000},
        {"request_id": "req-delta-direct", "arrival_ms": 1003, "object_id": "obj-delta", "shard_bitmap": "0x25", "priority": 7, "tolerable_latency_us": 5000},
        {"request_id": "req-epsilon-ec", "arrival_ms": 1004, "object_id": "obj-epsilon", "shard_bitmap": "0x3f", "priority": 6, "tolerable_latency_us": 10000},
        {"request_id": "req-iota-digest-repair", "arrival_ms": 1005, "object_id": "obj-iota", "shard_bitmap": "0x3f", "priority": 5, "tolerable_latency_us": 10000},
        {"request_id": "req-zeta-shed", "arrival_ms": 1006, "object_id": "obj-zeta", "shard_bitmap": "0x3f", "priority": 4, "tolerable_latency_us": 10000},
        {"request_id": "req-eta-shed", "arrival_ms": 1007, "object_id": "obj-eta", "shard_bitmap": "0x3f", "priority": 3, "tolerable_latency_us": 10000},
        {"request_id": "req-theta-latency", "arrival_ms": 1008, "object_id": "obj-theta", "shard_bitmap": "0x3f", "priority": 2, "tolerable_latency_us": 1},
    ],
}

QUEUE_TRACE = {
    "trace_id": "queue-full-demo",
    "layout_version": "layout-2026-11-22-v1",
    "options": {
        "rebuild_concurrency": 1,
        "rebuild_queue_capacity": 0,
        "round_byte_budget": 4096,
        "round_time_budget_us": 50000,
    },
    "requests": [
        {"request_id": "queue-zeta-01", "arrival_ms": 2000, "object_id": "obj-zeta", "shard_bitmap": "0x3f", "priority": 9, "tolerable_latency_us": 15000},
        {"request_id": "queue-eta-01", "arrival_ms": 2001, "object_id": "obj-eta", "shard_bitmap": "0x3f", "priority": 8, "tolerable_latency_us": 15000},
        {"request_id": "queue-theta-01", "arrival_ms": 2002, "object_id": "obj-theta", "shard_bitmap": "0x3f", "priority": 7, "tolerable_latency_us": 15000},
    ],
}

MISMATCH_TRACE = {
    "trace_id": "reconstruct-mismatch-demo",
    "layout_version": "layout-2026-11-22-v1",
    "options": {
        "rebuild_concurrency": 2,
        "rebuild_queue_capacity": 2,
        "round_byte_budget": 4096,
        "round_time_budget_us": 50000,
    },
    "requests": [
        {"request_id": "mismatch-kappa-01", "arrival_ms": 3000, "object_id": "obj-kappa", "shard_bitmap": "0x1f", "priority": 9, "tolerable_latency_us": 15000}
    ],
}

RECOVERY_TRACE = {
    "trace_id": "forced-termination-recovery-demo",
    "layout_version": "layout-2026-11-22-v1",
    "options": {
        "rebuild_concurrency": 2,
        "rebuild_queue_capacity": 2,
        "round_byte_budget": 4096,
        "round_time_budget_us": 50000,
    },
    "requests": [
        {"request_id": "recovery-alpha-01", "arrival_ms": 4000, "object_id": "obj-alpha", "shard_bitmap": "0x3f", "priority": 9, "tolerable_latency_us": 15000}
    ],
}


def main() -> None:
    DATA.mkdir(parents=True, exist_ok=True)
    files = {
        "sample_layout.json": build(),
        "sample_trace.json": MAIN_TRACE,
        "queue_trace.json": QUEUE_TRACE,
        "mismatch_trace.json": MISMATCH_TRACE,
        "recovery_trace.json": RECOVERY_TRACE,
    }
    for name, document in files.items():
        (DATA / name).write_text(json.dumps(document, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"wrote {len(files)} deterministic fixture files to {DATA}")


if __name__ == "__main__":
    main()
