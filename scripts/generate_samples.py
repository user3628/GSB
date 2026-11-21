from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from vgw.models import PATCH_SCHEMA, SNAPSHOT_SCHEMA, PatchDocument, SnapshotDocument
from vgw.snapshot import compute_digest, validate_patch, validate_snapshot


SAMPLES = ROOT / "samples"


def write_json(name: str, value: dict) -> None:
    (SAMPLES / name).write_text(
        json.dumps(value, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def snapshot_document() -> dict:
    raw = {
        "schema": SNAPSHOT_SCHEMA,
        "version": 1,
        "generated_at": "2026-11-22T00:00:00.000Z",
        "channel_count": 3,
        "round_budget_ms": 20,
        "queue_capacity": 2,
        "default_timeout_ms": 1500,
        "egress_endpoints": {
            "east": "127.0.0.1:18811",
            "west": "127.0.0.1:18812",
        },
        "bindings": {
            "alpha": {
                "priority": 40,
                "channel": 0,
                "egress": "east",
                "max_payload": 4096,
            },
            "beta": {
                "priority": 30,
                "channel": 1,
                "egress": "west",
                "max_payload": 4096,
            },
            "gamma": {
                "priority": 20,
                "channel": 2,
                "egress": "east",
                "max_payload": 4096,
            },
            "delta": {
                "priority": 15,
                "channel": 0,
                "egress": "west",
                "max_payload": 4096,
            },
            "epsilon": {
                "priority": 10,
                "channel": 1,
                "egress": "east",
                "max_payload": 4096,
            },
            "zeta": {
                "priority": 5,
                "channel": 2,
                "egress": "west",
                "max_payload": 4096,
            },
        },
        "digest": "sha256:" + "0" * 64,
    }
    model = SnapshotDocument.model_validate(raw)
    raw["digest"] = compute_digest(model)
    validate_snapshot(raw)
    return raw


def patch_document() -> dict:
    raw = {
        "schema": PATCH_SCHEMA,
        "base_version": 1,
        "version": 2,
        "generated_at": "2026-11-22T00:10:00.000Z",
        "bindings": {
            "beta": {
                "priority": 30,
                "channel": 1,
                "egress": "east",
                "max_payload": 4096,
            }
        },
        "egress_endpoints": {},
        "digest": "sha256:" + "0" * 64,
    }
    model = PatchDocument.model_validate(raw)
    raw["digest"] = compute_digest(model)
    validate_patch(raw)
    return raw


def topology_document() -> dict:
    return {
        "egress_timeout_ms": 5000,
        "carriers": {
            "east": {
                "listen": "127.0.0.1:18811",
                "egress": "127.0.0.1:18911",
                "egress_name": "decap-east",
            },
            "west": {
                "listen": "127.0.0.1:18812",
                "egress": "127.0.0.1:18912",
                "egress_name": "decap-west",
            },
        },
    }


def frames_document() -> dict:
    return {
        "schema": "vgw.frame-list/v1",
        "profile": "fixed-loopback-samples",
        "parallel": [
            {
                "frame_id": "par-alpha-1",
                "session_id": "alpha",
                "sequence": 1,
                "payload": {
                    "op": "echo",
                    "body": "alpha-1",
                    "options": {"delay_ms": 90},
                },
            },
            {
                "frame_id": "par-beta-1",
                "session_id": "beta",
                "sequence": 1,
                "payload": {
                    "op": "echo",
                    "body": "beta-1",
                    "options": {"delay_ms": 90},
                },
            },
            {
                "frame_id": "par-gamma-1",
                "session_id": "gamma",
                "sequence": 1,
                "payload": {
                    "op": "echo",
                    "body": "gamma-1",
                    "options": {"delay_ms": 90},
                },
            },
            {
                "frame_id": "par-alpha-2",
                "session_id": "alpha",
                "sequence": 2,
                "payload": {"op": "echo", "body": "alpha-2"},
            },
            {
                "frame_id": "par-beta-2",
                "session_id": "beta",
                "sequence": 2,
                "payload": {"op": "echo", "body": "beta-2"},
            },
            {
                "frame_id": "par-gamma-2",
                "session_id": "gamma",
                "sequence": 2,
                "payload": {"op": "echo", "body": "gamma-2"},
            },
        ],
        "post_patch": [
            {
                "frame_id": "patch-beta-3",
                "session_id": "beta",
                "sequence": 3,
                "payload": {"op": "echo", "body": "beta-after-rebuild"},
            }
        ],
        "pressure": {
            "active": [
                {
                    "frame_id": "pressure-alpha-3",
                    "session_id": "alpha",
                    "sequence": 3,
                    "payload": {
                        "op": "echo",
                        "body": "alpha-pressure",
                        "options": {"delay_ms": 500},
                    },
                },
                {
                    "frame_id": "pressure-beta-4",
                    "session_id": "beta",
                    "sequence": 4,
                    "payload": {
                        "op": "echo",
                        "body": "beta-pressure",
                        "options": {"delay_ms": 500},
                    },
                },
                {
                    "frame_id": "pressure-gamma-3",
                    "session_id": "gamma",
                    "sequence": 3,
                    "payload": {
                        "op": "echo",
                        "body": "gamma-pressure",
                        "options": {"delay_ms": 500},
                    },
                },
            ],
            "waiting": [
                {
                    "frame_id": "pressure-delta-1",
                    "session_id": "delta",
                    "sequence": 1,
                    "payload": {"op": "echo", "body": "delta-waiting"},
                },
                {
                    "frame_id": "pressure-epsilon-1",
                    "session_id": "epsilon",
                    "sequence": 1,
                    "payload": {"op": "echo", "body": "epsilon-waiting"},
                },
            ],
            "overflow": [
                {
                    "frame_id": "pressure-zeta-1",
                    "session_id": "zeta",
                    "sequence": 1,
                    "payload": {"op": "echo", "body": "zeta-overflow"},
                }
            ],
        },
    }


def expected_document() -> dict:
    return {
        "schema": "vgw.expected-order/v1",
        "parallel": {
            "alpha": [1, 2],
            "beta": [1, 2],
            "gamma": [1, 2],
        },
        "post_patch": {"beta": [1, 2, 3]},
        "pressure": {
            "accepted": {
                "alpha": [1, 2, 3],
                "beta": [1, 2, 3, 4],
                "gamma": [1, 2, 3],
                "delta": [1],
                "epsilon": [1],
            },
            "overflow_error": "E_CHANNEL_QUEUE_FULL",
        },
    }


def main() -> None:
    SAMPLES.mkdir(parents=True, exist_ok=True)
    write_json("snapshot_v1.json", snapshot_document())
    write_json("patch_v2.json", patch_document())
    write_json("topology.json", topology_document())
    write_json("frames.json", frames_document())
    write_json("expected.json", expected_document())
    print("wrote samples to", SAMPLES)


if __name__ == "__main__":
    main()
