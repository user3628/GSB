from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Iterable


CURRENT_FORMAT_VERSION = 3


@dataclass(frozen=True, slots=True)
class Policy:
    queue_id: str
    cir_bps: int
    pir_bps: int
    cbs_bytes: int
    pbs_bytes: int
    priority: int
    match_rule: str
    parent_queue_id: str | None
    timestamp_us: int

    def canonical_dict(self) -> dict[str, Any]:
        return {
            "queue_id": self.queue_id,
            "cir_bps": self.cir_bps,
            "pir_bps": self.pir_bps,
            "cbs_bytes": self.cbs_bytes,
            "pbs_bytes": self.pbs_bytes,
            "priority": self.priority,
            "match_rule": self.match_rule,
            "parent_queue_id": self.parent_queue_id,
            "timestamp_us": self.timestamp_us,
        }

    @classmethod
    def from_mapping(cls, value: dict[str, Any]) -> "Policy":
        return cls(
            queue_id=str(value["queue_id"]),
            cir_bps=int(value["cir_bps"]),
            pir_bps=int(value["pir_bps"]),
            cbs_bytes=int(value["cbs_bytes"]),
            pbs_bytes=int(value["pbs_bytes"]),
            priority=int(value["priority"]),
            match_rule=str(value["match_rule"]),
            parent_queue_id=(
                str(value["parent_queue_id"])
                if value.get("parent_queue_id") is not None
                else None
            ),
            timestamp_us=int(value["timestamp_us"]),
        )


@dataclass(frozen=True, slots=True)
class PolicySnapshot:
    format_version: int
    snapshot_no: int
    generated_at_us: int
    policies: tuple[Policy, ...]

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "format_version": self.format_version,
            "snapshot_no": self.snapshot_no,
            "generated_at_us": self.generated_at_us,
            "policies": [p.canonical_dict() for p in self.policies],
        }

    def canonical_json(self) -> str:
        return json.dumps(
            self.canonical_payload(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )

    @property
    def checksum(self) -> str:
        return hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()

    @property
    def by_queue(self) -> dict[str, Policy]:
        return {policy.queue_id: policy for policy in self.policies}

    @classmethod
    def from_mapping(cls, value: dict[str, Any]) -> "PolicySnapshot":
        policies = tuple(
            sorted(
                (Policy.from_mapping(item) for item in value["policies"]),
                key=lambda p: p.queue_id,
            )
        )
        return cls(
            format_version=int(value["format_version"]),
            snapshot_no=int(value["snapshot_no"]),
            generated_at_us=int(value["generated_at_us"]),
            policies=policies,
        )


@dataclass(frozen=True, slots=True)
class TrafficSample:
    timestamp_us: int
    queue_id: str
    packet_bytes: int
    color: str
    source_index: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "index": self.source_index,
            "timestamp_us": self.timestamp_us,
            "timestamp_ms": decimal_ms(self.timestamp_us),
            "queue_id": self.queue_id,
            "packet_bytes": self.packet_bytes,
            "color": self.color,
        }


def decimal_ms(timestamp_us: int) -> str:
    """Render microseconds as a fixed, lossless millisecond decimal string."""
    whole, fraction = divmod(timestamp_us, 1000)
    if fraction == 0:
        return str(whole)
    return f"{whole}.{fraction:03d}".rstrip("0")


def policies_to_dicts(policies: Iterable[Policy]) -> list[dict[str, Any]]:
    return [policy.canonical_dict() for policy in policies]

