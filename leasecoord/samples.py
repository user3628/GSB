"""Load and expand the fixed local deterministic sample datasets."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class SampleNode:
    id: str
    priority: int
    lease_ttl_ms: int
    last_heartbeat_ms: int


@dataclass(frozen=True, slots=True)
class SampleDataset:
    name: str
    base_epoch_ms: int
    heartbeat_timeout_ms: int
    exact_limit: int
    nodes: tuple[SampleNode, ...]
    heartbeat_sequence: tuple[dict[str, Any], ...]
    scenarios: dict[str, Any]
    source_path: str


def _expand_large(raw: dict[str, Any]) -> list[SampleNode]:
    generator = raw["generator"]
    if generator.get("kind") != "synthetic":
        raise ValueError("unsupported sample generator kind")
    count = int(generator["count"])
    id_format = generator["id_format"]
    priority_formula = generator["priority_formula"]
    if priority_formula != "100 - (index % 101)":
        raise ValueError("unsupported priority formula in fixed sample")
    ttl_cycle = [int(value) for value in generator["lease_ttl_cycle_ms"]]
    offset_formula = generator["last_heartbeat_offset_ms_formula"]
    if offset_formula != "-((index * 7) % 2500)":
        raise ValueError("unsupported heartbeat formula in fixed sample")
    base = int(raw["base_epoch_ms"])
    nodes = []
    for index in range(count):
        nodes.append(
            SampleNode(
                id=id_format.format(index=index),
                priority=100 - (index % 101),
                lease_ttl_ms=ttl_cycle[index % len(ttl_cycle)],
                last_heartbeat_ms=base - ((index * 7) % 2500),
            )
        )
    return nodes


def load_sample(path: str | Path) -> SampleDataset:
    source = Path(path)
    raw = json.loads(source.read_text(encoding="utf-8"))
    base = int(raw["base_epoch_ms"])
    if "nodes" in raw:
        nodes = [
            SampleNode(
                id=item["id"],
                priority=int(item["priority"]),
                lease_ttl_ms=int(item["lease_ttl_ms"]),
                last_heartbeat_ms=base + int(item.get("last_heartbeat_offset_ms", 0)),
            )
            for item in raw["nodes"]
        ]
    elif "generator" in raw:
        nodes = _expand_large(raw)
    else:
        raise ValueError("sample must contain nodes or generator")
    return SampleDataset(
        name=str(raw["name"]),
        base_epoch_ms=base,
        heartbeat_timeout_ms=int(raw["heartbeat_timeout_ms"]),
        exact_limit=int(raw["exact_limit"]),
        nodes=tuple(nodes),
        heartbeat_sequence=tuple(raw.get("heartbeat_sequence", [])),
        scenarios=dict(raw.get("scenarios", {})),
        source_path=str(source),
    )
