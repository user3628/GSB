from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from .aggregation import GradientBlock, aggregate_blocks
from .domain import FinalizationPolicy

MASTER_SEED = 20261120
SCHEMA_VERSION = 1


@dataclass(frozen=True)
class RoundSample:
    round_id: str
    seed: int
    shard_count: int
    shard_dim: int
    worker_ids: tuple[str, ...]
    sample_counts: dict[str, int]
    collect_seconds: float
    max_wait_seconds: float
    policy: FinalizationPolicy
    gradients: dict[tuple[int, str], tuple[float, ...]]

    def create_request(
        self,
        *,
        worker_endpoints: dict[str, str] | None = None,
        round_id: str | None = None,
        collect_seconds: float | None = None,
        max_wait_seconds: float | None = None,
        policy: str | None = None,
    ) -> dict[str, Any]:
        return {
            "round_id": round_id or self.round_id,
            "seed": self.seed,
            "shard_count": self.shard_count,
            "shard_dim": self.shard_dim,
            "worker_ids": list(self.worker_ids),
            "sample_counts": dict(self.sample_counts),
            "worker_endpoints": worker_endpoints or {},
            "policy": policy or self.policy.value,
            "collect_seconds": (
                self.collect_seconds if collect_seconds is None else collect_seconds
            ),
            "max_wait_seconds": (
                self.max_wait_seconds if max_wait_seconds is None else max_wait_seconds
            ),
        }

    def submissions_for(self, node_id: str, *, delay_seconds: float = 0.0) -> list[dict[str, Any]]:
        return [
            {
                "node_id": node_id,
                "shard_id": shard_id,
                "sample_count": self.sample_counts[node_id],
                "gradient": list(self.gradients[(shard_id, node_id)]),
                "delay_seconds": delay_seconds,
            }
            for shard_id in range(self.shard_count)
        ]

    def all_submissions(self, delays: dict[str, float] | None = None) -> list[dict[str, Any]]:
        delays = delays or {}
        items: list[dict[str, Any]] = []
        for node_id in self.worker_ids:
            items.extend(self.submissions_for(node_id, delay_seconds=delays.get(node_id, 0.0)))
        return items

    def expected_digest(self, included_nodes: tuple[str, ...] | list[str] | None = None) -> str:
        nodes = set(included_nodes or self.worker_ids)
        blocks = [
            GradientBlock(
                node_id=node_id,
                shard_id=shard_id,
                sample_count=self.sample_counts[node_id],
                gradient=np.asarray(self.gradients[(shard_id, node_id)], dtype=np.float64),
            )
            for shard_id in range(self.shard_count)
            for node_id in self.worker_ids
            if node_id in nodes
        ]
        return aggregate_blocks(
            round_id=self.round_id,
            shard_count=self.shard_count,
            shard_dim=self.shard_dim,
            blocks=blocks,
        ).digest

    def to_manifest(self) -> dict[str, Any]:
        return {
            "round_id": self.round_id,
            "seed": self.seed,
            "shard_count": self.shard_count,
            "shard_dim": self.shard_dim,
            "worker_ids": list(self.worker_ids),
            "sample_counts": dict(self.sample_counts),
            "collect_seconds": self.collect_seconds,
            "max_wait_seconds": self.max_wait_seconds,
            "policy": self.policy.value,
            "shards": [
                {
                    "shard_id": shard_id,
                    "gradients": {
                        node_id: list(self.gradients[(shard_id, node_id)])
                        for node_id in self.worker_ids
                    },
                }
                for shard_id in range(self.shard_count)
            ],
        }


def _make_round(
    *,
    round_id: str,
    seed: int,
    worker_ids: tuple[str, ...],
    sample_counts: dict[str, int],
    collect_seconds: float,
    max_wait_seconds: float,
    policy: FinalizationPolicy,
    shard_count: int = 3,
    shard_dim: int = 6,
) -> RoundSample:
    rng = np.random.Generator(np.random.PCG64(seed))
    worker_offset = {node_id: index + 1 for index, node_id in enumerate(worker_ids)}
    gradients: dict[tuple[int, str], tuple[float, ...]] = {}
    for shard_id in range(shard_count):
        for node_id in worker_ids:
            base = rng.standard_normal(shard_dim) * 0.25
            drift = np.linspace(-0.01, 0.01, shard_dim) * worker_offset[node_id]
            values = np.round(base + drift + shard_id * 0.05, 10)
            gradients[(shard_id, node_id)] = tuple(float(value) for value in values)
    return RoundSample(
        round_id=round_id,
        seed=seed,
        shard_count=shard_count,
        shard_dim=shard_dim,
        worker_ids=worker_ids,
        sample_counts=sample_counts,
        collect_seconds=collect_seconds,
        max_wait_seconds=max_wait_seconds,
        policy=policy,
        gradients=gradients,
    )


def build_sample_manifest() -> dict[str, Any]:
    workers = ("worker-a", "worker-b", "worker-c")
    weights = {"worker-a": 10, "worker-b": 20, "worker-c": 30}
    rounds = [
        _make_round(
            round_id="demo-complete",
            seed=11001,
            worker_ids=workers,
            sample_counts=weights,
            collect_seconds=8.0,
            max_wait_seconds=2.0,
            policy=FinalizationPolicy.STRICT,
        ),
        _make_round(
            round_id="demo-strict-late",
            seed=11002,
            worker_ids=workers,
            sample_counts=weights,
            collect_seconds=0.6,
            max_wait_seconds=2.5,
            policy=FinalizationPolicy.STRICT,
        ),
        _make_round(
            round_id="demo-degraded",
            seed=11003,
            worker_ids=workers,
            sample_counts=weights,
            collect_seconds=0.5,
            max_wait_seconds=2.0,
            policy=FinalizationPolicy.DEGRADED,
        ),
        _make_round(
            round_id="demo-idempotent-a",
            seed=11004,
            worker_ids=workers,
            sample_counts=weights,
            collect_seconds=8.0,
            max_wait_seconds=2.0,
            policy=FinalizationPolicy.STRICT,
        ),
        _make_round(
            round_id="demo-idempotent-b",
            seed=11004,
            worker_ids=workers,
            sample_counts=weights,
            collect_seconds=8.0,
            max_wait_seconds=2.0,
            policy=FinalizationPolicy.STRICT,
        ),
        _make_round(
            round_id="demo-crash",
            seed=11005,
            worker_ids=workers,
            sample_counts=weights,
            collect_seconds=8.0,
            max_wait_seconds=2.0,
            policy=FinalizationPolicy.STRICT,
        ),
        _make_round(
            round_id="demo-zero-weight",
            seed=11006,
            worker_ids=workers,
            sample_counts={"worker-a": 0, "worker-b": 0, "worker-c": 0},
            collect_seconds=8.0,
            max_wait_seconds=2.0,
            policy=FinalizationPolicy.STRICT,
        ),
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "master_seed": MASTER_SEED,
        "sample_date_utc": "2026-11-20T00:00:00Z",
        "rounds": {sample.round_id: sample.to_manifest() for sample in rounds},
    }


def manifest_to_rounds(manifest: dict[str, Any]) -> dict[str, RoundSample]:
    result: dict[str, RoundSample] = {}
    for round_id, raw in manifest["rounds"].items():
        gradients: dict[tuple[int, str], tuple[float, ...]] = {}
        for shard in raw["shards"]:
            shard_id = int(shard["shard_id"])
            for node_id, gradient in shard["gradients"].items():
                gradients[(shard_id, node_id)] = tuple(float(value) for value in gradient)
        result[round_id] = RoundSample(
            round_id=round_id,
            seed=int(raw["seed"]),
            shard_count=int(raw["shard_count"]),
            shard_dim=int(raw["shard_dim"]),
            worker_ids=tuple(raw["worker_ids"]),
            sample_counts={key: int(value) for key, value in raw["sample_counts"].items()},
            collect_seconds=float(raw["collect_seconds"]),
            max_wait_seconds=float(raw["max_wait_seconds"]),
            policy=FinalizationPolicy(raw["policy"]),
            gradients=gradients,
        )
    return result


def write_sample_manifest(path: Path) -> dict[str, Any]:
    manifest = build_sample_manifest()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(payload, encoding="utf-8", newline="\n")
    temp_path.replace(path)
    return manifest


def load_sample_manifest(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))

