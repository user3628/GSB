from __future__ import annotations

import hashlib

import numpy as np

from .config import LabConfig, NodeConfig
from .models import GradientBlock


def _seed_for(seed: int, node_id: str, round_no: int, shard_id: int) -> int:
    material = f"{seed}:{node_id}:{round_no}:{shard_id}".encode()
    return int.from_bytes(hashlib.sha256(material).digest()[:8], "little", signed=False)


def generate_tensor(
    seed: int,
    node_id: str,
    round_no: int,
    shard_id: int,
    shape: tuple[int, ...],
) -> np.ndarray:
    rng = np.random.default_rng(_seed_for(seed, node_id, round_no, shard_id))
    return rng.normal(loc=0.0, scale=0.5, size=shape).astype(np.float64)


def generate_block(
    config: LabConfig,
    node: NodeConfig,
    round_no: int,
    shard_id: int,
) -> GradientBlock:
    if not 0 <= shard_id < config.shards_per_node:
        raise ValueError(f"shard_id {shard_id} is outside node {node.node_id}'s shards")
    return GradientBlock(
        node_id=node.node_id,
        round=round_no,
        shard_id=shard_id,
        sample_weight=node.sample_weights[shard_id],
        tensor=generate_tensor(config.seed, node.node_id, round_no, shard_id, config.tensor_shape),
    )


def generate_round_blocks(config: LabConfig, round_no: int) -> list[GradientBlock]:
    return [
        generate_block(config, node, round_no, shard_id)
        for node in config.nodes
        for shard_id in range(config.shards_per_node)
    ]

