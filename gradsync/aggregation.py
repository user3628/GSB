from __future__ import annotations

import base64
import hashlib
import json
import math
import struct
from dataclasses import dataclass
from typing import Any, Iterable

import numpy as np

from .errors import ErrorCode, GradSyncError


@dataclass(frozen=True)
class GradientBlock:
    node_id: str
    shard_id: int
    sample_count: int
    gradient: np.ndarray
    content_hash: str | None = None


@dataclass(frozen=True)
class AggregateTensor:
    tensor: np.ndarray
    digest: str
    element_sum: float
    element_sum_text: str
    tensor_preview: list[list[str]]
    tensor_b64: str


def canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def sha256_json(value: Any) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def gradient_content_hash(sample_count: int, gradient: Iterable[float]) -> str:
    array = np.asarray(list(gradient), dtype="<f8", order="C")
    digest = hashlib.sha256()
    digest.update(b"gradsync-gradient-v1\0")
    digest.update(struct.pack("<Q", int(sample_count)))
    digest.update(struct.pack("<Q", int(array.size)))
    digest.update(array.tobytes(order="C"))
    return digest.hexdigest()


def tensor_digest(tensor: np.ndarray) -> str:
    array = np.asarray(tensor, dtype="<f8", order="C")
    digest = hashlib.sha256()
    digest.update(b"gradsync-aggregate-v1\0")
    digest.update(struct.pack("<II", int(array.shape[0]), int(array.shape[1])))
    digest.update(array.tobytes(order="C"))
    return digest.hexdigest()


def aggregate_blocks(
    *,
    round_id: str,
    shard_count: int,
    shard_dim: int,
    blocks: list[GradientBlock],
) -> AggregateTensor:
    """Aggregate blocks in canonical (shard, node) order.

    The caller may provide blocks in any arrival order. Sorting before the
    sequential float64 accumulation makes the numeric path independent of HTTP
    arrival order while remaining bit-for-bit reproducible for identical input.
    """

    by_shard: dict[int, list[GradientBlock]] = {shard: [] for shard in range(shard_count)}
    for block in blocks:
        if block.shard_id not in by_shard:
            raise GradSyncError(
                ErrorCode.E_INVALID_SUBMISSION,
                "提交包含未知分片",
                round_id=round_id,
                details={"shard_id": block.shard_id},
            )
        gradient = np.asarray(block.gradient, dtype=np.float64, order="C")
        if gradient.shape != (shard_dim,):
            raise GradSyncError(
                ErrorCode.E_INVALID_SUBMISSION,
                "梯度块维度不符合轮次元数据",
                round_id=round_id,
                details={
                    "node_id": block.node_id,
                    "shard_id": block.shard_id,
                    "expected_dim": shard_dim,
                    "actual_shape": list(gradient.shape),
                },
            )
        if block.sample_count < 0:
            raise GradSyncError(
                ErrorCode.E_INVALID_SUBMISSION,
                "样本数不能为负数",
                round_id=round_id,
                details={"node_id": block.node_id, "shard_id": block.shard_id},
            )
        by_shard[block.shard_id].append(block)

    output = np.zeros((shard_count, shard_dim), dtype=np.float64, order="C")
    for shard_id in range(shard_count):
        shard_blocks = sorted(by_shard[shard_id], key=lambda item: item.node_id)
        if not shard_blocks:
            raise GradSyncError(
                ErrorCode.E_ZERO_WEIGHT,
                f"分片 {shard_id} 没有可用梯度块，权重和为 0",
                round_id=round_id,
                details={"shard_id": shard_id, "available_nodes": []},
            )

        accumulator = [0.0] * shard_dim
        total_weight = 0
        for block in shard_blocks:
            weight = int(block.sample_count)
            total_weight += weight
            gradient = np.asarray(block.gradient, dtype=np.float64, order="C")
            for index in range(shard_dim):
                accumulator[index] += float(gradient[index]) * weight

        if total_weight == 0:
            raise GradSyncError(
                ErrorCode.E_ZERO_WEIGHT,
                f"分片 {shard_id} 的可用权重和为 0",
                round_id=round_id,
                details={
                    "shard_id": shard_id,
                    "available_nodes": [block.node_id for block in shard_blocks],
                    "total_weight": 0,
                },
            )
        for index in range(shard_dim):
            output[shard_id, index] = accumulator[index] / total_weight

    digest = tensor_digest(output)
    element_sum = math.fsum(float(value) for value in output.reshape(-1))
    preview = [[f"{float(value):.8f}" for value in row] for row in output]
    encoded = base64.b64encode(
        np.asarray(output, dtype="<f8", order="C").tobytes(order="C")
    ).decode("ascii")
    return AggregateTensor(
        tensor=output,
        digest=digest,
        element_sum=element_sum,
        element_sum_text=f"{element_sum:.8f}",
        tensor_preview=preview,
        tensor_b64=encoded,
    )

