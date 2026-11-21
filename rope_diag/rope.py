"""RoPE frequency generation and deterministic block-wise rotation."""

from __future__ import annotations

from typing import Any, Callable

import numpy as np

from .config import RopeConfig
from .errors import PipelineStopped


def _ntk_piecewise_inverse_frequencies(
    base: float, factor: float, pair_count: int, rotated_dim: int
) -> np.ndarray:
    """Piecewise NTK: enhanced NTK for high frequencies, linear for low frequencies.

    Pair index is normalized to [0, 1] over the rotated head dimensions. The
    first half uses the NTK-aware enlarged base, while the second half uses
    linear frequency interpolation. This makes the two supported scaling
    behaviors explicit and reproducible without extra YAML fields.
    """

    pair_index = np.arange(pair_count, dtype=np.float64)
    exponent = -2.0 * pair_index / float(rotated_dim)
    standard = np.power(base, exponent)
    if rotated_dim > 2:
        ntk_base = base * np.power(factor, rotated_dim / (rotated_dim - 2.0))
    else:
        ntk_base = base * factor
    ntk = np.power(ntk_base, exponent)
    linear = standard / factor
    if pair_count == 1:
        return ntk.astype(np.float64, copy=False)
    normalized = pair_index / float(pair_count - 1)
    return np.where(normalized < 0.5, ntk, linear).astype(np.float64, copy=False)


def build_frequency_artifact(
    sequence_length: int, head_dim: int, config: RopeConfig
) -> dict[str, Any]:
    if sequence_length <= 0 or head_dim <= 0:
        raise ValueError("sequence_length and head_dim must be positive")
    rotated_dim = head_dim - (head_dim % 2)
    pair_count = rotated_dim // 2
    pair_index = np.arange(pair_count, dtype=np.int64)
    exponent = -2.0 * pair_index.astype(np.float64) / float(rotated_dim)
    standard = np.power(float(config.base_frequency), exponent)

    if config.scaling_mode == "none":
        inverse_frequencies = standard
    elif config.scaling_mode == "linear":
        inverse_frequencies = standard / float(config.scaling_factor)
    elif config.scaling_mode == "ntk_piecewise":
        inverse_frequencies = _ntk_piecewise_inverse_frequencies(
            float(config.base_frequency),
            float(config.scaling_factor),
            pair_count,
            rotated_dim,
        )
    else:  # pragma: no cover - config validation is authoritative
        raise ValueError(f"unsupported scaling mode: {config.scaling_mode}")

    positions = np.arange(sequence_length, dtype=np.float64)
    pair_angles = positions[:, None] * inverse_frequencies[None, :]
    angle_table = np.zeros((sequence_length, head_dim), dtype=np.float64)
    for pair in range(pair_count):
        channel = pair * 2
        angle_table[:, channel] = pair_angles[:, pair]
        angle_table[:, channel + 1] = pair_angles[:, pair]

    rotated_mask = np.zeros(head_dim, dtype=np.bool_)
    if pair_count:
        rotated_mask[: pair_count * 2] = True
    unrotated_channels = np.flatnonzero(~rotated_mask).astype(np.int64)
    channel_pair_index = np.full(head_dim, -1, dtype=np.int64)
    for pair in range(pair_count):
        channel_pair_index[pair * 2 : pair * 2 + 2] = pair

    arrays = {
        "angle_table": angle_table,
        "inverse_frequencies": inverse_frequencies.astype(np.float64, copy=False),
        "rotated_mask": rotated_mask,
        "unrotated_channels": unrotated_channels,
        "channel_pair_index": channel_pair_index,
    }
    metadata = {
        "sequence_length": int(sequence_length),
        "head_dim": int(head_dim),
        "rotated_dim": int(rotated_dim),
        "pair_count": int(pair_count),
        "unrotated_channels": unrotated_channels.tolist(),
        "scaling_mode": config.scaling_mode,
        "base_frequency": float(config.base_frequency),
        "scaling_factor": float(config.scaling_factor),
        "rotation_block_size": int(config.rotation_block_size),
        "odd_head_dim_recorded": bool(head_dim % 2 == 1),
    }
    return {"arrays": arrays, "metadata": metadata}


def _rotation_blocks(head_dim: int, block_size: int) -> list[tuple[int, int]]:
    blocks: list[tuple[int, int]] = []
    for start in range(0, head_dim, block_size):
        width = min(block_size, head_dim - start)
        pair_count = width // 2
        if pair_count:
            blocks.append((start, pair_count))
    return blocks


def rotate_tensors(
    query: np.ndarray,
    key: np.ndarray,
    angle_table: np.ndarray,
    config: RopeConfig,
    *,
    batch_chunk_size: int | None = None,
    should_stop: Callable[[], bool] | None = None,
) -> dict[str, np.ndarray]:
    """Rotate q/k using adjacent pairs inside each configured channel block."""

    if query.shape != key.shape:
        raise ValueError("query and key shapes must match")
    if query.ndim != 4:
        raise ValueError("query and key must be 4D")
    batch, heads, sequence_length, head_dim = query.shape
    if angle_table.shape != (sequence_length, head_dim):
        raise ValueError("angle table shape does not match tensors")

    cos_table = np.cos(angle_table)
    sin_table = np.sin(angle_table)
    query_rotated = query.astype(np.float64, copy=True)
    key_rotated = key.astype(np.float64, copy=True)
    blocks = _rotation_blocks(head_dim, int(config.rotation_block_size))
    chunk_size = int(batch_chunk_size or batch)
    if chunk_size <= 0 or chunk_size > batch:
        chunk_size = batch

    for batch_start in range(0, batch, chunk_size):
        batch_end = min(batch, batch_start + chunk_size)
        q_chunk = query[batch_start:batch_end].astype(np.float64, copy=False)
        k_chunk = key[batch_start:batch_end].astype(np.float64, copy=False)
        for start, pair_count in blocks:
            stop = start + pair_count * 2
            channel_index = np.arange(start, stop, 2, dtype=np.int64)
            cosine = cos_table[:, channel_index][None, None, :, :]
            sine = sin_table[:, channel_index][None, None, :, :]

            q_even = q_chunk[..., start:stop:2]
            q_odd = q_chunk[..., start + 1 : stop : 2]
            k_even = k_chunk[..., start:stop:2]
            k_odd = k_chunk[..., start + 1 : stop : 2]

            q0 = q_even * cosine - q_odd * sine
            q1 = q_even * sine + q_odd * cosine
            k0 = k_even * cosine - k_odd * sine
            k1 = k_even * sine + k_odd * cosine

            query_rotated[batch_start:batch_end, ..., start:stop:2] = q0
            query_rotated[batch_start:batch_end, ..., start + 1 : stop : 2] = q1
            key_rotated[batch_start:batch_end, ..., start:stop:2] = k0
            key_rotated[batch_start:batch_end, ..., start + 1 : stop : 2] = k1

        if should_stop is not None and should_stop():
            raise PipelineStopped("block_rotation", f"batch_chunk:{batch_start}")

    return {"query_rotated": query_rotated, "key_rotated": key_rotated}


def rotation_block_layout(head_dim: int, block_size: int) -> list[dict[str, int]]:
    return [
        {"start_channel": start, "rotated_channels": pair_count * 2, "pairs": pair_count}
        for start, pair_count in _rotation_blocks(head_dim, block_size)
    ]
