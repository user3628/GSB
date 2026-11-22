"""Fixed-point quantization, deterministic bucket hashing, and collision audit."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

from .config import ShardConfig
from .errors import E_BUCKET_COLLISION, FingerprintError
from .pairing import FingerprintPair


UINT64_MASK = (1 << 64) - 1
FNV64_OFFSET = 14695981039346656037
FNV64_PRIME = 1099511628211
MICRO = 1_000_000


def quantize_value(value: int, scale: float, levels: int) -> int:
    if value < 0:
        raise ValueError("negative fixed-point values are not supported")
    raw = int(math.floor(value / scale + 0.5))
    return min(levels, max(0, raw))


def fnv1a64_vector(seed: int, values: tuple[int, ...]) -> int:
    digest = (FNV64_OFFSET ^ (seed & UINT64_MASK)) & UINT64_MASK
    for value in values:
        word = value & UINT64_MASK
        for shift in (0, 32):
            digest ^= (word >> shift) & 0xFFFFFFFF
            digest = (digest * FNV64_PRIME) & UINT64_MASK
    return digest


def _effective_bits(value_range: float, actual_error: float, configured_bits: int) -> float:
    if actual_error <= 0.0:
        return float(configured_bits)
    bits = math.log2(value_range / (2.0 * actual_error))
    return float(min(configured_bits, bits))


@dataclass(frozen=True)
class QuantizedPair:
    pair_index: int
    bucket: int
    anchor_freq_q: int
    target_freq_q: int
    dt_q: int

    @property
    def key(self) -> tuple[int, int, int]:
        return (self.anchor_freq_q, self.target_freq_q, self.dt_q)

    def as_dict(self) -> dict[str, int]:
        return {
            "pair_index": self.pair_index,
            "bucket": self.bucket,
            "anchor_freq_q": self.anchor_freq_q,
            "target_freq_q": self.target_freq_q,
            "dt_q": self.dt_q,
        }


@dataclass(frozen=True)
class QuantizationResult:
    vectors: tuple[QuantizedPair, ...]
    fields: dict[str, dict[str, Any]]
    collisions: dict[str, Any]
    levels: int


def quantize_pairs(pairs: tuple[FingerprintPair, ...], config: ShardConfig) -> QuantizationResult:
    bits = config.quantization_bits
    levels = (1 << bits) - 1
    frequency_range = config.frame_length // 2
    dt_range = config.time_delta_max
    freq_scale = frequency_range / levels
    dt_scale = dt_range / levels

    vectors: list[QuantizedPair] = []
    anchor_errors: list[float] = []
    target_errors: list[float] = []
    dt_errors: list[float] = []
    for pair in pairs:
        anchor_q = quantize_value(pair.anchor_freq, freq_scale, levels)
        target_q = quantize_value(pair.target_freq, freq_scale, levels)
        dt_q = quantize_value(pair.dt, dt_scale, levels)
        anchor_errors.append(abs(anchor_q * freq_scale - pair.anchor_freq))
        target_errors.append(abs(target_q * freq_scale - pair.target_freq))
        dt_errors.append(abs(dt_q * dt_scale - pair.dt))
        bucket = fnv1a64_vector(config.hash_seed, (anchor_q, target_q, dt_q)) % config.bucket_width
        vectors.append(
            QuantizedPair(
                pair_index=pair.pair_index,
                bucket=bucket,
                anchor_freq_q=anchor_q,
                target_freq_q=target_q,
                dt_q=dt_q,
            )
        )

    anchor_max_error = max(anchor_errors, default=0.0)
    target_max_error = max(target_errors, default=0.0)
    dt_max_error = max(dt_errors, default=0.0)
    fields = {
        "anchor_freq": {
            "unit": "frequency_bin",
            "configured_bits": bits,
            "value_range": frequency_range,
            "levels": levels,
            "scale_num": frequency_range,
            "scale_den": levels,
            "scale": freq_scale,
            "quantization_step": freq_scale,
            "upper_bound": 0.5 * freq_scale,
            "actual_max_abs_error": anchor_max_error,
            "effective_bits": _effective_bits(float(frequency_range), anchor_max_error, bits),
        },
        "target_freq": {
            "unit": "frequency_bin",
            "configured_bits": bits,
            "value_range": frequency_range,
            "levels": levels,
            "scale_num": frequency_range,
            "scale_den": levels,
            "scale": freq_scale,
            "quantization_step": freq_scale,
            "upper_bound": 0.5 * freq_scale,
            "actual_max_abs_error": target_max_error,
            "effective_bits": _effective_bits(float(frequency_range), target_max_error, bits),
        },
        "time_delta": {
            "unit": "frame",
            "configured_bits": bits,
            "value_range": dt_range,
            "levels": levels,
            "scale_num": dt_range,
            "scale_den": levels,
            "scale": dt_scale,
            "quantization_step": dt_scale,
            "upper_bound": 0.5 * dt_scale,
            "actual_max_abs_error": dt_max_error,
            "effective_bits": _effective_bits(float(dt_range), dt_max_error, bits),
        },
    }

    bucket_members: dict[int, list[int]] = {}
    for vector in vectors:
        bucket_members.setdefault(vector.bucket, []).append(vector.pair_index)
    total = len(vectors)
    unique_buckets = len(bucket_members)
    collision_count = total - unique_buckets
    collision_ppm = (collision_count * MICRO + total // 2) // total if total else 0
    strongest_bucket: int | None = None
    strongest_count = 0
    if bucket_members:
        strongest_bucket, members = min(
            bucket_members.items(), key=lambda item: (-len(item[1]), item[0])
        )
        strongest_count = len(members)
    limit_ppm = int(round(config.max_collision_rate * MICRO))
    collisions = {
        "bucket_width": config.bucket_width,
        "quantized_value_domain_size": levels**3,
        "bucket_width_less_than_quantized_domain": config.bucket_width < levels**3,
        "vector_count": total,
        "unique_bucket_count": unique_buckets,
        "collision_count": collision_count,
        "collision_rate_ppm": collision_ppm,
        "max_collision_rate_ppm": limit_ppm,
        "strongest_bucket": strongest_bucket,
        "strongest_bucket_vector_count": strongest_count,
        "strongest_bucket_collision_count": max(0, strongest_count - 1),
        "within_limit": collision_ppm <= limit_ppm,
    }
    if collision_ppm > limit_ppm:
        raise FingerprintError(
            code=E_BUCKET_COLLISION,
            message="bucket collision rate exceeds the configured limit",
            shard_id=config.shard_id,
            stage="quantize",
            details={
                "bucket": strongest_bucket,
                "strongest_bucket": strongest_bucket,
                "collision_count": collision_count,
                "collision_rate_ppm": collision_ppm,
                "max_collision_rate_ppm": limit_ppm,
                "vector_count": total,
            },
        )
    return QuantizationResult(
        vectors=tuple(vectors),
        fields=fields,
        collisions=collisions,
        levels=levels,
    )


def quantization_artifact(
    result: QuantizationResult,
    config: ShardConfig,
    pairs_digest: str,
    stage_version: str,
) -> dict:
    return {
        "schema": "audiofp.quantized.v1",
        "shard_id": config.shard_id,
        "stage_version": stage_version,
        "input_digest": pairs_digest,
        "quantization_bits": config.quantization_bits,
        "levels": result.levels,
        "hash_algorithm": "fnv1a64-v1",
        "hash_seed": config.hash_seed,
        "bucket_width": config.bucket_width,
        "frequency_offset_upper_bound_bins": max(
            result.fields["anchor_freq"]["upper_bound"], result.fields["target_freq"]["upper_bound"]
        ),
        "time_delta_offset_upper_bound_frames": result.fields["time_delta"]["upper_bound"],
        "fields": result.fields,
        "collisions": result.collisions,
        "vector_count": len(result.vectors),
        "vectors": [vector.as_dict() for vector in result.vectors],
    }

