"""Deterministic local text embedding and guidance schedule."""

from __future__ import annotations

import hashlib
import math

import numpy as np

from .config import EncoderConfig, ShardConfig
from .errors import BusinessError, ErrorCode
from .util import canonical_json_bytes, normalize_prompt


EMBEDDING_DIMENSION = 16


def encode_prompt(prompt: str, encoder: EncoderConfig) -> tuple[np.ndarray, str]:
    normalized = normalize_prompt(prompt)
    material = canonical_json_bytes(
        {
            "domain": encoder.domain,
            "encoder": encoder.name,
            "encoder_version": encoder.version,
            "dimension": encoder.dimension,
            "prompt": normalized,
        }
    )
    raw = hashlib.shake_256(material).digest(encoder.dimension * 8)
    words = np.frombuffer(raw, dtype="<u8").astype(np.uint64, copy=False)
    # Map integer words exactly into [-1, 1), then normalize with float64.
    values = ((words >> np.uint64(11)).astype(np.float64) * (2.0 / 9007199254740992.0)) - 1.0
    norm = math.sqrt(float(np.dot(values, values)))
    if norm == 0.0:
        values = np.zeros(encoder.dimension, dtype=np.float64)
        values[0] = 1.0
    else:
        values = values / norm
    return np.ascontiguousarray(values, dtype="<f8"), normalized


def schedule_weights(guidance_scale: float, steps: int, *, shard_id: str | None = None) -> np.ndarray:
    if isinstance(guidance_scale, bool) or not isinstance(guidance_scale, (int, float)):
        raise _invalid_guidance(shard_id)
    guidance = float(guidance_scale)
    if not math.isfinite(guidance) or guidance < 0.0:
        raise _invalid_guidance(shard_id)
    if isinstance(steps, bool) or not isinstance(steps, int) or steps < 1 or steps > 512:
        raise BusinessError(
            ErrorCode.E_INVALID_CONFIG,
            "steps must be an integer in [1, 512]",
            shard=shard_id,
            field="steps",
        )
    if steps == 1:
        progress = np.asarray([0.0], dtype=np.float64)
    else:
        progress = np.arange(steps, dtype=np.float64) / float(steps - 1)
    weights = 1.0 / (1.0 + guidance * (0.25 + 0.75 * progress))
    return np.ascontiguousarray(weights, dtype="<f8")


def shard_embedding(shard: ShardConfig, encoder: EncoderConfig) -> tuple[np.ndarray, str]:
    return encode_prompt(shard.prompt, encoder)


def _invalid_guidance(shard_id: str | None) -> BusinessError:
    return BusinessError(
        ErrorCode.E_INVALID_CONFIG,
        "guidance_scale must be a finite non-negative number",
        shard=shard_id,
        field="guidance_scale",
    )

