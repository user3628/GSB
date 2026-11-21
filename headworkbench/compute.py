from __future__ import annotations

import math
from typing import Any

import numpy as np

from .data import HeadData


def quantize_fixed_point(activation: np.ndarray, bits: int, clip_abs: float) -> np.ndarray:
    """Apply deterministic signed fixed-point truncation toward zero.

    ``bits`` includes the sign bit. Values are represented with one step per
    ``2*clip_abs / (2**bits)``; the scaled value is clipped to the signed
    integer range and truncated toward zero before scaling back.
    """

    if bits < 2:
        raise ValueError("bits must be at least 2")
    qmax = (1 << (bits - 1)) - 1
    step = (2.0 * clip_abs) / float(1 << bits)
    scaled = np.asarray(activation, dtype=np.float64) / step
    clipped = np.clip(np.trunc(scaled), -qmax, qmax)
    return clipped * step


def head_forward(activation: np.ndarray, query_proj: np.ndarray, key_proj: np.ndarray) -> np.ndarray:
    """Run one deterministic attention head over a batch of activations.

    Input shape is ``[samples, sequence, head_dim]``. Queries and keys are
    projected from the same activations; values are the activations themselves.
    Only the selected head is ever quantized by the probe stage.
    """

    x = np.asarray(activation, dtype=np.float64)
    wq = np.asarray(query_proj, dtype=np.float64)
    wk = np.asarray(key_proj, dtype=np.float64)
    q = x @ wq.T
    k = x @ wk.T
    scale = 1.0 / math.sqrt(float(x.shape[-1]))
    scores = (q @ np.swapaxes(k, -1, -2)) * scale
    scores = scores - np.max(scores, axis=-1, keepdims=True)
    weights = np.exp(scores)
    weights = weights / np.sum(weights, axis=-1, keepdims=True)
    return weights @ x


def full_precision_reference(head: HeadData) -> np.ndarray:
    return head_forward(head.activation, head.query_proj, head.key_proj)


def relative_error_by_sample(candidate: np.ndarray, reference: np.ndarray, eps: float = 1e-15) -> np.ndarray:
    difference = np.linalg.norm(candidate - reference, axis=(-2, -1))
    denominator = np.maximum(np.linalg.norm(reference, axis=(-2, -1)), eps)
    return difference / denominator


def quantization_error_bound(activation: np.ndarray, bits: int, clip_abs: float) -> float:
    """Return a deterministic absolute per-element truncation bound.

    Truncation moves a value by less than one step, while clipping can add the
    distance to the representable edge. The bound is reported alongside the
    empirical window bound so operators can inspect why a width was selected.
    """

    step = (2.0 * clip_abs) / float(1 << bits)
    clipped = np.clip(activation, -clip_abs, clip_abs)
    truncation = step * np.ones_like(activation, dtype=np.float64)
    clipping = np.abs(activation - clipped)
    return float(np.max(truncation + clipping))


def sample_error_item(
    *,
    sample_index: int,
    candidate_output: np.ndarray,
    reference_output: np.ndarray,
) -> dict[str, Any]:
    difference = float(np.linalg.norm(candidate_output - reference_output))
    reference_norm = float(np.linalg.norm(reference_output))
    relative = difference / max(reference_norm, 1e-15)
    return {
        "sample_index": int(sample_index),
        "absolute_error": difference,
        "reference_norm": reference_norm,
        "relative_error": relative,
    }

