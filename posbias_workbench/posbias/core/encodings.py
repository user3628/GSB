"""Position-encoding bias computation for RoPE family and ALiBi.

The pipeline never edits attention weights. For every scheme it computes the
position-dependent component of the Q/K attention logits, averaged over batch
and heads into an ``[L, L]`` bias matrix, then stores it in fixed-point int32
form via :class:`QuantizedBias`.
"""

from __future__ import annotations

import math

import numpy as np

from ..errors import InvalidParamError, NumericError
from .numeric import QuantizedBias, finite_guard, log_domain_product, log_domain_ratio

STAGE = "bias"


def pad_head_dim(head_dim: int, *, rule: str):
    """Return the even head dimension and a padding registry entry."""
    if head_dim % 2 == 0:
        return head_dim, {"raw_head_dim": head_dim,
                          "padded_head_dim": head_dim, "padded": False,
                          "padding_rule": rule, "pad_width": 0}
    if rule == "zero_pad":
        padded = head_dim + 1
    elif rule == "replicate_last":
        padded = head_dim + 1
    else:
        raise InvalidParamError(f"unsupported padding rule: {rule}",
                                stage=STAGE)
    return padded, {"raw_head_dim": head_dim,
                    "padded_head_dim": padded, "padded": True,
                    "padding_rule": rule, "pad_width": 1}


def apply_padding(tensor, *, raw_dim: int, padded_dim: int, rule: str):
    if padded_dim == raw_dim:
        return tensor
    if rule == "zero_pad":
        return np.pad(tensor, ((0, 0), (0, 0), (0, 0), (0, 1)),
                      mode="constant", constant_values=0.0)
    if rule == "replicate_last":
        return np.concatenate(
            [tensor, tensor[..., -1:]], axis=-1)
    raise InvalidParamError(f"unsupported padding rule: {rule}", stage=STAGE)


def inverse_frequencies(kind, *, base_freq, scale, head_dim, scheme_id):
    """Log-domain frequency construction for the three RoPE variants."""
    if not math.isfinite(base_freq) or base_freq <= 0.0:
        raise NumericError(
            "invalid base frequency", stage=STAGE, scheme=scheme_id,
            context={"base_freq": base_freq})
    if kind != "rope" and (not math.isfinite(scale) or scale <= 0.0):
        raise NumericError(
            "invalid scaling coefficient", stage=STAGE, scheme=scheme_id,
            context={"scale": scale})
    half = head_dim // 2
    exponents = np.arange(half, dtype=np.float64) / float(half)
    if kind == "rope":
        # theta_i = base^(-2i/d), computed as exp(-exponent * log(base)).
        finite_guard(exponents, stage=STAGE, scheme=scheme_id)
        return np.exp(-exponents * math.log(base_freq))
    if kind == "linear_rope":
        # theta_i = base^(-2i/d) / scale; ratio evaluated in log domain.
        inv_freq = np.empty(half, dtype=np.float64)
        for idx, exp in enumerate(exponents):
            raw = math.exp(-exp * math.log(base_freq))
            inv_freq[idx] = log_domain_ratio(
                numerator=raw, denominator=float(scale), stage=STAGE,
                scheme=scheme_id, bin_id=f"freq_{idx}")
        return inv_freq
    if kind == "ntk_rope":
        # effective base = base * scale^(d/(d-2)); product/power in log domain.
        power = float(head_dim) / float(head_dim - 2)
        log_effective = (
            math.log(base_freq)
            + power * math.log(log_domain_product(
                base_freq=1.0, scale=float(scale), stage=STAGE,
                scheme=scheme_id, bin_id="effective_base"))
        )
        return np.exp(-exponents * log_effective)
    raise InvalidParamError(f"unsupported scheme kind: {kind}", stage=STAGE,
                            scheme=scheme_id)


def rotate(tensor, positions, inv_freq, *, scheme_id):
    """Apply rotary embedding to an ``[B,H,L,D]`` tensor."""
    angles = np.outer(positions, inv_freq)
    cos = np.cos(angles)[None, None, :, :]
    sin = np.sin(angles)[None, None, :, :]
    half = tensor.shape[-1] // 2
    first = tensor[..., :half]
    second = tensor[..., half:]
    rotated_first = first * cos - second * sin
    rotated_second = second * cos + first * sin
    out = np.concatenate([rotated_first, rotated_second], axis=-1)
    finite_guard(out, stage=STAGE, scheme=scheme_id,
                 context={"step": "rotate"})
    return out


def rope_bias_matrix(query, key, *, scheme, head_dim_padded, positions):
    scheme_id = scheme["id"]
    inv_freq = inverse_frequencies(
        scheme["type"], base_freq=float(scheme["base_freq"]),
        scale=float(scheme.get("scale", 1.0)),
        head_dim=head_dim_padded, scheme_id=scheme_id)
    rotated_q = rotate(query, positions, inv_freq, scheme_id=scheme_id)
    rotated_k = rotate(key, positions, inv_freq, scheme_id=scheme_id)
    # Attention logits Q@K^T / sqrt(d), average over batch and heads.
    scale_norm = math.sqrt(float(head_dim_padded))
    logits = np.matmul(rotated_q, np.swapaxes(rotated_k, -1, -2)) / scale_norm
    finite_guard(logits, stage=STAGE, scheme=scheme_id,
                 context={"step": "logits"})
    bias = logits.mean(axis=(0, 1))
    finite_guard(bias, stage=STAGE, scheme=scheme_id,
                 context={"step": "mean_logits"})
    return bias, {"inv_freq": inv_freq.tolist()}


def alibi_slopes_for(heads: int, scheme):
    if scheme.get("slopes"):
        slopes = np.asarray(scheme["slopes"], dtype=np.float64)
        if slopes.size != heads:
            raise InvalidParamError(
                "alibi slope count must match head count",
                stage=STAGE, scheme=scheme["id"],
                context={"slopes": int(slopes.size), "heads": heads})
    else:
        base = 2.0 ** (-8.0 / float(heads))
        slopes = np.power(base, np.arange(1, heads + 1, dtype=np.float64))
    if np.any(slopes <= 0) or not np.all(np.isfinite(slopes)):
        raise NumericError("invalid alibi slopes", stage=STAGE,
                           scheme=scheme["id"],
                           context={"slopes": slopes.tolist()})
    return slopes


def alibi_bias_matrix(seq_len, heads, *, scheme):
    scheme_id = scheme["id"]
    slopes = alibi_slopes_for(heads, scheme)
    distance = np.arange(seq_len, dtype=np.float64)[None, :] - \
        np.arange(seq_len, dtype=np.float64)[:, None]
    distance = np.abs(distance)
    # [H,L,L] slope-scaled distance, then average over heads.
    per_head = -slopes[:, None, None] * distance[None, :, :]
    finite_guard(per_head, stage=STAGE, scheme=scheme_id,
                 context={"step": "alibi_distance"})
    bias = per_head.mean(axis=0)
    return bias, {"slopes": slopes.tolist()}


def compute_scheme_bias(*, query, key, scheme, padding_info, positions):
    """Return a quantized bias matrix plus dimensional metadata."""
    scheme_id = scheme["id"]
    kind = scheme["type"]
    raw_dim = int(query.shape[-1])
    padded_dim = padding_info["padded_head_dim"]
    q_padded = apply_padding(
        query, raw_dim=raw_dim, padded_dim=padded_dim,
        rule=padding_info["padding_rule"])
    k_padded = apply_padding(
        key, raw_dim=raw_dim, padded_dim=padded_dim,
        rule=padding_info["padding_rule"])
    if kind == "alibi":
        bias_float, scheme_meta = alibi_bias_matrix(
            seq_len=query.shape[2], heads=query.shape[1], scheme=scheme)
    elif kind in ("rope", "ntk_rope", "linear_rope"):
        bias_float, scheme_meta = rope_bias_matrix(
            q_padded, k_padded, scheme=scheme,
            head_dim_padded=padded_dim, positions=positions)
    else:
        raise InvalidParamError(f"unsupported scheme type: {kind}",
                                stage=STAGE, scheme=scheme_id)
    quantized, clipped = QuantizedBias.from_float(
        bias_float, scheme=scheme_id)
    metadata = {
        "scheme": scheme_id,
        "type": kind,
        "matrix_shape": list(quantized.shape),
        "dtype": "int32",
        "quant_scale": quantized.scale,
        "clipped_cells": clipped,
        "padding": padding_info,
        "params": scheme_meta,
    }
    return quantized, metadata
