from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

import numpy as np

from .util import array_digest, canonical_json_bytes, float_token, json_digest, sha256_hex


NUMERIC_CONTRACT = "fp16_div_loss_scale_then_fixed_point_2^-14_rne_v1"
FIXED_POINT_FRACTION_BITS = 14
FIXED_POINT_DENOMINATOR = float(1 << FIXED_POINT_FRACTION_BITS)
CONSISTENCY_ABS_TOL = 1.0e-2
CONSISTENCY_REL_TOL = 1.0e-2
RELATIVE_EPSILON = 1.0e-12


@dataclass(frozen=True)
class ShardDerivation:
    precision: str
    loss_scale: float
    restore_digest: str
    summary_digest: str
    dependency_fingerprint: str
    recovery_baseline_digest: str
    consistency_ok: bool
    consistency_reason: str
    max_abs: float
    max_rel: float
    mean_abs: float
    cumulative_rounding_error: float
    memory_bytes: int
    details: dict[str, Any]


def restore_from_fp16(fp16_copy: np.ndarray, loss_scale: float) -> np.ndarray:
    """Restore FP32 parameters with an explicit deterministic rounding contract.

    The stored FP16 tensor is assumed to contain ``parameter * loss_scale``.  It
    is divided in float64 and rounded to the nearest even multiple of
    ``2**-14`` before conversion to float32.  No BLAS or GPU operation is used.
    """

    source = np.asarray(fp16_copy, dtype=np.float16)
    scaled = source.astype(np.float64) / float(loss_scale)
    grid = scaled * FIXED_POINT_DENOMINATOR
    # np.rint is round-half-to-even and is deterministic for these finite inputs.
    quantized = np.rint(grid) / FIXED_POINT_DENOMINATOR
    return quantized.astype(np.float32)


def memory_estimate(numel: int, precision: str) -> int:
    """Steady-state resident estimate for master, momentum and compute copy."""

    master_bytes = int(numel) * 4
    momentum_bytes = int(numel) * 4
    compute_copy_bytes = int(numel) * (2 if precision == "FP16" else 4)
    metadata_bytes = 256
    return master_bytes + momentum_bytes + compute_copy_bytes + metadata_bytes


def _consistency(
    *,
    precision: str,
    max_abs: float,
    max_rel: float,
) -> tuple[bool, str]:
    if precision == "FP32":
        return True, "fp32_master_used_directly"
    if max_abs <= CONSISTENCY_ABS_TOL and max_rel <= CONSISTENCY_REL_TOL:
        return True, "restored_within_declared_tolerance"
    return False, "restored_outside_declared_tolerance"


def derive_shard(
    *,
    shard_id: str,
    master: np.ndarray,
    fp16_copy: np.ndarray,
    momentum: np.ndarray,
    precision: str,
    loss_scale: float,
    loss_scale_initial: float,
    growth_factor: float,
    stored_master_digest: str,
    stored_fp16_digest: str,
    stored_momentum_digest: str,
    checkpoint_version: int,
    scheduler_position: Any,
    data_cursor: Any,
) -> ShardDerivation:
    master32 = np.asarray(master, dtype=np.float32)
    fp16 = np.asarray(fp16_copy, dtype=np.float16)
    momentum32 = np.asarray(momentum, dtype=np.float32)

    actual_master_digest = array_digest(master32, declared_dtype="float32")
    actual_fp16_digest = array_digest(fp16, declared_dtype="float16")
    actual_momentum_digest = array_digest(momentum32, declared_dtype="float32")

    if precision == "FP16":
        restored = restore_from_fp16(fp16, loss_scale)
    else:
        restored = master32.copy()

    master64 = master32.astype(np.float64)
    restored64 = restored.astype(np.float64)
    abs_error = np.abs(restored64 - master64)
    denominator = np.maximum(np.abs(master64), RELATIVE_EPSILON)
    relative_error = abs_error / denominator

    max_abs = float(np.max(abs_error)) if abs_error.size else 0.0
    max_rel = float(np.max(relative_error)) if relative_error.size else 0.0
    mean_abs = float(np.mean(abs_error)) if abs_error.size else 0.0
    cumulative = float(np.sum(abs_error))

    checksum_ok = (
        actual_master_digest == stored_master_digest
        and actual_fp16_digest == stored_fp16_digest
        and actual_momentum_digest == stored_momentum_digest
    )
    within_tolerance, tolerance_reason = _consistency(
        precision=precision, max_abs=max_abs, max_rel=max_rel
    )
    consistency_ok = checksum_ok and within_tolerance
    consistency_reason = tolerance_reason if checksum_ok else "stored_checksum_mismatch"

    restore_digest = array_digest(restored, declared_dtype="float32")
    memory_bytes = memory_estimate(int(master32.size), precision)

    dependency_payload = {
        "contract": NUMERIC_CONTRACT,
        "shard_id": shard_id,
        "precision": precision,
        "loss_scale": float_token(loss_scale),
        "loss_scale_initial": float_token(loss_scale_initial),
        "growth_factor": float_token(growth_factor),
        "master_digest": actual_master_digest,
        "fp16_digest": actual_fp16_digest,
        "momentum_digest": actual_momentum_digest,
        "shape": list(master32.shape),
        "dtype": master32.dtype.str,
    }
    dependency_fingerprint = json_digest(dependency_payload)

    summary_payload = {
        "dependency_fingerprint": dependency_fingerprint,
        "restore_digest": restore_digest,
        "consistency_ok": consistency_ok,
        "consistency_reason": consistency_reason,
        "max_abs": float.hex(max_abs),
        "max_rel": float.hex(max_rel),
        "mean_abs": float.hex(mean_abs),
        "cumulative_rounding_error": float.hex(cumulative),
        "memory_bytes": memory_bytes,
        "rounding_contract": NUMERIC_CONTRACT,
    }
    summary_digest = sha256_hex(canonical_json_bytes(summary_payload))

    baseline_payload = {
        "checkpoint_version": checkpoint_version,
        "shard_id": shard_id,
        "precision": precision,
        "loss_scale": float_token(loss_scale),
        "loss_scale_initial": float_token(loss_scale_initial),
        "growth_factor": float_token(growth_factor),
        "master_digest": actual_master_digest,
        "fp16_digest": actual_fp16_digest,
        "momentum_digest": actual_momentum_digest,
        "restore_digest": restore_digest,
        "scheduler_position": scheduler_position,
        "data_cursor": data_cursor,
        "memory_bytes": memory_bytes,
    }
    recovery_baseline_digest = json_digest(baseline_payload)

    details = {
        "shard_id": shard_id,
        "shape": list(master32.shape),
        "numel": int(master32.size),
        "dtype": "float32",
        "rounding_contract": NUMERIC_CONTRACT,
        "fixed_point_fraction_bits": FIXED_POINT_FRACTION_BITS,
        "consistency_tolerance": {
            "max_abs": CONSISTENCY_ABS_TOL,
            "max_rel": CONSISTENCY_REL_TOL,
        },
        "checksums_match": checksum_ok,
        "master_digest": actual_master_digest,
        "fp16_digest": actual_fp16_digest,
        "momentum_digest": actual_momentum_digest,
        "restore_digest": restore_digest,
        "max_abs": max_abs,
        "max_rel": max_rel,
        "mean_abs": mean_abs,
        "cumulative_rounding_error": cumulative,
        "memory_bytes": memory_bytes,
    }

    return ShardDerivation(
        precision=precision,
        loss_scale=float(loss_scale),
        restore_digest=restore_digest,
        summary_digest=summary_digest,
        dependency_fingerprint=dependency_fingerprint,
        recovery_baseline_digest=recovery_baseline_digest,
        consistency_ok=consistency_ok,
        consistency_reason=consistency_reason,
        max_abs=max_abs,
        max_rel=max_rel,
        mean_abs=mean_abs,
        cumulative_rounding_error=cumulative,
        memory_bytes=memory_bytes,
        details=details,
    )


def aggregate_derivations(
    shard_derivations: list[tuple[str, ShardDerivation]],
    *,
    checkpoint_version: int,
    scheduler_position: Any,
    data_cursor: Any,
) -> dict[str, Any]:
    shard_derivations = sorted(shard_derivations, key=lambda item: item[0])
    per_shard_fingerprints = [
        {"shard_id": shard_id, "dependency_fingerprint": derivation.dependency_fingerprint}
        for shard_id, derivation in shard_derivations
    ]
    restore_payload = [
        {"shard_id": shard_id, "restore_digest": derivation.restore_digest}
        for shard_id, derivation in shard_derivations
    ]
    summary_payload = [
        {"shard_id": shard_id, "summary_digest": derivation.summary_digest}
        for shard_id, derivation in shard_derivations
    ]
    dependency_payload = {
        "checkpoint_version": checkpoint_version,
        "scheduler_position": scheduler_position,
        "data_cursor": data_cursor,
        "shards": per_shard_fingerprints,
    }
    baseline_payload = {
        "checkpoint_version": checkpoint_version,
        "scheduler_position": scheduler_position,
        "data_cursor": data_cursor,
        "shards": [
            {
                "shard_id": shard_id,
                "recovery_baseline_digest": derivation.recovery_baseline_digest,
            }
            for shard_id, derivation in shard_derivations
        ],
    }
    max_abs = max((d.max_abs for _, d in shard_derivations), default=0.0)
    max_rel = max((d.max_rel for _, d in shard_derivations), default=0.0)
    cumulative = math.fsum(d.cumulative_rounding_error for _, d in shard_derivations)
    memory_bytes = sum(d.memory_bytes for _, d in shard_derivations)
    return {
        "checkpoint_version": checkpoint_version,
        "shard_count": len(shard_derivations),
        "restore_digest": json_digest(restore_payload),
        "summary_digest": json_digest(summary_payload),
        "consistency_ok": all(d.consistency_ok for _, d in shard_derivations),
        "max_abs": max_abs,
        "max_rel": max_rel,
        "cumulative_rounding_error": cumulative,
        "memory_bytes": memory_bytes,
        "recovery_baseline_digest": json_digest(baseline_payload),
        "dependency_fingerprint": json_digest(dependency_payload),
    }
