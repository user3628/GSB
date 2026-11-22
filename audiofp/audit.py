"""Reference-truth retrieval audit and deterministic corpus summaries."""

from __future__ import annotations

import math
from typing import Any

from .config import ReferencePair, ShardConfig
from .errors import E_INVALID_CONFIG, FingerprintError
from .quantize import MICRO, quantize_value


def _ppm(numerator: int, denominator: int) -> int:
    if denominator <= 0:
        return 0
    return (numerator * MICRO + denominator // 2) // denominator


def _reference_key(
    reference: ReferencePair,
    frame_length: int,
    bits: int,
    time_delta_max: int,
    shard_id: str,
) -> tuple[int, int, int]:
    levels = (1 << bits) - 1
    frequency_max = frame_length // 2
    if not 0 <= reference.anchor_freq <= frequency_max:
        raise FingerprintError(
            E_INVALID_CONFIG,
            "reference anchor frequency is outside the quantizer range",
            shard_id=shard_id,
            stage="audit",
            details={"field": "anchor_freq", "value": reference.anchor_freq, "maximum": frequency_max},
        )
    if not 0 <= reference.target_freq <= frequency_max:
        raise FingerprintError(
            E_INVALID_CONFIG,
            "reference target frequency is outside the quantizer range",
            shard_id=shard_id,
            stage="audit",
            details={"field": "target_freq", "value": reference.target_freq, "maximum": frequency_max},
        )
    if not 0 <= reference.dt <= time_delta_max:
        raise FingerprintError(
            E_INVALID_CONFIG,
            "reference time difference is outside the quantizer range",
            shard_id=shard_id,
            stage="audit",
            details={"field": "dt", "value": reference.dt, "maximum": time_delta_max},
        )
    frequency_scale = (frame_length // 2) / levels
    dt_scale = time_delta_max / levels
    return (
        quantize_value(reference.anchor_freq, frequency_scale, levels),
        quantize_value(reference.target_freq, frequency_scale, levels),
        quantize_value(reference.dt, dt_scale, levels),
    )


def _wilson_upper(successes: int, trials: int, z: float = 1.96) -> float:
    if trials <= 0:
        return 0.0
    proportion = successes / trials
    denominator = 1.0 + (z * z) / trials
    centre = proportion + (z * z) / (2.0 * trials)
    margin = z * math.sqrt(
        proportion * (1.0 - proportion) / trials + (z * z) / (4.0 * trials * trials)
    )
    return min(1.0, (centre + margin) / denominator)


def _confidence_ppm(peak_count: int, pair_count: int, collision_rate_ppm: int) -> int:
    # Three normalized fixed-point factors; no floating point participates in
    # the weighting or in the confidence threshold comparison.
    peak_factor = min(MICRO, peak_count * 10_000)
    pair_factor = min(MICRO, pair_count * 4_000)
    collision_factor = max(0, MICRO - collision_rate_ppm)
    return (30 * peak_factor + 30 * pair_factor + 40 * collision_factor + 50) // 100


def audit_shard(
    config: ShardConfig,
    peak_count: int,
    reference_pairs: list[ReferencePair],
    reference_digest: str,
    pairs_digest: str,
    quantized: dict[str, Any],
    quantized_digest: str,
    stage_version: str,
) -> dict[str, Any]:
    """Audit quantized vectors against the reference list only.

    A reference hit requires exact agreement with the quantized
    ``(anchor frequency bin, target frequency bin, time difference)`` tuple.
    Generated vectors absent from the reference key set are counted as false
    matches.  No generated pair is ever promoted to ground truth.
    """

    generated_keys = [
        (int(vector["anchor_freq_q"]), int(vector["target_freq_q"]), int(vector["dt_q"]))
        for vector in quantized["vectors"]
    ]
    generated_key_set = set(generated_keys)
    reference_keys = [
        _reference_key(
            reference,
            config.frame_length,
            config.quantization_bits,
            config.time_delta_max,
            config.shard_id,
        )
        for reference in reference_pairs
    ]
    reference_key_set = set(reference_keys)
    hits = sum(1 for key in reference_keys if key in generated_key_set)
    misses = len(reference_keys) - hits
    false_matches = sum(1 for key in generated_keys if key not in reference_key_set)
    hit_rate_ppm = _ppm(hits, len(reference_keys))
    false_match_rate_ppm = _ppm(false_matches, len(generated_keys))

    dt_scale = float(quantized["fields"]["time_delta"]["scale"])
    matched_shifts: list[float] = []
    for reference, key in zip(reference_pairs, reference_keys):
        if key in generated_key_set:
            dequantized_dt = key[2] * dt_scale
            matched_shifts.append(abs(dequantized_dt - reference.dt))
    average_dt_shift = sum(matched_shifts) / len(matched_shifts) if matched_shifts else 0.0
    average_dt_shift_ms = average_dt_shift * config.hop_length * 1000.0 / 16_000.0

    limit_ppm = int(round(config.expected_false_match_rate * MICRO))
    violations: list[dict[str, Any]] = []
    if false_match_rate_ppm > limit_ppm:
        violations.append(
            {
                "metric": "false_match_rate",
                "estimate_ppm": false_match_rate_ppm,
                "limit_ppm": limit_ppm,
                "estimate": false_match_rate_ppm / MICRO,
            }
        )
    passed = not violations

    collision_rate_ppm = int(quantized["collisions"]["collision_rate_ppm"])
    confidence_ppm = _confidence_ppm(
        peak_count=peak_count,
        pair_count=len(generated_keys),
        collision_rate_ppm=collision_rate_ppm,
    )
    confidence_limit_ppm = int(round(config.confidence_threshold * MICRO))
    suspicious = confidence_ppm < confidence_limit_ppm

    fields = quantized["fields"]
    quantization_error = {
        "derivation": "upper_bound = 0.5 * scale = value_range / (2 * (2**bits - 1))",
        "effective_bits_formula": "min(bits, log2(value_range / (2 * actual_max_abs_error)))",
        "frequency_offset_upper_bound_bins": max(
            fields["anchor_freq"]["upper_bound"], fields["target_freq"]["upper_bound"]
        ),
        "time_delta_offset_upper_bound_frames": fields["time_delta"]["upper_bound"],
        "fields": fields,
    }
    return {
        "schema": "audiofp.audit.v1",
        "shard_id": config.shard_id,
        "stage_version": stage_version,
        "reference_digest": reference_digest,
        "input_digest": quantized_digest,
        "pairs_digest": pairs_digest,
        "metrics": {
            "reference_pair_count": len(reference_keys),
            "generated_vector_count": len(generated_keys),
            "hits": hits,
            "misses": misses,
            "false_matches": false_matches,
            "hit_rate_ppm": hit_rate_ppm,
            "hit_rate": hit_rate_ppm / MICRO,
            "false_match_rate_ppm": false_match_rate_ppm,
            "false_match_rate": false_match_rate_ppm / MICRO,
            "average_dt_shift_frames": average_dt_shift,
            "average_dt_shift_ms": average_dt_shift_ms,
            "matched_reference_count": len(matched_shifts),
        },
        "quantization_error": quantization_error,
        "collision_rate_ppm": collision_rate_ppm,
        "confidence": {
            "ppm": confidence_ppm,
            "value": confidence_ppm / MICRO,
            "threshold_ppm": confidence_limit_ppm,
            "suspicious": suspicious,
            "derivation": "normalised fixed-point weights 0.30*peak + 0.30*pair + 0.40*(1-collision)",
        },
        "passed": passed,
        "violations": violations,
    }


def _normalise_weights(confidences: list[int]) -> list[int]:
    total = sum(confidences)
    if total <= 0:
        return [0 for _ in confidences]
    weights = [(value * MICRO) // total for value in confidences]
    remainder = MICRO - sum(weights)
    order = sorted(range(len(confidences)), key=lambda index: (-confidences[index], index))
    for index in order[:remainder]:
        weights[index] += 1
    return weights


def build_summary(
    shard_order: list[str],
    config_versions: dict[str, str],
    audits: dict[str, dict[str, Any]],
    errors: dict[str, dict[str, Any]],
    dataset_digest: str,
) -> dict[str, Any]:
    """Build a timing-free, canonical-order corpus report."""

    audited_ids = [shard_id for shard_id in shard_order if shard_id in audits]
    confidences = [int(audits[shard_id]["confidence"]["ppm"]) for shard_id in audited_ids]
    weights = _normalise_weights(confidences)
    weights_by_id = dict(zip(audited_ids, weights))

    failed_shards: list[dict[str, Any]] = []
    suspicious_shards: list[dict[str, Any]] = []
    shard_rows: list[dict[str, Any]] = []
    for shard_id in shard_order:
        if shard_id in errors:
            failure = dict(errors[shard_id])
            failure["kind"] = "pipeline_error"
            failed_shards.append(failure)
            shard_rows.append(
                {
                    "shard_id": shard_id,
                    "status": "error",
                    "error_code": failure.get("code"),
                    "config_version": config_versions.get(shard_id),
                }
            )
            continue
        if shard_id not in audits:
            failure = {
                "shard_id": shard_id,
                "kind": "missing_audit",
                "code": "E_INVALID_STAGE",
                "message": "shard has no audit artifact in the completed run",
            }
            failed_shards.append(failure)
            shard_rows.append(
                {
                    "shard_id": shard_id,
                    "status": "missing_audit",
                    "error_code": "E_INVALID_STAGE",
                    "config_version": config_versions.get(shard_id),
                }
            )
            continue

        audit = audits[shard_id]
        metrics = audit["metrics"]
        weight_ppm = weights_by_id[shard_id]
        if not audit["passed"]:
            failed_shards.append(
                {
                    "shard_id": shard_id,
                    "kind": "audit_threshold",
                    "code": "AUDIT_THRESHOLD_EXCEEDED",
                    "violations": audit["violations"],
                }
            )
        if audit["confidence"]["suspicious"]:
            suspicious_shards.append(
                {
                    "shard_id": shard_id,
                    "confidence_ppm": audit["confidence"]["ppm"],
                    "confidence_threshold_ppm": audit["confidence"]["threshold_ppm"],
                }
            )
        shard_rows.append(
            {
                "shard_id": shard_id,
                "status": "passed" if audit["passed"] else "failed",
                "config_version": config_versions.get(shard_id),
                "stage_version": audit["stage_version"],
                "reference_pair_count": metrics["reference_pair_count"],
                "generated_vector_count": metrics["generated_vector_count"],
                "hit_rate_ppm": metrics["hit_rate_ppm"],
                "false_match_rate_ppm": metrics["false_match_rate_ppm"],
                "average_dt_shift_frames": metrics["average_dt_shift_frames"],
                "confidence_ppm": audit["confidence"]["ppm"],
                "weight_ppm": weight_ppm,
                "suspicious": audit["confidence"]["suspicious"],
            }
        )

    total_vectors = sum(int(audits[shard_id]["metrics"]["generated_vector_count"]) for shard_id in audited_ids)
    total_references = sum(int(audits[shard_id]["metrics"]["reference_pair_count"]) for shard_id in audited_ids)
    total_hits = sum(int(audits[shard_id]["metrics"]["hits"]) for shard_id in audited_ids)
    total_false = sum(int(audits[shard_id]["metrics"]["false_matches"]) for shard_id in audited_ids)
    unweighted_false_ppm = _ppm(total_false, total_vectors)
    weighted_false_ppm = (
        sum(
            weights_by_id[shard_id] * int(audits[shard_id]["metrics"]["false_match_rate_ppm"])
            for shard_id in audited_ids
        )
        + MICRO // 2
    ) // MICRO
    weighted_hit_ppm = (
        sum(
            weights_by_id[shard_id] * int(audits[shard_id]["metrics"]["hit_rate_ppm"])
            for shard_id in audited_ids
        )
        + MICRO // 2
    ) // MICRO

    cumulative: list[dict[str, Any]] = []
    cumulative_false = 0
    cumulative_vectors = 0
    for index, shard_id in enumerate(audited_ids, start=1):
        metrics = audits[shard_id]["metrics"]
        cumulative_false += int(metrics["false_matches"])
        cumulative_vectors += int(metrics["generated_vector_count"])
        cumulative.append(
            {
                "shard_count": index,
                "vector_count": cumulative_vectors,
                "false_matches": cumulative_false,
                "false_match_rate_ppm": _ppm(cumulative_false, cumulative_vectors),
                "wilson_95_upper_ppm": int(
                    round(_wilson_upper(cumulative_false, cumulative_vectors) * MICRO)
                ),
            }
        )

    return {
        "schema": "audiofp.summary.v1",
        "dataset_digest": dataset_digest,
        "shard_order": list(shard_order),
        "shard_count": len(shard_order),
        "audited_shard_count": len(audited_ids),
        "failed_shard_count": len(failed_shards),
        "suspicious_shard_count": len(suspicious_shards),
        "failed_shards": failed_shards,
        "suspicious_shards": suspicious_shards,
        "pipeline_errors": [errors[shard_id] for shard_id in shard_order if shard_id in errors],
        "corpus": {
            "reference_pair_count": total_references,
            "generated_vector_count": total_vectors,
            "hits": total_hits,
            "false_matches": total_false,
            "hit_rate_ppm": _ppm(total_hits, total_references),
            "unweighted_false_match_rate_ppm": unweighted_false_ppm,
            "weighted_false_match_rate_ppm": weighted_false_ppm,
            "weighted_hit_rate_ppm": weighted_hit_ppm,
            "wilson_95_upper_ppm": int(round(_wilson_upper(total_false, total_vectors) * MICRO)),
            "cumulative_by_shard_count": cumulative,
            "weight_normalisation": "integer confidence weights normalised to exactly 1000000 ppm",
        },
        "shards": shard_rows,
    }

