"""The four deterministic request stages.

Stage order: postprocess -> normalize -> decode -> summarize.
All probability normalization uses log-sum-exp in float64. Sampling uses a
fixed PCG64 stream and Gumbel-max over normalized log probabilities.
"""

from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from .canonical import digest_array, digest_json
from .errors import (
    invalid_param,
    numeric_error,
    snapshot_conflict,
)
from .metrics import build_precision_report
from .models import SamplingRequest


ALGORITHM_VERSIONS = {
    "postprocess": "postprocess-v1",
    "normalize": "normalize-v2-logsumexp",
    "decode": "decode-v1-gumbel-pcg64",
    "summarize": "summarize-v1-decimal-audit",
}

TIE_BREAK_RULE = (
    "For equal logits or probabilities, token_id ascending is authoritative. "
    "Candidate ordering uses lexsort(token_id, -log_prob); no hash iteration order "
    "participates."
)


@dataclass(frozen=True, slots=True)
class StageArtifact:
    artifact_digest: str
    metadata: dict[str, Any]


def _write_staging_json(path: Path, payload: Any) -> None:
    data = (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")
    with open(path, "xb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())


def _write_staging_npy(path: Path, array: np.ndarray) -> None:
    with open(path, "xb") as handle:
        np.save(handle, np.asarray(array, dtype=np.float64), allow_pickle=False)
        handle.flush()
        os.fsync(handle.fileno())


def _write_staging_npz(path: Path, **arrays: np.ndarray) -> None:
    with open(path, "xb") as handle:
        np.savez(handle, **arrays)
        handle.flush()
        os.fsync(handle.fileno())


def _load_npy(path: str | Path, request: SamplingRequest) -> np.ndarray:
    try:
        return np.load(path, allow_pickle=False)
    except (OSError, ValueError) as exc:
        raise numeric_error(
            "logits shard could not be loaded",
            request_id=request.request_id,
            stage="postprocess",
            reason="unreadable_logits",
            path=str(path),
            detail=str(exc),
        )


def _validate_temperature(request: SamplingRequest, maximum: float) -> None:
    if not math.isfinite(request.temperature) or request.temperature <= 0.0:
        raise invalid_param(
            "temperature must be finite and greater than zero",
            request_id=request.request_id,
            parameter="temperature",
            value=request.temperature,
        )
    if request.temperature > maximum:
        raise invalid_param(
            "temperature exceeds configured maximum",
            request_id=request.request_id,
            parameter="temperature",
            value=request.temperature,
            maximum=maximum,
        )


def _validate_request_numbers(request: SamplingRequest) -> None:
    if not math.isfinite(request.repetition_penalty) or request.repetition_penalty <= 0.0:
        raise invalid_param(
            "repetition_penalty must be finite and positive",
            request_id=request.request_id,
            parameter="repetition_penalty",
            value=request.repetition_penalty,
        )
    if request.top_k is not None and request.top_k <= 0:
        raise invalid_param(
            "top_k must be positive or null",
            request_id=request.request_id,
            parameter="top_k",
            value=request.top_k,
        )
    if not (math.isfinite(request.top_p) and 0.0 < request.top_p <= 1.0):
        raise invalid_param(
            "top_p must be in (0, 1]",
            request_id=request.request_id,
            parameter="top_p",
            value=request.top_p,
        )
    if request.steps <= 0:
        raise invalid_param(
            "steps must be positive",
            request_id=request.request_id,
            parameter="steps",
            value=request.steps,
        )


def _check_step_finite(
    values: np.ndarray, request: SamplingRequest, step: int, *, phase: str
) -> None:
    if np.isnan(values).any():
        raise numeric_error(
            "NaN encountered in logits",
            request_id=request.request_id,
            stage="postprocess",
            step_index=step,
            phase=phase,
            reason="nan",
            context_digest=digest_array(values),
        )
    if np.isposinf(values).any():
        raise numeric_error(
            "positive infinity encountered in logits",
            request_id=request.request_id,
            stage="postprocess",
            step_index=step,
            phase=phase,
            reason="positive_infinity",
            context_digest=digest_array(values),
        )
    if np.all(np.isneginf(values)):
        raise numeric_error(
            "all logits are negative infinity",
            request_id=request.request_id,
            stage="postprocess",
            step_index=step,
            phase=phase,
            reason="all_negative_infinity",
            context_digest=digest_array(values),
        )


def run_postprocess(
    request: SamplingRequest,
    logits_path: str | Path,
    staging_path: str | Path,
    *,
    max_temperature: float,
) -> StageArtifact:
    _validate_temperature(request, max_temperature)
    _validate_request_numbers(request)
    logits = _load_npy(logits_path, request)
    if logits.ndim != 2:
        raise invalid_param(
            "logits shard must be a two-dimensional [steps, vocab] matrix",
            request_id=request.request_id,
            parameter="logits_shape",
            shape=list(logits.shape),
        )
    if logits.shape[1] != request.vocab_size:
        raise invalid_param(
            "logits vocabulary width does not match the manifest vocabulary",
            request_id=request.request_id,
            parameter="vocab_size",
            logits_vocab_size=int(logits.shape[1]),
            manifest_vocab_size=request.vocab_size,
        )
    if logits.shape[0] < request.steps:
        raise invalid_param(
            "logits shard has fewer steps than requested",
            request_id=request.request_id,
            parameter="steps",
            requested=request.steps,
            available=int(logits.shape[0]),
        )
    source = np.asarray(logits[: request.steps], dtype=np.float64)
    if not np.all(np.isfinite(source) | np.isneginf(source)):
        bad_step = int(
            np.argmax(np.any(~(np.isfinite(source) | np.isneginf(source)), axis=1))
        )
        _check_step_finite(source[bad_step], request, bad_step, phase="input")

    context_tokens = tuple(sorted(set(request.prompt_tokens)))
    result = np.empty_like(source, dtype=np.float64)
    tie_max_indices: list[list[int]] = []
    tie_max_steps: list[int] = []
    for step in range(request.steps):
        values = np.array(source[step], dtype=np.float64, copy=True)
        _check_step_finite(values, request, step, phase="input")
        for token_id in context_tokens:
            if token_id >= values.size:
                raise invalid_param(
                    "prompt token is outside the vocabulary",
                    request_id=request.request_id,
                    parameter="prompt_tokens",
                    step_index=step,
                    value=token_id,
                )
            value = values[token_id]
            if value > 0.0:
                values[token_id] = value / request.repetition_penalty
            else:
                values[token_id] = value * request.repetition_penalty
        with np.errstate(over="ignore", invalid="ignore", divide="ignore"):
            values = values / request.temperature
        _check_step_finite(values, request, step, phase="after_repetition_and_temperature")
        if not np.all(np.isfinite(values) | np.isneginf(values)):
            raise numeric_error(
                "non-finite values were produced by post-processing",
                request_id=request.request_id,
                stage="postprocess",
                step_index=step,
                phase="after_repetition_and_temperature",
                reason="overflow",
                context_digest=digest_array(values),
            )
        maximum = float(np.max(values))
        ties = np.flatnonzero(values == maximum).astype(np.int64).tolist()
        if len(ties) > 1:
            tie_max_steps.append(step)
        tie_max_indices.append(ties)
        result[step] = values

    metadata = {
        "stage": "postprocess",
        "algorithm_version": ALGORITHM_VERSIONS["postprocess"],
        "steps": request.steps,
        "vocab_size": request.vocab_size,
        "temperature": request.temperature,
        "repetition_penalty": request.repetition_penalty,
        "repetition_context_tokens": list(context_tokens),
        "tie_max_indices": tie_max_indices,
        "tie_max_steps": tie_max_steps,
        "tie_break_rule": TIE_BREAK_RULE,
        "tensor_digest": digest_array(result),
    }
    artifact_digest = digest_json(metadata)
    _write_staging_npy(Path(staging_path), result)
    return StageArtifact(artifact_digest=artifact_digest, metadata=metadata)


def postprocess_digest(array: np.ndarray, metadata: dict[str, Any]) -> str:
    expected_metadata = dict(metadata)
    tensor_digest = digest_array(array)
    if expected_metadata.get("tensor_digest") != tensor_digest:
        raise ValueError("post-process tensor digest mismatch")
    return digest_json(expected_metadata)


def load_postprocess_artifact(
    path: str | Path,
    request: SamplingRequest,
    metadata: dict[str, Any],
    expected_digest: str,
) -> np.ndarray:
    try:
        array = np.load(path, allow_pickle=False)
    except (OSError, ValueError) as exc:
        raise snapshot_conflict(
            "post-process artifact is unreadable",
            request_id=request.request_id,
            stage="postprocess",
            reason="digest_mismatch",
            detail=str(exc),
        )
    try:
        actual = postprocess_digest(array, metadata)
    except ValueError as exc:
        raise snapshot_conflict(
            str(exc),
            request_id=request.request_id,
            stage="postprocess",
            reason="digest_mismatch",
        )
    if actual != expected_digest:
        raise snapshot_conflict(
            "post-process logical digest does not match the checkpoint",
            request_id=request.request_id,
            stage="postprocess",
            reason="digest_mismatch",
            expected=expected_digest,
            actual=actual,
        )
    return np.asarray(array, dtype=np.float64)


def run_normalize(
    request: SamplingRequest,
    postprocess_path: str | Path,
    postprocess_metadata: dict[str, Any],
    postprocess_artifact_digest: str,
    staging_path: str | Path,
) -> StageArtifact:
    logits = load_postprocess_artifact(
        postprocess_path,
        request,
        postprocess_metadata,
        postprocess_artifact_digest,
    )
    if logits.shape != (request.steps, request.vocab_size):
        raise snapshot_conflict(
            "post-process tensor shape does not match the request",
            request_id=request.request_id,
            stage="normalize",
            reason="digest_mismatch",
            shape=list(logits.shape),
        )
    log_probs_out = np.full_like(logits, -np.inf, dtype=np.float64)
    probs_out = np.zeros_like(logits, dtype=np.float64)
    candidate_indices = np.full(
        (request.steps, request.vocab_size), -1, dtype=np.int64
    )
    candidate_counts = np.zeros(request.steps, dtype=np.int64)
    step_entropies: list[float] = []
    effective_top_k: list[int] = []
    token_ids = np.arange(request.vocab_size, dtype=np.int64)
    for step in range(request.steps):
        values = logits[step]
        if np.isnan(values).any() or np.isposinf(values).any():
            raise numeric_error(
                "non-finite logits reached normalization",
                request_id=request.request_id,
                stage="normalize",
                step_index=step,
                reason="non_finite_logits",
            )
        finite_mask = np.isfinite(values)
        finite_indices = np.flatnonzero(finite_mask).astype(np.int64)
        if finite_indices.size == 0:
            raise numeric_error(
                "all logits are negative infinity in normalization",
                request_id=request.request_id,
                stage="normalize",
                step_index=step,
                reason="all_negative_infinity",
            )
        maximum = float(np.max(values[finite_indices]))
        shifted = values - maximum
        with np.errstate(under="ignore", invalid="ignore"):
            sum_exp = float(np.sum(np.exp(shifted[finite_indices]), dtype=np.float64))
        if not math.isfinite(sum_exp) or sum_exp <= 0.0:
            raise numeric_error(
                "log-sum-exp normalization failed",
                request_id=request.request_id,
                stage="normalize",
                step_index=step,
                reason="normalization_zero_or_nonfinite",
                sum_exp=sum_exp,
            )
        log_norm = maximum + math.log(sum_exp)
        full_log_probs = np.full(request.vocab_size, -np.inf, dtype=np.float64)
        full_log_probs[finite_indices] = values[finite_indices] - log_norm
        if not np.all(np.isfinite(full_log_probs[finite_indices])):
            raise numeric_error(
                "non-finite normalized log probabilities",
                request_id=request.request_id,
                stage="normalize",
                step_index=step,
                reason="non_finite_log_probability",
            )
        order = np.lexsort((finite_indices, -full_log_probs[finite_indices]))
        sorted_indices = finite_indices[order]
        k = request.vocab_size if request.top_k is None else min(
            request.top_k, request.vocab_size
        )
        k = max(1, int(k))
        top_k_indices = sorted_indices[:k]
        top_k_log_probs = full_log_probs[top_k_indices]
        with np.errstate(under="ignore"):
            top_k_probs = np.exp(top_k_log_probs)
        cumulative = np.cumsum(top_k_probs, dtype=np.float64)
        crossing = int(np.searchsorted(cumulative, request.top_p, side="left"))
        selected_count = min(top_k_indices.size, max(1, crossing + 1))
        selected = top_k_indices[:selected_count]
        selected_log_weights = full_log_probs[selected]
        selected_max = float(np.max(selected_log_weights))
        with np.errstate(under="ignore"):
            selected_sum = float(
                np.sum(np.exp(selected_log_weights - selected_max), dtype=np.float64)
            )
        if not math.isfinite(selected_sum) or selected_sum <= 0.0:
            raise numeric_error(
                "truncated probability sum is zero or non-finite",
                request_id=request.request_id,
                stage="normalize",
                step_index=step,
                reason="truncated_sum_zero",
                selected_count=int(selected.size),
            )
        selected_log_norm = selected_max + math.log(selected_sum)
        normalized_log_probs = selected_log_weights - selected_log_norm
        with np.errstate(under="ignore", invalid="ignore"):
            normalized_probs = np.exp(normalized_log_probs)
        total = float(np.sum(normalized_probs, dtype=np.float64))
        if not math.isfinite(total) or total <= 0.0:
            raise numeric_error(
                "normalized truncated probabilities do not sum to a positive value",
                request_id=request.request_id,
                stage="normalize",
                step_index=step,
                reason="probability_sum_zero",
                probability_sum=total,
            )
        if not np.all(np.isfinite(normalized_log_probs)) or not np.all(
            np.isfinite(normalized_probs)
        ):
            raise numeric_error(
                "truncated normalization produced non-finite values",
                request_id=request.request_id,
                stage="normalize",
                step_index=step,
                reason="non_finite_probability",
            )
        log_probs_out[step, selected] = normalized_log_probs
        probs_out[step, selected] = normalized_probs / total
        candidate_indices[step, : selected.size] = selected
        candidate_counts[step] = selected.size
        effective_top_k.append(int(k))
        entropy_values = probs_out[step, selected]
        entropy = float(-np.sum(entropy_values * normalized_log_probs))
        step_entropies.append(entropy)

    metadata = {
        "stage": "normalize",
        "algorithm_version": ALGORITHM_VERSIONS["normalize"],
        "top_k": request.top_k,
        "effective_top_k": effective_top_k,
        "top_p": request.top_p,
        "candidate_counts": candidate_counts.astype(int).tolist(),
        "tie_break_rule": TIE_BREAK_RULE,
        "top_p_rule": (
            "Candidates are sorted by descending log probability with token_id "
            "ascending as the tie-break. The candidate that crosses top-p is "
            "included; the truncated set is then re-normalized in log space."
        ),
        "arrays": {
            "log_probs": digest_array(log_probs_out),
            "probs": digest_array(probs_out),
            "candidate_indices": digest_array(candidate_indices.astype(np.float64)),
        },
    }
    artifact_digest = digest_json(metadata)
    _write_staging_npz(
        Path(staging_path),
        log_probs=log_probs_out,
        probs=probs_out,
        candidate_indices=candidate_indices,
    )
    return StageArtifact(artifact_digest=artifact_digest, metadata=metadata)


def normalize_digest(arrays: Any, metadata: dict[str, Any]) -> str:
    actual_arrays = {
        "log_probs": digest_array(np.asarray(arrays["log_probs"], dtype=np.float64)),
        "probs": digest_array(np.asarray(arrays["probs"], dtype=np.float64)),
        "candidate_indices": digest_array(
            np.asarray(arrays["candidate_indices"], dtype=np.int64).astype(np.float64)
        ),
    }
    expected_metadata = dict(metadata)
    if expected_metadata.get("arrays") != actual_arrays:
        raise ValueError("normalize array digests do not match")
    return digest_json(expected_metadata)


def load_normalize_artifact(
    path: str | Path,
    request: SamplingRequest,
    metadata: dict[str, Any],
    expected_digest: str,
) -> dict[str, np.ndarray]:
    try:
        with np.load(path, allow_pickle=False) as loaded:
            arrays = {
                "log_probs": np.asarray(loaded["log_probs"], dtype=np.float64),
                "probs": np.asarray(loaded["probs"], dtype=np.float64),
                "candidate_indices": np.asarray(
                    loaded["candidate_indices"], dtype=np.int64
                ),
            }
    except (OSError, ValueError, KeyError) as exc:
        raise snapshot_conflict(
            "normalize artifact is unreadable",
            request_id=request.request_id,
            stage="normalize",
            reason="digest_mismatch",
            detail=str(exc),
        )
    try:
        actual = normalize_digest(arrays, metadata)
    except ValueError as exc:
        raise snapshot_conflict(
            str(exc),
            request_id=request.request_id,
            stage="normalize",
            reason="digest_mismatch",
        )
    if actual != expected_digest:
        raise snapshot_conflict(
            "normalize logical digest does not match the checkpoint",
            request_id=request.request_id,
            stage="normalize",
            reason="digest_mismatch",
            expected=expected_digest,
            actual=actual,
        )
    return arrays


def run_decode(
    request: SamplingRequest,
    normalize_path: str | Path,
    normalize_metadata: dict[str, Any],
    normalize_artifact_digest: str,
    staging_path: str | Path,
) -> StageArtifact:
    arrays = load_normalize_artifact(
        normalize_path, request, normalize_metadata, normalize_artifact_digest
    )
    log_probs = arrays["log_probs"]
    candidate_indices = arrays["candidate_indices"]
    expected_shape = (request.steps, request.vocab_size)
    if log_probs.shape != expected_shape or candidate_indices.shape != expected_shape:
        raise snapshot_conflict(
            "normalize tensor shape does not match the request",
            request_id=request.request_id,
            stage="decode",
            reason="digest_mismatch",
            shape=list(log_probs.shape),
        )
    rng = np.random.Generator(np.random.PCG64(request.seed))
    tokens: list[int] = []
    token_text: list[str] = []
    step_log_probs: list[float] = []
    step_entropies: list[float] = []
    gumbel_scores: list[float] = []
    candidate_counts: list[int] = []
    for step in range(request.steps):
        candidates = candidate_indices[step][candidate_indices[step] >= 0]
        if candidates.size == 0:
            raise numeric_error(
                "no candidate tokens remain after truncation",
                request_id=request.request_id,
                stage="decode",
                step_index=step,
                reason="empty_candidate_set",
            )
        gumbel = rng.gumbel(loc=0.0, scale=1.0, size=request.vocab_size)
        scores = log_probs[step] + gumbel
        token = int(np.argmax(scores))
        if token not in set(int(value) for value in candidates.tolist()):
            raise numeric_error(
                "sampler selected a token outside the truncated candidate set",
                request_id=request.request_id,
                stage="decode",
                step_index=step,
                reason="invalid_sampled_token",
                token_id=token,
            )
        log_prob = float(log_probs[step, token])
        if not math.isfinite(log_prob):
            raise numeric_error(
                "sampled token has a non-finite log probability",
                request_id=request.request_id,
                stage="decode",
                step_index=step,
                reason="non_finite_sampled_log_probability",
                token_id=token,
            )
        selected_log_probs = log_probs[step, candidates]
        selected_probs = np.exp(selected_log_probs)
        entropy = float(-np.sum(selected_probs * selected_log_probs))
        tokens.append(token)
        token_text.append(request.vocab[token])
        step_log_probs.append(log_prob)
        step_entropies.append(entropy)
        gumbel_scores.append(float(gumbel[token]))
        candidate_counts.append(int(candidates.size))

    payload = {
        "decode_schema": 1,
        "stage": "decode",
        "request_id": request.request_id,
        "algorithm_version": ALGORITHM_VERSIONS["decode"],
        "sampler": "gumbel-max on normalized log probabilities",
        "rng": "numpy.Generator(PCG64(seed))",
        "seed": request.seed,
        "tokens": tokens,
        "token_text": token_text,
        "step_log_probs": step_log_probs,
        "step_entropies": step_entropies,
        "gumbel_scores": gumbel_scores,
        "candidate_counts": candidate_counts,
        "tie_break_rule": TIE_BREAK_RULE,
    }
    artifact_digest = digest_json(payload)
    metadata = {
        "tokens": tokens,
        "token_text": token_text,
        "candidate_counts": candidate_counts,
        "artifact_digest": artifact_digest,
    }
    _write_staging_json(Path(staging_path), payload)
    return StageArtifact(artifact_digest=artifact_digest, metadata=metadata)


def load_decode_artifact(
    path: str | Path,
    request: SamplingRequest,
    expected_digest: str,
) -> dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise snapshot_conflict(
            "decode artifact is unreadable",
            request_id=request.request_id,
            stage="decode",
            reason="digest_mismatch",
            detail=str(exc),
        )
    if not isinstance(payload, dict):
        raise snapshot_conflict(
            "decode artifact is not an object",
            request_id=request.request_id,
            stage="decode",
            reason="digest_mismatch",
        )
    actual = digest_json(payload)
    if actual != expected_digest:
        raise snapshot_conflict(
            "decode logical digest does not match the checkpoint",
            request_id=request.request_id,
            stage="decode",
            reason="digest_mismatch",
            expected=expected_digest,
            actual=actual,
        )
    tokens = payload.get("tokens")
    if not isinstance(tokens, list) or len(tokens) != request.steps:
        raise snapshot_conflict(
            "decode token sequence length does not match the request",
            request_id=request.request_id,
            stage="decode",
            reason="digest_mismatch",
            token_count=len(tokens) if isinstance(tokens, list) else None,
            expected_steps=request.steps,
        )
    for token in tokens:
        if isinstance(token, bool) or not isinstance(token, int) or not (
            0 <= token < request.vocab_size
        ):
            raise snapshot_conflict(
                "decode token is outside the vocabulary",
                request_id=request.request_id,
                stage="decode",
                reason="digest_mismatch",
                token=token,
            )
    return payload


def run_summarize(
    request: SamplingRequest,
    postprocess_path: str | Path,
    postprocess_metadata: dict[str, Any],
    postprocess_artifact_digest: str,
    normalize_path: str | Path,
    normalize_metadata: dict[str, Any],
    normalize_artifact_digest: str,
    decode_path: str | Path,
    decode_artifact_digest: str,
    staging_path: str | Path,
    *,
    version_fingerprint: str,
    config_version: str,
) -> StageArtifact:
    post_logits = load_postprocess_artifact(
        postprocess_path,
        request,
        postprocess_metadata,
        postprocess_artifact_digest,
    )
    normalize = load_normalize_artifact(
        normalize_path,
        request,
        normalize_metadata,
        normalize_artifact_digest,
    )
    decode = load_decode_artifact(decode_path, request, decode_artifact_digest)
    tokens = [int(token) for token in decode["tokens"]]
    token_text = [str(request.vocab[token]) for token in tokens]
    step_log_probs = [float(value) for value in decode["step_log_probs"]]
    step_entropies = [float(value) for value in decode["step_entropies"]]
    total_nll = float(-sum(step_log_probs))
    mean_nll = total_nll / request.steps
    try:
        perplexity = math.exp(mean_nll)
    except OverflowError:
        perplexity = float("inf")
    mean_entropy = float(sum(step_entropies) / request.steps)
    precision, components = build_precision_report(
        post_logits,
        normalize["candidate_indices"],
        tokens,
        float_nll=[-value for value in step_log_probs],
        float_entropy=step_entropies,
    )
    candidate_counts = [int(value) for value in decode["candidate_counts"]]
    metrics = {
        "steps": request.steps,
        "total_nll": total_nll,
        "mean_nll": mean_nll,
        "perplexity": perplexity,
        "mean_entropy_nats": mean_entropy,
        "candidate_count_min": min(candidate_counts),
        "candidate_count_max": max(candidate_counts),
        "candidate_count_mean": sum(candidate_counts) / len(candidate_counts),
    }
    payload = {
        "summary_schema": 1,
        "stage": "summarize",
        "request_id": request.request_id,
        "manifest_version": request.manifest_version,
        "algorithm_version": ALGORITHM_VERSIONS["summarize"],
        "tokens": tokens,
        "token_text": token_text,
        "metrics": metrics,
        "precision": precision,
        "precision_components": components,
        "stage_artifact_digests": {
            "postprocess": postprocess_artifact_digest,
            "normalize": normalize_artifact_digest,
            "decode": decode_artifact_digest,
        },
        "version_fingerprint": version_fingerprint,
        "config_version": config_version,
    }
    artifact_digest = digest_json(payload)
    metadata = {
        "tokens": tokens,
        "token_text": token_text,
        "metrics": metrics,
        "artifact_digest": artifact_digest,
    }
    _write_staging_json(Path(staging_path), payload)
    return StageArtifact(artifact_digest=artifact_digest, metadata=metadata)


def load_summary_artifact(
    path: str | Path,
    request: SamplingRequest,
    expected_digest: str,
) -> dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise snapshot_conflict(
            "summary artifact is unreadable",
            request_id=request.request_id,
            stage="summarize",
            reason="digest_mismatch",
            detail=str(exc),
        )
    if not isinstance(payload, dict):
        raise snapshot_conflict(
            "summary artifact is not an object",
            request_id=request.request_id,
            stage="summarize",
            reason="digest_mismatch",
        )
    actual = digest_json(payload)
    if actual != expected_digest:
        raise snapshot_conflict(
            "summary logical digest does not match the checkpoint",
            request_id=request.request_id,
            stage="summarize",
            reason="digest_mismatch",
            expected=expected_digest,
            actual=actual,
        )
    return payload
