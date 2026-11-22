from __future__ import annotations

import math
from pathlib import Path
from typing import Any

from .config import ExperimentConfig
from .errors import MtlError
from .util import round_float, sha256_json, write_json_atomic

POLICY_SCHEMA = "scale-policy-v1"
ALIGNMENT_MODES = ("subword_approx", "word_exact")
PHRASE_LIMITS = (128, 256, 512, 1024, 2048, 4096)
BEAM_WIDTHS = (2, 4, 6, 8, 10)
FORMULA = {
    "alignment_exact": "0.74 + 0.18*min(1,n/1200) + 0.02*min(1,avg_len/10)",
    "alignment_approx": "0.80 + 0.06*min(1,n/12000) + 0.01*min(1,avg_len/10)",
    "phrase_quality": "0.10*log2(1+phrase_limit)/log2(1+4096)",
    "beam_quality": "0.06*log2(1+beam_width)/log2(1+10)",
    "cost_penalty": "0.05*estimated_work/budget",
    "estimated_work": "n_pairs*max(avg_len,1)*max(beam_width,1)*(1+log2(1+phrase_limit))",
    "score": "alignment_quality + phrase_quality + beam_quality - cost_penalty",
    "hard_caps": {
        "n<500": {"max_phrase_limit": 256, "max_beam_width": 4},
        "500<=n<5000": {"max_phrase_limit": 1024, "max_beam_width": 6},
        "n>=5000": {"max_phrase_limit": 4096, "max_beam_width": 10},
    },
}


def _features(manifest: dict[str, Any]) -> dict[str, Any]:
    return {
        "pair_count": int(manifest["pair_count"]),
        "avg_token_length": float(manifest["avg_token_length"]),
        "avg_source_tokens": float(manifest["avg_source_tokens"]),
        "avg_target_tokens": float(manifest["avg_target_tokens"]),
        "source_vocab_size": int(manifest["source_vocab_size"]),
        "target_vocab_size": int(manifest["target_vocab_size"]),
        "vocab_size_union": int(manifest["vocab_size_union"]),
        "total_token_upper_bound": int(manifest["total_token_upper_bound"]),
    }


def _hard_caps(n_pairs: int) -> tuple[int, int]:
    if n_pairs < 500:
        return 256, 4
    if n_pairs < 5000:
        return 1024, 6
    return 4096, 10


def _alignment_quality(mode: str, n_pairs: int, avg_len: float) -> float:
    if mode == "word_exact":
        return (
            0.74
            + 0.18 * min(1.0, n_pairs / 1200.0)
            + 0.02 * min(1.0, avg_len / 10.0)
        )
    return (
        0.80
        + 0.06 * min(1.0, n_pairs / 12000.0)
        + 0.01 * min(1.0, avg_len / 10.0)
    )


def _score_candidate(
    *,
    mode: str,
    phrase_limit: int,
    beam_width: int,
    n_pairs: int,
    avg_len: float,
    budget: int,
    max_phrase: int,
    max_beam: int,
    beam_override: int | None,
    phrase_override: int | None,
) -> dict[str, Any]:
    alignment_quality = _alignment_quality(mode, n_pairs, avg_len)
    phrase_quality = 0.10 * math.log2(1 + phrase_limit) / math.log2(1 + 4096)
    beam_quality = 0.06 * math.log2(1 + max(0, beam_width)) / math.log2(1 + 10)
    estimated_work = int(
        n_pairs
        * max(avg_len, 1.0)
        * max(beam_width, 1)
        * (1.0 + math.log2(1 + phrase_limit))
    )
    cost_penalty = 0.05 * estimated_work / max(1, budget)
    raw_score = alignment_quality + phrase_quality + beam_quality - cost_penalty

    cap_reasons: list[str] = []
    if phrase_override is None and phrase_limit > max_phrase:
        cap_reasons.append(f"phrase_limit>{max_phrase}_is_large-tier-only")
    if beam_override is None and beam_width > max_beam:
        cap_reasons.append(f"beam_width>{max_beam}_is_large-tier-only")
    if estimated_work > budget:
        cap_reasons.append(f"estimated_work={estimated_work}>budget={budget}")
    feasible = not cap_reasons

    return {
        "alignment_mode": mode,
        "phrase_limit": phrase_limit,
        "beam_width": beam_width,
        "alignment_quality": round_float(alignment_quality),
        "phrase_quality": round_float(phrase_quality),
        "beam_quality": round_float(beam_quality),
        "cost_penalty": round_float(cost_penalty),
        "estimated_work": estimated_work,
        "score": round_float(raw_score) if feasible else None,
        "feasible": feasible,
        "rejection_reason": ";".join(cap_reasons) if cap_reasons else None,
    }


def make_policy_decision(
    tier: str,
    manifest: dict[str, Any],
    config: ExperimentConfig,
) -> dict[str, Any]:
    features = _features(manifest)
    n_pairs = features["pair_count"]
    avg_len = features["avg_token_length"]
    budget = int(config.policy_budget)
    max_phrase, max_beam = _hard_caps(n_pairs)
    beam_values = (
        (int(config.beam_width_override),)
        if config.beam_width_override is not None
        else BEAM_WIDTHS
    )
    phrase_values = (
        (int(config.phrase_limit_override),)
        if config.phrase_limit_override is not None
        else PHRASE_LIMITS
    )
    candidates: list[dict[str, Any]] = []
    for mode in ALIGNMENT_MODES:
        for phrase_limit in phrase_values:
            for beam_width in beam_values:
                candidates.append(
                    _score_candidate(
                        mode=mode,
                        phrase_limit=phrase_limit,
                        beam_width=beam_width,
                        n_pairs=n_pairs,
                        avg_len=avg_len,
                        budget=budget,
                        max_phrase=max_phrase,
                        max_beam=max_beam,
                        beam_override=config.beam_width_override,
                        phrase_override=config.phrase_limit_override,
                    )
                )

    feasible = [candidate for candidate in candidates if candidate["feasible"]]
    if not feasible:
        raise MtlError(
            code="E_POLICY_FAILED",
            message="no feasible policy candidate under hard caps and training budget",
            details={
                "tier": tier,
                "budget": budget,
                "hard_caps": {"max_phrase_limit": max_phrase, "max_beam_width": max_beam},
                "candidate_count": len(candidates),
            },
        )
    ranked = sorted(
        feasible,
        key=lambda item: (
            -float(item["score"]),
            int(item["estimated_work"]),
            ALIGNMENT_MODES.index(item["alignment_mode"]),
            int(item["phrase_limit"]),
            int(item["beam_width"]),
        ),
    )
    selected = dict(ranked[0])
    selected["reason"] = (
        "highest deterministic feasible score under scale caps, budget, and sample-size uncertainty"
    )
    winner_score = float(selected["score"])
    for candidate in candidates:
        if (
            candidate["alignment_mode"] == selected["alignment_mode"]
            and candidate["phrase_limit"] == selected["phrase_limit"]
            and candidate["beam_width"] == selected["beam_width"]
        ):
            continue
        if not candidate["feasible"]:
            continue
        candidate["rejection_reason"] = (
            f"lower_score:{candidate['score']}<{selected['score']}"
        )
        if candidate["score"] == selected["score"]:
            candidate["rejection_reason"] = "lost_deterministic_tie_break"
    runner_up_score = float(ranked[1]["score"]) if len(ranked) > 1 else winner_score
    uncertainty = {
        "method": "sample-size standard error proxy; selection margin is deterministic",
        "alignment_quality_standard_error": round_float(0.10 / math.sqrt(max(1, n_pairs))),
        "error_band_95": round_float(1.96 * 0.10 / math.sqrt(max(1, n_pairs))),
        "selection_margin_to_runner_up": round_float(winner_score - runner_up_score),
        "sample_pair_count": n_pairs,
    }
    payload: dict[str, Any] = {
        "schema_version": 1,
        "policy_schema": POLICY_SCHEMA,
        "tier": tier,
        "features": features,
        "budget": budget,
        "hard_caps": {"max_phrase_limit": max_phrase, "max_beam_width": max_beam},
        "overrides": {
            "beam_width": config.beam_width_override,
            "phrase_limit": config.phrase_limit_override,
        },
        "formula": FORMULA,
        "candidates": candidates,
        "selected": selected,
        "selection_reason": {
            "summary": "Maximise reproducible expected quality after scale-aware feasibility caps.",
            "small_tier_guard": "pair_count<500 caps phrase table at 256 and beam at 4; approximate alignment wins the score at this sample size.",
            "large_tier_enablement": "pair_count>=5000 permits 4096 phrases and beam 10, while exact alignment overtakes approximation.",
        },
        "uncertainty": uncertainty,
    }
    payload["decision_fingerprint"] = sha256_json(payload)
    return payload


def policy_input_descriptor(
    tier: str,
    config: ExperimentConfig,
    manifest: dict[str, Any],
    alignment_artifact_digest: str,
) -> dict[str, Any]:
    features = _features(manifest)
    return {
        "engine_version": config.engine_version,
        "stage": "policy",
        "policy_schema": POLICY_SCHEMA,
        "tier": tier,
        "alignment_digest": alignment_artifact_digest,
        "manifest_feature_digest": sha256_json(features),
        "budget": int(config.policy_budget),
        "beam_width_override": config.beam_width_override,
        "phrase_limit_override": config.phrase_limit_override,
    }


def run_policy(
    tier: str,
    manifest: dict[str, Any],
    config: ExperimentConfig,
    alignment_artifact_digest: str,
    out_dir: Path,
) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    decision = make_policy_decision(tier, manifest, config)
    artifact = {
        "schema_version": 1,
        "tier": tier,
        "alignment_artifact_digest": alignment_artifact_digest,
        "decision": decision,
        "decision_fingerprint": decision["decision_fingerprint"],
    }
    write_json_atomic(
        out_dir / "_meta.json",
        {
            "schema_version": 1,
            "tier": tier,
            "stage": "policy",
            "stage_index": 3,
            "output_schema": POLICY_SCHEMA,
            "input_fingerprint": sha256_json(
                policy_input_descriptor(tier, config, manifest, alignment_artifact_digest)
            ),
        },
    )
    write_json_atomic(out_dir / "policy.json", artifact)
    return artifact


def load_policy(artifact: Path) -> dict[str, Any]:
    import json

    with (artifact / "policy.json").open("r", encoding="utf-8") as handle:
        return json.load(handle)

