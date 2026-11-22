"""Deterministic arc-standard transition parser with hashed features."""

from __future__ import annotations

import hashlib
import math
from typing import Any

from .config import AnalysisConfig
from .constants import (
    ACTION_LABEL,
    ACTION_ORDER,
    DECODER_SCHEMA_VERSION,
    E_EMPTY_SENTENCE,
    E_NUMERIC,
)
from .tokenize import tokenize


def _unit_hash(feature: str) -> float:
    """Map a feature string to a stable value in [-1, 1]."""

    digest = hashlib.sha256(
        f"{DECODER_SCHEMA_VERSION}\x1f{feature}".encode("utf-8")
    ).digest()
    integer = int.from_bytes(digest[:8], "big")
    return 2.0 * (integer / 18446744073709551616.0) - 1.0


def _bucket(value: int, width: int = 3) -> int:
    return value // width


def _features(
    action: str,
    stack: list[int],
    buffer_start: int,
    heads: list[int],
    tokens: list[str],
    step: int,
) -> list[str]:
    n = len(tokens)
    top_index = stack[-1] if stack else -1
    second_index = stack[-2] if len(stack) >= 2 else -1
    next_index = buffer_start if buffer_start < n else -1
    next2_index = buffer_start + 1 if buffer_start + 1 < n else -1

    def token(index: int) -> str:
        return tokens[index] if 0 <= index < n else "<BOS/EOS>"

    top_token = token(top_index)
    second_token = token(second_index)
    next_token = token(next_index)
    next2_token = token(next2_index)
    assigned = sum(1 for head in heads if head >= 0)

    features = [
        "bias",
        f"action={action}",
        f"stack_bucket={_bucket(len(stack))}",
        f"buffer_bucket={_bucket(n - buffer_start)}",
        f"assigned_bucket={_bucket(assigned)}",
        f"top={top_token}",
        f"top_len={len(top_token)}",
        f"top2={second_token}",
        f"next={next_token}",
        f"next_len={len(next_token)}",
        f"next2={next2_token}",
        f"step_bucket={_bucket(step, 4)}",
    ]

    if action == "SHIFT":
        features.extend(
            [
                f"shift_pair={top_token}|{next_token}",
                f"shift_gap={_bucket(abs(top_index - next_index)) if top_index >= 0 and next_index >= 0 else -1}",
            ]
        )
    elif action == "LEFT_REDUCE" and len(stack) >= 2:
        head = stack[-2]
        dependent = stack[-1]
        features.extend(
            [
                f"left_head={tokens[head]}",
                f"left_dep={tokens[dependent]}",
                f"left_pair={tokens[head]}|{tokens[dependent]}",
                f"left_dist={abs(head - dependent)}",
                f"left_dist_bucket={_bucket(abs(head - dependent))}",
                f"left_head_seen={1 if heads[head] >= 0 else 0}",
            ]
        )
    elif action == "RIGHT_REDUCE" and len(stack) >= 2:
        dependent = stack[-2]
        head = stack[-1]
        features.extend(
            [
                f"right_head={tokens[head]}",
                f"right_dep={tokens[dependent]}",
                f"right_pair={tokens[head]}|{tokens[dependent]}",
                f"right_dist={abs(head - dependent)}",
                f"right_dist_bucket={_bucket(abs(head - dependent))}",
                f"right_head_seen={1 if heads[head] >= 0 else 0}",
            ]
        )
    return features


def _action_score(
    action: str,
    stack: list[int],
    buffer_start: int,
    heads: list[int],
    tokens: list[str],
    step: int,
    config: AnalysisConfig,
) -> tuple[float, list[str]]:
    features = _features(action, stack, buffer_start, heads, tokens, step)
    weight = config.score_weights[action.lower()]
    signed = sum(_unit_hash(feature) for feature in features)
    norm = math.sqrt(float(len(features)))
    smooth_term = math.tanh(signed / (norm + 1.0))
    base_term = 0.35 + signed / norm
    score = weight * base_term + config.smoothing_strength * smooth_term
    return score, features


def _score_repr(value: float) -> str:
    try:
        return format(value, ".17g")
    except (TypeError, ValueError):
        return repr(value)


def _error_result(
    *,
    shard_index: int,
    sentence_index: int,
    sentence_id: str,
    text: str,
    tokens: list[str],
    code: str,
    message: str,
    details: dict[str, Any],
    transitions: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "shard_index": shard_index,
        "sentence_index": sentence_index,
        "sentence_id": sentence_id,
        "text": text,
        "tokens": tokens,
        "heads": [],
        "labels": [],
        "distances": [],
        "transitions": transitions or [],
        "low_confidence_steps": [],
        "min_confidence": None,
        "confidence_threshold": None,
        "status": "error",
        "error": {
            "code": code,
            "message": message,
            "details": details,
        },
    }


def _numeric_failure(
    *,
    shard_index: int,
    sentence_index: int,
    sentence_id: str,
    text: str,
    tokens: list[str],
    source: str,
    action: str,
    step: int,
    buffer_position: int,
    stack_size: int,
    score: float | None,
    transitions: list[dict[str, Any]],
) -> dict[str, Any]:
    token_index = min(buffer_position, max(0, len(tokens) - 1))
    details: dict[str, Any] = {
        "score_source": source,
        "action": action,
        "action_step": step,
        "buffer_position": buffer_position,
        "stack_size": stack_size,
        "token_index": token_index,
    }
    if score is not None:
        details["score_repr"] = _score_repr(score)
    return _error_result(
        shard_index=shard_index,
        sentence_index=sentence_index,
        sentence_id=sentence_id,
        text=text,
        tokens=tokens,
        code=E_NUMERIC,
        message="a transition score or scoring parameter is not finite",
        details=details,
        transitions=transitions,
    )


def parse_sentence(
    *,
    text: str,
    sentence_id: str,
    shard_index: int,
    sentence_index: int,
    config: AnalysisConfig,
) -> dict[str, Any]:
    """Parse one sentence into a deterministic dependency tree.

    The parser never repairs a non-finite score.  It returns a sentence-level
    E_NUMERIC record containing the origin and parser position instead.
    """

    tokens = tokenize(text)
    if not tokens:
        return _error_result(
            shard_index=shard_index,
            sentence_index=sentence_index,
            sentence_id=sentence_id,
            text=text,
            tokens=[],
            code=E_EMPTY_SENTENCE,
            message="sentence is empty or contains punctuation only",
            details={"token_index": 0, "diagnostic": "no word tokens after tokenization"},
        )

    if not math.isfinite(config.confidence_threshold):
        return _numeric_failure(
            shard_index=shard_index,
            sentence_index=sentence_index,
            sentence_id=sentence_id,
            text=text,
            tokens=tokens,
            source="confidence_threshold",
            action="CONFIG",
            step=0,
            buffer_position=0,
            stack_size=0,
            score=None,
            transitions=[],
        )

    n = len(tokens)
    heads = [-1] * n
    labels = ["ROOT"] * n
    transitions: list[dict[str, Any]] = []
    low_confidence_steps: list[dict[str, Any]] = []
    confidence_values: list[float] = []
    stack: list[int] = []
    buffer_start = 0
    step = 0

    while True:
        if buffer_start >= n and len(stack) <= 1:
            break

        legal: list[str] = []
        if buffer_start < n:
            legal.append("SHIFT")
        if len(stack) >= 2 and heads[stack[-1]] == -1:
            legal.append("LEFT_REDUCE")
        if len(stack) >= 2 and heads[stack[-2]] == -1:
            legal.append("RIGHT_REDUCE")

        if not legal:
            # Defensive fallback: leave any unassigned roots for validation to
            # report rather than inventing an arc or silently repairing input.
            break

        scored: list[tuple[float, str, list[str]]] = []
        for action in legal:
            weight_source = f"score_weights.{action.lower()}"
            if not math.isfinite(config.score_weights[action.lower()]):
                return _numeric_failure(
                    shard_index=shard_index,
                    sentence_index=sentence_index,
                    sentence_id=sentence_id,
                    text=text,
                    tokens=tokens,
                    source=weight_source,
                    action=action,
                    step=step,
                    buffer_position=buffer_start,
                    stack_size=len(stack),
                    score=config.score_weights[action.lower()],
                    transitions=transitions,
                )
            if not math.isfinite(config.smoothing_strength):
                return _numeric_failure(
                    shard_index=shard_index,
                    sentence_index=sentence_index,
                    sentence_id=sentence_id,
                    text=text,
                    tokens=tokens,
                    source="smoothing_strength",
                    action=action,
                    step=step,
                    buffer_position=buffer_start,
                    stack_size=len(stack),
                    score=config.smoothing_strength,
                    transitions=transitions,
                )

            score, features = _action_score(
                action,
                stack,
                buffer_start,
                heads,
                tokens,
                step,
                config,
            )
            if not math.isfinite(score):
                return _numeric_failure(
                    shard_index=shard_index,
                    sentence_index=sentence_index,
                    sentence_id=sentence_id,
                    text=text,
                    tokens=tokens,
                    source=f"computed_score.{action.lower()}",
                    action=action,
                    step=step,
                    buffer_position=buffer_start,
                    stack_size=len(stack),
                    score=score,
                    transitions=transitions,
                )
            scored.append((score, action, features))

        best_score = scored[0][0]
        best_action = scored[0][1]
        for score, action, _features in scored[1:]:
            # Strict comparison preserves ACTION_ORDER as the deterministic
            # tie-breaker (SHIFT, LEFT_REDUCE, RIGHT_REDUCE).
            if score > best_score:
                best_score = score
                best_action = action

        if len(scored) > 1:
            second_score = max(
                score for score, action, _features in scored if action != best_action
            )
            margin = max(0.0, best_score - second_score)
            confidence = 1.0 - math.exp(-margin)
        else:
            confidence = 1.0
        if not math.isfinite(confidence):
            return _numeric_failure(
                shard_index=shard_index,
                sentence_index=sentence_index,
                sentence_id=sentence_id,
                text=text,
                tokens=tokens,
                source="confidence",
                action=best_action,
                step=step,
                buffer_position=buffer_start,
                stack_size=len(stack),
                score=confidence,
                transitions=transitions,
            )
        confidence_values.append(confidence)
        step_record = {
            "step": step,
            "action": best_action,
            "score": best_score,
            "confidence": confidence,
            "stack_size": len(stack),
            "buffer_position": buffer_start,
        }
        if confidence < config.confidence_threshold:
            step_record["low_confidence"] = True
            low_confidence_steps.append(
                {
                    "step": step,
                    "action": best_action,
                    "confidence": confidence,
                    "threshold": config.confidence_threshold,
                }
            )

        if best_action == "SHIFT":
            stack.append(buffer_start)
            buffer_start += 1
            step_record["head_index"] = None
            step_record["dependent_index"] = None
        elif best_action == "LEFT_REDUCE":
            head = stack[-2]
            dependent = stack[-1]
            stack.pop()
            heads[dependent] = head
            labels[dependent] = ACTION_LABEL[best_action]
            step_record["head_index"] = head
            step_record["dependent_index"] = dependent
        else:  # RIGHT_REDUCE
            head = stack[-1]
            dependent = stack[-2]
            stack.pop(-2)
            heads[dependent] = head
            labels[dependent] = ACTION_LABEL[best_action]
            step_record["head_index"] = head
            step_record["dependent_index"] = dependent
        transitions.append(step_record)
        step += 1

    # Do not infer a unique root if a defensive break left multiple or zero
    # roots.  Validation is the layer that must expose such a condition.
    for index, head in enumerate(heads):
        if head < 0:
            labels[index] = "ROOT"
    distances = [
        abs(index - head) if 0 <= head < n else 0 for index, head in enumerate(heads)
    ]
    min_confidence = min(confidence_values) if confidence_values else 1.0

    return {
        "shard_index": shard_index,
        "sentence_index": sentence_index,
        "sentence_id": sentence_id,
        "text": text,
        "tokens": tokens,
        "heads": heads,
        "labels": labels,
        "distances": distances,
        "transitions": transitions,
        "low_confidence_steps": low_confidence_steps,
        "min_confidence": min_confidence,
        "confidence_threshold": config.confidence_threshold,
        "status": "ok",
        "error": None,
    }
