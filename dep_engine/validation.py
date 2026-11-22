"""Dependency-tree constraint validation and structured diagnostics."""

from __future__ import annotations

from typing import Any

from .canonical import sha256_obj
from .config import AnalysisConfig
from .constants import (
    E_EMPTY_SENTENCE,
    E_NUMERIC,
    E_TREE_INVALID,
    VALIDATION_SCHEMA_VERSION,
)


def _tree_projection(decoded: dict[str, Any]) -> dict[str, Any]:
    return {
        "sentence_id": decoded.get("sentence_id"),
        "tokens": decoded.get("tokens", []),
        "heads": decoded.get("heads", []),
        "labels": decoded.get("labels", []),
        "distances": decoded.get("distances", []),
        "status": decoded.get("status"),
        "error": decoded.get("error"),
    }


def _find_cycles(heads: list[int]) -> list[list[int]]:
    n = len(heads)
    finished: set[int] = set()
    cycles: list[list[int]] = []
    cycle_keys: set[tuple[int, ...]] = set()

    for start in range(n):
        if start in finished:
            continue
        path: list[int] = []
        positions: dict[int, int] = {}
        current = start
        while (
            0 <= current < n
            and current not in finished
            and current not in positions
        ):
            positions[current] = len(path)
            path.append(current)
            head = heads[current]
            if head < 0 or head >= n:
                current = -1
                break
            current = head
        if 0 <= current < n and current in positions:
            cycle = path[positions[current] :]
            key = tuple(sorted(cycle))
            if key not in cycle_keys:
                cycle_keys.add(key)
                cycles.append(cycle)
        finished.update(path)
    return cycles


def validate_sentence(
    decoded: dict[str, Any],
    config: AnalysisConfig,
) -> dict[str, Any]:
    sentence_id = decoded.get("sentence_id")
    tokens = decoded.get("tokens", [])
    heads = decoded.get("heads", [])
    labels = decoded.get("labels", [])
    distances = decoded.get("distances", [])

    base = {
        "shard_index": decoded.get("shard_index"),
        "sentence_index": decoded.get("sentence_index"),
        "sentence_id": sentence_id,
        "token_count": len(tokens),
        "tree_digest": sha256_obj(_tree_projection(decoded)),
    }

    decode_error = decoded.get("error")
    if decode_error:
        code = decode_error.get("code", E_NUMERIC)
        return {
            **base,
            "status": "error",
            "code": code,
            "violations": [],
            "checks": {
                "acyclic": False,
                "root_unique": False,
                "distance_ok": False,
                "propagated_from": "decoding",
            },
            "decode_error": decode_error,
        }

    violations: list[dict[str, Any]] = []
    n = len(tokens)

    if not (len(heads) == len(labels) == len(distances) == n):
        violations.append(
            {
                "type": "TREE_SHAPE_MISMATCH",
                "token_index": 0,
                "token_count": n,
                "head_count": len(heads),
                "label_count": len(labels),
                "distance_count": len(distances),
            }
        )

    out_of_range = [
        index
        for index, head in enumerate(heads)
        if head < -1 or head >= n or head == index
    ]
    for index in out_of_range:
        violations.append(
            {
                "type": "HEAD_OUT_OF_RANGE_OR_SELF",
                "token_index": index,
                "head_index": heads[index],
            }
        )

    for cycle in _find_cycles(heads):
        for token_index in cycle:
            violations.append(
                {
                    "type": "CYCLE",
                    "token_index": token_index,
                    "cycle": cycle,
                }
            )

    roots = [index for index, head in enumerate(heads) if head == -1]
    root_unique = len(roots) == 1
    if config.unique_root:
        if not roots:
            violations.append(
                {
                    "type": "NO_ROOT",
                    "token_index": 0,
                    "root_count": 0,
                }
            )
        elif len(roots) > 1:
            for token_index in roots:
                violations.append(
                    {
                        "type": "MULTIPLE_ROOTS",
                        "token_index": token_index,
                        "root_indices": roots,
                    }
                )

    distance_ok = True
    for token_index, head in enumerate(heads):
        if 0 <= head < n:
            distance = abs(token_index - head)
            if distance > config.max_dependency_distance:
                distance_ok = False
                violations.append(
                    {
                        "type": "MAX_DEPENDENCY_DISTANCE",
                        "token_index": token_index,
                        "head_index": head,
                        "distance": distance,
                        "limit": config.max_dependency_distance,
                    }
                )

    acyclic = not any(v["type"] == "CYCLE" for v in violations)
    checks = {
        "acyclic": acyclic,
        "root_unique": root_unique,
        "distance_ok": distance_ok,
    }
    if violations:
        return {
            **base,
            "status": "error",
            "code": E_TREE_INVALID,
            "violations": violations,
            "checks": checks,
            "decode_error": None,
        }
    return {
        **base,
        "status": "ok",
        "code": None,
        "violations": [],
        "checks": checks,
        "decode_error": None,
    }


def validate_artifact(
    *,
    shard_index: int,
    shard_name: str,
    decoded_artifact: dict[str, Any],
    config: AnalysisConfig,
) -> dict[str, Any]:
    decoded_sentences = decoded_artifact.get("sentences", [])
    diagnostics = [validate_sentence(sentence, config) for sentence in decoded_sentences]
    return {
        "schema_version": VALIDATION_SCHEMA_VERSION,
        "shard_index": shard_index,
        "shard_name": shard_name,
        "max_dependency_distance": config.max_dependency_distance,
        "unique_root": config.unique_root,
        "sentences": decoded_sentences,
        "diagnostics": diagnostics,
    }


__all__ = [
    "E_EMPTY_SENTENCE",
    "E_NUMERIC",
    "E_TREE_INVALID",
    "validate_artifact",
    "validate_sentence",
]
