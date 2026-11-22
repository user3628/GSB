"""Stage 2 - categorical cross-combination generation.

Combinations of the inferred categorical columns are generated up to the
configured order cap in a deterministic sorted order.  When the number of
combinations exceeds the configured cap the tail is truncated and every
dropped combination is recorded with the reason.  The crossed design
matrix is stored as fixed-point counts with an explicit per-column scale.
"""
from __future__ import annotations

import itertools

from .errors import E_EMPTY_FEATURE, E_INVALID_CONFIG, PipelineError
from .quantize import QUANT_SCALE, quantize

TRUNCATION_REASON = "exceeds max_crosses cap"


def _crossable_columns(infer_artifact: dict) -> list[str]:
    names = [
        col["name"]
        for col in infer_artifact["columns"]
        if col["type"] == "categorical" and not col.get("excluded_from_crossing")
    ]
    return sorted(names)


def build_crosses(infer_artifact: dict, cfg: dict) -> dict:
    cross_cfg = cfg["crossing"]
    max_order = cross_cfg.get("max_order")
    max_crosses = cross_cfg.get("max_crosses")

    problems: dict[str, str] = {}
    if not isinstance(max_order, int) or max_order < 1:
        problems["crossing.max_order"] = (
            f"combination order cap must be a positive integer, got {max_order!r}"
        )
    columns = _crossable_columns(infer_artifact)
    if not columns:
        problems["categorical_columns"] = (
            "zero categorical columns available for crossing"
        )
    if problems:
        raise PipelineError(E_INVALID_CONFIG, "cannot generate crosses", **problems)

    cardinalities = {
        col["name"]: col.get("cardinality", 0)
        for col in infer_artifact["columns"]
        if col["name"] in columns
    }
    if all(cardinalities[name] <= 1 for name in columns):
        raise PipelineError(
            E_EMPTY_FEATURE,
            "every categorical column has cardinality one; cross space is empty",
            columns=columns,
            cardinalities=cardinalities,
        )

    row_tokens = infer_artifact["row_tokens"]
    row_count = infer_artifact["row_count"]

    all_combos: list[tuple[str, ...]] = []
    for order in range(1, min(max_order, len(columns)) + 1):
        all_combos.extend(itertools.combinations(columns, order))
    # Deterministic order: by order, then lexicographic column tuple.
    all_combos.sort(key=lambda combo: (len(combo), combo))

    kept = all_combos[:max_crosses]
    truncated = all_combos[max_crosses:]

    combinations: list[dict] = []
    features: list[str] = []
    columns_matrix: list[list[int]] = []  # per feature, per row 0/1 count

    for combo in kept:
        crossed_tokens = [
            "&".join(f"{col}={row_tokens[col][i]}" for col in combo)
            for i in range(row_count)
        ]
        distinct = sorted(set(crossed_tokens))
        index = {token: pos for pos, token in enumerate(distinct)}
        combo_features = [f"{'+'.join(combo)}::{token}" for token in distinct]
        for token in distinct:
            columns_matrix.append([0] * row_count)
        base = len(features)
        for i, token in enumerate(crossed_tokens):
            columns_matrix[base + index[token]][i] = 1
        features.extend(combo_features)
        combinations.append({
            "id": "+".join(combo),
            "columns": list(combo),
            "order": len(combo),
            "cardinality": len(distinct),
            "feature_count": len(distinct),
        })

    if not features:
        raise PipelineError(
            E_EMPTY_FEATURE, "cross generation produced an empty feature space",
            columns=columns,
        )

    # Fixed-point count matrix (rows x features); counts are exact integers.
    matrix_rows = [
        [columns_matrix[f][i] for f in range(len(features))]
        for i in range(row_count)
    ]
    import numpy as np

    quantized = quantize(np.asarray(matrix_rows, dtype=np.float64), scale=1.0)
    feature_counts = [int(sum(col)) for col in columns_matrix]

    return {
        "stage": "cross",
        "source_columns": columns,
        "max_order": max_order,
        "max_crosses": max_crosses,
        "combinations": combinations,
        "truncated": [
            {
                "columns": list(combo),
                "order": len(combo),
                "reason": f"{TRUNCATION_REASON} {max_crosses}",
            }
            for combo in truncated
        ],
        "features": features,
        "feature_counts": feature_counts,
        "matrix": quantized.digest_obj(),
        "quantization": {"scale": QUANT_SCALE, "count_scale": 1.0},
    }
