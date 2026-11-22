"""Stage 4 - result summary.

Aggregates the per-candidate reduction outcomes into a deterministic
summary document with the pipeline-wide digest.
"""
from __future__ import annotations

from .fingerprint import hash_obj


def build_summary(
    infer_artifact: dict,
    cross_artifact: dict,
    reduce_artifact: dict,
    stage_fingerprints: dict[str, str],
) -> dict:
    candidates = []
    for cand in reduce_artifact["candidates"]:
        candidates.append({
            "method": cand["method"],
            "basis": cand["basis"],
            "target_dim": cand["target_dim"],
            "input_dim": cand["input_dim"],
            "explained_variance_ratio": cand["explained_variance_ratio"],
            "components_fingerprint": cand["components_fingerprint"],
            "projected_fingerprint": cand["projected_fingerprint"],
            "projection_matrix_fingerprint": cand.get("projection_matrix_fingerprint"),
            "error_bound": cand["error_bound"],
        })
    column_types = {
        col["name"]: col["type"]
        for col in sorted(infer_artifact["columns"], key=lambda c: c["name"])
    }
    summary = {
        "stage": "summary",
        "row_count": infer_artifact["row_count"],
        "column_types": column_types,
        "cross": {
            "combination_count": len(cross_artifact["combinations"]),
            "truncated_count": len(cross_artifact["truncated"]),
            "feature_count": len(cross_artifact["features"]),
        },
        "candidates": candidates,
        "stage_fingerprints": {
            k: stage_fingerprints[k] for k in sorted(stage_fingerprints)
        },
    }
    summary["pipeline_digest"] = hash_obj({
        "infer": hash_obj(infer_artifact),
        "cross": hash_obj(cross_artifact),
        "reduce": hash_obj(reduce_artifact),
    })
    return summary
