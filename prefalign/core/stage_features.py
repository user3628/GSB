"""Stage 3: hashed reward-model pair features and deterministic fold stats."""
from __future__ import annotations

import numpy as np

from .errors import FeatureInvalidError
from .hashing import canonical_json, seeded_hash64, stable_hash64, tokenize

STAGE = "features"


def unit_embedding(text: str, dim: int, seed: int) -> np.ndarray:
    """Bag-of-tokens signed-hash embedding, L2-normalized to a unit vector."""
    vector = np.zeros(dim, dtype=np.float64)
    for token in tokenize(text):
        token_hash = seeded_hash64(token, seed)
        index = token_hash % dim
        sign = 1.0 if (token_hash >> 63) & 1 == 0 else -1.0
        vector[index] += sign
    norm = float(np.linalg.norm(vector))
    if norm > 0.0:
        vector = vector / norm
    return vector


def pair_feature(chosen_vec: np.ndarray, rejected_vec: np.ndarray, rule: str) -> np.ndarray:
    if rule == "concat":
        return np.concatenate([chosen_vec, rejected_vec])
    if rule == "diff":
        return chosen_vec - rejected_vec
    if rule == "normalized":
        diff = chosen_vec - rejected_vec
        norm = float(np.linalg.norm(diff))
        return diff / norm if norm > 0.0 else diff
    raise FeatureInvalidError(f"unknown pair feature rule: {rule}",
                              details={"rule": rule})


def _rounded_list(vector: np.ndarray, digits: int = 12) -> list[float]:
    return [round(float(value), digits) for value in vector]


def run(normalized: dict, quality: dict, cfg_raw: dict) -> dict:
    feature_cfg = cfg_raw["features"]
    folds_cfg = cfg_raw["folds"]
    dim = int(feature_cfg["embedding_dim"])
    seed = int(feature_cfg["hash_seed"])
    token_seed = int(feature_cfg["token_hash_seed"])
    rule = feature_cfg["rule"]
    n_folds = int(folds_cfg["n_folds"])
    fold_seed = int(folds_cfg["seed"])

    norm_by_id = {record["pair_id"]: record for record in normalized["records"]}
    expected_dim = dim * 2 if rule == "concat" else dim

    # Deterministic fold assignment at group granularity prevents leakage.
    groups = sorted({record["group_id"] for record in normalized["records"]})
    group_fold: dict[str, int] = {}
    for group_id in groups:
        group_fold[group_id] = seeded_hash64(group_id, fold_seed) % n_folds

    records: list[dict] = []
    fold_groups: dict[int, set[str]] = {fold: set() for fold in range(n_folds)}
    diagnostics: list[dict] = list(normalized.get("diagnostics", []))
    for quality_record in sorted(quality["records"], key=lambda item: item["pair_id"]):
        pair_id = quality_record["pair_id"]
        base = norm_by_id[pair_id]
        chosen_vec = unit_embedding(base["norm_chosen"], dim, token_seed)
        rejected_vec = unit_embedding(base["norm_rejected"], dim, token_seed)
        feature = pair_feature(chosen_vec, rejected_vec, rule)
        if feature.shape[0] != expected_dim:
            raise FeatureInvalidError(
                "feature dimension mismatch",
                details={"pair_id": pair_id, "group_id": base["group_id"],
                         "expected_dim": expected_dim, "actual_dim": int(feature.shape[0])},
            )
        fold = group_fold[base["group_id"]]
        fold_groups[fold].add(base["group_id"])
        records.append({
            "pair_id": pair_id,
            "group_id": base["group_id"],
            "fold": fold,
            "dim": expected_dim,
            "rule": rule,
            "feature": _rounded_list(feature),
        })

    for fold in range(n_folds):
        if not fold_groups[fold]:
            diagnostics.append({
                "code": "E_FEATURE_INVALID",
                "group_id": None,
                "fold": fold,
                "message": f"fold {fold} contains no prompt groups",
            })

    fold_stats = []
    for fold in range(n_folds):
        fold_records = [item for item in records if item["fold"] == fold]
        if fold_records:
            matrix = np.array([item["feature"] for item in fold_records], dtype=np.float64)
            mean = np.mean(matrix, axis=0)
            std = np.std(matrix, axis=0)
        else:
            mean = np.zeros(expected_dim, dtype=np.float64)
            std = np.zeros(expected_dim, dtype=np.float64)
        fold_stats.append({
            "fold": fold,
            "dim": expected_dim,
            "group_count": len(fold_groups[fold]),
            "pair_count": len(fold_records),
            "group_ids": sorted(fold_groups[fold]),
            "mean": _rounded_list(mean),
            "std": _rounded_list(std),
        })

    diagnostics.sort(key=lambda item: (str(item.get("pair_id") or ""),
                                       str(item.get("group_id") or ""), item["code"]))
    return {
        "stage": STAGE,
        "rule": rule,
        "embedding_dim": dim,
        "feature_dim": expected_dim,
        "n_folds": n_folds,
        "group_folds": [{"group_id": gid, "fold": group_fold[gid]} for gid in groups],
        "records": records,
        "fold_stats": fold_stats,
        "diagnostics": diagnostics,
        "stats": {"feature_pairs": len(records), "groups": len(groups), "folds": n_folds},
    }


def artifact_digest(result: dict) -> str:
    payload = {
        "rule": result["rule"],
        "feature_dim": result["feature_dim"],
        "group_folds": result["group_folds"],
        "records": [
            {"pair_id": item["pair_id"], "group_id": item["group_id"],
             "fold": item["fold"], "dim": item["dim"], "feature": item["feature"]}
            for item in result["records"]
        ],
        "fold_stats": result["fold_stats"],
        "diagnostics": result["diagnostics"],
    }
    return stable_hash64(canonical_json(payload))
