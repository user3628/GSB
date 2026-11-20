"""Stage 2: missing-value imputation + onehot / target / hash encoding.

Target encoding is leakage-safe: each row is encoded from out-of-fold label
statistics only (never its own label), with configurable smoothing and a
global-mean fallback for categories unseen in the training folds.
"""

from __future__ import annotations

import hashlib

import numpy as np

from .errors import ConfigError, SampleError
from .util import sha256_bytes


def parse_label(columns: dict, cfg: dict) -> np.ndarray:
    name = cfg["label_column"]
    if name not in columns:
        raise ConfigError("label column not found in sample", {"label_column": name})
    values = columns[name]
    out = np.empty(len(values), dtype=np.float64)
    for i, v in enumerate(values):
        try:
            f = float(v)
        except ValueError:
            raise SampleError("label value is not numeric", {"row": i, "value": v})
        if f not in (0.0, 1.0):
            raise SampleError("label column is not binary 0/1", {"row": i, "value": v})
        out[i] = f
    return out


def _impute_numeric(values, cfg):
    missing = cfg["missing"]
    arr = np.empty(len(values), dtype=np.float64)
    mask = np.zeros(len(values), dtype=bool)
    for i, v in enumerate(values):
        if v == "":
            mask[i] = True
            arr[i] = np.nan
        else:
            try:
                arr[i] = float(v)
            except ValueError:
                raise SampleError("non-numeric value in numeric column",
                                  {"row": i, "value": v})
    strategy = missing["strategy_numeric"]
    observed = arr[~mask]
    if strategy == "mean":
        fill = float(observed.mean()) if observed.size else float(missing["numeric_constant"])
    elif strategy == "median":
        fill = float(np.median(observed)) if observed.size else float(missing["numeric_constant"])
    elif strategy == "constant":
        fill = float(missing["numeric_constant"])
    else:
        raise ConfigError("unknown numeric imputation strategy", {"strategy": strategy})
    arr[mask] = fill
    return arr, fill, int(mask.sum())


def _impute_categorical(values, cfg):
    missing = cfg["missing"]
    strategy = missing["strategy_categorical"]
    if strategy == "constant":
        token = missing["constant"]
    elif strategy == "mode":
        counts = {}
        for v in values:
            if v != "":
                counts[v] = counts.get(v, 0) + 1
        token = min(counts, key=lambda k: (-counts[k], k)) if counts else missing["constant"]
    else:
        raise ConfigError("unknown categorical imputation strategy", {"strategy": strategy})
    out = [v if v != "" else token for v in values]
    n_imputed = sum(1 for v in values if v == "")
    return out, token, n_imputed


def hash_bucket(category: str, n_buckets: int) -> int:
    digest = hashlib.sha256(category.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "little") % n_buckets


def target_encode(values, y: np.ndarray, n_folds: int, smoothing: float, seed: int):
    """Out-of-fold target mean encoding with smoothing.

    Row i's encoded value uses only rows whose fold != fold(i); a category
    unseen in the training folds falls back to the training-fold global mean.
    Returns (encoded values, per-category diagnostics with per-fold values).
    """
    n = len(values)
    folds = np.random.RandomState(seed).permutation(n) % n_folds
    enc = np.empty(n, dtype=np.float64)
    cats = sorted(set(values))
    idx_by_cat = {
        c: np.array([i for i, v in enumerate(values) if v == c], dtype=np.int64) for c in cats
    }
    diag = {c: {"fold_values": [], "unseen_folds": 0} for c in cats}
    for k in range(n_folds):
        train_mask = folds != k
        y_train = y[train_mask]
        global_mean = float(y_train.mean())
        for c in cats:
            ci = idx_by_cat[c]
            train_rows = ci[train_mask[ci]]
            count = int(train_rows.size)
            if count == 0:
                value = global_mean
                diag[c]["unseen_folds"] += 1
            else:
                value = (float(y[train_rows].sum()) + smoothing * global_mean) / (
                    count + smoothing
                )
            diag[c]["fold_values"].append(value)
            enc[ci[~train_mask[ci]]] = value
    for c in cats:
        fold_values = diag[c]["fold_values"]
        diag[c]["spread"] = max(fold_values) - min(fold_values)
    return enc, diag


def encode_stage(columns: dict, infer_art: dict, cfg: dict, deadline):
    """Returns (meta_artifact, matrix_bytes, label_vector)."""
    y = parse_label(columns, cfg)
    enc_cfg = cfg["encoding"]
    missing = cfg["missing"]
    feats = []
    metas = []
    stats = {"numeric": 0, "onehot": 0, "hash": 0, "target": 0}
    imputed_cells = 0
    te_diag = {}
    for col in infer_art["columns"]:
        deadline.check("encode")
        name = col["name"]
        kind = col["kind"]
        if kind == "label":
            continue
        values = columns[name]
        if kind == "numeric":
            arr, fill, n_imputed = _impute_numeric(values, cfg)
            imputed_cells += n_imputed
            feats.append(arr)
            metas.append(
                {
                    "name": name,
                    "source": name,
                    "encoder": "numeric",
                    "domain": {
                        "kind": "numeric",
                        "min": float(arr.min()),
                        "max": float(arr.max()),
                        "impute_strategy": missing["strategy_numeric"],
                        "impute_value": fill,
                    },
                }
            )
            stats["numeric"] += 1
            continue
        cats, token, n_imputed = _impute_categorical(values, cfg)
        imputed_cells += n_imputed
        encoder = enc_cfg["per_column"].get(name, enc_cfg["categorical_default"])
        if encoder == "onehot":
            uniq = sorted(set(cats))
            max_categories = enc_cfg["onehot"]["max_categories"]
            if len(uniq) > max_categories:
                raise ConfigError(
                    "one-hot category count exceeds max_categories",
                    {"column": name, "categories": len(uniq), "max_categories": max_categories},
                )
            for cat in uniq:
                arr = np.array([1.0 if v == cat else 0.0 for v in cats], dtype=np.float64)
                feats.append(arr)
                metas.append(
                    {
                        "name": "%s__oh__%s" % (name, cat),
                        "source": name,
                        "encoder": "onehot",
                        "domain": {"kind": "onehot", "category": cat, "impute_token": token},
                    }
                )
                stats["onehot"] += 1
        elif encoder == "hash":
            n_buckets = enc_cfg["hash"]["n_buckets"]
            buckets = np.array([hash_bucket(v, n_buckets) for v in cats], dtype=np.int64)
            for b in range(n_buckets):
                arr = (buckets == b).astype(np.float64)
                feats.append(arr)
                metas.append(
                    {
                        "name": "%s__hash__%d" % (name, b),
                        "source": name,
                        "encoder": "hash",
                        "domain": {"kind": "hash", "bucket": b, "impute_token": token},
                    }
                )
                stats["hash"] += 1
        elif encoder == "target":
            tcfg = enc_cfg["target"]
            arr, diag = target_encode(
                cats, y, tcfg["n_folds"], float(tcfg["smoothing"]), tcfg["seed"]
            )
            feats.append(arr)
            metas.append(
                {
                    "name": "%s__te" % name,
                    "source": name,
                    "encoder": "target",
                    "domain": {
                        "kind": "target",
                        "min": float(arr.min()),
                        "max": float(arr.max()),
                        "impute_token": token,
                    },
                }
            )
            te_diag[name] = {
                "n_folds": tcfg["n_folds"],
                "smoothing": float(tcfg["smoothing"]),
                "seed": tcfg["seed"],
                "categories": diag,
            }
            stats["target"] += 1
        else:
            raise ConfigError("unknown encoder", {"column": name, "encoder": encoder})
    if not feats:
        raise ConfigError("encoding produced zero features")
    matrix = np.column_stack(feats).astype("<f8")
    matrix_bytes = matrix.tobytes(order="C")
    meta = {
        "stage": "encode",
        "rows": int(matrix.shape[0]),
        "cols": int(matrix.shape[1]),
        "columns": metas,
        "matrix_sha256": sha256_bytes(matrix_bytes),
        "matrix_nbytes": len(matrix_bytes),
        "stats": {
            "numeric_features": stats["numeric"],
            "onehot_features": stats["onehot"],
            "hash_features": stats["hash"],
            "target_features": stats["target"],
            "imputed_cells": imputed_cells,
        },
        "target_encoding": te_diag,
    }
    return meta, matrix_bytes, y
