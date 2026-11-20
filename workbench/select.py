"""Stage 3: feature selection on the encoded matrix (variance / MI / RFE)."""

from __future__ import annotations

import math

import numpy as np

from .errors import ConfigError


def _mi_from_labels(bins: np.ndarray, y: np.ndarray) -> float:
    """Mutual information between binned discrete features and binary label."""
    n = y.shape[0]
    yi = y.astype(np.int64)
    key = bins.astype(np.int64) * 2 + yi
    keys, counts = np.unique(key, return_counts=True)
    pb = np.bincount(bins.astype(np.int64)).astype(np.float64) / n
    py = np.bincount(yi).astype(np.float64) / n
    mi = 0.0
    for k, c in zip(keys.tolist(), counts.tolist()):
        bi, yj = divmod(k, 2)
        pxy = c / n
        mi += pxy * math.log(pxy / (pb[bi] * py[yj]))
    return mi


def _mi_column(x: np.ndarray, y: np.ndarray, n_bins: int) -> float:
    uniq = np.unique(x)
    if uniq.size <= n_bins:
        bins = np.searchsorted(uniq, x)
    else:
        quantiles = np.linspace(0.0, 1.0, n_bins + 1)[1:-1]
        edges = np.unique(np.quantile(x, quantiles))
        bins = np.searchsorted(edges, x, side="right")
    return _mi_from_labels(bins, y)


def _variance_select(X, names, params):
    threshold = float(params["threshold"])
    scores = np.var(X, axis=0)
    kept, dropped = [], []
    for i, name in enumerate(names):
        score = float(scores[i])
        if score > threshold:
            kept.append({"feature": name, "score": score})
        else:
            dropped.append(
                {"feature": name, "score": score, "reason": "variance_below_threshold"}
            )
    return kept, dropped


def _mi_select(X, names, y, params, deadline):
    threshold = float(params["threshold"])
    top_k = int(params["top_k"])
    n_bins = int(params["n_bins"])
    scored = []
    for j, name in enumerate(names):
        deadline.check("select")
        scored.append((name, _mi_column(X[:, j], y, n_bins)))
    above = sorted((t for t in scored if t[1] >= threshold), key=lambda t: (-t[1], t[0]))
    kept = [{"feature": n, "score": s} for n, s in above[:top_k]]
    dropped = [
        {"feature": n, "score": s, "reason": "mi_top_k_cut"} for n, s in above[top_k:]
    ]
    dropped += [
        {"feature": n, "score": s, "reason": "mi_below_threshold"}
        for n, s in scored
        if s < threshold
    ]
    return kept, dropped


def _ridge_weights(X, y, lam):
    mu = X.mean(axis=0)
    sd = X.std(axis=0)
    sd = np.where(sd == 0.0, 1.0, sd)
    Xs = (X - mu) / sd
    gram = Xs.T @ Xs + lam * np.eye(X.shape[1])
    return np.linalg.solve(gram, Xs.T @ y)


def _rfe_select(X, names, y, params, deadline):
    k_select = int(params["n_features_to_select"])
    step = int(params["step"])
    lam = float(params["ridge_lambda"])
    if k_select > X.shape[1]:
        raise ConfigError("rfe.n_features_to_select exceeds encoded feature count",
                          {"n_features_to_select": k_select, "features": X.shape[1]})
    remaining = list(range(X.shape[1]))
    dropped = []
    round_no = 0
    while len(remaining) > k_select:
        round_no += 1
        deadline.check("select")
        weights = np.abs(_ridge_weights(X[:, remaining], y, lam))
        order = sorted(range(len(remaining)), key=lambda i: (float(weights[i]), names[remaining[i]]))
        n_drop = min(step, len(remaining) - k_select)
        for i in order[:n_drop]:
            dropped.append(
                {
                    "feature": names[remaining[i]],
                    "score": float(weights[i]),
                    "reason": "rfe_eliminated",
                    "round": round_no,
                }
            )
        drop_set = set(order[:n_drop])
        remaining = [r for i, r in enumerate(remaining) if i not in drop_set]
    weights = np.abs(_ridge_weights(X[:, remaining], y, lam))
    kept = [
        {"feature": names[r], "score": float(weights[i])} for i, r in enumerate(remaining)
    ]
    return kept, dropped


def select_stage(matrix_bytes: bytes, rows: int, cols: int, names, y, cfg: dict, deadline):
    X = np.frombuffer(matrix_bytes, dtype="<f8").reshape(rows, cols)
    sel = cfg["selection"]
    method = sel["method"]
    if method == "variance":
        kept, dropped = _variance_select(X, names, sel["variance"])
    elif method == "mutual_info":
        kept, dropped = _mi_select(X, names, y, sel["mutual_info"], deadline)
    elif method == "rfe":
        kept, dropped = _rfe_select(X, names, y, sel["rfe"], deadline)
    else:
        raise ConfigError("unknown selection method", {"method": method})
    kept.sort(key=lambda d: (-d["score"], d["feature"]))
    dropped.sort(key=lambda d: (d["score"], d["feature"]))
    artifact = {
        "stage": "select",
        "method": method,
        "params": sel[method],
        "kept": kept,
        "dropped": dropped,
        "kept_count": len(kept),
        "dropped_count": len(dropped),
    }
    return artifact
