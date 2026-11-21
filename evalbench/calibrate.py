"""Calibrators: Platt scaling (log-domain Newton) and isotonic regression.

Both calibrators return a fitted model plus fit metadata (iterations,
convergence). Degenerate input raises FitFailedError (E_FIT_FAILED)
instead of returning an empty result.
"""
from __future__ import annotations

import numpy as np

from .config import METHOD_PLATT
from .errors import FitFailedError

_EPS = 1e-12


def _logit(p: np.ndarray) -> np.ndarray:
    p = np.clip(p, _EPS, 1.0 - _EPS)
    return np.log(p / (1.0 - p))


def _sigmoid(x: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-x))


# ------------------------------------------------------------------- Platt

def fit_platt(scores: np.ndarray, labels: np.ndarray,
              *, max_iter: int = 200, tol: float = 1e-10) -> dict:
    """Fit p = sigmoid(A * logit(score) + B) by Newton iteration in the
    log-odds domain on Platt's smoothed targets.

    Records iteration count and convergence state. Raises E_FIT_FAILED on
    degenerate labels, singular Hessian or non-convergence.
    """
    scores = np.asarray(scores, dtype=float)
    labels = np.asarray(labels, dtype=float)
    n_pos = float(labels.sum())
    n_neg = float(len(labels) - labels.sum())
    if len(labels) < 2 or n_pos == 0.0 or n_neg == 0.0:
        raise FitFailedError(
            "platt scaling requires at least one positive and one negative sample",
            method="platt", n_samples=int(len(labels)),
            n_pos=int(n_pos), n_neg=int(n_neg),
        )
    # Platt's smoothed targets
    t_pos = (n_pos + 1.0) / (n_pos + 2.0)
    t_neg = 1.0 / (n_neg + 2.0)
    targets = np.where(labels > 0.5, t_pos, t_neg)

    f = _logit(scores)
    a, b = 0.0, float(np.log((n_pos + 1.0) / (n_neg + 1.0)))

    def nll(a_: float, b_: float) -> float:
        p = _sigmoid(a_ * f + b_)
        p = np.clip(p, _EPS, 1.0 - _EPS)
        return float(-np.sum(targets * np.log(p) + (1.0 - targets) * np.log(1.0 - p)))

    converged = False
    iterations = 0
    prev = nll(a, b)
    for iterations in range(1, max_iter + 1):
        p = _sigmoid(a * f + b)
        w = p * (1.0 - p)
        d = p - targets
        g_a = float(np.sum(d * f))
        g_b = float(np.sum(d))
        h_aa = float(np.sum(w * f * f)) + 1e-9
        h_ab = float(np.sum(w * f))
        h_bb = float(np.sum(w)) + 1e-9
        det = h_aa * h_bb - h_ab * h_ab
        if not np.isfinite(det) or abs(det) < 1e-18:
            raise FitFailedError(
                "platt newton iteration hit a singular Hessian",
                method="platt", iteration=iterations, det=det,
            )
        step_a = (h_bb * g_a - h_ab * g_b) / det
        step_b = (-h_ab * g_a + h_aa * g_b) / det
        # damped Newton: halve the step while the objective increases
        damping = 1.0
        for _ in range(64):
            na, nb = a - damping * step_a, b - damping * step_b
            cur = nll(na, nb)
            if cur <= prev + 1e-15:
                break
            damping *= 0.5
        else:
            raise FitFailedError(
                "platt newton iteration failed to decrease the objective",
                method="platt", iteration=iterations,
            )
        a, b = na, nb
        if max(abs(damping * step_a), abs(damping * step_b)) < tol or abs(prev - cur) < 1e-14:
            converged = True
            prev = cur
            break
        prev = cur
    if not converged:
        raise FitFailedError(
            "platt newton iteration did not converge",
            method="platt", iterations=iterations, max_iter=max_iter,
        )
    return {
        "method": "platt",
        "params": {"A": a, "B": b},
        "fit": {
            "iterations": iterations,
            "converged": converged,
            "final_nll": prev,
            "n_samples": int(len(labels)),
            "n_pos": int(n_pos),
            "n_neg": int(n_neg),
        },
    }


def apply_platt(model: dict, scores: np.ndarray) -> np.ndarray:
    a = model["params"]["A"]
    b = model["params"]["B"]
    return _sigmoid(a * _logit(np.asarray(scores, dtype=float)) + b)


# ---------------------------------------------------------------- isotonic

def _pava(values: np.ndarray, weights: np.ndarray) -> np.ndarray:
    """Pool Adjacent Violators: weighted monotone (non-decreasing) fit."""
    stack: list[list] = []  # [start, end, weighted_mean, weight]
    for k, (value, weight) in enumerate(zip(values.astype(float), weights.astype(float))):
        stack.append([k, k + 1, float(value), float(weight)])
        while len(stack) >= 2 and stack[-2][2] > stack[-1][2]:
            b2 = stack.pop()
            b1 = stack.pop()
            tot_w = b1[3] + b2[3]
            avg = (b1[2] * b1[3] + b2[2] * b2[3]) / tot_w
            stack.append([b1[0], b2[1], avg, tot_w])
    fitted = np.empty(len(values), dtype=float)
    for start, end, avg, _w in stack:
        fitted[start:end] = avg
    return fitted


def fit_isotonic(scores: np.ndarray, labels: np.ndarray) -> dict:
    """Isotonic regression on score-sorted labels + linear interpolation
    between knot points to obtain a monotone mapping."""
    scores = np.asarray(scores, dtype=float)
    labels = np.asarray(labels, dtype=float)
    if len(labels) < 2:
        raise FitFailedError(
            "isotonic regression requires at least two samples",
            method="isotonic", n_samples=int(len(labels)),
        )
    if float(labels.sum()) == 0.0 or float(labels.sum()) == float(len(labels)):
        raise FitFailedError(
            "isotonic regression requires both positive and negative samples",
            method="isotonic", n_samples=int(len(labels)),
            n_pos=int(labels.sum()),
        )
    order = np.argsort(scores, kind="stable")
    xs = scores[order]
    ys = labels[order]
    # aggregate duplicate x values (weighted means) so knots are unique
    ux, inv, counts = np.unique(xs, return_inverse=True, return_counts=True)
    sums = np.zeros(len(ux), dtype=float)
    np.add.at(sums, inv, ys)
    means = sums / counts
    if len(ux) < 2:
        raise FitFailedError(
            "isotonic regression requires at least two distinct score values",
            method="isotonic", n_samples=int(len(labels)), n_distinct=int(len(ux)),
        )
    fitted = _pava(means, counts.astype(float))
    return {
        "method": "isotonic",
        "params": {
            "knots_x": [float(x) for x in ux],
            "knots_y": [float(y) for y in fitted],
        },
        "fit": {
            "n_samples": int(len(labels)),
            "n_knots": int(len(ux)),
            "monotone": bool(np.all(np.diff(fitted) >= -1e-15)),
        },
    }


def apply_isotonic(model: dict, scores: np.ndarray) -> np.ndarray:
    xs = np.asarray(model["params"]["knots_x"], dtype=float)
    ys = np.asarray(model["params"]["knots_y"], dtype=float)
    scores = np.asarray(scores, dtype=float)
    out = np.interp(scores, xs, ys)  # linear interpolation, clamped at ends
    return np.clip(out, 0.0, 1.0)


# ------------------------------------------------------------------ facade

def fit_calibrator(scores: np.ndarray, labels: np.ndarray, method: str) -> dict:
    if method == METHOD_PLATT:
        return fit_platt(scores, labels)
    return fit_isotonic(scores, labels)


def apply_calibrator(model: dict, scores: np.ndarray) -> np.ndarray:
    if model["method"] == METHOD_PLATT:
        return apply_platt(model, scores)
    return apply_isotonic(model, scores)
