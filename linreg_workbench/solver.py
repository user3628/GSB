"""Deterministic regularized logistic regression solvers.

Both solvers use the mean negative log-likelihood and the parameterization

    lambda * (rho * ||w||_1 + (1 - rho) / 2 * ||w||_2^2)

where ``lambda`` is the configured strength and ``rho`` is ``l1_ratio``.  The
intercept is never regularized.  A zero strength is a valid, division-safe
unregularized fit.
"""

from __future__ import annotations

import math
from typing import Any, Callable

import numpy as np

from .canonical import coefficient_summary, digest
from .errors import NumericError

StopCallback = Callable[[int, float], None]


def sigmoid(z: np.ndarray) -> np.ndarray:
    result = np.empty_like(z, dtype=np.float64)
    positive = z >= 0.0
    result[positive] = 1.0 / (1.0 + np.exp(-z[positive]))
    exp_z = np.exp(z[~positive])
    result[~positive] = exp_z / (1.0 + exp_z)
    return result


def softplus(z: np.ndarray) -> np.ndarray:
    # log(1 + exp(z)), evaluated without overflow.
    return np.logaddexp(0.0, z)


def data_objective(X: np.ndarray, y: np.ndarray, weights: np.ndarray, intercept: float) -> float:
    z = X @ weights + intercept
    return float(np.mean(softplus(z) - y * z))


def objective_with_regularization(
    X: np.ndarray,
    y: np.ndarray,
    weights: np.ndarray,
    intercept: float,
    strength: float,
    l1_ratio: float,
) -> float:
    data = data_objective(X, y, weights, intercept)
    l1 = strength * l1_ratio
    l2 = strength * (1.0 - l1_ratio)
    return float(data + l1 * np.abs(weights).sum() + 0.5 * l2 * np.dot(weights, weights))


def validation_metrics(X: np.ndarray, y: np.ndarray, weights: np.ndarray, intercept: float) -> dict[str, float]:
    z = X @ weights + intercept
    log_loss = float(np.mean(softplus(z) - y * z))
    probabilities = sigmoid(z)
    accuracy = float(np.mean((probabilities >= 0.5).astype(np.float64) == y))
    if not math.isfinite(log_loss) or not math.isfinite(accuracy):
        raise NumericError(
            "Validation metric is not finite",
            details={"reason": "NaN or Inf in validation metric", "log_loss": log_loss},
        )
    return {
        "log_loss": log_loss,
        "accuracy": accuracy,
        "objective": log_loss,
    }


def _numeric_failure(
    *,
    fold: int | None,
    iteration: int,
    strength: float,
    l1_ratio: float,
    reason: str,
    objective: float | None = None,
    solver: dict[str, Any] | None = None,
) -> NumericError:
    return NumericError(
        "Numerical failure during fold solve",
        details={
            "fold": fold,
            "iteration": iteration,
            "strength": float(strength),
            "l1_ratio": float(l1_ratio),
            "solver": solver or {},
            "reason": reason,
            "objective": objective,
        },
    )


def _check_finite(
    *,
    weights: np.ndarray,
    intercept: float,
    objective: float,
    iteration: int,
    fold: int | None,
    strength: float,
    l1_ratio: float,
    solver: dict[str, Any],
) -> None:
    if not np.all(np.isfinite(weights)) or not math.isfinite(intercept):
        raise _numeric_failure(
            fold=fold,
            iteration=iteration,
            strength=strength,
            l1_ratio=l1_ratio,
            reason="coefficient vector contains NaN or Inf",
            objective=objective if math.isfinite(objective) else None,
            solver=solver,
        )
    if not math.isfinite(objective):
        raise _numeric_failure(
            fold=fold,
            iteration=iteration,
            strength=strength,
            l1_ratio=l1_ratio,
            reason="objective is NaN or Inf",
            solver=solver,
        )
    limit = float(solver["objective_limit"])
    if objective > limit:
        raise _numeric_failure(
            fold=fold,
            iteration=iteration,
            strength=strength,
            l1_ratio=l1_ratio,
            reason=f"objective {objective:.12g} exceeds configured limit {limit:.12g}",
            objective=objective,
            solver=solver,
        )


def _coordinate_descent(
    X: np.ndarray,
    y: np.ndarray,
    *,
    strength: float,
    l1_ratio: float,
    solver: dict[str, Any],
    fold: int | None,
    stop_callback: StopCallback | None,
) -> tuple[np.ndarray, float, list[dict[str, Any]], bool]:
    n, p = X.shape
    weights = np.zeros(p, dtype=np.float64)
    intercept = 0.0
    z = np.zeros(n, dtype=np.float64)
    l1 = strength * l1_ratio
    l2 = strength * (1.0 - l1_ratio)
    trajectory: list[dict[str, Any]] = []
    initial = objective_with_regularization(X, y, weights, intercept, strength, l1_ratio)
    _check_finite(
        weights=weights,
        intercept=intercept,
        objective=initial,
        iteration=0,
        fold=fold,
        strength=strength,
        l1_ratio=l1_ratio,
        solver=solver,
    )
    trajectory.append(
        {
            "iteration": 0,
            "objective": initial,
            "coefficient_delta": 0.0,
            "l1_norm": 0.0,
        }
    )
    converged = False
    tolerance = float(solver["tolerance"])
    for iteration in range(1, int(solver["max_iter"]) + 1):
        max_delta = 0.0

        # Intercept: unregularized one-dimensional Newton step.
        probabilities = sigmoid(z)
        gradient_b = float(np.mean(probabilities - y))
        hessian_b = float(np.mean(probabilities * (1.0 - probabilities)))
        if hessian_b > 1e-15:
            delta = -gradient_b / hessian_b
            intercept += delta
            z += delta
            max_delta = max(max_delta, abs(delta))

        # Cyclic coordinate updates.  The quadratic diagonal uses the current
        # probabilities; the soft threshold handles the L1 component exactly.
        for column in range(p):
            xj = X[:, column]
            probabilities = sigmoid(z)
            residual = probabilities - y
            gradient = float(np.mean(xj * residual)) + l2 * weights[column]
            curvature = float(np.mean(xj * xj * probabilities * (1.0 - probabilities))) + l2
            if curvature <= 1e-15:
                continue
            unpenalized = weights[column] - gradient / curvature
            if l1 > 0.0:
                threshold = l1 / curvature
                if unpenalized > threshold:
                    new_value = unpenalized - threshold
                elif unpenalized < -threshold:
                    new_value = unpenalized + threshold
                else:
                    new_value = 0.0
            else:
                new_value = unpenalized
            delta = float(new_value - weights[column])
            if delta != 0.0:
                weights[column] = float(new_value)
                z += delta * xj
                max_delta = max(max_delta, abs(delta))

        objective = objective_with_regularization(
            X, y, weights, intercept, strength, l1_ratio
        )
        _check_finite(
            weights=weights,
            intercept=intercept,
            objective=objective,
            iteration=iteration,
            fold=fold,
            strength=strength,
            l1_ratio=l1_ratio,
            solver=solver,
        )
        trajectory.append(
            {
                "iteration": iteration,
                "objective": objective,
                "coefficient_delta": float(max_delta),
                "l1_norm": float(np.abs(weights).sum()),
            }
        )
        if stop_callback is not None:
            # This call is made only after a complete cyclic sweep.
            stop_callback(iteration, objective)
        if max_delta <= tolerance:
            converged = True
            break
    return weights, float(intercept), trajectory, converged


def _largest_augmented_hessian(X: np.ndarray) -> float:
    augmented = np.column_stack([X, np.ones(X.shape[0], dtype=np.float64)])
    gram = (augmented.T @ augmented) / float(X.shape[0])
    eigenvalues = np.linalg.eigvalsh(gram)
    largest = float(eigenvalues[-1])
    return max(largest, 1e-12)


def _proximal_gradient(
    X: np.ndarray,
    y: np.ndarray,
    *,
    strength: float,
    l1_ratio: float,
    solver: dict[str, Any],
    fold: int | None,
    stop_callback: StopCallback | None,
) -> tuple[np.ndarray, float, list[dict[str, Any]], bool]:
    n, p = X.shape
    weights = np.zeros(p, dtype=np.float64)
    intercept = 0.0
    l1 = strength * l1_ratio
    l2 = strength * (1.0 - l1_ratio)
    lipschitz = 0.25 * _largest_augmented_hessian(X) + l2
    step = 1.0 / lipschitz if lipschitz > 0.0 else 1.0
    trajectory: list[dict[str, Any]] = []
    initial = objective_with_regularization(X, y, weights, intercept, strength, l1_ratio)
    _check_finite(
        weights=weights,
        intercept=intercept,
        objective=initial,
        iteration=0,
        fold=fold,
        strength=strength,
        l1_ratio=l1_ratio,
        solver=solver,
    )
    trajectory.append(
        {
            "iteration": 0,
            "objective": initial,
            "coefficient_delta": 0.0,
            "l1_norm": 0.0,
        }
    )
    converged = False
    tolerance = float(solver["tolerance"])
    for iteration in range(1, int(solver["max_iter"]) + 1):
        probabilities = sigmoid(X @ weights + intercept)
        residual = probabilities - y
        gradient_w = (X.T @ residual) / float(n) + l2 * weights
        gradient_b = float(np.mean(residual))
        candidate = weights - step * gradient_w
        if l1 > 0.0:
            threshold = step * l1
            candidate = np.sign(candidate) * np.maximum(np.abs(candidate) - threshold, 0.0)
        new_intercept = intercept - step * gradient_b
        delta = max(
            float(np.max(np.abs(candidate - weights))) if p else 0.0,
            abs(float(new_intercept - intercept)),
        )
        weights = candidate
        intercept = float(new_intercept)
        objective = objective_with_regularization(
            X, y, weights, intercept, strength, l1_ratio
        )
        _check_finite(
            weights=weights,
            intercept=intercept,
            objective=objective,
            iteration=iteration,
            fold=fold,
            strength=strength,
            l1_ratio=l1_ratio,
            solver=solver,
        )
        trajectory.append(
            {
                "iteration": iteration,
                "objective": objective,
                "coefficient_delta": float(delta),
                "l1_norm": float(np.abs(weights).sum()),
            }
        )
        if stop_callback is not None:
            stop_callback(iteration, objective)
        if delta <= tolerance:
            converged = True
            break
    return weights, float(intercept), trajectory, converged


def solve_logistic(
    X: np.ndarray,
    y: np.ndarray,
    *,
    strength: float,
    l1_ratio: float,
    config: dict[str, Any],
    fold: int | None,
    feature_names: list[str],
    stop_callback: StopCallback | None = None,
) -> dict[str, Any]:
    if not (0.0 <= l1_ratio <= 1.0):
        raise ValueError("l1_ratio must be between zero and one")
    if strength < 0.0:
        raise ValueError("strength must be non-negative")
    solver = dict(config["solver"])
    if solver["name"] == "coordinate_descent":
        weights, intercept, trajectory, converged = _coordinate_descent(
            X,
            y,
            strength=float(strength),
            l1_ratio=float(l1_ratio),
            solver=solver,
            fold=fold,
            stop_callback=stop_callback,
        )
    elif solver["name"] == "proximal_gradient":
        weights, intercept, trajectory, converged = _proximal_gradient(
            X,
            y,
            strength=float(strength),
            l1_ratio=float(l1_ratio),
            solver=solver,
            fold=fold,
            stop_callback=stop_callback,
        )
    else:
        raise ValueError("unsupported solver")
    if not np.all(np.isfinite(X)) or not np.all(np.isfinite(y)):
        raise _numeric_failure(
            fold=fold,
            iteration=len(trajectory) - 1,
            strength=strength,
            l1_ratio=l1_ratio,
            reason="training matrix or target contains NaN or Inf",
            solver=solver,
        )
    final_objective = trajectory[-1]["objective"]
    _check_finite(
        weights=weights,
        intercept=intercept,
        objective=final_objective,
        iteration=trajectory[-1]["iteration"],
        fold=fold,
        strength=strength,
        l1_ratio=l1_ratio,
        solver=solver,
    )
    summary = {
        "initial_objective": trajectory[0]["objective"],
        "final_objective": final_objective,
        "iterations": trajectory[-1]["iteration"],
        "coefficient_change": trajectory[-1]["coefficient_delta"],
        "converged": bool(converged),
        "max_abs_coefficient": float(np.max(np.abs(weights))) if weights.size else 0.0,
        "non_zero_coefficients": int(np.count_nonzero(weights)),
        "l1_norm": float(np.abs(weights).sum()),
        "l2_norm": float(math.sqrt(float(np.dot(weights, weights)))),
        "trajectory_digest": digest(trajectory),
        "coefficient_summary": coefficient_summary(weights, intercept),
    }
    return {
        "feature_names": feature_names,
        "coefficients": weights.tolist(),
        "intercept": float(intercept),
        "trajectory": trajectory,
        "trajectory_summary": summary,
    }
