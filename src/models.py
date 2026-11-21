"""Three deterministic candidate forecasters.

Every forecaster is a pure function of the training window and the
configuration.  Only :mod:`numpy` is used and every computation runs in
``float64``, so identical inputs always yield byte-identical parameters,
predictions and residuals (also across worker processes).

Each forecaster returns a :class:`FitResult` containing:
    params        deterministic fitted parameters
    in_sample     one-step fitted values over the training window
    residuals     training residuals (observed - fitted)
    predictions   multi-step point forecasts
    sigma         residual standard deviation used for prediction intervals
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, List

import numpy as np

from .config import BacktestConfig


@dataclass
class FitResult:
    method: str
    params: Dict[str, float]
    in_sample: List[float]
    residuals: List[float]
    predictions: List[float]
    sigma: float


def _normal_quantile(probability: float) -> float:
    """Inverse standard-normal CDF (Acklam rational approximation)."""
    if not 0.0 < probability < 1.0:
        raise ValueError("quantile probability must lie inside (0, 1)")
    a = [-3.969683028665376e+01, 2.209460984245205e+02,
         -2.759285104469687e+02, 1.383577518672690e+02,
         -3.066479806614716e+01, 2.506628277459239e+00]
    b = [-5.447609879822406e+01, 1.615858368580409e+02,
         -1.556989798598866e+02, 6.680131188771972e+01,
         -1.328068155288572e+01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01,
         -2.400758277161838e+00, -2.549732539343734e+00,
         4.374664141464968e+00, 2.938163982698783e+00]
    d = [7.784695709041462e-03, 3.224671290700398e-01,
         2.445134137142996e+00, 3.754408661907416e+00]
    p_low, p_high = 0.02425, 1.0 - 0.02425
    if probability < p_low:
        q = math.sqrt(-2.0 * math.log(probability))
        return (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q
                + c[5]) / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0)
    if probability > p_high:
        q = math.sqrt(-2.0 * math.log(1.0 - probability))
        return -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q
                 + c[5]) / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0)
    q = probability - 0.5
    r = q * q
    return (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r
            + a[5]) * q / (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r
                            + b[4]) * r + 1.0)


def interval_halfwidth(sigma: float, confidence_level: float) -> float:
    if not math.isfinite(sigma) or sigma <= 0.0:
        return 0.0
    return float(_normal_quantile(0.5 + confidence_level / 2.0) * sigma)


def _residual_sigma(values: np.ndarray, fitted: np.ndarray) -> float:
    residuals = values - fitted
    # population standard deviation of residuals, including a zero mean
    return float(math.sqrt(float(np.mean(residuals * residuals))))


def fit_moving_average(window: np.ndarray, cfg: BacktestConfig) -> FitResult:
    values = np.asarray(window, dtype=np.float64)
    n = values.shape[0]
    mean_value = float(np.mean(values))
    in_sample = np.full(n, mean_value, dtype=np.float64)
    sigma = _residual_sigma(values, in_sample)
    predictions = [mean_value for _ in range(cfg.horizon)]
    return FitResult(
        method="moving_average",
        params={"mean": mean_value, "n": float(n)},
        in_sample=[float(x) for x in in_sample],
        residuals=[float(x) for x in (values - in_sample)],
        predictions=predictions,
        sigma=sigma,
    )


def fit_exponential_smoothing(window: np.ndarray,
                              cfg: BacktestConfig) -> FitResult:
    """Damped-trend Holt linear exponential smoothing with explicit formulas."""
    values = np.asarray(window, dtype=np.float64)
    n = values.shape[0]
    alpha, phi = cfg.es_alpha, cfg.es_phi

    # deterministic initial level / trend from the first two observations
    level = float(values[0])
    trend = float(values[1] - values[0]) if n > 1 else 0.0

    in_sample = np.empty(n, dtype=np.float64)
    in_sample[0] = level
    if n > 1:
        in_sample[1] = level + phi * trend

    for t in range(1, n):
        observed = float(values[t])
        previous_level = level
        level = alpha * observed + (1.0 - alpha) * (level + phi * trend)
        trend = (alpha * (level - previous_level)
                 + (1.0 - alpha) * phi * trend)
        if t + 1 < n:
            in_sample[t + 1] = level + phi * trend

    residuals = values - in_sample
    sigma = float(math.sqrt(float(np.mean(residuals * residuals))))

    # damped multi-step forecast:
    # y_hat(h) = level + trend * sum(phi^k, k=1..h)
    predictions: List[float] = []
    cumulative = 0.0
    phi_power = 1.0
    for _ in range(cfg.horizon):
        phi_power *= phi
        cumulative += phi_power
        predictions.append(float(level + trend * cumulative))

    return FitResult(
        method="exponential_smoothing",
        params={"level": float(level), "trend": float(trend),
                "alpha": float(alpha), "phi": float(phi)},
        in_sample=[float(x) for x in in_sample],
        residuals=[float(x) for x in residuals],
        predictions=predictions,
        sigma=sigma,
    )


def fit_linear_ar(window: np.ndarray, cfg: BacktestConfig) -> FitResult:
    """Linear autoregression solved once with deterministic least squares."""
    values = np.asarray(window, dtype=np.float64)
    n = values.shape[0]
    lags = cfg.ar_lags

    rows = n - lags
    if rows < 1:
        raise ValueError("linear_ar needs at least ar_lags + 1 observations")
    design = np.empty((rows, lags), dtype=np.float64)
    target = np.empty(rows, dtype=np.float64)
    for row in range(rows):
        design[row] = values[row:row + lags][::-1]
        target[row] = values[row + lags]

    # deterministic least squares; lstsq handles rank-deficient windows
    coefficients, _, _, _ = np.linalg.lstsq(design, target, rcond=None)
    intercept = float(target.mean() - float(design.mean(axis=0) @ coefficients))

    in_sample = np.full(n, np.nan, dtype=np.float64)
    in_sample[:lags] = values[:lags]  # warm-up observations, residual excluded
    fitted = design @ coefficients + intercept
    in_sample[lags:] = fitted

    residual_values = target - fitted
    sigma = float(math.sqrt(float(np.mean(residual_values ** 2))))

    history = list(values)
    predictions: List[float] = []
    for _ in range(cfg.horizon):
        feature = np.asarray(history[-lags:][::-1], dtype=np.float64)
        forecast = float(feature @ coefficients + intercept)
        predictions.append(forecast)
        history.append(forecast)

    params = {"intercept": intercept}
    params.update({f"coef_lag{k}": float(coefficients[k - 1])
                   for k in range(1, lags + 1)})
    valid_residuals = [float(x) for x in residual_values]
    valid_in_sample = [float(x) for x in in_sample[lags:]]
    return FitResult(
        method="linear_ar",
        params=params,
        in_sample=valid_in_sample,
        residuals=valid_residuals,
        predictions=predictions,
        sigma=sigma,
    )


_FITTERS = {
    "moving_average": fit_moving_average,
    "exponential_smoothing": fit_exponential_smoothing,
    "linear_ar": fit_linear_ar,
}


def fit_window(window: np.ndarray, cfg: BacktestConfig) -> FitResult:
    try:
        return _FITTERS[cfg.method](window, cfg)
    except KeyError as exc:  # pragma: no cover - guarded by config validation
        raise ValueError(f"unknown method {cfg.method!r}") from exc
