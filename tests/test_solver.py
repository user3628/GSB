from __future__ import annotations

import numpy as np
import pytest

from conftest import fast_config
from linreg_workbench.canonical import digest
from linreg_workbench.errors import NumericError
from linreg_workbench.solver import solve_logistic


def data() -> tuple[np.ndarray, np.ndarray]:
    rng = np.random.Generator(np.random.PCG64(1234))
    X = rng.normal(size=(180, 6))
    linear = 0.8 * X[:, 0] - 0.5 * X[:, 1] + 0.2 * X[:, 2]
    y = rng.binomial(1, 1.0 / (1.0 + np.exp(-linear))).astype(float)
    return X, y


def run(mode: str, strength: float, ratio: float, solver: str = "coordinate_descent") -> dict:
    X, y = data()
    config = fast_config("small")
    config["regularization"] = {"mode": mode, "strengths": [strength], "l1_ratios": [ratio]}
    config["solver"]["name"] = solver
    return solve_logistic(
        X,
        y,
        strength=strength,
        l1_ratio=ratio,
        config=config,
        fold=1,
        feature_names=[f"f{i}" for i in range(X.shape[1])],
    )


@pytest.mark.parametrize("mode,ratio", [("l1", 1.0), ("l2", 0.0), ("elasticnet", 0.4)])
def test_all_regularizers_are_deterministic(mode: str, ratio: float) -> None:
    one = run(mode, 0.03, ratio)
    two = run(mode, 0.03, ratio)
    assert digest(one) == digest(two)
    assert np.all(np.isfinite(one["coefficients"]))
    assert np.isfinite(one["intercept"])
    assert one["trajectory"][0]["iteration"] == 0
    assert one["trajectory"][-1]["iteration"] == one["trajectory_summary"]["iterations"]


def test_zero_strength_is_division_safe() -> None:
    result = run("elasticnet", 0.0, 0.5)
    assert np.all(np.isfinite(result["coefficients"]))
    assert np.isfinite(result["intercept"])
    assert len(result["trajectory"]) > 1


def test_l1_promotes_sparsity() -> None:
    l1 = run("l1", 0.12, 1.0)
    l2 = run("l2", 0.12, 0.0)
    assert l1["trajectory_summary"]["non_zero_coefficients"] <= l2["trajectory_summary"]["non_zero_coefficients"]


def test_proximal_gradient_is_deterministic() -> None:
    one = run("elasticnet", 0.04, 0.3, solver="proximal_gradient")
    two = run("elasticnet", 0.04, 0.3, solver="proximal_gradient")
    assert digest(one) == digest(two)
    assert one["trajectory_summary"]["converged"] is True


def test_objective_limit_returns_numeric_error() -> None:
    X, y = data()
    config = fast_config("small")
    config["solver"]["objective_limit"] = 0.001
    with pytest.raises(NumericError) as caught:
        solve_logistic(
            X,
            y,
            strength=0.01,
            l1_ratio=0.0,
            config=config,
            fold=2,
            feature_names=[f"f{i}" for i in range(X.shape[1])],
        )
    details = caught.value.details
    assert details["fold"] == 2
    assert details["iteration"] >= 0
    assert details["strength"] == 0.01
    assert details["l1_ratio"] == 0.0
