import numpy as np
import pytest

from evalbench.calibrate import (apply_calibrator, apply_isotonic,
                                 fit_calibrator, fit_isotonic, fit_platt)
from evalbench.errors import FitFailedError


@pytest.fixture()
def sample():
    rng = np.random.default_rng(42)
    scores = rng.random(300)
    labels = (rng.random(300) < scores).astype(int)
    return scores, labels


def test_platt_converges_with_iteration_log(sample):
    scores, labels = sample
    model = fit_platt(scores, labels)
    assert model["fit"]["converged"] is True
    assert model["fit"]["iterations"] >= 1
    assert set(model["params"]) == {"A", "B"}
    cal = apply_calibrator(model, scores)
    assert np.all((cal >= 0) & (cal <= 1))


def test_platt_improves_log_loss(sample):
    from evalbench.diagnostics import log_loss
    scores, labels = sample
    model = fit_platt(scores, labels)
    cal = apply_calibrator(model, scores)
    assert log_loss(labels, cal) <= log_loss(labels, scores) + 1e-9


def test_isotonic_monotone_mapping(sample):
    scores, labels = sample
    model = fit_isotonic(scores, labels)
    assert model["fit"]["monotone"] is True
    grid = np.linspace(0, 1, 101)
    out = apply_isotonic(model, grid)
    assert np.all(np.diff(out) >= -1e-12)


def test_platt_single_class_fails_with_business_code():
    with pytest.raises(FitFailedError) as exc:
        fit_platt(np.array([0.1, 0.2, 0.3]), np.ones(3))
    assert exc.value.code == "E_FIT_FAILED"


def test_isotonic_degenerate_fails_with_business_code():
    with pytest.raises(FitFailedError) as exc:
        fit_isotonic(np.array([0.5, 0.5]), np.array([0, 1]))
    assert exc.value.code == "E_FIT_FAILED"


def test_fit_calibrator_dispatch(sample):
    scores, labels = sample
    assert fit_calibrator(scores, labels, "platt")["method"] == "platt"
    assert fit_calibrator(scores, labels, "isotonic")["method"] == "isotonic"
