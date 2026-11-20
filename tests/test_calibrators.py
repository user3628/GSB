from __future__ import annotations

import numpy as np
import pytest

from gsb.calibrators import apply_calibrator, fit_calibrator
from gsb.config import Config
from gsb.errors import WorkbenchError


def calibration_fixture(seed=3, n=300):
    rng = np.random.default_rng(seed)
    scores = np.linspace(-3.0, 3.0, n) + rng.normal(0.0, 0.08, n)
    probability = 1.0 / (1.0 + np.exp(-(1.4 * scores - 0.2)))
    labels = (rng.random(n) < probability).astype(np.uint8)
    return scores, labels


@pytest.mark.parametrize("kind", ["platt", "isotonic"])
def test_calibrator_is_byte_reproducible_and_records_iterations(kind):
    scores, labels = calibration_fixture()
    config = Config(calibrator=kind).validate()
    first = fit_calibrator(config, scores, labels)
    second = fit_calibrator(config, scores, labels)
    assert first.to_payload() == second.to_payload()
    assert first.iterations >= 0
    probability_a = apply_calibrator(first, scores)
    probability_b = apply_calibrator(second, scores)
    assert np.array_equal(probability_a, probability_b)
    assert np.all((probability_a >= 0.0) & (probability_a <= 1.0))


def test_isotonic_mapping_is_monotone_and_records_parameters():
    scores, labels = calibration_fixture(seed=9)
    calibrator = fit_calibrator(Config(calibrator="isotonic").validate(), scores, labels)
    probability = apply_calibrator(calibrator, scores)
    order = np.argsort(scores, kind="mergesort")
    assert np.all(np.diff(probability[order]) >= 0.0)
    assert "x_hex" in calibrator.parameters and "y_hex" in calibrator.parameters
    assert calibrator.iterations == calibrator.parameters["merge_count"]


@pytest.mark.parametrize("kind", ["platt", "isotonic"])
def test_single_class_calibration_is_infeasible(kind):
    scores, _ = calibration_fixture()
    labels = np.ones(scores.shape[0], dtype=np.uint8)
    with pytest.raises(WorkbenchError) as caught:
        fit_calibrator(Config(calibrator=kind).validate(), scores, labels)
    assert caught.value.code == "E_CALIBRATION_INFEASIBLE"
    assert caught.value.details["sample_count"] == scores.size


def test_calibration_nan_has_position_and_stage():
    scores, labels = calibration_fixture()
    scores[17] = np.nan
    with pytest.raises(WorkbenchError) as caught:
        fit_calibrator(Config().validate(), scores, labels)
    assert caught.value.code == "E_NUMERIC"
    assert caught.value.details["sample_index"] == 17
    assert caught.value.details["stage"] == "calibration"
