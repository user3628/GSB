from __future__ import annotations

import numpy as np
import pytest

from gsb.pipeline import Pipeline
from gsb.precision import (
    probability_terms,
    quantize_probabilities,
    quantize_raw_scores,
    reliability_table,
)
from gsb.errors import WorkbenchError


@pytest.fixture
def completed_pipeline(make_workspace, small_dataset, fast_config):
    workspace = make_workspace("batch_complete")
    workspace.import_dataset(small_dataset)
    workspace.set_config(fast_config)
    pipeline = Pipeline(workspace, small_dataset)
    result = pipeline.run_all()
    assert result["status"]["overall"] == "complete"
    return pipeline


def test_different_batch_sizes_and_orders_are_byte_identical(completed_pipeline):
    result = completed_pipeline.compare_batch_modes([7, 64, 800])
    assert result["all_equal"] is True
    assert len({run["result_digest"] for run in result["runs"]}) == 1
    assert len({run["probability_vector_digest"] for run in result["runs"]}) == 1
    forward = completed_pipeline.predict_batches(7, reverse=False)
    reverse = completed_pipeline.predict_batches(64, reverse=True)
    assert forward["probabilities_q"] == reverse["probabilities_q"]
    assert forward["log_loss_q"] == reverse["log_loss_q"]
    assert forward["brier_q"] == reverse["brier_q"]
    assert forward["accumulator"]["batches"] != reverse["accumulator"]["batches"]
    assert forward["accumulator"]["samples"] == reverse["accumulator"]["samples"] == 800


def test_duplicate_and_empty_batches_preserve_accumulator_state(completed_pipeline):
    dataset = completed_pipeline.dataset
    result = completed_pipeline.predict_batches(13)
    assert result["accumulator"]["samples"] == dataset.rows

    _, boosting = completed_pipeline.workspace.read_stage_payload("boosting")
    _, calibration = completed_pipeline.workspace.read_stage_payload("calibration")
    from gsb.batch import PredictionAccumulator
    from gsb.calibrators import Calibrator
    from gsb.tree import GBDTModel

    accumulator = PredictionAccumulator(completed_pipeline.config, Calibrator.from_payload(calibration["calibrator"]))
    model = GBDTModel.from_payload(boosting["model"])
    _, binned_payload = completed_pipeline.workspace.read_stage_payload("binning")
    from gsb.binning import binned_from_payload
    from gsb.tree import predict_model_raw

    _, binned = binned_from_payload(binned_payload)
    indices = np.arange(5)
    accumulator.add_raw_batch(
        predict_model_raw(model, binned[indices]),
        dataset.y[indices],
        dataset.sample_ids[indices],
        batch_size=5,
    )
    before = accumulator.summary()
    with pytest.raises(WorkbenchError) as duplicate:
        accumulator.add_raw_batch(
            predict_model_raw(model, binned[indices]),
            dataset.y[indices],
            dataset.sample_ids[indices],
            batch_size=5,
        )
    assert duplicate.value.code == "E_BATCH_STATE"
    assert accumulator.summary() == before
    with pytest.raises(WorkbenchError) as empty:
        accumulator.add_raw_batch(np.array([]), np.array([], dtype=np.uint8), [], batch_size=0)
    assert empty.value.code == "E_BATCH_STATE"
    assert accumulator.summary() == before


def test_numeric_errors_have_sample_position_and_stage():
    with pytest.raises(WorkbenchError) as raw_error:
        quantize_raw_scores(np.array([0.0, np.nan, 1.0]), 8, stage="boosting")
    assert raw_error.value.code == "E_NUMERIC"
    assert raw_error.value.details == {"sample_index": 1, "stage": "boosting"}

    with pytest.raises(WorkbenchError) as probability_error:
        quantize_probabilities(np.array([0.2, 1.2]), 8, stage="diagnostics")
    assert probability_error.value.code == "E_NUMERIC"
    assert probability_error.value.details["sample_index"] == 1
    assert probability_error.value.details["stage"] == "diagnostics"

    with pytest.raises(WorkbenchError) as table_error:
        reliability_table(np.array([10, 101]), np.array([0, 1], dtype=np.uint8), 5, 2)
    assert table_error.value.code == "E_NUMERIC"
    assert table_error.value.details["stage"] == "diagnostics"


def test_log_loss_can_handle_exact_one_for_positive_label():
    loss_q, brier_q = probability_terms(np.array([100]), np.array([1], dtype=np.uint8), 2, stage="diagnostics")
    assert loss_q.tolist() == [0]
    assert brier_q.tolist() == [0]
