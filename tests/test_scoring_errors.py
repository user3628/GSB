from __future__ import annotations

import numpy as np
import pytest

from tracklab.appearance import model_from_region
from tracklab.errors import E_NUMERIC, WorkbenchError
from tracklab.manifest import validate_manifest
from tracklab.scoring import bhattacharyya, build_candidate_grid, score_candidates


def test_search_window_outside_frame_returns_numeric_with_grid(workspace):
    manifest = validate_manifest(workspace, verify_frame_files=True)
    sequence = dict(manifest.sequences[0])
    sequence["_current_frame_index"] = 3
    with pytest.raises(WorkbenchError) as error:
        build_candidate_grid([1.5, 0.2, 0.1, 0.1], sequence, workspace)
    assert error.value.code == E_NUMERIC
    assert error.value.context["sequence_id"] == sequence["sequence_id"]
    assert error.value.context["frame_index"] == 3
    assert error.value.context["diagnostic"] == "search_window_outside_frame"
    assert "search_window_box_xywh_norm" in error.value.context


def test_zero_pixel_frame_returns_numeric_with_grid(workspace):
    manifest = validate_manifest(workspace, verify_frame_files=True)
    sequence = dict(manifest.sequences[0])
    sequence["_current_frame_index"] = 2
    predicted = list(sequence["initial_box"])
    candidates, grid = build_candidate_grid(predicted, sequence, workspace)
    frame = np.zeros(tuple(sequence["image_shape"]), dtype=np.uint8)
    template = {
        "gray_histogram": [0.25] * 16,
        "gradient_histogram": [0.125] * 8,
        "spatial_signature": [0.0] * 64,
        "model_digest": "seed",
    }
    with pytest.raises(WorkbenchError) as error:
        score_candidates(frame, template, candidates, predicted, grid, sequence, workspace)
    assert error.value.code == E_NUMERIC
    assert error.value.context["diagnostic"] == "all_candidates_invalid_regions"
    assert error.value.context["sequence_id"] == sequence["sequence_id"]


def test_non_finite_histogram_returns_numeric():
    with pytest.raises(WorkbenchError) as error:
        bhattacharyya([float("nan"), 1.0], [0.5, 0.5])
    assert error.value.code == E_NUMERIC
    assert error.value.context["diagnostic"] == "non_finite_histogram"
