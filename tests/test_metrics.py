import pytest
import numpy as np

from iqa import metrics
from iqa import normalize as stage1
from iqa.config import Config
from iqa.errors import BusinessError, E_NUMERIC, E_INVALID_CONFIG
from iqa import fusion as stage4


def _pixels():
    rng = np.random.default_rng(7)
    return rng.random((16, 16, 3))


def test_sharpness_bounds_and_fields():
    result = metrics.sharpness(_pixels())
    assert 0.0 <= result["score"] <= 1.0
    assert result["valid_pixels"] > 0
    assert result["skipped_reason"] is None


def test_structure_unavailable_without_reference_reason():
    pixels = _pixels()
    record = metrics.run(pixels, {"image_id": "x"}, "")
    assert record["product"]["structure"]["score"] is None
    assert "not applicable" in record["product"]["structure"]["skipped_reason"]


def test_structure_with_reference():
    pixels = _pixels()
    ref = _pixels() * 0.9
    result = metrics.structure_fidelity(pixels, ref)
    assert 0.0 <= result["score"] <= 1.0
    assert result["valid_pixels"] > 0


def test_color_modes_both_valid():
    pixels = _pixels()
    no_ref = metrics.color_no_reference(pixels)
    with_ref = metrics.color_with_reference(pixels, pixels.copy())
    assert 0.0 <= no_ref["score"] <= 1.0
    assert with_ref["histogram_distance"] == 0.0
    assert with_ref["score"] == 1.0


def test_all_zero_scores_raises_numeric():
    pixels = np.zeros((12, 12, 3), dtype=np.float64)
    metrics_record = metrics.run(pixels, {"image_id": "z"}, "")
    from iqa import semantic as stage3

    semantic_record = stage3.run(pixels, {"image_id": "z", "prompt": "black"})
    metrics_record["product"]["sharpness"]["score"] = 0.0
    metrics_record["product"]["color"]["score"] = 0.0
    semantic_record["product"]["score"] = 0.0
    with pytest.raises(BusinessError) as exc:
        stage4.fuse(metrics_record, semantic_record, Config())
    assert exc.value.code == E_NUMERIC
    assert exc.value.context["image_id"] == "z"


def test_negative_weight_invalid_config():
    config = Config(weights={"sharpness": -1.0, "color": 0.0, "structure": 0.0, "semantic": 0.0})
    with pytest.raises(BusinessError) as exc:
        config.validate()
    assert exc.value.code == E_INVALID_CONFIG
