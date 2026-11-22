from __future__ import annotations

from types import SimpleNamespace

import pytest

from audioq.errors import AudioQError
from audioq.fusion import fuse_metrics
from audioq.models import EvaluationConfig, FusionConfig, ReferenceConfig


def _config(domain="log", weights=None):
    return EvaluationConfig(
        evaluation_id="fusion-test",
        weights=weights or {"snr": 0.4, "spectral": 0.35, "clipping": 0.25},
        fusion=FusionConfig(domain=domain, epsilon=1e-6, decimals=12, high_precision=50),
        default_reference=ReferenceConfig(0.0, 5.0, 4.0),
    )


def test_fusion_is_deterministic_and_has_precision_evidence():
    metrics = {"scores": {"snr": 4.0, "spectral": 3.0, "clipping": 5.0}}
    first = fuse_metrics(metrics, _config(), ReferenceConfig(2.0, 5.0, 4.0), shard_id="s")
    second = fuse_metrics(metrics, _config(), ReferenceConfig(2.0, 5.0, 4.0), shard_id="s")
    assert first == second
    assert 3.0 < first["fused_score"] < 5.0
    assert set(first["contributions"]) == {"snr", "spectral", "clipping"}
    assert first["weighting"]["renormalised"] is False
    assert first["precision"]["error_bound"] != ""
    assert first["precision"]["comparison"].startswith("abs(")
    assert first["precision"]["effective_digits"] >= 1


def test_skipped_metric_is_renormalised_and_observable():
    metrics = {"scores": {"snr": 4.0, "spectral": None, "clipping": 2.0}}
    result = fuse_metrics(metrics, _config(), ReferenceConfig(0.0, 5.0, 4.0), shard_id="s")
    assert result["skipped_metrics"][0]["metric"] == "spectral"
    assert result["weighting"]["renormalised"] is True
    assert result["weighting"]["normalised_available_weights"]["snr"] == pytest.approx(0.4 / 0.65)
    assert "spectral" not in result["contributions"]


def test_all_unavailable_and_zero_weights_are_invalid_weight():
    with pytest.raises(AudioQError) as caught:
        fuse_metrics(
            {"scores": {"snr": None, "spectral": None, "clipping": None}},
            _config(),
            ReferenceConfig(0.0, 5.0, 4.0),
            shard_id="empty",
        )
    assert caught.value.code == "E_INVALID_WEIGHT"
    assert caught.value.shard_id == "empty"

    zero_config = SimpleNamespace(
        weights={"snr": 0.0, "spectral": 0.0, "clipping": 0.0},
        fusion=FusionConfig(),
    )
    with pytest.raises(AudioQError) as caught:
        fuse_metrics(
            {"scores": {"snr": 4.0, "spectral": 4.0, "clipping": 4.0}},
            zero_config,
            ReferenceConfig(0.0, 5.0, 4.0),
        )
    assert caught.value.code == "E_INVALID_WEIGHT"


def test_nan_metric_is_numeric_error():
    with pytest.raises(AudioQError) as caught:
        fuse_metrics(
            {"scores": {"snr": float("nan"), "spectral": 4.0, "clipping": 4.0}},
            _config(),
            ReferenceConfig(0.0, 5.0, 4.0),
            shard_id="nan",
        )
    assert caught.value.code == "E_NUMERIC"
    assert caught.value.shard_id == "nan"
    assert "nan" in caught.value.observed["snr_score"].lower()


def test_out_of_range_deviation_is_reported():
    result = fuse_metrics(
        {"scores": {"snr": 1.0, "spectral": 1.0, "clipping": 1.0}},
        _config("normalized"),
        ReferenceConfig(3.0, 5.0, 4.0),
        shard_id="poor",
    )
    assert result["reference"]["out_of_range"] is True
    assert result["reference"]["position"] == "below"
    assert result["reference"]["deviation"] == pytest.approx(2.0)
    assert result["grade"] == "poor"
