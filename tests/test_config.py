from __future__ import annotations

import copy

import pytest

from audioq.errors import AudioQError
from audioq.models import (
    EvaluationConfig,
    FusionConfig,
    ReferenceConfig,
    decode_config,
    encode_config,
    load_config_file,
)


def test_v1_import_derives_threshold_and_normalises_percentages(workspace):
    root = workspace["root"]
    config = load_config_file(
        root / workspace["legacy_config"],
        normalize_policy="normalize",
        snr_threshold_rule="fixed",
        default_snr_threshold_db=7.25,
    )
    assert config.schema_version == 1
    assert config.metrics.snr_threshold_db == 7.25
    assert config.weights == {"snr": 0.4, "spectral": 0.35, "clipping": 0.25}
    assert config.compatibility["snr_threshold_source"] == "fixed"


def test_v1_weight_scaled_threshold_rule(workspace):
    root = workspace["root"]
    config = load_config_file(root / workspace["legacy_config"], snr_threshold_rule="weight_scaled")
    assert config.metrics.snr_threshold_db == pytest.approx(8.0)


def test_v1_export_records_downgrade_and_restores_exact_fields(workspace):
    root = workspace["root"]
    original = load_config_file(root / workspace["legacy_config"])
    exported = encode_config(original, 1)
    assert exported["weights_percent"] == {"snr": 40, "spectral": 35, "clipping": 25}
    assert exported["downgrade_items"] == ["metrics.snr_threshold_db"]
    assert exported["downgrade_restore"]["metrics.snr_threshold_db"] == 8.0
    assert "snr_threshold_db" not in exported["metrics"]
    restored = decode_config(exported, default_snr_threshold_db=99.0)
    assert restored.semantic_dict() == original.semantic_dict()


def test_v1_to_v2_roundtrip_preserves_semantics(workspace):
    root = workspace["root"]
    original = load_config_file(root / workspace["legacy_config"])
    v2 = encode_config(original, 2)
    decoded = decode_config(v2)
    assert decoded.schema_version == 2
    for key in ("evaluation_id", "frame", "metrics", "weights", "fusion", "default_reference"):
        assert decoded.semantic_dict()[key] == original.semantic_dict()[key]


def test_v1_non_integer_precision_is_explicitly_restored():
    config = EvaluationConfig(
        evaluation_id="precise",
        weights={"snr": 1 / 3, "spectral": 1 / 3, "clipping": 1 / 3},
        fusion=FusionConfig(decimals=12),
        default_reference=ReferenceConfig(0.0, 5.0, 4.0),
        schema_version=1,
    )
    exported = config.to_v1_dict()
    assert sum(exported["weights_percent"].values()) == 100
    assert "weights" in exported["downgrade_restore"]
    assert "weights.integer_percent_precision" in exported["downgrade_items"]
    restored = decode_config(exported)
    assert restored.weights == config.weights


def test_invalid_weight_and_percentage_sum_errors(workspace):
    root = workspace["root"]
    raw = copy.deepcopy(__import__("json").loads((root / workspace["legacy_config"]).read_text()))
    raw["weights_percent"] = {"snr": 0, "spectral": 0, "clipping": 0}
    with pytest.raises(AudioQError) as caught:
        decode_config(raw)
    assert caught.value.code == "E_INVALID_WEIGHT"

    raw = copy.deepcopy(__import__("json").loads((root / workspace["legacy_config"]).read_text()))
    raw["weights_percent"] = {"snr": 40, "spectral": 35, "clipping": 24}
    with pytest.raises(AudioQError) as caught:
        decode_config(raw, normalize_policy="strict_100")
    assert caught.value.code == "E_INVALID_WEIGHT"


def test_weight_normalisation_from_non_100_percent_source():
    raw = {
        "schema_version": 1,
        "evaluation_id": "odd-percent",
        "frame": {"length": 400, "hop": 160, "window": "hann"},
        "weights_percent": {"snr": 20, "spectral": 20, "clipping": 10},
        "metrics": {},
        "fusion": {},
        "default_reference": {"min_mos": 0, "max_mos": 5, "target_mos": 4},
    }
    config = decode_config(raw)
    assert config.weights == {"snr": 0.4, "spectral": 0.4, "clipping": 0.2}
    assert config.compatibility["percentage_sum"] == 50


def test_reference_target_defaults_to_interval_midpoint(workspace):
    root = workspace["root"]
    raw = copy.deepcopy(__import__("json").loads((root / workspace["config"]).read_text()))
    raw["default_reference"] = {"min_mos": 1.0, "max_mos": 3.0}
    config = decode_config(raw)
    assert config.default_reference.target_mos == 2.0
