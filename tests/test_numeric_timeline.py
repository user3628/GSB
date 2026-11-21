from __future__ import annotations

import copy
import json
import math
from pathlib import Path

import numpy as np
import pytest

from tonelab.config import Config
from tonelab.errors import ToneLabError
from tonelab.features import extract_features
from tonelab.formats import load_manifest
from tonelab.modeling import build_articulation
from tonelab.quantization import quantize_shard
from tonelab.util import canonical_json_bytes
from tonelab.validation import validate_shard


@pytest.mark.parametrize(
    "name,code",
    [("anomaly_timeline.json", "E_TIMELINE_INVALID"), ("anomaly_numeric.json", "E_NUMERIC")],
)
def test_anomaly_manifests_return_required_codes(name: str, code: str, demo_data: Path) -> None:
    with pytest.raises(ToneLabError) as captured:
        load_manifest(demo_data / name, Config())
    error = captured.value
    assert error.code == code
    assert error.shard_id
    assert error.position is not None
    if code == "E_TIMELINE_INVALID":
        assert error.unit_index == 0


def test_timeline_negative_reversed_and_overflow(demo_data: Path) -> None:
    config = Config()
    project = load_manifest(
        demo_data / "manifest_v2.json",
        config,
    )
    shard = copy.deepcopy(project.shards[0])
    shard.units[0].start_ms = -1.0
    with pytest.raises(ToneLabError) as negative:
        validate_shard(shard, config)
    assert negative.value.code == "E_TIMELINE_INVALID"
    assert negative.value.unit_index == 0
    shard = copy.deepcopy(project.shards[0])
    shard.units[0].start_ms = 100.0
    shard.units[0].end_ms = 90.0
    with pytest.raises(ToneLabError) as reversed_interval:
        validate_shard(shard, config)
    assert reversed_interval.value.code == "E_TIMELINE_INVALID"
    shard = copy.deepcopy(project.shards[0])
    shard.units[-1].end_ms = shard.duration_ms + 1.0
    with pytest.raises(ToneLabError) as overflow:
        validate_shard(shard, config)
    assert overflow.value.code == "E_TIMELINE_INVALID"
    assert overflow.value.context["overflow_ms"] > 0


def test_pitch_nan_inf_and_range(demo_data: Path) -> None:
    config = Config()
    project = load_manifest(
        demo_data / "manifest_v2.json",
        config,
    )
    for bad in (math.nan, math.inf, 9.0):
        shard = copy.deepcopy(project.shards[0])
        shard.pitch_curve[1] = (shard.pitch_curve[1][0], bad)
        with pytest.raises(ToneLabError) as captured:
            validate_shard(shard, config)
        assert captured.value.code == "E_NUMERIC"
        assert captured.value.position["kind"] == "pitch_point"
        assert captured.value.position["index"] == 1


def test_feature_model_quantize_are_deterministic_and_bounded(demo_data: Path) -> None:
    config = Config()
    project = load_manifest(demo_data / "manifest_v2.json", config)
    shard = project.shards[0]
    features_a = extract_features(shard, config)
    features_b = extract_features(shard, config)
    assert canonical_json_bytes(features_a) == canonical_json_bytes(features_b)
    assert features_a["truncated_frames"] > 0
    assert all(frame["end_sample"] <= shard.sample_count for frame in features_a["frames"])
    articulation_a = build_articulation(shard, features_a, config)
    articulation_b = build_articulation(shard, features_b, config)
    assert canonical_json_bytes(articulation_a) == canonical_json_bytes(articulation_b)
    quantized_a = quantize_shard(shard, articulation_a, config)
    quantized_b = quantize_shard(shard, articulation_b, config)
    assert canonical_json_bytes(quantized_a) == canonical_json_bytes(quantized_b)
    for section in ("pitch_curve", "formant_trajectory", "f0_trajectory"):
        part = quantized_a[section]
        assert part["max_observed_error"] <= part["error_bound"] + 1e-15
        assert part["error_bound"] > 0
    assert quantized_a["per_shard_scales"]["pitch_scale"] == config.get("quantization.pitch_scale")
    assert quantized_a["per_shard_scales"]["formant_scale"] == config.get("quantization.formant_scale")

