from __future__ import annotations

import numpy as np
import pytest

from vadkit import VADConfig, VadError, detect_samples, iter_frames, score_frame


def test_invalid_thresholds_fail_before_session_starts() -> None:
    bad = VADConfig(high_threshold=0.2, low_threshold=0.7)
    with pytest.raises(VadError) as caught:
        bad.validate()
    assert caught.value.code == "E_INVALID_CONFIG"
    assert caught.value.details["high_threshold"] == 0.2


def test_negative_minimum_segment_fails_configuration() -> None:
    with pytest.raises(VadError) as caught:
        VADConfig(min_speech_ms=-1).validate()
    assert caught.value.code == "E_INVALID_CONFIG"
    assert caught.value.details["min_speech_ms"] == -1


def test_short_tail_frame_is_marked(dataset) -> None:
    chunk = dataset.chunks[0]
    config = VADConfig()
    frames = list(iter_frames(chunk.samples, config))
    assert frames[-1].is_short
    assert frames[-1].length == chunk.sample_count - frames[-1].start_sample
    assert frames[-1].start_sample + frames[-1].length == chunk.sample_count
    result = detect_samples(chunk.samples, config, chunk_id=chunk.chunk_id)
    assert result.truncated_frame_count == 1
    assert result.frames[-1].is_short


def test_dual_feature_score_is_weighted_and_normalized(dataset) -> None:
    chunk = dataset.chunks[0]
    config = VADConfig()
    frame = next(iter_frames(chunk.samples, config))
    feature = score_frame(frame, config)
    expected = (
        config.energy_weight * feature.normalized_energy
        + config.flatness_weight * feature.normalized_flatness
    )
    assert abs(feature.weighted_score - expected) < 1e-8
    assert 0.0 <= feature.weighted_score <= 1.0
    assert feature.normalized_flatness == pytest.approx(1.0 - feature.spectral_flatness, abs=1e-8)
    assert feature.dimensions == 2


def test_feature_dimension_mismatch_is_stable(dataset) -> None:
    chunk = dataset.chunks[0]
    with pytest.raises(VadError) as caught:
        detect_samples(
            chunk.samples,
            VADConfig(),
            chunk_id=chunk.chunk_id,
            inject_feature_dimensions=4,
        )
    error = caught.value
    assert error.code == "E_FEATURE_MISMATCH"
    assert error.chunk_id == chunk.chunk_id
    assert error.frame_index == 0
    assert error.sample_position == 0


def test_all_zero_input_raises_instead_of_empty_endpoints() -> None:
    with pytest.raises(VadError) as caught:
        detect_samples(np.zeros(8_000, dtype=np.int16), VADConfig(), chunk_id="zero")
    assert caught.value.code == "E_SILENT_INPUT"
    assert caught.value.chunk_id == "zero"
    assert caught.value.sample_position == 0

