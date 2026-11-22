from __future__ import annotations

import json
import copy
from pathlib import Path

import numpy as np
import pytest
import soundfile as sf
import yaml

from sonic_sep.config import validate_config_data
from sonic_sep.errors import AppError
from sonic_sep.features import extract_features, validate_feature_matrix
from sonic_sep.generate import generate_dataset
from sonic_sep.manifest import load_manifest, load_manifest_object, load_slice_audio
from sonic_sep.masks import initialize_masks, mask_digest, source_priors
from sonic_sep.stft import compute_stft, frame_count
from sonic_sep.util import digest_array


def test_generated_wav_manifest_is_consistent_and_repeatable(tmp_path: Path) -> None:
    first = generate_dataset(tmp_path / "samples", seed=20261123, durations=(2.0, 2.2, 2.4))
    first_hashes = [slice_item["sha256"] for slice_item in json.loads(first.read_text())["slices"]]
    second = generate_dataset(tmp_path / "samples", seed=20261123, durations=(2.0, 2.2, 2.4))
    second_hashes = [slice_item["sha256"] for slice_item in json.loads(second.read_text())["slices"]]
    assert first_hashes == second_hashes

    manifest = load_manifest(first)
    assert len(manifest.slices) == 3
    assert manifest.total_samples == sum(item.n_samples for item in manifest.slices)
    for item in manifest.slices:
        info = sf.info(item.path)
        assert info.samplerate == 16_000
        assert info.channels == 1
        assert info.subtype == "PCM_16"
        assert 2.0 <= item.duration_seconds <= 4.0
        assert 2 <= item.source_count <= 3
        audio = load_slice_audio(item)
        assert audio.dtype == np.int16
        assert audio.size == item.n_samples


def test_stft_is_byte_deterministic_and_marks_zero_padding(dataset_root: Path) -> None:
    manifest = load_manifest(dataset_root / "samples" / "manifest.json")
    truth = manifest.get("slice-001")
    audio = load_slice_audio(truth)
    config = validate_config_data(yaml.safe_load((dataset_root / "configs" / "default.yaml").read_text()))
    first = compute_stft(audio, config, slice_id=truth.slice_id)
    second = compute_stft(audio, config, slice_id=truth.slice_id)
    assert first.digest == second.digest
    assert np.array_equal(first.spectra, second.spectra)
    assert first.spectra.shape[0] == frame_count(truth.n_samples, config.hop_length)
    assert first.spectra.shape[1] == config.frame_length // 2 + 1
    assert bool(first.padded[-1]) is True
    assert int(first.valid_lengths[-1]) < config.frame_length
    assert first.summary()["padded_frame_count"] > 0
    assert np.array_equal(first.frame_starts, np.arange(first.spectra.shape[0]) * config.hop_length)


@pytest.mark.parametrize(
    ("overrides", "param"),
    [
        ({"frame_length": 0}, "frame_length"),
        ({"hop_length": 0}, "hop_length"),
        ({"frame_length": 128, "hop_length": 256}, "hop_length"),
    ],
)
def test_invalid_stft_config_errors_include_slice_and_param(overrides: dict, param: str) -> None:
    data = yaml.safe_load(Path("configs/default.yaml").read_text())
    with pytest.raises(AppError) as caught:
        validate_config_data({**data, **overrides}, slice_id="slice-001")
    assert caught.value.code == "E_INVALID_CONFIG"
    assert caught.value.details["slice_id"] == "slice-001"
    assert caught.value.details["param"] == param


def test_numeric_errors_include_band_and_frame(dataset_root: Path) -> None:
    manifest = load_manifest(dataset_root / "samples" / "manifest.json")
    truth = manifest.get("slice-001")
    audio = load_slice_audio(truth)
    config = validate_config_data(yaml.safe_load((dataset_root / "configs" / "default.yaml").read_text()))
    stft = compute_stft(audio, config, slice_id=truth.slice_id)
    features = extract_features(stft.spectra, config, slice_id=truth.slice_id)
    broken = np.array(features.features, copy=True)
    broken[37, 5, 1] = np.nan
    with pytest.raises(AppError) as caught:
        validate_feature_matrix(broken, config.band_count, slice_id=truth.slice_id)
    assert caught.value.code == "E_NUMERIC"
    assert caught.value.details["band_index"] == 5
    assert caught.value.details["frame_index"] == 37

    with pytest.raises(AppError) as caught_shape:
        extract_features(stft.spectra[:, :-1], config, slice_id=truth.slice_id)
    assert caught_shape.value.code == "E_NUMERIC"
    assert caught_shape.value.details["band_index"] == 0
    assert caught_shape.value.details["frame_index"] == 0

    bad_sources = [type(truth.sources[0])(0, 300.0, 200.0, True)]
    with pytest.raises(AppError) as caught_interval:
        source_priors(bad_sources, config)
    assert caught_interval.value.code == "E_NUMERIC"

    raw = copy.deepcopy(manifest.raw)
    del raw["slices"][0]["sources"][0]["f0_min_hz"]
    with pytest.raises(AppError) as caught_empty:
        load_manifest_object(raw, base_dir=(dataset_root / "samples"), source_path=(dataset_root / "samples" / "manifest.json"))
    assert caught_empty.value.code == "E_NUMERIC"
    assert caught_empty.value.details["slice_id"] == "slice-001"
    assert "band_index" in caught_empty.value.details and "frame_index" in caught_empty.value.details


def test_masks_normalize_and_have_finite_values(dataset_root: Path) -> None:
    manifest = load_manifest(dataset_root / "samples" / "manifest.json")
    truth = manifest.get("slice-001")
    audio = load_slice_audio(truth)
    config = validate_config_data(yaml.safe_load((dataset_root / "configs" / "default.yaml").read_text()))
    stft = compute_stft(audio, config, slice_id=truth.slice_id)
    features = extract_features(stft.spectra, config, slice_id=truth.slice_id)
    masks = initialize_masks(features, truth.sources, config, slice_id=truth.slice_id)
    assert masks.masks.shape == (truth.source_count, stft.spectra.shape[0], config.band_count)
    assert np.all(np.isfinite(masks.masks))
    assert np.allclose(np.sum(masks.masks, axis=0), 1.0, atol=1e-12)
    assert masks.digest == mask_digest(masks.masks)

