from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest

from audioq.audio import evaluate_metrics, frame_audio, read_pcm_wav
from audioq.canonical import canonical_json_bytes
from audioq.errors import AudioQError
from audioq.models import FrameConfig, MetricsConfig


def test_metrics_are_byte_deterministic(workspace):
    root = workspace["root"]
    path = root / "demo/samples/clean_speech.wav"
    audio = read_pcm_wav(path, shard_id="clean")
    frame = frame_audio(audio, FrameConfig(), shard_id="clean")
    first = evaluate_metrics(audio, frame, MetricsConfig(), shard_id="clean")
    second = evaluate_metrics(audio, frame, MetricsConfig(), shard_id="clean")
    assert canonical_json_bytes(first) == canonical_json_bytes(second)
    assert set(first["scores"]) == {"snr", "spectral", "clipping"}
    assert all(value is not None and 0 <= value <= 5 for value in first["scores"].values())
    assert len(first["snr"]["frame_snr_db"]) == first["frame_count"]
    assert len(first["spectral"]["frame_distortion_db"]) == first["frame_count"]
    assert len(first["clipping"]["frame_clipping_ratios"]) == first["frame_count"]
    assert first["clipping"]["clipping_ratio"] == 0.0


def test_clipped_audio_reports_stable_error_without_zeroing_metrics(workspace):
    root = workspace["root"]
    audio = read_pcm_wav(root / "demo/samples/clipped_distortion.wav", shard_id="clipped")
    frame = frame_audio(audio, FrameConfig(), shard_id="clipped")
    metrics = evaluate_metrics(audio, frame, MetricsConfig(), shard_id="clipped")
    assert metrics["status"] == "error"
    assert metrics["scores"]["clipping"] == 0.0
    assert metrics["errors"][0]["code"] == "E_CLIPPED_AUDIO"
    assert metrics["errors"][0]["shard_id"] == "clipped"
    assert metrics["errors"][0]["observed"]["clipping_ratio"] > 0


def test_all_silent_and_too_short_are_empty_audio(workspace, tmp_path):
    root = workspace["root"]
    silent = read_pcm_wav(root / "demo/samples/all_silent.wav", shard_id="silent")
    with pytest.raises(AudioQError) as caught:
        frame_audio(silent, FrameConfig(), shard_id="silent")
    assert caught.value.code == "E_EMPTY_AUDIO"
    assert caught.value.shard_id == "silent"
    assert "silent" in caught.value.message

    import wave

    short_path = tmp_path / "short.wav"
    with wave.open(str(short_path), "wb") as wav:
        wav.setnchannels(1)
        wav.setsampwidth(2)
        wav.setframerate(16000)
        wav.writeframes(np.ones(100, dtype="<i2").tobytes())
    short_audio = read_pcm_wav(short_path, shard_id="short")
    with pytest.raises(AudioQError) as caught:
        frame_audio(short_audio, FrameConfig(length=400, hop=160), shard_id="short")
    assert caught.value.code == "E_EMPTY_AUDIO"
    assert caught.value.observed["frame_length"] == 400


def test_wav_format_error(tmp_path):
    bad = tmp_path / "bad.wav"
    bad.write_bytes(b"not a wav")
    with pytest.raises(AudioQError) as caught:
        read_pcm_wav(bad, shard_id="bad")
    assert caught.value.code == "E_AUDIO_FORMAT"
    assert caught.value.to_dict()["ok"] is False


def test_zero_byte_audio_is_empty_audio(tmp_path):
    empty = tmp_path / "empty.wav"
    empty.write_bytes(b"")
    with pytest.raises(AudioQError) as caught:
        read_pcm_wav(empty, shard_id="empty-bytes")
    assert caught.value.code == "E_EMPTY_AUDIO"
    assert caught.value.shard_id == "empty-bytes"
