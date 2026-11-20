from __future__ import annotations

import math
from pathlib import Path

import pytest
import numpy as np
from scipy.io import wavfile

from dialogue_engine.config import ClassifierConfig, FeatureConfig
from dialogue_engine.emotion import score_features
from dialogue_engine.emotion import validate_emotion_artifact
from dialogue_engine.engine import DialogueEngine
from dialogue_engine.errors import (
    E_EMPTY_TURN,
    E_FEATURE_DIM,
    E_INPUT_LENGTH,
    E_INVALID_STAGE,
    E_INVALID_TURN,
    E_NUMERIC,
    EngineError,
)
from dialogue_engine.features import extract_turn_features
from dialogue_engine.manifest import load_dataset

from helpers import clone_data, mutate_manifest


def _assert_code(exc: pytest.ExceptionInfo[EngineError], code: str) -> None:
    assert exc.value.code == code
    assert exc.value.to_dict()["code"] == code


def test_declared_sample_count_mismatch(demo_data: Path, tmp_path: Path) -> None:
    data = clone_data(demo_data, tmp_path / "bad_length")

    def mutate(manifest: object) -> None:
        assert isinstance(manifest, dict)
        manifest["shards"][0]["sample_count"] += 1

    mutate_manifest(data, mutate)
    with pytest.raises(EngineError) as exc:
        load_dataset(data)
    _assert_code(exc, E_INPUT_LENGTH)


def test_overlapping_turn_boundary(demo_data: Path, tmp_path: Path) -> None:
    data = clone_data(demo_data, tmp_path / "bad_turn")

    def mutate(manifest: object) -> None:
        assert isinstance(manifest, dict)
        first = manifest["shards"][0]["turns"][0]
        second = manifest["shards"][0]["turns"][1]
        second["start_ms"] = first["end_ms"] - 1

    mutate_manifest(data, mutate)
    with pytest.raises(EngineError) as exc:
        DialogueEngine(tmp_path / "bad_turn_ws").run(data)
    _assert_code(exc, E_INVALID_TURN)


def test_empty_turn_returns_business_error(demo_data: Path, tmp_path: Path) -> None:
    data = clone_data(demo_data, tmp_path / "empty_turn")

    def mutate(manifest: object) -> None:
        assert isinstance(manifest, dict)
        turn = manifest["shards"][0]["turns"][0]
        turn["end_ms"] = turn["start_ms"] + 5

    mutate_manifest(data, mutate)
    with pytest.raises(EngineError) as exc:
        DialogueEngine(tmp_path / "empty_turn_ws").run(data)
    _assert_code(exc, E_EMPTY_TURN)
    assert exc.value.stage == "features"
    assert exc.value.turn_index == 0


def test_digital_silence_turn_is_empty(demo_data: Path, tmp_path: Path) -> None:
    data = clone_data(demo_data, tmp_path / "silent_turn")
    dataset = load_dataset(data)
    shard = dataset.shards[0]
    turn = shard.turns[0]
    sample_rate, samples = wavfile.read(shard.wav_path)
    samples = np.asarray(samples, dtype=np.int16).copy()
    samples[turn.start_sample : turn.end_sample] = 0
    wavfile.write(shard.wav_path, sample_rate, samples)
    with pytest.raises(EngineError) as exc:
        DialogueEngine(tmp_path / "silent_turn_ws").run(data)
    _assert_code(exc, E_EMPTY_TURN)


def test_feature_dimension_mismatch(demo_data: Path) -> None:
    dataset = load_dataset(demo_data)
    shard = dataset.shards[0]
    turn = shard.turns[0]
    feature_config = FeatureConfig()
    artifact = extract_turn_features(shard, turn, feature_config)
    artifact["vector"] = artifact["vector"][:-1]
    with pytest.raises(EngineError) as exc:
        score_features(
            artifact,
            feature_config=feature_config,
            classifier_config=ClassifierConfig(),
            shard_id=shard.shard_id,
            turn_index=turn.index,
        )
    _assert_code(exc, E_FEATURE_DIM)
    assert exc.value.stage == "features"


def test_numeric_error_is_not_silently_skipped(demo_data: Path) -> None:
    dataset = load_dataset(demo_data)
    shard = dataset.shards[0]
    turn = shard.turns[0]
    feature_config = FeatureConfig()
    artifact = extract_turn_features(shard, turn, feature_config)
    artifact["vector"][0] = math.nan
    with pytest.raises(EngineError) as exc:
        score_features(
            artifact,
            feature_config=feature_config,
            classifier_config=ClassifierConfig(),
            shard_id=shard.shard_id,
            turn_index=turn.index,
        )
    _assert_code(exc, E_NUMERIC)
    assert exc.value.stage == "features"


def test_invalid_stage_and_missing_prerequisite(demo_data: Path, tmp_path: Path) -> None:
    engine = DialogueEngine(tmp_path / "empty_workspace")
    engine.import_dataset(demo_data)
    with pytest.raises(EngineError) as exc:
        engine.query_stage("shard_01", 0, "emotion")
    _assert_code(exc, E_INVALID_STAGE)
    with pytest.raises(EngineError) as exc2:
        engine.query_stage("shard_01", 0, "not-a-stage")
    _assert_code(exc2, E_INVALID_STAGE)


def test_non_finite_emotion_scores_return_numeric_error() -> None:
    with pytest.raises(EngineError) as exc:
        validate_emotion_artifact(
            {
                "turn_index": 0,
                "scores": {
                    "平静": math.nan,
                    "高兴": 0.0,
                    "愤怒": 0.0,
                    "悲伤": 0.0,
                },
            },
            shard_id="shard_01",
            turn_index=0,
        )
    _assert_code(exc, E_NUMERIC)
    assert exc.value.stage == "emotion"

