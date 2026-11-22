from __future__ import annotations

import math
from dataclasses import replace

import pytest

from mmcoop.canonical import canonical_bytes
from mmcoop.errors import AppError
from mmcoop.injection import Injection
from mmcoop.stages import (
    compute_fusion_scoring,
    compute_projection_alignment,
    compute_text_encoding,
    hash_embedding,
    normalize_text,
    tokenize_offsets,
)

from .conftest import clone_dataset, config_with


def test_normalization_tokens_and_fixed_hash_are_deterministic(service, demo_dataset):
    config = service._load()[0]
    assert normalize_text("  ＴＥＳＴ  Mixed　中文！！！\t") == "test mixed 中文!!!"
    normalized = normalize_text("多模态 fusion checkpoint")
    tokens = tokenize_offsets(normalized)
    assert tokens
    for token in tokens:
        assert normalized[token["start"] : token["end"]] == token["token"]
    embedding = hash_embedding(tokens, config.hash_salt)
    assert len(embedding) == 64
    assert math.isclose(math.sqrt(sum(value * value for value in embedding)), 1.0, abs_tol=1e-9)
    first = compute_text_encoding(demo_dataset, config)
    second = compute_text_encoding(demo_dataset, config)
    assert canonical_bytes(first) == canonical_bytes(second)
    assert all(record["embedding"] for record in first["records"])
    assert any(record["tokens"] for record in first["records"])


@pytest.mark.parametrize("text", ["", "   ", "!!!？？！！", "……—"])
def test_empty_or_punctuation_only_text_is_rejected(service, demo_dataset, text):
    config = service._load()[0]
    dataset = clone_dataset(
        demo_dataset,
        texts=[{"text_id": "empty", "text": text, "lang": "zh-en", "source_channel": "test"}],
        images=[],
    )
    with pytest.raises(AppError) as captured:
        compute_text_encoding(dataset, config)
    assert captured.value.code == "E_EMPTY_INPUT"
    assert captured.value.stage == "text_encoding"
    assert captured.value.sample_id == "empty"
    assert captured.value.field == "text"


def test_zero_embedding_is_rejected_with_field_name(service, demo_dataset, monkeypatch):
    config = service._load()[0]
    monkeypatch.setattr("mmcoop.stages._token_hash_vector", lambda token, salt: [0.0] * 64)
    with pytest.raises(AppError) as captured:
        compute_text_encoding(demo_dataset, config)
    assert captured.value.code == "E_EMPTY_INPUT"
    assert captured.value.field == "embedding"
    assert captured.value.sample_id in {"t01", "t02", "t03", "t04", "t05"}


def test_text_length_limit_has_stable_code(service, demo_dataset):
    config = service._load()[0]
    dataset = clone_dataset(
        demo_dataset,
        texts=[
            {
                "text_id": "long",
                "text": "a" * (config.max_chars + 1),
                "lang": "en",
                "source_channel": "test",
            }
        ],
        images=[],
    )
    with pytest.raises(AppError) as captured:
        compute_text_encoding(dataset, config)
    assert captured.value.code == "E_TEXT_TOO_LONG"
    assert captured.value.sample_id == "long"
    assert captured.value.diagnostics["length"] == config.max_chars + 1

    with pytest.raises(AppError) as injected:
        compute_text_encoding(demo_dataset, config, injection=Injection("text_too_long", "t01"))
    assert injected.value.code == "E_TEXT_TOO_LONG"
    assert injected.value.diagnostics["injected"] is True


def test_projection_missing_reference_and_dimension_errors(service, demo_dataset):
    config = service._load()[0]
    text_artifact = compute_text_encoding(demo_dataset, config)

    broken_images = [dict(record) for record in demo_dataset.images]
    broken_images[0]["text_id"] = "does-not-exist"
    missing_dataset = clone_dataset(demo_dataset, images=broken_images)
    with pytest.raises(AppError) as missing:
        compute_projection_alignment(text_artifact, missing_dataset, config)
    assert missing.value.code == "E_MISSING_REF"
    assert missing.value.stage == "projection_alignment"
    assert missing.value.sample_id == "i01"
    assert missing.value.diagnostics["missing_text_id"] == "does-not-exist"
    assert "t01" in missing.value.diagnostics["available_text_ids"]

    short_feature_images = [dict(record) for record in demo_dataset.images]
    short_feature_images[0]["feature"] = short_feature_images[0]["feature"][:-1]
    short_dataset = clone_dataset(demo_dataset, images=short_feature_images)
    with pytest.raises(AppError) as dimension:
        compute_projection_alignment(text_artifact, short_dataset, config)
    assert dimension.value.code == "E_PROJECTION_INVALID"
    assert dimension.value.sample_id == "i01"
    assert dimension.value.diagnostics["original_dim"] == 63
    assert dimension.value.diagnostics["expected_dim"] == 64

    with pytest.raises(AppError) as injected:
        compute_projection_alignment(
            text_artifact,
            demo_dataset,
            config,
            injection=Injection("projection_dim", "i01"),
        )
    assert injected.value.code == "E_PROJECTION_INVALID"
    assert injected.value.diagnostics["injected"] is True
    assert injected.value.diagnostics["matrix_shape"] == [64, 64]


def test_projection_nan_injection_is_numeric_error(service, demo_dataset):
    config = service._load()[0]
    text_artifact = compute_text_encoding(demo_dataset, config)
    with pytest.raises(AppError) as captured:
        compute_projection_alignment(
            text_artifact,
            demo_dataset,
            config,
            injection=Injection("similarity_nan", "i01"),
        )
    assert captured.value.code == "E_NUMERIC"
    assert captured.value.stage == "projection_alignment"
    assert captured.value.sample_id == "i01"
    assert captured.value.diagnostics["similarity"] == "NaN"


def test_fusion_contributions_renormalization_and_weight_zero(service, demo_dataset):
    config = service._load()[0]
    text_artifact = compute_text_encoding(demo_dataset, config)
    projection_artifact = compute_projection_alignment(text_artifact, demo_dataset, config)
    fusion = compute_fusion_scoring(projection_artifact, demo_dataset, config)
    assert fusion["summary"]["verdict_counts"] == {"conflict": 1, "consistent": 4, "weak": 1}
    for record in fusion["records"]:
        assert set(record["log_contributions"]) == {
            "similarity",
            "operator_confidence",
            "language_consistency",
        }
        assert math.isclose(sum(record["weights"]["normalized"].values()), 1.0, abs_tol=1e-9)
        assert record["weights"]["original"] == config.weights

    skipped_config = config_with(
        config.path,
        lambda raw: raw["fusion"]["weights"].__setitem__("operator_confidence", 0.0),
    )
    skipped = compute_fusion_scoring(projection_artifact, demo_dataset, skipped_config)
    first = skipped["records"][0]
    assert first["weights"]["active_sum"] == 0.75
    assert first["weights"]["renormalized"] is True
    assert any(item["reason"] == "zero_weight" for item in first["weights"]["skipped"])
    assert "operator_confidence" not in first["weights"]["normalized"]

    zero_config = config_with(
        config.path,
        lambda raw: raw["fusion"].__setitem__("weights", {"similarity": 0, "operator_confidence": 0, "language_consistency": 0}),
    )
    with pytest.raises(AppError) as captured:
        compute_fusion_scoring(projection_artifact, demo_dataset, zero_config)
    assert captured.value.code == "E_WEIGHT_ZERO"
    assert captured.value.stage == "fusion_scoring"
    assert captured.value.sample_id == "i01"
    assert "skipped_components" in captured.value.diagnostics


def test_invalid_operator_is_stable_error(service, demo_dataset):
    config = service._load()[0]
    text_artifact = compute_text_encoding(demo_dataset, config)
    projection_artifact = compute_projection_alignment(text_artifact, demo_dataset, config)
    with pytest.raises(AppError) as captured:
        compute_fusion_scoring(
            projection_artifact,
            demo_dataset,
            config,
            injection=Injection("invalid_operator", "i01"),
        )
    assert captured.value.code == "E_INVALID_OPERATOR"
    assert captured.value.stage == "fusion_scoring"
    assert captured.value.sample_id == "i01"
    assert captured.value.diagnostics["injected"] is True
