from __future__ import annotations

import pytest

from sampling_engine.config import EngineConfig
from sampling_engine.errors import EngineError, E_INVALID_MANIFEST, E_INVALID_PARAM
from sampling_engine.manifest import ManifestCodec, stable_request_id

from conftest import SMALL_VOCAB


def _request(**overrides):
    payload = {
        "request_id": "roundtrip",
        "logits_path": "logits.npy",
        "temperature": 0.733,
        "top_k": 6,
        "top_p": 0.873,
        "repetition_penalty": 1.17,
        "seed": 987654321,
        "steps": 4,
        "prompt_tokens": [1, 3],
    }
    payload.update(overrides)
    return payload


def _v2_document(request=None):
    return {
        "manifest_version": 2,
        "vocab": SMALL_VOCAB,
        "requests": [request or _request()],
    }


def test_v2_to_v1_to_v2_preserves_required_fields():
    codec = ManifestCodec(EngineConfig(v1_default_top_p=0.91))
    original = codec.decode_document(_v2_document())
    v1_doc = codec.export_document(original, 1)
    legacy_request = v1_doc["requests"][0]
    assert "top_p" not in legacy_request
    assert isinstance(legacy_request["temp_percent"], int)
    assert legacy_request["temp_percent"] == 73
    assert legacy_request["x_downgrade"]["omitted_fields"] == ["top_p"]
    assert legacy_request["x_downgrade"]["temperature_quantization"]["exact"] is False
    decoded_v1 = codec.decode_document(v1_doc)
    v2_again = codec.export_document(decoded_v1, 2)
    restored = codec.decode_document(v2_again)
    before = original.requests[0]
    after = restored.requests[0]
    for field in ("temperature", "top_k", "top_p", "seed", "steps", "prompt_tokens"):
        assert getattr(before, field) == getattr(after, field)


def test_plain_legacy_v1_infers_temperature_and_top_p():
    codec = ManifestCodec(EngineConfig(v1_default_top_p=0.93, v1_temperature_scale=100.0))
    document = {
        "manifest_version": 1,
        "vocab": SMALL_VOCAB,
        "requests": [
            {
                "request_id": "legacy",
                "logits_path": "x.npy",
                "temp_percent": 75,
                "top_k": 3,
                "repetition_penalty": 1.0,
                "seed": 7,
                "steps": 2,
            }
        ],
    }
    manifest = codec.decode_document(document)
    request = manifest.requests[0]
    assert request.temperature == 0.75
    assert request.top_p == 0.93
    conversion = manifest.conversions["legacy"]
    assert conversion["temperature"]["mode"] == "percent_scaled"
    assert conversion["top_p"]["mode"] == "v1_default_inference"
    assert "no top-p" in conversion["top_p"]["detail"]


def test_missing_request_id_is_invalid_manifest():
    codec = ManifestCodec()
    request = _request()
    request.pop("request_id")
    with pytest.raises(EngineError) as info:
        codec.decode_document(_v2_document(request))
    assert info.value.code == E_INVALID_MANIFEST
    assert info.value.context["field"] == "request_id"


@pytest.mark.parametrize("field", ["top_p", "temperature", "top_k", "seed", "steps", "logits_path"])
def test_missing_v2_fields_are_invalid_manifest(field):
    codec = ManifestCodec()
    request = _request()
    request.pop(field)
    with pytest.raises(EngineError) as info:
        codec.decode_document(_v2_document(request))
    assert info.value.code == E_INVALID_MANIFEST


def test_legacy_top_p_field_is_rejected():
    codec = ManifestCodec()
    document = {
        "manifest_version": 1,
        "vocab": SMALL_VOCAB,
        "requests": [
            {
                "request_id": "legacy",
                "logits_path": "x.npy",
                "temp_percent": 80,
                "top_k": 2,
                "top_p": 0.9,
                "repetition_penalty": 1.0,
                "seed": 1,
                "steps": 1,
            }
        ],
    }
    with pytest.raises(EngineError) as info:
        codec.decode_document(document)
    assert info.value.code == E_INVALID_MANIFEST


@pytest.mark.parametrize("temperature", [0.0, -0.5, 5.01])
def test_invalid_temperature_has_request_and_parameter(temperature):
    codec = ManifestCodec(EngineConfig(max_temperature=5.0))
    with pytest.raises(EngineError) as info:
        codec.decode_document(_v2_document(_request(temperature=temperature)))
    assert info.value.code == E_INVALID_PARAM
    assert info.value.context["request_id"] == "roundtrip"
    assert info.value.context["parameter"] == "temperature"


def test_stable_request_id_helper_is_deterministic():
    payload = _request()
    without_id = {key: value for key, value in payload.items() if key != "request_id"}
    first = stable_request_id(without_id)
    second = stable_request_id(dict(reversed(list(without_id.items()))))
    assert first == second
    assert first.startswith("req_") and len(first) == 28


def test_manifest_source_digest_and_warnings_are_visible():
    codec = ManifestCodec(EngineConfig(v1_default_top_p=0.9))
    document = {
        "manifest_version": 1,
        "vocab": SMALL_VOCAB,
        "requests": [
            {
                "request_id": "legacy",
                "logits_path": "x.npy",
                "temp_percent": 80,
                "top_k": 2,
                "repetition_penalty": 1.0,
                "seed": 1,
                "steps": 1,
            }
        ],
    }
    manifest = codec.decode_document(document)
    assert manifest.manifest_digest
    assert manifest.warnings and "top-p" in manifest.warnings[0]


def test_legacy_aliases_id_logits_and_temperature_percent():
    codec = ManifestCodec(EngineConfig(v1_default_top_p=0.9))
    document = {
        "version": 1,
        "vocab": SMALL_VOCAB,
        "requests": [
            {
                "id": "alias_request",
                "logits": "x.npy",
                "temperature_percent": 110,
                "top_k": 2,
                "repetition_penalty": 1.0,
                "seed": 1,
                "steps": 1,
            }
        ],
    }
    manifest = codec.decode_document(document)
    assert manifest.version == 1
    assert manifest.requests[0].request_id == "alias_request"
    assert manifest.requests[0].temperature == 1.1
