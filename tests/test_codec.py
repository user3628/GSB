"""Schema v1/v2 bidirectional codec tests."""
import pytest

from sft_workbench.codec import V1ImportRules, decode_sample, encode_sample, encode_v1, encode_v2
from sft_workbench.errors import E_DECODE_SAMPLE, E_UNSUPPORTED_VERSION, WorkbenchError


def make_v1(**overrides):
    doc = {
        "id": "t-1",
        "version": 1,
        "instruction": "请总结以下文章",
        "input": "",
        "output": "这是总结。",
        "source": "wiki",
        "lang": "zh",
        "quality": 85,
    }
    doc.update(overrides)
    return doc


def make_v2(**overrides):
    doc = {
        "id": "t-2",
        "version": 2,
        "instruction": "Summarize this",
        "input": "",
        "output": {"format": {"type": "text"}, "text": "A summary."},
        "source": "wiki",
        "lang": "en",
        "quality": {"score": 0.5, "lang_consistent": True},
    }
    doc.update(overrides)
    return doc


def test_v1_percent_conversion():
    sample = decode_sample(make_v1(quality=85))
    assert sample.quality.score == 0.85
    assert sample.schema_version == 1
    assert "lang_consistency_inferred" in sample.quality.downgraded
    assert "percent/100" in sample.quality.basis


def test_v1_negative_quality_clamped():
    sample = decode_sample(make_v1(quality=-30))
    assert sample.quality.score == 0.0
    assert "negative_score_clamped" in sample.quality.downgraded


def test_v1_overflow_quality_clamped():
    sample = decode_sample(make_v1(quality=150))
    assert sample.quality.score == 1.0
    assert "percent_overflow_clamped" in sample.quality.downgraded


def test_v1_missing_quality_defaulted():
    doc = make_v1()
    del doc["quality"]
    sample = decode_sample(doc)
    assert sample.quality.score == 0.5
    assert "missing_quality_defaulted" in sample.quality.downgraded


def test_v1_lang_consistency_inference():
    zh = decode_sample(make_v1())
    assert zh.quality.lang_consistent is True
    mixed = decode_sample(make_v1(output="This is English output."))
    assert mixed.quality.lang_consistent is False


def test_v1_format_hint_parsing():
    sample = decode_sample(make_v1(output='[format:json]\n{"a": 1}'))
    assert sample.output_format == {"type": "json"}
    assert sample.output == '{"a": 1}'
    plain = decode_sample(make_v1(output="no hint here"))
    assert plain.output_format == {"type": "text"}
    assert plain.output == "no hint here"


def test_v1_to_v2_round_trip_keeps_content_and_fingerprint():
    internal = decode_sample(make_v1(quality=77))
    as_v2 = encode_v2(internal)
    assert as_v2["version"] == 2
    assert as_v2["quality"]["score"] == 0.77
    reimported = decode_sample(as_v2)
    assert reimported.fingerprint == internal.fingerprint
    assert reimported.quality.score == internal.quality.score
    assert reimported.schema_version == 2
    assert reimported.quality.downgraded == internal.quality.downgraded
    assert reimported.quality.basis == internal.quality.basis


def test_v1_round_trip_with_format_hint():
    internal = decode_sample(make_v1(output='[format:json]\n{"a": 1}'))
    as_v1 = encode_v1(internal)
    assert as_v1["output"].startswith("[format:json]")
    assert decode_sample(as_v1).fingerprint == internal.fingerprint


def test_v2_decode_and_upgrade_metadata():
    internal = decode_sample(make_v1(quality=-5))
    as_v2 = encode_v2(internal)
    assert as_v2["quality"]["upgrade"]["downgraded"] == internal.quality.downgraded
    again = decode_sample(as_v2)
    assert again.quality.downgraded == internal.quality.downgraded


def test_missing_output_raises_structured_error():
    doc = make_v1()
    del doc["output"]
    with pytest.raises(WorkbenchError) as excinfo:
        decode_sample(doc)
    err = excinfo.value
    assert err.code == E_DECODE_SAMPLE
    assert err.details["field"] == "output"
    assert err.details["sample_id"] == "t-1"


def test_missing_id_raises_structured_error():
    doc = make_v1()
    del doc["id"]
    with pytest.raises(WorkbenchError) as excinfo:
        decode_sample(doc)
    assert excinfo.value.details["field"] == "id"


def test_unsupported_version():
    with pytest.raises(WorkbenchError) as excinfo:
        decode_sample(make_v1(version=3))
    err = excinfo.value
    assert err.code == E_UNSUPPORTED_VERSION
    assert err.details["version"] == 3
    assert err.details["sample_id"] == "t-1"


def test_missing_version_is_decode_error():
    doc = make_v1()
    del doc["version"]
    with pytest.raises(WorkbenchError) as excinfo:
        decode_sample(doc)
    assert excinfo.value.code == E_DECODE_SAMPLE
    assert excinfo.value.details["field"] == "version"


def test_v2_requires_structured_output_and_score():
    with pytest.raises(WorkbenchError) as excinfo:
        decode_sample(make_v2(output="free text"))
    assert excinfo.value.details["field"] == "output"
    bad = make_v2()
    bad["quality"] = {"lang_consistent": True}
    with pytest.raises(WorkbenchError) as excinfo:
        decode_sample(bad)
    assert excinfo.value.details["field"] == "quality.score"


def test_encode_unsupported_version():
    internal = decode_sample(make_v1())
    with pytest.raises(WorkbenchError) as excinfo:
        encode_sample(internal, 9)
    assert excinfo.value.code == E_UNSUPPORTED_VERSION


def test_custom_v1_rules():
    rules = V1ImportRules(percent_scale=50.0, default_score=0.25)
    sample = decode_sample(make_v1(quality=25), rules)
    assert sample.quality.score == 0.5
    assert "percent/50" in sample.quality.basis
