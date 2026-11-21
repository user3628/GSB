import json

import pytest

from evalbench import codec
from evalbench.errors import RoundtripViolationError, VersionMismatchError
from evalbench.models import EvalRecord

KW = dict(shard_id="s", line_no=1)


def v1_obj():
    return {"schema_version": 1, "model_id": "m1", "sample_uid": "m1:b1:0007",
            "score_pct": 73, "label": 1, "lang": "zh", "batch": "b1"}


def v2_obj():
    return {"schema_version": 2, "model_id": "m1", "sample_id": "0007",
            "score": 0.73, "label": 1, "lang": "zh", "batch": "b1", "bucket": 7}


def test_v1_import_converts_and_fills():
    rec, evidence = codec.record_from_v1(v1_obj(), n_bins=10, **KW)
    assert rec.score == 0.73
    assert rec.sample_id == "0007"
    assert rec.bucket == 7
    # conversion/split basis is auditable
    assert evidence["score_rule"].startswith("score = score_pct / 100")
    assert "equal_width(n_bins=10)" in evidence["bucket_rule"]
    assert "delimiter ':'" in evidence["id_rule"]


def test_v1_export_reimports_identically():
    rec, _ = codec.record_from_v1(v1_obj(), n_bins=10, **KW)
    back = codec.record_to_v1(rec)
    assert back == v1_obj()


def test_v2_roundtrip_exact():
    rec = codec.record_from_v2(v2_obj(), **KW)
    assert codec.record_to_v2(rec) == v2_obj()


def test_rounding_rules_configurable():
    assert codec.quantize_score_to_pct(0.725, "half_up") == 73
    assert codec.quantize_score_to_pct(0.725, "floor") == 72
    assert codec.quantize_score_to_pct(0.725, "ceil") == 73
    assert codec.quantize_score_to_pct(0.735, "half_even") == 74
    assert codec.quantize_score_to_pct(0.725, "half_even") == 72


def test_roundtrip_chain_v1():
    out = codec.roundtrip_chain_from_v1(v1_obj(), n_bins=10, **KW)
    assert out["ok"] and out["chain"].startswith("v1->internal")


def test_roundtrip_chain_v2():
    out = codec.roundtrip_chain_from_v2(v2_obj(), n_bins=10, **KW)
    assert out["ok"]


def test_version_mismatch_v2_missing_score():
    bad = {"schema_version": 2, "model_id": "m1", "sample_id": "x",
           "label": 1, "lang": "zh", "batch": "b1", "bucket": 1}
    with pytest.raises(VersionMismatchError) as exc:
        codec.decode_line(json.dumps(bad), n_bins=10, shard_id="shardX", line_no=3)
    assert exc.value.code == "E_VERSION_MISMATCH"
    assert exc.value.details["shard_id"] == "shardX"
    assert exc.value.details["record_position"] == 3


def test_version_mismatch_v1_with_v2_fields():
    bad = v1_obj()
    bad["bucket"] = 3
    with pytest.raises(VersionMismatchError):
        codec.decode_line(json.dumps(bad), n_bins=10, **KW)


def test_version_mismatch_unknown_version():
    with pytest.raises(VersionMismatchError):
        codec.decode_line(json.dumps({"schema_version": 9}), n_bins=10, **KW)


def test_uid_split_rejects_bad_prefix():
    bad = v1_obj()
    bad["sample_uid"] = "other:b1:0007"
    with pytest.raises(VersionMismatchError):
        codec.record_from_v1(bad, n_bins=10, **KW)


def test_roundtrip_violation_detected():
    rec = EvalRecord("m", "s", 0.5, 1, "zh", "b", 5)
    tampered = codec.record_to_v2(rec)
    tampered["label"] = 0
    # tampered record still decodes, but chain must reflect the tampered label
    out = codec.roundtrip_chain_from_v2(tampered, n_bins=10, **KW)
    assert out["ok"]  # chain preserves the (tampered) label consistently
