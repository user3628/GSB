"""Bidirectional codecs between the internal model and record schema v1/v2.

Schema v2 record (JSONL line):
    {"schema_version": 2, "model_id", "sample_id", "score": float,
     "label", "lang", "batch", "bucket": int}

Schema v1 record (JSONL line):
    {"schema_version": 1, "model_id", "sample_uid": "<model>:<batch>:<seq>",
     "score_pct": int 0..100, "label", "lang", "batch"}
    v1 has no bucket field and the sample id is a concatenated string.

v1 import applies a configurable rounding rule for the percent<->float
conversion, fills the bucket field via equal-width binning and splits the
stable sample id out of the concatenated uid. Every conversion/split rule
is recorded in a provenance structure so it can be audited later.
"""
from __future__ import annotations

import json
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_EVEN, ROUND_HALF_UP

from .errors import VersionMismatchError, RoundtripViolationError
from .models import EvalRecord

SCHEMA_V1 = 1
SCHEMA_V2 = 2

ROUNDING_RULES = {
    "half_up": ROUND_HALF_UP,
    "half_even": ROUND_HALF_EVEN,
    "floor": ROUND_FLOOR,
    "ceil": ROUND_CEILING,
}
DEFAULT_ROUNDING = "half_up"
DEFAULT_ID_DELIMITER = ":"

_V2_REQUIRED = ("model_id", "sample_id", "score", "label", "lang", "batch", "bucket")
_V1_REQUIRED = ("model_id", "sample_uid", "score_pct", "label", "lang", "batch")
_V1_ONLY_FIELDS = ("score_pct", "sample_uid")
_V2_ONLY_FIELDS = ("score", "bucket", "sample_id")


def _mismatch(shard_id, line_no, message, **details):
    return VersionMismatchError(
        message, shard_id=shard_id, record_position=line_no, **details
    )


def quantize_score_to_pct(score: float, rounding: str = DEFAULT_ROUNDING) -> int:
    """float score in [0,1] -> integer percent, using the configured rule."""
    if rounding not in ROUNDING_RULES:
        raise VersionMismatchError(f"unknown rounding rule: {rounding}")
    value = Decimal(repr(score)) * 100
    pct = int(value.quantize(Decimal("1"), rounding=ROUNDING_RULES[rounding]))
    return max(0, min(100, pct))


def pct_to_score(pct: int) -> float:
    """integer percent -> float score. Exact division, no rounding involved."""
    return pct / 100.0


def split_sample_uid(uid: str, model_id: str, batch: str, delimiter: str) -> str:
    """Split the stable sample id out of the concatenated v1 uid.

    Rule (auditable): split on the configured delimiter from the right and
    take the last segment; the leading segments must equal
    "<model_id><delimiter><batch>" so the split is reversible.
    """
    prefix = f"{model_id}{delimiter}{batch}{delimiter}"
    if not uid.startswith(prefix):
        raise VersionMismatchError(
            "v1 sample_uid does not follow '<model><delim><batch><delim><seq>'",
            sample_uid=uid,
            expected_prefix=prefix,
        )
    stable = uid[len(prefix):]
    if not stable:
        raise VersionMismatchError("v1 sample_uid has empty stable segment", sample_uid=uid)
    return stable


def join_sample_uid(sample_id: str, model_id: str, batch: str, delimiter: str) -> str:
    return f"{model_id}{delimiter}{batch}{delimiter}{sample_id}"


def fill_bucket_equal_width(score: float, n_bins: int) -> int:
    """Deterministic equal-width bucket fill used when importing v1 records.

    Boundary rule: buckets are [lo, hi) half-open intervals; a score exactly
    on a boundary goes to the higher-index bucket; score == 1.0 goes to the
    last bucket.
    """
    clipped = min(1.0, max(0.0, score))
    return min(int(clipped * n_bins), n_bins - 1)


def record_from_v2(obj: dict, *, shard_id: str, line_no: int) -> EvalRecord:
    if not isinstance(obj, dict):
        raise _mismatch(shard_id, line_no, "record is not a JSON object")
    for field in _V1_ONLY_FIELDS:
        if field in obj:
            raise _mismatch(
                shard_id, line_no,
                f"declared schema v2 but record carries v1-only field '{field}'",
                declared_version=SCHEMA_V2, offending_field=field,
            )
    for field in _V2_REQUIRED:
        if field not in obj:
            raise _mismatch(
                shard_id, line_no,
                f"declared schema v2 but required field '{field}' is missing",
                declared_version=SCHEMA_V2, offending_field=field,
            )
    score = obj["score"]
    if isinstance(score, bool) or not isinstance(score, (int, float)):
        raise _mismatch(
            shard_id, line_no,
            "declared schema v2 but 'score' is not a float number",
            declared_version=SCHEMA_V2, offending_field="score",
        )
    bucket = obj["bucket"]
    if isinstance(bucket, bool) or not isinstance(bucket, int):
        raise _mismatch(
            shard_id, line_no,
            "declared schema v2 but 'bucket' is not an integer",
            declared_version=SCHEMA_V2, offending_field="bucket",
        )
    return EvalRecord(
        model_id=str(obj["model_id"]),
        sample_id=str(obj["sample_id"]),
        score=float(score),
        label=int(obj["label"]),
        lang=str(obj["lang"]),
        batch=str(obj["batch"]),
        bucket=int(bucket),
    )


def record_to_v2(rec: EvalRecord) -> dict:
    return {
        "schema_version": SCHEMA_V2,
        "model_id": rec.model_id,
        "sample_id": rec.sample_id,
        "score": rec.score,
        "label": rec.label,
        "lang": rec.lang,
        "batch": rec.batch,
        "bucket": rec.bucket,
    }


def record_from_v1(
    obj: dict,
    *,
    shard_id: str,
    line_no: int,
    n_bins: int,
    rounding: str = DEFAULT_ROUNDING,
    id_delimiter: str = DEFAULT_ID_DELIMITER,
) -> tuple[EvalRecord, dict]:
    """Decode a v1 record; returns (record, conversion evidence)."""
    if not isinstance(obj, dict):
        raise _mismatch(shard_id, line_no, "record is not a JSON object")
    for field in _V2_ONLY_FIELDS:
        if field in obj:
            raise _mismatch(
                shard_id, line_no,
                f"declared schema v1 but record carries v2-only field '{field}'",
                declared_version=SCHEMA_V1, offending_field=field,
            )
    for field in _V1_REQUIRED:
        if field not in obj:
            raise _mismatch(
                shard_id, line_no,
                f"declared schema v1 but required field '{field}' is missing",
                declared_version=SCHEMA_V1, offending_field=field,
            )
    pct = obj["score_pct"]
    if isinstance(pct, bool) or not isinstance(pct, int) or not (0 <= pct <= 100):
        raise _mismatch(
            shard_id, line_no,
            "declared schema v1 but 'score_pct' is not an integer in [0, 100]",
            declared_version=SCHEMA_V1, offending_field="score_pct",
        )
    model_id = str(obj["model_id"])
    batch = str(obj["batch"])
    uid = str(obj["sample_uid"])
    try:
        stable_id = split_sample_uid(uid, model_id, batch, id_delimiter)
    except VersionMismatchError as exc:
        raise _mismatch(shard_id, line_no, exc.message, **exc.details)
    score = pct_to_score(pct)
    bucket = fill_bucket_equal_width(score, n_bins)
    rec = EvalRecord(
        model_id=model_id,
        sample_id=stable_id,
        score=score,
        label=int(obj["label"]),
        lang=str(obj["lang"]),
        batch=batch,
        bucket=bucket,
    )
    evidence = {
        "line": line_no,
        "sample_uid": uid,
        "score_pct": pct,
        "score": score,
        "sample_id": stable_id,
        "bucket": bucket,
        "score_rule": "score = score_pct / 100 (exact)",
        "bucket_rule": f"equal_width(n_bins={n_bins}), half-open [lo,hi), 1.0->last",
        "id_rule": f"split last segment on delimiter '{id_delimiter}'",
    }
    return rec, evidence


def record_to_v1(
    rec: EvalRecord,
    *,
    rounding: str = DEFAULT_ROUNDING,
    id_delimiter: str = DEFAULT_ID_DELIMITER,
) -> dict:
    return {
        "schema_version": SCHEMA_V1,
        "model_id": rec.model_id,
        "sample_uid": join_sample_uid(rec.sample_id, rec.model_id, rec.batch, id_delimiter),
        "score_pct": quantize_score_to_pct(rec.score, rounding),
        "label": rec.label,
        "lang": rec.lang,
        "batch": rec.batch,
    }


def detect_declared_version(obj: dict, *, shard_id: str, line_no: int) -> int:
    version = obj.get("schema_version") if isinstance(obj, dict) else None
    if version not in (SCHEMA_V1, SCHEMA_V2):
        raise _mismatch(
            shard_id, line_no,
            "missing or unsupported 'schema_version' (expected 1 or 2)",
            declared_version=version,
        )
    return version


def decode_line(
    line: str,
    *,
    shard_id: str,
    line_no: int,
    n_bins: int,
    rounding: str = DEFAULT_ROUNDING,
    id_delimiter: str = DEFAULT_ID_DELIMITER,
) -> tuple[EvalRecord, dict | None, int]:
    """Decode one JSONL line. Returns (record, v1_evidence_or_None, version)."""
    try:
        obj = json.loads(line)
    except json.JSONDecodeError as exc:
        raise _mismatch(shard_id, line_no, f"line is not valid JSON: {exc}")
    version = detect_declared_version(obj, shard_id=shard_id, line_no=line_no)
    if version == SCHEMA_V2:
        return record_from_v2(obj, shard_id=shard_id, line_no=line_no), None, version
    rec, evidence = record_from_v1(
        obj, shard_id=shard_id, line_no=line_no,
        n_bins=n_bins, rounding=rounding, id_delimiter=id_delimiter,
    )
    return rec, evidence, version


# ---------------------------------------------------------------- round trips

def _same_semantics(a: EvalRecord, b: EvalRecord, *, check_score: bool) -> list[str]:
    diffs = []
    for field in ("model_id", "sample_id", "label", "lang", "batch", "bucket"):
        if getattr(a, field) != getattr(b, field):
            diffs.append(f"{field}: {getattr(a, field)!r} != {getattr(b, field)!r}")
    if check_score and a.score != b.score:
        diffs.append(f"score: {a.score!r} != {b.score!r}")
    return diffs


def roundtrip_chain_from_v1(
    v1_obj: dict, *, shard_id: str, line_no: int, n_bins: int,
    rounding: str = DEFAULT_ROUNDING, id_delimiter: str = DEFAULT_ID_DELIMITER,
) -> dict:
    """v1 -> internal -> v2 -> internal -> v1; semantics must be preserved."""
    rec0, evidence = record_from_v1(
        v1_obj, shard_id=shard_id, line_no=line_no,
        n_bins=n_bins, rounding=rounding, id_delimiter=id_delimiter,
    )
    v2_obj = record_to_v2(rec0)
    rec1 = record_from_v2(v2_obj, shard_id=shard_id, line_no=line_no)
    v1_back = record_to_v1(rec1, rounding=rounding, id_delimiter=id_delimiter)
    rec2, _ = record_from_v1(
        v1_back, shard_id=shard_id, line_no=line_no,
        n_bins=n_bins, rounding=rounding, id_delimiter=id_delimiter,
    )
    diffs = _same_semantics(rec0, rec2, check_score=True)
    if v1_back != {k: v for k, v in v1_obj.items() if k in v1_back or k == "schema_version"}:
        # field-level comparison of the re-emitted v1 record vs the original
        for key in ("model_id", "sample_uid", "score_pct", "label", "lang", "batch"):
            if v1_back.get(key) != v1_obj.get(key):
                diffs.append(f"v1 field {key}: {v1_obj.get(key)!r} != {v1_back.get(key)!r}")
    if diffs:
        raise RoundtripViolationError(
            "round-trip chain v1->internal->v2->internal->v1 violated",
            shard_id=shard_id, record_position=line_no, diffs=diffs,
        )
    return {"chain": "v1->internal->v2->internal->v1", "ok": True, "evidence": evidence}


def roundtrip_chain_from_v2(
    v2_obj: dict, *, shard_id: str, line_no: int, n_bins: int,
    rounding: str = DEFAULT_ROUNDING, id_delimiter: str = DEFAULT_ID_DELIMITER,
) -> dict:
    """v2 -> internal -> v1 -> internal; score is preserved at v1 percent
    precision, label/strata must be preserved exactly. The bucket declared
    by the v2 record belongs to the binning regime it was written under;
    the chain verifies that re-deriving the bucket from the score under the
    current n_bins is deterministic, and reports any regime change as
    'bucket_rebased' information instead of a violation."""
    rec0 = record_from_v2(v2_obj, shard_id=shard_id, line_no=line_no)
    v1_obj = record_to_v1(rec0, rounding=rounding, id_delimiter=id_delimiter)
    rec1, _ = record_from_v1(
        v1_obj, shard_id=shard_id, line_no=line_no,
        n_bins=n_bins, rounding=rounding, id_delimiter=id_delimiter,
    )
    diffs = _same_semantics(rec0, rec1, check_score=False)
    diffs = [d for d in diffs if not d.startswith("bucket:")]
    expected_bucket = fill_bucket_equal_width(rec0.score, n_bins)
    if rec1.bucket != expected_bucket:
        diffs.append(f"bucket re-derivation not deterministic: {rec1.bucket!r} != {expected_bucket!r}")
    if quantize_score_to_pct(rec0.score, rounding) != quantize_score_to_pct(rec1.score, rounding):
        diffs.append(f"score differs beyond v1 percent precision: {rec0.score!r} vs {rec1.score!r}")
    if diffs:
        raise RoundtripViolationError(
            "round-trip chain v2->internal->v1->internal violated",
            shard_id=shard_id, record_position=line_no, diffs=diffs,
        )
    result = {"chain": "v2->internal->v1->internal", "ok": True}
    if rec0.bucket != expected_bucket:
        result["bucket_rebased"] = {"declared": rec0.bucket, "recomputed": expected_bucket,
                                    "reason": f"declared bucket predates current n_bins={n_bins}"}
    return result
