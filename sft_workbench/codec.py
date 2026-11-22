"""Bidirectional codecs between schema v1/v2 payloads and the internal model.

Schema v2 describes the output format with structured fields and carries a
structured quality annotation.  Schema v1 stores the output as free text
(optionally prefixed with a ``[format:<type>]`` hint line) and its legacy
quality annotation is an integer percentage that may be negative and lacks
a language-consistency flag.

Import rules are configurable via :class:`V1ImportRules`.  Decode failures
raise :class:`WorkbenchError` with ``E_DECODE_SAMPLE`` (or
``E_UNSUPPORTED_VERSION`` for unknown schema versions) and always carry the
sample id and the offending field name.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from .errors import E_DECODE_SAMPLE, E_UNSUPPORTED_VERSION, WorkbenchError
from .models import SCHEMA_V1, SCHEMA_V2, InternalSample, QualityAnnotation
from .textnorm import has_cjk
from .util import quantize

DEFAULT_HINT_PATTERN = r"^\s*\[format:([a-z0-9_]+)\][ \t]*\r?\n?"


@dataclass
class V1ImportRules:
    """Configurable rules for importing schema v1 samples."""

    format_hint_pattern: str = DEFAULT_HINT_PATTERN
    default_format: str = "text"
    percent_scale: float = 100.0
    clamp_min: float = 0.0
    clamp_max: float = 1.0
    default_score: float = 0.5

    def to_dict(self) -> dict[str, Any]:
        return {
            "format_hint_pattern": self.format_hint_pattern,
            "default_format": self.default_format,
            "percent_scale": self.percent_scale,
            "clamp_min": self.clamp_min,
            "clamp_max": self.clamp_max,
            "default_score": self.default_score,
        }


def _require(raw: dict[str, Any], name: str, sample_id: Any) -> Any:
    value = raw.get(name)
    if value is None:
        raise WorkbenchError(
            E_DECODE_SAMPLE,
            f"missing required field '{name}'",
            {"sample_id": sample_id, "field": name},
        )
    return value


def _require_str(raw: dict[str, Any], name: str, sample_id: Any) -> str:
    value = _require(raw, name, sample_id)
    if not isinstance(value, str):
        raise WorkbenchError(
            E_DECODE_SAMPLE,
            f"field '{name}' must be a string",
            {"sample_id": sample_id, "field": name},
        )
    return value


def _infer_lang_consistent(instruction: str, output: str) -> bool:
    """Default language-consistency inference: CJK-ness of in- and output."""
    return has_cjk(instruction) == has_cjk(output)


def decode_sample(raw: dict[str, Any], rules: V1ImportRules | None = None) -> InternalSample:
    """Decode a raw JSON object of schema v1 or v2 into the internal model."""
    if not isinstance(raw, dict):
        raise WorkbenchError(
            E_DECODE_SAMPLE,
            "sample payload must be a JSON object",
            {"sample_id": None, "field": "$"},
        )
    rules = rules or V1ImportRules()
    sample_id = raw.get("id")
    version = raw.get("version")
    if version is None:
        raise WorkbenchError(
            E_DECODE_SAMPLE,
            "missing required field 'version'",
            {"sample_id": sample_id, "field": "version"},
        )
    if version == SCHEMA_V1:
        return _decode_v1(raw, rules)
    if version == SCHEMA_V2:
        return _decode_v2(raw)
    raise WorkbenchError(
        E_UNSUPPORTED_VERSION,
        f"unsupported schema version: {version!r}",
        {"sample_id": sample_id, "field": "version", "version": version},
    )


def _decode_v1(raw: dict[str, Any], rules: V1ImportRules) -> InternalSample:
    sample_id = _require(raw, "id", raw.get("id"))
    instruction = _require_str(raw, "instruction", sample_id)
    input_text = raw.get("input", "")
    if not isinstance(input_text, str):
        raise WorkbenchError(
            E_DECODE_SAMPLE,
            "field 'input' must be a string",
            {"sample_id": sample_id, "field": "input"},
        )
    output_raw = _require_str(raw, "output", sample_id)
    source = raw.get("source", "unknown")
    lang = raw.get("lang", "unknown")

    # Parse the free-text output structure per configurable hint rules.
    output_format: dict[str, Any] = {"type": rules.default_format}
    body = output_raw
    match = re.match(rules.format_hint_pattern, output_raw)
    if match:
        output_format = {"type": match.group(1)}
        body = output_raw[match.end():]

    # Legacy quality upgrade: percentage conversion, negative clamping and
    # default language-consistency inference.  Degraded items and the
    # conversion basis are recorded explicitly.
    downgraded: list[str] = []
    legacy = raw.get("quality")
    if legacy is None:
        score = rules.default_score
        downgraded.append("missing_quality_defaulted")
    elif isinstance(legacy, bool) or not isinstance(legacy, (int, float)):
        raise WorkbenchError(
            E_DECODE_SAMPLE,
            "field 'quality' must be a number (legacy percentage)",
            {"sample_id": sample_id, "field": "quality"},
        )
    else:
        score = float(legacy) / rules.percent_scale
        if score < rules.clamp_min:
            score = rules.clamp_min
            downgraded.append("negative_score_clamped")
        elif score > rules.clamp_max:
            score = rules.clamp_max
            downgraded.append("percent_overflow_clamped")
    lang_consistent = _infer_lang_consistent(instruction, body)
    downgraded.append("lang_consistency_inferred")
    basis = (
        f"percent/{rules.percent_scale:g};"
        f"clamp=[{rules.clamp_min:g},{rules.clamp_max:g}];"
        "lang_consistency=inferred"
    )
    quality = QualityAnnotation(
        score=quantize(score),
        lang_consistent=lang_consistent,
        downgraded=downgraded,
        basis=basis,
    )
    return InternalSample(
        sample_id=str(sample_id),
        instruction=instruction,
        input=input_text,
        output=body,
        output_format=output_format,
        source=str(source),
        lang=str(lang),
        quality=quality,
        schema_version=SCHEMA_V1,
    )


def _decode_v2(raw: dict[str, Any]) -> InternalSample:
    sample_id = _require(raw, "id", raw.get("id"))
    instruction = _require_str(raw, "instruction", sample_id)
    input_text = raw.get("input", "")
    if not isinstance(input_text, str):
        raise WorkbenchError(
            E_DECODE_SAMPLE,
            "field 'input' must be a string",
            {"sample_id": sample_id, "field": "input"},
        )
    output = _require(raw, "output", sample_id)
    if not isinstance(output, dict):
        raise WorkbenchError(
            E_DECODE_SAMPLE,
            "field 'output' must be an object with 'format' and 'text'",
            {"sample_id": sample_id, "field": "output"},
        )
    text = output.get("text")
    if not isinstance(text, str):
        raise WorkbenchError(
            E_DECODE_SAMPLE,
            "field 'output.text' must be a string",
            {"sample_id": sample_id, "field": "output.text"},
        )
    fmt = output.get("format")
    if not isinstance(fmt, dict) or not isinstance(fmt.get("type"), str):
        raise WorkbenchError(
            E_DECODE_SAMPLE,
            "field 'output.format.type' must be a string",
            {"sample_id": sample_id, "field": "output.format.type"},
        )
    quality_raw = _require(raw, "quality", sample_id)
    if not isinstance(quality_raw, dict):
        raise WorkbenchError(
            E_DECODE_SAMPLE,
            "field 'quality' must be an object",
            {"sample_id": sample_id, "field": "quality"},
        )
    score = quality_raw.get("score")
    if isinstance(score, bool) or not isinstance(score, (int, float)):
        raise WorkbenchError(
            E_DECODE_SAMPLE,
            "field 'quality.score' must be a number",
            {"sample_id": sample_id, "field": "quality.score"},
        )
    upgrade = quality_raw.get("upgrade") or {}
    quality = QualityAnnotation(
        score=quantize(float(score)),
        lang_consistent=bool(quality_raw.get("lang_consistent", True)),
        downgraded=list(upgrade.get("downgraded", [])),
        basis=str(upgrade.get("basis", "")),
    )
    return InternalSample(
        sample_id=str(sample_id),
        instruction=instruction,
        input=input_text,
        output=text,
        output_format=dict(fmt),
        source=str(raw.get("source", "unknown")),
        lang=str(raw.get("lang", "unknown")),
        quality=quality,
        schema_version=SCHEMA_V2,
    )


def encode_v2(sample: InternalSample) -> dict[str, Any]:
    """Encode an internal sample as a schema v2 payload."""
    return {
        "id": sample.sample_id,
        "version": SCHEMA_V2,
        "instruction": sample.instruction,
        "input": sample.input,
        "output": {
            "format": dict(sample.output_format),
            "text": sample.output,
        },
        "source": sample.source,
        "lang": sample.lang,
        "quality": {
            "score": quantize(sample.quality.score),
            "lang_consistent": bool(sample.quality.lang_consistent),
            "upgrade": {
                "downgraded": list(sample.quality.downgraded),
                "basis": sample.quality.basis,
            },
        },
    }


def encode_v1(sample: InternalSample) -> dict[str, Any]:
    """Encode an internal sample as a schema v1 payload (lossy quality)."""
    fmt_type = str(sample.output_format.get("type", "text"))
    if fmt_type and fmt_type != "text":
        output_text = f"[format:{fmt_type}]\n{sample.output}"
    else:
        output_text = sample.output
    return {
        "id": sample.sample_id,
        "version": SCHEMA_V1,
        "instruction": sample.instruction,
        "input": sample.input,
        "output": output_text,
        "source": sample.source,
        "lang": sample.lang,
        "quality": int(round(sample.quality.score * 100)),
    }


def encode_sample(sample: InternalSample, version: int) -> dict[str, Any]:
    if version == SCHEMA_V1:
        return encode_v1(sample)
    if version == SCHEMA_V2:
        return encode_v2(sample)
    raise WorkbenchError(
        E_UNSUPPORTED_VERSION,
        f"unsupported schema version: {version!r}",
        {"sample_id": sample.sample_id, "field": "version", "version": version},
    )
