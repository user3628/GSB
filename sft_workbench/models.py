"""Unified internal data model for instruction samples.

An internal sample is described by: sample id, instruction text, input,
output text, output format descriptor, source, language, quality annotation,
schema version and a content fingerprint.  The fingerprint is computed over
canonical content fields only, so it is stable across schema-version
conversions (v1 -> internal -> v2 -> internal) as long as content survives.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .util import digest_obj, quantize

SCHEMA_V1 = 1
SCHEMA_V2 = 2
SUPPORTED_SCHEMA_VERSIONS = (SCHEMA_V1, SCHEMA_V2)


@dataclass
class QualityAnnotation:
    score: float
    lang_consistent: bool
    downgraded: list[str] = field(default_factory=list)
    basis: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "score": quantize(self.score),
            "lang_consistent": bool(self.lang_consistent),
            "downgraded": list(self.downgraded),
            "basis": self.basis,
        }

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "QualityAnnotation":
        return QualityAnnotation(
            score=quantize(float(data["score"])),
            lang_consistent=bool(data.get("lang_consistent", True)),
            downgraded=list(data.get("downgraded", [])),
            basis=str(data.get("basis", "")),
        )


@dataclass
class InternalSample:
    sample_id: str
    instruction: str
    input: str
    output: str
    output_format: dict[str, Any]
    source: str
    lang: str
    quality: QualityAnnotation
    schema_version: int
    fingerprint: str = ""

    def __post_init__(self) -> None:
        if not self.fingerprint:
            self.fingerprint = self.compute_fingerprint()

    def content_fields(self) -> dict[str, Any]:
        """Fields covered by the content fingerprint (schema version excluded)."""
        return {
            "id": self.sample_id,
            "instruction": self.instruction,
            "input": self.input,
            "output": self.output,
            "output_format": self.output_format,
            "source": self.source,
            "lang": self.lang,
            "quality": {
                "score": quantize(self.quality.score),
                "lang_consistent": bool(self.quality.lang_consistent),
            },
        }

    def compute_fingerprint(self) -> str:
        return digest_obj(self.content_fields())

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.sample_id,
            "instruction": self.instruction,
            "input": self.input,
            "output": self.output,
            "output_format": self.output_format,
            "source": self.source,
            "lang": self.lang,
            "quality": self.quality.to_dict(),
            "schema_version": self.schema_version,
            "fingerprint": self.fingerprint,
        }

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "InternalSample":
        return InternalSample(
            sample_id=str(data["id"]),
            instruction=str(data["instruction"]),
            input=str(data.get("input", "")),
            output=str(data["output"]),
            output_format=dict(data.get("output_format", {"type": "text"})),
            source=str(data.get("source", "unknown")),
            lang=str(data.get("lang", "unknown")),
            quality=QualityAnnotation.from_dict(data["quality"]),
            schema_version=int(data["schema_version"]),
            fingerprint=str(data.get("fingerprint", "")),
        )
