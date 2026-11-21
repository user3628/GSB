"""Unified internal evaluation model.

One internal record describes a single evaluation sample:
model id, stable sample id, float score in [0, 1], binary label,
language tag, source batch and bucket number.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict


@dataclass(frozen=True)
class EvalRecord:
    model_id: str
    sample_id: str
    score: float
    label: int
    lang: str
    batch: str
    bucket: int

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(d: dict) -> "EvalRecord":
        return EvalRecord(
            model_id=str(d["model_id"]),
            sample_id=str(d["sample_id"]),
            score=float(d["score"]),
            label=int(d["label"]),
            lang=str(d["lang"]),
            batch=str(d["batch"]),
            bucket=int(d["bucket"]),
        )
