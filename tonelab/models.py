"""Unified internal project model."""

from __future__ import annotations

import copy
import json
from dataclasses import dataclass, field
from typing import Any, Iterable

import numpy as np

from .util import decode_pcm_int16, encode_pcm_int16, sha256_bytes


def _round(value: float, digits: int = 12) -> float:
    return float(round(float(value), digits))


@dataclass
class PronUnit:
    symbol: str
    start_ms: float
    end_ms: float
    formants_hz: list[float]
    formant_ranges_hz: list[list[float]] = field(default_factory=list)
    unit_id: str = ""

    def to_dict(self) -> dict[str, Any]:
        ranges = self.formant_ranges_hz
        if not ranges:
            ranges = [
                [_round(max(0.0, value * 0.9), 6), _round(value * 1.1, 6)]
                for value in self.formants_hz
            ]
        return {
            "unit_id": self.unit_id,
            "symbol": self.symbol,
            "start_ms": _round(self.start_ms),
            "end_ms": _round(self.end_ms),
            "formants_hz": [_round(x) for x in self.formants_hz],
            "formant_ranges_hz": [
                [_round(float(pair[0]), 6), _round(float(pair[1]), 6)]
                for pair in ranges
            ],
        }


@dataclass
class Shard:
    shard_id: str
    timbre_id: str
    pcm: np.ndarray
    pitch_curve: list[tuple[float, float]]
    units: list[PronUnit]
    structure_version: int = 2
    source_structure_version: int = 2
    reference_f0_mean: float = 180.0
    sample_rate: int = 16000
    channels: int = 1
    bit_depth: int = 16
    origin: dict[str, Any] = field(default_factory=dict)
    quantization: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.pcm = np.asarray(self.pcm, dtype=np.int16)
        if self.pcm.ndim != 1:
            raise ValueError("Shard.pcm must be mono")
        self.pitch_curve = [
            (_round(float(t)), _round(float(v))) for t, v in self.pitch_curve
        ]
        for index, unit in enumerate(self.units):
            unit.start_ms = _round(unit.start_ms)
            unit.end_ms = _round(unit.end_ms)
            unit.formants_hz = [_round(x) for x in unit.formants_hz]
            if not unit.formant_ranges_hz:
                unit.formant_ranges_hz = [
                    [_round(max(0.0, value * 0.9), 6), _round(value * 1.1, 6)]
                    for value in unit.formants_hz
                ]
            if not unit.unit_id:
                unit.unit_id = f"{self.shard_id}:u{index:04d}"

    @property
    def sample_count(self) -> int:
        return int(self.pcm.size)

    @property
    def duration_ms(self) -> float:
        return self.sample_count * 1000.0 / float(self.sample_rate)

    @property
    def pcm_sha256(self) -> str:
        return sha256_bytes(np.ascontiguousarray(self.pcm.astype("<i2", copy=False)).tobytes())

    def clone(self) -> "Shard":
        return copy.deepcopy(self)

    def to_internal_dict(self, *, embed_pcm: bool = True) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "shard_id": self.shard_id,
            "timbre_id": self.timbre_id,
            "sample_count": self.sample_count,
            "sample_rate": self.sample_rate,
            "channels": self.channels,
            "bit_depth": self.bit_depth,
            "reference_f0_mean": _round(self.reference_f0_mean),
            "structure_version": self.structure_version,
            "source_structure_version": self.source_structure_version,
            "pitch_curve": [[_round(t), _round(v)] for t, v in self.pitch_curve],
            "units": [unit.to_dict() for unit in self.units],
            "origin": self.origin,
            "quantization": self.quantization,
            "pcm_sha256": self.pcm_sha256,
        }
        if embed_pcm:
            payload["pcm_encoding"] = "int16le-base64"
            payload["pcm_b64"] = encode_pcm_int16(self.pcm)
        return payload

    @classmethod
    def from_internal_dict(cls, data: dict[str, Any]) -> "Shard":
        pcm = decode_pcm_int16(data["pcm_b64"])
        if len(pcm) != int(data["sample_count"]):
            raise ValueError("embedded PCM length disagrees with sample_count")
        units = [
            PronUnit(
                symbol=str(item["symbol"]),
                start_ms=float(item["start_ms"]),
                end_ms=float(item["end_ms"]),
                formants_hz=[float(x) for x in item.get("formants_hz", [])],
                formant_ranges_hz=[
                    [float(pair[0]), float(pair[1])]
                    for pair in item.get("formant_ranges_hz", [])
                ],
                unit_id=str(item.get("unit_id", "")),
            )
            for item in data.get("units", [])
        ]
        return cls(
            shard_id=str(data["shard_id"]),
            timbre_id=str(data["timbre_id"]),
            pcm=pcm,
            pitch_curve=[(float(p[0]), float(p[1])) for p in data["pitch_curve"]],
            units=units,
            structure_version=int(data.get("structure_version", 2)),
            source_structure_version=int(data.get("source_structure_version", 2)),
            reference_f0_mean=float(data.get("reference_f0_mean", 180.0)),
            sample_rate=int(data.get("sample_rate", 16000)),
            channels=int(data.get("channels", 1)),
            bit_depth=int(data.get("bit_depth", 16)),
            origin=dict(data.get("origin", {})),
            quantization=dict(data.get("quantization", {})),
        )


@dataclass
class Project:
    project_id: str
    shards: list[Shard]
    description: str = ""
    sample_rate: int = 16000
    channels: int = 1
    bit_depth: int = 16
    declared_total_samples: int = 0
    source_manifests: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.declared_total_samples:
            self.declared_total_samples = sum(s.sample_count for s in self.shards)

    @property
    def total_samples(self) -> int:
        return sum(shard.sample_count for shard in self.shards)

    @property
    def structure_version(self) -> int:
        return max((shard.structure_version for shard in self.shards), default=2)

    def shard(self, shard_id: str) -> Shard:
        for item in self.shards:
            if item.shard_id == shard_id:
                return item
        raise KeyError(shard_id)

    def clone(self) -> "Project":
        return copy.deepcopy(self)

    def version_fingerprint(self) -> str:
        payload = [
            {
                "shard_id": shard.shard_id,
                "structure_version": shard.structure_version,
                "source_structure_version": shard.source_structure_version,
                "timbre_id": shard.timbre_id,
                "pcm_sha256": shard.pcm_sha256,
                "reference_f0_mean": shard.reference_f0_mean,
                "pitch_curve": shard.pitch_curve,
                "units": [unit.to_dict() for unit in shard.units],
            }
            for shard in self.shards
        ]
        return sha256_bytes(
            json.dumps(
                payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            ).encode("utf-8")
        )

    def to_internal_dict(self) -> dict[str, Any]:
        return {
            "format": "tonelab.internal-project",
            "schema": 1,
            "project_id": self.project_id,
            "description": self.description,
            "sample_rate": self.sample_rate,
            "channels": self.channels,
            "bit_depth": self.bit_depth,
            "declared_total_samples": self.declared_total_samples,
            "source_manifests": list(self.source_manifests),
            "metadata": self.metadata,
            "shards": [shard.to_internal_dict() for shard in self.shards],
        }

    @classmethod
    def from_internal_dict(cls, data: dict[str, Any]) -> "Project":
        if data.get("format") != "tonelab.internal-project":
            raise ValueError("not a tonelab internal project")
        return cls(
            project_id=str(data["project_id"]),
            shards=[Shard.from_internal_dict(item) for item in data["shards"]],
            description=str(data.get("description", "")),
            sample_rate=int(data.get("sample_rate", 16000)),
            channels=int(data.get("channels", 1)),
            bit_depth=int(data.get("bit_depth", 16)),
            declared_total_samples=int(data.get("declared_total_samples", 0)),
            source_manifests=list(data.get("source_manifests", [])),
            metadata=dict(data.get("metadata", {})),
        )


def find_unit(shard: Shard, index: int) -> PronUnit | None:
    if 0 <= index < len(shard.units):
        return shard.units[index]
    return None

