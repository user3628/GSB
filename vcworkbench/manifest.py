from __future__ import annotations

import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .canonical import digest_obj, read_json
from .errors import WorkbenchError, fail
from .version import MANIFEST_SCHEMA


@dataclass(frozen=True)
class ReferencePitch:
    min_hz: float
    max_hz: float

    def to_dict(self) -> dict[str, float]:
        return {"min_hz": self.min_hz, "max_hz": self.max_hz}


@dataclass
class ShardSpec:
    shard_id: str
    wav: str
    source_speaker: str
    target_speaker: str
    pitch_shift: float
    mel_warp: float
    frame_shift: int
    quant_bits: int
    expected_output_samples: int
    reference_pitch: ReferencePitch | None
    reference_tolerance_hz: float | None
    sample_rate: int
    channels: int
    sample_count: int
    transcript: str = ""
    raw: dict[str, Any] = field(default_factory=dict)
    manifest_path: Path | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "shard_id": self.shard_id,
            "wav": self.wav,
            "source_speaker": self.source_speaker,
            "target_speaker": self.target_speaker,
            "pitch_shift": self.pitch_shift,
            "mel_warp": self.mel_warp,
            "frame_shift": self.frame_shift,
            "quant_bits": self.quant_bits,
            "expected_output_samples": self.expected_output_samples,
            "reference_pitch_hz": None
            if self.reference_pitch is None
            else self.reference_pitch.to_dict(),
            "reference_tolerance_hz": self.reference_tolerance_hz,
            "sample_rate": self.sample_rate,
            "channels": self.channels,
            "sample_count": self.sample_count,
            "transcript": self.transcript,
        }

    def resolve_wav(self, base: Path) -> Path:
        path = Path(self.wav)
        if path.is_absolute():
            return path
        return base / path


@dataclass
class Manifest:
    version: str
    shards: list[ShardSpec]
    raw: dict[str, Any]
    path: Path | None = None

    def by_id(self, shard_id: str) -> ShardSpec:
        for spec in self.shards:
            if spec.shard_id == shard_id:
                return spec
        fail(
            "E_INVALID_PARAM",
            f"unknown shard id: {shard_id}",
            shard_id=shard_id,
            stage="ingest",
            known_shard_ids=[s.shard_id for s in self.shards],
        )
        raise AssertionError("unreachable")

    def digest(self) -> str:
        return digest_obj(self.raw)

    def to_dict(self) -> dict[str, Any]:
        return self.raw


def _require(obj: dict[str, Any], key: str, shard_id: str | None = None) -> Any:
    if key not in obj:
        fail(
            "E_MANIFEST_MISMATCH",
            f"manifest field is required: {key}",
            shard_id=shard_id,
            stage="ingest",
            field=key,
        )
    return obj[key]


_MISSING = object()


def _alias(obj: dict[str, Any], names: tuple[str, ...]) -> Any:
    for name in names:
        if name in obj:
            return obj[name]
    return _MISSING


def _require_alias(
    obj: dict[str, Any], names: tuple[str, ...], shard_id: str | None = None
) -> Any:
    value = _alias(obj, names)
    if value is _MISSING:
        fail(
            "E_MANIFEST_MISMATCH",
            f"manifest field is required: {names[0]}",
            shard_id=shard_id,
            stage="ingest",
            field=names[0],
            accepted_aliases=list(names),
        )
    return value


def _number(value: Any, field_name: str, shard_id: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        fail(
            "E_MANIFEST_MISMATCH",
            f"manifest field {field_name} must be numeric",
            shard_id=shard_id,
            stage="ingest",
            field=field_name,
        )
    value = float(value)
    if not math.isfinite(value):
        fail(
            "E_MANIFEST_MISMATCH",
            f"manifest field {field_name} must be finite",
            shard_id=shard_id,
            stage="ingest",
            field=field_name,
        )
    return value


def _integer(value: Any, field_name: str, shard_id: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        fail(
            "E_MANIFEST_MISMATCH",
            f"manifest field {field_name} must be an integer",
            shard_id=shard_id,
            stage="ingest",
            field=field_name,
        )
    return value


def parse_shard(obj: dict[str, Any]) -> ShardSpec:
    if not isinstance(obj, dict):
        fail("E_MANIFEST_MISMATCH", "each shard must be an object", stage="ingest")
    shard_id = _require_alias(obj, ("shard_id", "id", "segment_id"))
    if not isinstance(shard_id, str) or not shard_id:
        fail("E_MANIFEST_MISMATCH", "shard_id must be a non-empty string", stage="ingest")
    wav = _require_alias(obj, ("wav", "wav_path", "file"), shard_id)
    if not isinstance(wav, str) or not wav:
        fail(
            "E_MANIFEST_MISMATCH",
            "wav must be a non-empty path",
            shard_id=shard_id,
            stage="ingest",
        )
    transcript = obj.get("transcript", "")
    if not isinstance(transcript, str):
        fail(
            "E_MANIFEST_MISMATCH",
            "transcript must be a string",
            shard_id=shard_id,
            stage="ingest",
        )
    ref_obj = _alias(
        obj,
        (
            "reference_pitch_hz",
            "reference_pitch_mean_hz",
            "reference_pitch_mean_interval_hz",
            "reference_f0_hz",
        ),
    )
    if ref_obj is _MISSING:
        ref_obj = None
    reference = None
    if ref_obj is not None:
        if isinstance(ref_obj, (list, tuple)) and len(ref_obj) == 2:
            ref_obj = {"min_hz": ref_obj[0], "max_hz": ref_obj[1]}
        if not isinstance(ref_obj, dict):
            fail(
                "E_MANIFEST_MISMATCH",
                "reference pitch must be an object or a [min, max] pair",
                shard_id=shard_id,
                stage="ingest",
            )
        lo_value = _alias(ref_obj, ("min_hz", "min", "lower_hz"))
        hi_value = _alias(ref_obj, ("max_hz", "max", "upper_hz"))
        if lo_value is _MISSING or hi_value is _MISSING:
            fail(
                "E_MANIFEST_MISMATCH",
                "reference pitch must contain min/min_hz and max/max_hz",
                shard_id=shard_id,
                stage="ingest",
            )
        lo = _number(lo_value, "reference_pitch_hz.min_hz", shard_id)
        hi = _number(hi_value, "reference_pitch_hz.max_hz", shard_id)
        if hi < lo:
            fail(
                "E_MANIFEST_MISMATCH",
                "reference pitch interval is reversed",
                shard_id=shard_id,
                stage="ingest",
            )
        reference = ReferencePitch(lo, hi)
    tolerance = _alias(obj, ("reference_tolerance_hz", "tolerance_hz"))
    if tolerance is _MISSING:
        tolerance = None
    tolerance_value = None if tolerance is None else _number(
        tolerance, "reference_tolerance_hz", shard_id
    )
    if tolerance_value is not None and tolerance_value < 0:
        fail(
            "E_MANIFEST_MISMATCH",
            "reference_tolerance_hz must be non-negative",
            shard_id=shard_id,
            stage="ingest",
        )
    spec = ShardSpec(
        shard_id=shard_id,
        wav=wav,
        source_speaker=str(
            _require_alias(obj, ("source_speaker", "source_speaker_id", "source"), shard_id)
        ),
        target_speaker=str(
            _require_alias(obj, ("target_speaker", "target_speaker_id", "target"), shard_id)
        ),
        pitch_shift=_number(
            _require_alias(
                obj, ("pitch_shift", "pitch_shift_factor", "f0_shift"), shard_id
            ),
            "pitch_shift",
            shard_id,
        ),
        mel_warp=_number(
            _require_alias(obj, ("mel_warp", "spectral_warp", "envelope_warp"), shard_id),
            "mel_warp",
            shard_id,
        ),
        frame_shift=_integer(
            _require_alias(obj, ("frame_shift", "hop", "hop_size", "frame_hop"), shard_id),
            "frame_shift",
            shard_id,
        ),
        quant_bits=_integer(obj.get("quant_bits", 16), "quant_bits", shard_id),
        expected_output_samples=_integer(
            _require_alias(
                obj,
                ("expected_output_samples", "expected_samples", "max_output_samples"),
                shard_id,
            ),
            "expected_output_samples",
            shard_id,
        ),
        reference_pitch=reference,
        reference_tolerance_hz=tolerance_value,
        sample_rate=_integer(obj.get("sample_rate", obj.get("declared_sample_rate", 16000)), "sample_rate", shard_id),
        channels=_integer(obj.get("channels", 1), "channels", shard_id),
        sample_count=_integer(
            _require_alias(obj, ("sample_count", "num_samples"), shard_id),
            "sample_count",
            shard_id,
        ),
        transcript=transcript,
        raw=dict(obj),
    )
    if spec.frame_shift <= 0:
        fail(
            "E_INVALID_PARAM",
            "frame_shift must be positive",
            shard_id=shard_id,
            stage="analysis",
            value=spec.frame_shift,
        )
    return spec


def parse_manifest(data: Any, *, path: Path | None = None) -> Manifest:
    if isinstance(data, list):
        data = {"version": MANIFEST_SCHEMA, "shards": data}
    if not isinstance(data, dict):
        fail("E_MANIFEST_MISMATCH", "manifest root must be an object", stage="ingest")
    version = data.get("version", MANIFEST_SCHEMA)
    shards_obj = _alias(data, ("shards", "segments", "items"))
    if shards_obj is _MISSING:
        shards_obj = None
    if not isinstance(shards_obj, list) or not shards_obj:
        fail(
            "E_MANIFEST_MISMATCH",
            "manifest must contain a non-empty shards list",
            stage="ingest",
        )
    shards = [parse_shard(item) for item in shards_obj]
    seen: set[str] = set()
    for spec in shards:
        if spec.shard_id in seen:
            fail(
                "E_MANIFEST_MISMATCH",
                "duplicate shard_id in manifest",
                shard_id=spec.shard_id,
                stage="ingest",
            )
        seen.add(spec.shard_id)
        if path is not None:
            spec.manifest_path = path
    return Manifest(version=str(version), shards=shards, raw=dict(data), path=path)


def load_manifest(path: Path) -> Manifest:
    try:
        data = read_json(path)
    except FileNotFoundError:
        fail(
            "E_WORKSPACE_NOT_READY",
            f"manifest not found: {path}",
            stage="ingest",
            path=str(path),
        )
    except Exception as exc:
        raise WorkbenchError(
            "E_MANIFEST_MISMATCH",
            f"cannot parse manifest: {exc}",
            stage="ingest",
            details={"path": str(path)},
        ) from exc
    return parse_manifest(data, path=path)
