"""Configuration and reference-truth loading with strict validation."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from .errors import E_INVALID_CONFIG, FingerprintError
from .jsonutil import read_json, sha256_file, version_fingerprint


PARAMETER_FIELDS = (
    "frame_length",
    "hop_length",
    "neighborhood",
    "min_freq_bin",
    "time_delta_min",
    "time_delta_max",
    "freq_delta_min",
    "freq_delta_max",
    "quantization_bits",
    "max_peaks_per_frame",
    "max_pairs",
    "targets_per_anchor",
    "bucket_width",
    "expected_false_match_rate",
    "confidence_threshold",
    "max_collision_rate",
    "peak_amplitude_floor",
    "hash_seed",
    "pairing_policy",
)


DEFAULTS: dict[str, Any] = {
    "frame_length": 1024,
    "hop_length": 256,
    "neighborhood": 2,
    "min_freq_bin": 1,
    "time_delta_min": 1,
    "time_delta_max": 12,
    "freq_delta_min": 2,
    "freq_delta_max": 96,
    "quantization_bits": 9,
    "max_peaks_per_frame": 6,
    "max_pairs": 400,
    "targets_per_anchor": 3,
    "bucket_width": 4096,
    "expected_false_match_rate": 0.9,
    "confidence_threshold": 0.45,
    "max_collision_rate": 0.25,
    "peak_amplitude_floor": 0.00001,
    "hash_seed": 20261123,
    "pairing_policy": "bounded_no_wrap",
}


def _config_error(message: str, shard_id: str | None = None, field: str | None = None) -> FingerprintError:
    details: dict[str, Any] = {}
    if field is not None:
        details["field"] = field
    return FingerprintError(E_INVALID_CONFIG, message, shard_id=shard_id, stage="config", details=details)


def _require_int(value: Any, field: str, minimum: int, shard_id: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise _config_error(f"configuration field {field} must be an integer", shard_id, field)
    if value < minimum:
        raise _config_error(f"configuration field {field} must be >= {minimum}", shard_id, field)
    return value


def _require_float(value: Any, field: str, minimum: float, maximum: float, shard_id: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _config_error(f"configuration field {field} must be numeric", shard_id, field)
    number = float(value)
    if not minimum <= number <= maximum:
        raise _config_error(
            f"configuration field {field} must be in [{minimum}, {maximum}]", shard_id, field
        )
    return number


@dataclass(frozen=True)
class ShardConfig:
    shard_id: str
    wav_path: str
    reference_path: str
    frame_length: int
    hop_length: int
    neighborhood: int
    min_freq_bin: int
    time_delta_min: int
    time_delta_max: int
    freq_delta_min: int
    freq_delta_max: int
    quantization_bits: int
    max_peaks_per_frame: int
    max_pairs: int
    targets_per_anchor: int
    bucket_width: int
    expected_false_match_rate: float
    confidence_threshold: float
    max_collision_rate: float
    peak_amplitude_floor: float
    hash_seed: int
    pairing_policy: str

    def auditable_dict(self) -> dict[str, Any]:
        return {"shard_id": self.shard_id, **{field: getattr(self, field) for field in PARAMETER_FIELDS}}

    def peak_params(self) -> dict[str, Any]:
        return {
            "frame_length": self.frame_length,
            "hop_length": self.hop_length,
            "neighborhood": self.neighborhood,
            "min_freq_bin": self.min_freq_bin,
            "max_peaks_per_frame": self.max_peaks_per_frame,
            "peak_amplitude_floor": self.peak_amplitude_floor,
            "algorithm": "hanning-rfft-local-max-2d-v1",
        }

    def pair_params(self) -> dict[str, Any]:
        return {
            "time_delta_min": self.time_delta_min,
            "time_delta_max": self.time_delta_max,
            "freq_delta_min": self.freq_delta_min,
            "freq_delta_max": self.freq_delta_max,
            "max_pairs": self.max_pairs,
            "targets_per_anchor": self.targets_per_anchor,
            "pairing_policy": self.pairing_policy,
        }

    def quant_params(self) -> dict[str, Any]:
        return {
            "quantization_bits": self.quantization_bits,
            "bucket_width": self.bucket_width,
            "hash_seed": self.hash_seed,
            "hash_algorithm": "fnv1a64-v1",
            "max_collision_rate": self.max_collision_rate,
        }

    def audit_params(self) -> dict[str, Any]:
        return {
            "expected_false_match_rate": self.expected_false_match_rate,
            "confidence_threshold": self.confidence_threshold,
        }

    def config_version(self, reference_digest: str) -> str:
        return version_fingerprint("audiofp.config.v1", self.auditable_dict(), reference_digest)

    def snapshot(self) -> dict[str, Any]:
        return {
            **self.auditable_dict(),
            "wav": self.wav_path,
            "reference": self.reference_path,
        }


@dataclass(frozen=True)
class Manifest:
    path: str
    schema: str
    seed: int | None
    shards: tuple[ShardConfig, ...]

    def by_id(self) -> dict[str, ShardConfig]:
        return {shard.shard_id: shard for shard in self.shards}


@dataclass(frozen=True)
class ReferencePair:
    anchor_frame: int
    anchor_freq: int
    target_frame: int
    target_freq: int
    dt: int

    def as_dict(self) -> dict[str, int]:
        return {
            "anchor_frame": self.anchor_frame,
            "anchor_freq": self.anchor_freq,
            "target_frame": self.target_frame,
            "target_freq": self.target_freq,
            "dt": self.dt,
        }


def _resolve(base: Path, value: str) -> str:
    candidate = Path(value)
    if not candidate.is_absolute():
        candidate = base / candidate
    return str(candidate.resolve())


def load_manifest(path: str | Path) -> Manifest:
    manifest_path = Path(path).resolve()
    try:
        payload = read_json(manifest_path)
    except (OSError, json.JSONDecodeError) as exc:
        raise _config_error(f"cannot load manifest: {exc}") from exc
    if not isinstance(payload, dict):
        raise _config_error("manifest root must be an object")
    schema = payload.get("schema", "audiofp.manifest.v1")
    if schema != "audiofp.manifest.v1":
        raise _config_error("unsupported manifest schema", field="schema")
    defaults = payload.get("defaults", {})
    if not isinstance(defaults, dict):
        raise _config_error("defaults must be an object", field="defaults")
    unknown_defaults = sorted(set(defaults) - set(PARAMETER_FIELDS))
    if unknown_defaults:
        raise _config_error("unknown defaults fields", field=unknown_defaults[0])
    effective_defaults = {**DEFAULTS, **defaults}
    top_reference = payload.get("reference_pairs")
    raw_shards = payload.get("shards")
    if not isinstance(raw_shards, list) or not raw_shards:
        raise _config_error("manifest must declare at least one shard", field="shards")

    shards: list[ShardConfig] = []
    seen: set[str] = set()
    for index, raw in enumerate(raw_shards):
        if not isinstance(raw, dict):
            raise _config_error(f"shard entry {index} must be an object")
        shard_id = raw.get("shard_id")
        if not isinstance(shard_id, str) or not shard_id.strip():
            raise _config_error(f"shard entry {index} has no valid shard_id")
        if shard_id in seen:
            raise _config_error("duplicate shard_id", shard_id, "shard_id")
        seen.add(shard_id)
        wav_value = raw.get("wav")
        if not isinstance(wav_value, str) or not wav_value:
            raise _config_error("shard must declare wav", shard_id, "wav")
        reference_value = raw.get("reference", top_reference)
        if not isinstance(reference_value, str) or not reference_value:
            raise _config_error("shard must declare a reference pair file", shard_id, "reference")
        values = dict(effective_defaults)
        values.update({key: value for key, value in raw.items() if key in PARAMETER_FIELDS})

        frame_length = _require_int(values["frame_length"], "frame_length", 64, shard_id)
        hop_length = _require_int(values["hop_length"], "hop_length", 1, shard_id)
        if hop_length > frame_length:
            raise _config_error("hop_length must not exceed frame_length", shard_id, "hop_length")
        neighborhood = _require_int(values["neighborhood"], "neighborhood", 0, shard_id)
        min_freq_bin = _require_int(values["min_freq_bin"], "min_freq_bin", 0, shard_id)
        time_min = _require_int(values["time_delta_min"], "time_delta_min", 1, shard_id)
        time_max = _require_int(values["time_delta_max"], "time_delta_max", 1, shard_id)
        if time_max < time_min:
            raise _config_error("time_delta_max must be >= time_delta_min", shard_id, "time_delta_max")
        freq_min = _require_int(values["freq_delta_min"], "freq_delta_min", 0, shard_id)
        freq_max = _require_int(values["freq_delta_max"], "freq_delta_max", 0, shard_id)
        if freq_max < freq_min:
            raise _config_error("freq_delta_max must be >= freq_delta_min", shard_id, "freq_delta_max")
        bits = _require_int(values["quantization_bits"], "quantization_bits", 1, shard_id)
        if bits > 24:
            raise _config_error("quantization_bits above 24 is not supported", shard_id, "quantization_bits")
        max_peaks = _require_int(values["max_peaks_per_frame"], "max_peaks_per_frame", 1, shard_id)
        max_pairs = _require_int(values["max_pairs"], "max_pairs", 0, shard_id)
        targets = _require_int(values["targets_per_anchor"], "targets_per_anchor", 1, shard_id)
        bucket_width = _require_int(values["bucket_width"], "bucket_width", 1, shard_id)
        expected_rate = _require_float(
            values["expected_false_match_rate"], "expected_false_match_rate", 0.0, 1.0, shard_id
        )
        confidence = _require_float(values["confidence_threshold"], "confidence_threshold", 0.0, 1.0, shard_id)
        max_collision = _require_float(values["max_collision_rate"], "max_collision_rate", 0.0, 1.0, shard_id)
        floor = _require_float(values["peak_amplitude_floor"], "peak_amplitude_floor", 0.0, 1.0, shard_id)
        hash_seed = _require_int(values["hash_seed"], "hash_seed", 0, shard_id)
        policy = values["pairing_policy"]
        if policy != "bounded_no_wrap":
            raise _config_error(
                "only pairing_policy=bounded_no_wrap is supported", shard_id, "pairing_policy"
            )

        shards.append(
            ShardConfig(
                shard_id=shard_id,
                wav_path=_resolve(manifest_path.parent, wav_value),
                reference_path=_resolve(manifest_path.parent, reference_value),
                frame_length=frame_length,
                hop_length=hop_length,
                neighborhood=neighborhood,
                min_freq_bin=min_freq_bin,
                time_delta_min=time_min,
                time_delta_max=time_max,
                freq_delta_min=freq_min,
                freq_delta_max=freq_max,
                quantization_bits=bits,
                max_peaks_per_frame=max_peaks,
                max_pairs=max_pairs,
                targets_per_anchor=targets,
                bucket_width=bucket_width,
                expected_false_match_rate=expected_rate,
                confidence_threshold=confidence,
                max_collision_rate=max_collision,
                peak_amplitude_floor=floor,
                hash_seed=hash_seed,
                pairing_policy=policy,
            )
        )
    shards.sort(key=lambda item: item.shard_id)
    seed = payload.get("seed")
    if seed is not None and (isinstance(seed, bool) or not isinstance(seed, int)):
        raise _config_error("seed must be an integer", field="seed")
    return Manifest(path=str(manifest_path), schema=schema, seed=seed, shards=tuple(shards))


def load_reference_pairs(path: str | Path, shard_id: str) -> tuple[list[ReferencePair], str]:
    reference_path = Path(path)
    try:
        raw_bytes = reference_path.read_bytes()
        payload = json.loads(raw_bytes.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise _config_error(f"cannot load reference pairs: {exc}", shard_id, "reference") from exc
    if isinstance(payload, dict):
        if payload.get("schema") not in (None, "audiofp.reference.v1"):
            raise _config_error("unsupported reference pair schema", shard_id, "reference")
        shards = payload.get("shards")
        if not isinstance(shards, dict):
            raise _config_error("reference pairs must contain a shards object", shard_id, "reference")
        entries = shards.get(shard_id)
        if entries is None:
            entries = []
    elif isinstance(payload, list):
        entries = payload
    else:
        raise _config_error("reference pairs must be an object or array", shard_id, "reference")
    if not isinstance(entries, list):
        raise _config_error("reference shard entry must be an array", shard_id, "reference")

    pairs: list[ReferencePair] = []
    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise _config_error(f"reference pair {index} must be an object", shard_id, "reference")
        try:
            pair = ReferencePair(
                anchor_frame=int(entry["anchor_frame"]),
                anchor_freq=int(entry["anchor_freq"]),
                target_frame=int(entry["target_frame"]),
                target_freq=int(entry["target_freq"]),
                dt=int(entry.get("dt", int(entry["target_frame"]) - int(entry["anchor_frame"]))),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise _config_error(f"reference pair {index} is malformed", shard_id, "reference") from exc
        if pair.dt != pair.target_frame - pair.anchor_frame:
            raise _config_error(f"reference pair {index} has inconsistent dt", shard_id, "reference")
        if pair.anchor_frame < 0 or pair.target_frame < 0:
            raise _config_error(f"reference pair {index} has a negative frame", shard_id, "reference")
        if pair.anchor_freq < 0 or pair.target_freq < 0:
            raise _config_error(f"reference pair {index} has a negative frequency bin", shard_id, "reference")
        pairs.append(pair)
    pairs.sort(key=lambda item: (item.anchor_frame, item.anchor_freq, item.target_frame, item.target_freq))
    return pairs, sha256_file(reference_path)


def iter_parameter_values(shards: Iterable[ShardConfig]) -> dict[str, Any]:
    return {shard.shard_id: shard.auditable_dict() for shard in shards}

