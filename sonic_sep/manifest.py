"""WAV manifest parsing, truth validation, and audio loading."""

from __future__ import annotations

import copy
import hashlib
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping

import numpy as np
import soundfile as sf

from .errors import manifest_error, numeric_error
from .util import digest_json, json_safe


@dataclass(frozen=True)
class SourceTruth:
    source_id: int
    f0_min_hz: float
    f0_max_hz: float
    visible: bool = True

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SliceTruth:
    slice_id: str
    file: str
    path: Path
    n_samples: int
    sample_rate: int
    sources: tuple[SourceTruth, ...]
    duration_seconds: float
    sha256: str
    channels: int = 1
    subtype: str = "PCM_16"
    declared_sha256: str | None = None
    sha_matches_manifest: bool = True

    @property
    def source_count(self) -> int:
        return len(self.sources)

    def to_dict(self) -> dict[str, Any]:
        return {
            "slice_id": self.slice_id,
            "file": self.file,
            "n_samples": self.n_samples,
            "sample_rate": self.sample_rate,
            "duration_seconds": self.duration_seconds,
            "source_count": self.source_count,
            "sources": [source.to_dict() for source in self.sources],
            "sha256": self.sha256,
            "channels": self.channels,
            "subtype": self.subtype,
            "declared_sha256": self.declared_sha256,
            "sha_matches_manifest": self.sha_matches_manifest,
        }


@dataclass(frozen=True)
class AudioManifest:
    path: Path
    seed: int
    sample_rate: int
    total_samples: int
    slices: tuple[SliceTruth, ...]
    raw: Mapping[str, Any] = field(repr=False, compare=False)

    def get(self, slice_id: str) -> SliceTruth:
        for item in self.slices:
            if item.slice_id == slice_id:
                return item
        raise manifest_error(f"slice {slice_id!r} is not present in manifest", slice_id=slice_id)

    def slice_ids(self) -> list[str]:
        return [item.slice_id for item in self.slices]

    def digest(self) -> str:
        return digest_json(
            {
                "sample_rate": self.sample_rate,
                "total_samples": self.total_samples,
                "slices": [item.to_dict() for item in self.slices],
            }
        )

    def with_source_updates(
        self,
        updates: Mapping[str, Mapping[int, Mapping[str, float]]],
    ) -> "AudioManifest":
        """Return a manifest with per-slice/per-source f0 overrides applied."""
        raw = copy.deepcopy(dict(self.raw))
        for slice_obj in raw.get("slices", []):
            slice_id = str(slice_obj.get("slice_id"))
            for source_obj in slice_obj.get("sources", []):
                source_id = int(source_obj.get("source_id"))
                update = updates.get(slice_id, {}).get(source_id)
                if update:
                    if "f0_min_hz" in update:
                        source_obj["f0_min_hz"] = float(update["f0_min_hz"])
                    if "f0_max_hz" in update:
                        source_obj["f0_max_hz"] = float(update["f0_max_hz"])
                    if "visible" in update:
                        source_obj["visible"] = bool(update["visible"])
        return load_manifest_object(raw, base_dir=self.path.parent, source_path=self.path)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _resolve_slice_path(file_value: Any, base_dir: Path) -> Path:
    if not isinstance(file_value, str) or not file_value:
        raise manifest_error("slice file must be a non-empty string", param="file")
    candidate = Path(file_value)
    if not candidate.is_absolute():
        candidate = base_dir / candidate
    return candidate.resolve()


def _validate_sources(raw_sources: Any, slice_id: str, nyquist: int) -> tuple[SourceTruth, ...]:
    if not isinstance(raw_sources, list) or not (2 <= len(raw_sources) <= 3):
        raise manifest_error("each slice must contain exactly 2 or 3 sources", slice_id=slice_id)
    sources: list[SourceTruth] = []
    seen: set[int] = set()
    for raw_source in raw_sources:
        if not isinstance(raw_source, Mapping):
            raise manifest_error("source entry must be an object", slice_id=slice_id)
        source_id = raw_source.get("source_id")
        if isinstance(source_id, bool) or not isinstance(source_id, int) or source_id < 0:
            raise manifest_error("source_id must be a non-negative integer", slice_id=slice_id)
        if source_id in seen:
            raise manifest_error("source_id values must be unique", slice_id=slice_id)
        seen.add(source_id)
        try:
            f0_min = float(raw_source["f0_min_hz"])
            f0_max = float(raw_source["f0_max_hz"])
        except (KeyError, TypeError, ValueError) as exc:
            raise numeric_error(
                "source reference f0 interval is empty or malformed",
                slice_id=slice_id,
                band_index=0,
                frame_index=0,
            ) from exc
        if not np.isfinite(f0_min) or not np.isfinite(f0_max) or f0_min <= 0 or f0_max <= 0:
            raise numeric_error(
                "source reference f0 values must be finite and positive",
                slice_id=slice_id,
                band_index=0,
                frame_index=0,
            )
        if f0_max < f0_min:
            raise numeric_error(
                "source reference f0 upper bound must be greater than or equal to lower bound",
                slice_id=slice_id,
                band_index=0,
                frame_index=0,
            )
        if f0_min > nyquist:
            raise numeric_error(
                "source reference f0 lower bound lies above Nyquist",
                slice_id=slice_id,
                band_index=0,
                frame_index=0,
            )
        visible = raw_source.get("visible", True)
        if not isinstance(visible, bool):
            raise manifest_error("source visibility must be boolean", slice_id=slice_id)
        sources.append(SourceTruth(int(source_id), f0_min, f0_max, visible))
    sources.sort(key=lambda item: item.source_id)
    if [item.source_id for item in sources] != list(range(len(sources))):
        raise manifest_error("source ids must be contiguous starting at zero", slice_id=slice_id)
    return tuple(sources)


def load_manifest_object(raw: Any, *, base_dir: Path, source_path: Path | None = None) -> AudioManifest:
    if not isinstance(raw, Mapping):
        raise manifest_error("manifest root must be an object")
    sample_rate = raw.get("sample_rate", 16_000)
    if isinstance(sample_rate, bool) or not isinstance(sample_rate, int) or sample_rate <= 0:
        raise manifest_error("manifest sample_rate must be a positive integer")
    if sample_rate % 2:
        raise manifest_error("manifest sample_rate must be even")
    raw_slices = raw.get("slices")
    if not isinstance(raw_slices, list) or len(raw_slices) < 1:
        raise manifest_error("manifest slices must be a non-empty list")
    slices: list[SliceTruth] = []
    seen_slices: set[str] = set()
    for raw_slice in raw_slices:
        if not isinstance(raw_slice, Mapping):
            raise manifest_error("slice entry must be an object")
        slice_id = raw_slice.get("slice_id")
        if not isinstance(slice_id, str) or not slice_id:
            raise manifest_error("slice_id must be a non-empty string")
        if slice_id in seen_slices:
            raise manifest_error("slice_id values must be unique", slice_id=slice_id)
        seen_slices.add(slice_id)
        path = _resolve_slice_path(raw_slice.get("file"), base_dir)
        if not path.is_file():
            raise manifest_error("slice WAV file does not exist", slice_id=slice_id, file=str(path))
        try:
            info = sf.info(path)
        except RuntimeError as exc:
            raise manifest_error(f"cannot read WAV header: {exc}", slice_id=slice_id, file=str(path)) from exc
        if info.samplerate != sample_rate:
            raise manifest_error(
                "WAV sample rate does not match manifest",
                slice_id=slice_id,
                expected= sample_rate,
                actual=info.samplerate,
            )
        if info.channels != 1:
            raise manifest_error("WAV must be mono", slice_id=slice_id, actual_channels=info.channels)
        if info.subtype != "PCM_16":
            raise manifest_error("WAV must be 16-bit PCM", slice_id=slice_id, actual_subtype=info.subtype)
        n_samples = raw_slice.get("n_samples", info.frames)
        if isinstance(n_samples, bool) or not isinstance(n_samples, int) or n_samples <= 0:
            raise manifest_error("slice n_samples must be a positive integer", slice_id=slice_id)
        if n_samples != info.frames:
            raise manifest_error(
                "manifest n_samples does not match WAV frame count",
                slice_id=slice_id,
                declared=n_samples,
                actual=info.frames,
            )
        duration = n_samples / sample_rate
        if not (2.0 <= duration <= 4.0):
            raise manifest_error(
                "slice duration must be within 2 to 4 seconds",
                slice_id=slice_id,
                duration_seconds=duration,
            )
        sources = _validate_sources(raw_slice.get("sources"), slice_id, sample_rate // 2)
        declared_source_count = raw_slice.get("source_count")
        if declared_source_count is not None and declared_source_count != len(sources):
            raise manifest_error(
                "source_count does not match sources list",
                slice_id=slice_id,
                declared=declared_source_count,
                actual=len(sources),
            )
        digest = _sha256_file(path)
        declared_digest = raw_slice.get("sha256")
        declared_text = None if declared_digest is None else str(declared_digest)
        sha_matches = declared_text is None or declared_text == digest
        slices.append(
            SliceTruth(
                slice_id=slice_id,
                file=str(raw_slice.get("file")),
                path=path,
                n_samples=n_samples,
                sample_rate=sample_rate,
                sources=sources,
                duration_seconds=duration,
                sha256=digest,
                channels=info.channels,
                subtype=info.subtype,
                declared_sha256=declared_text,
                sha_matches_manifest=sha_matches,
            )
        )
    total_samples = raw.get("total_samples")
    actual_total = sum(item.n_samples for item in slices)
    if total_samples is None:
        raise manifest_error("manifest total_samples is required for consistency checks")
    if isinstance(total_samples, bool) or not isinstance(total_samples, int) or total_samples <= 0:
        raise manifest_error("manifest total_samples must be a positive integer")
    if total_samples != actual_total:
        raise manifest_error(
            "manifest total_samples does not equal the sum of slice sample counts",
            declared=total_samples,
            actual=actual_total,
        )
    slice_count = raw.get("slice_count")
    if slice_count is not None and slice_count != len(slices):
        raise manifest_error(
            "slice_count does not match slices list",
            declared=slice_count,
            actual=len(slices),
        )
    seed = raw.get("seed", 0)
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise manifest_error("manifest seed must be an integer")
    return AudioManifest(
        path=source_path or Path("<memory>"),
        seed=seed,
        sample_rate=sample_rate,
        total_samples=actual_total,
        slices=tuple(slices),
        raw=raw,
    )


def load_manifest(path: str | Path) -> AudioManifest:
    manifest_path = Path(path).resolve()
    try:
        import json

        with open(manifest_path, "r", encoding="utf-8") as handle:
            raw = json.load(handle)
    except OSError as exc:
        raise manifest_error(f"cannot read manifest: {exc}", file=str(manifest_path)) from exc
    except ValueError as exc:
        raise manifest_error(f"invalid JSON manifest: {exc}", file=str(manifest_path)) from exc
    return load_manifest_object(raw, base_dir=manifest_path.parent, source_path=manifest_path)


def load_slice_audio(slice_truth: SliceTruth) -> np.ndarray:
    try:
        audio, sample_rate = sf.read(slice_truth.path, dtype="int16", always_2d=False)
    except RuntimeError as exc:
        raise manifest_error(f"cannot read WAV data: {exc}", slice_id=slice_truth.slice_id) from exc
    if sample_rate != slice_truth.sample_rate or audio.ndim != 1 or audio.size != slice_truth.n_samples:
        raise manifest_error("WAV data no longer matches manifest", slice_id=slice_truth.slice_id)
    return np.asarray(audio, dtype=np.int16)


def effective_manifest(
    manifest: AudioManifest,
    overrides: Mapping[str, Mapping[int, Mapping[str, float]]] | None,
) -> AudioManifest:
    if not overrides:
        return manifest
    return manifest.with_source_updates(overrides)


def verify_manifest_files(manifest: AudioManifest) -> dict[str, Any]:
    checked = []
    for item in manifest.slices:
        actual = _sha256_file(item.path)
        checked.append(
            {
                "slice_id": item.slice_id,
                "sha256": actual,
                "declared_sha256": item.declared_sha256,
                "matches": item.sha_matches_manifest and actual == item.sha256,
            }
        )
    return {"ok": all(item["matches"] for item in checked), "slices": checked}

