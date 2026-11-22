"""Stage 3/4: deterministic mask initialization and iterative refinement."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Sequence

import numpy as np

from .config import SeparationConfig
from .errors import numeric_error
from .features import BandFeatures
from .manifest import SourceTruth
from .util import digest_array, digest_json, fixed_hash_unit


EPS = 1e-12


@dataclass
class MaskState:
    masks: np.ndarray
    digest: str

    def summary(self) -> dict[str, Any]:
        return {"shape": list(self.masks.shape), "mask_digest": self.digest}


def source_priors(
    sources: Sequence[SourceTruth],
    config: SeparationConfig,
) -> np.ndarray:
    """Fixed prior matrix with shape (source, band)."""
    if not sources:
        raise numeric_error(
            "at least one source truth entry is required",
            slice_id="<unknown>",
            band_index=0,
            frame_index=0,
        )
    edges = np.linspace(0.0, float(config.nyquist_hz), config.band_count + 1)
    priors = np.empty((len(sources), config.band_count), dtype=np.float64)
    band_width = float(config.band_width_hz)
    for source_index, source in enumerate(sources):
        if not np.isfinite(source.f0_min_hz) or not np.isfinite(source.f0_max_hz):
            raise numeric_error(
                "source f0 interval contains NaN or Inf",
                slice_id="<unknown>",
                band_index=0,
                frame_index=0,
            )
        if source.f0_max_hz < source.f0_min_hz:
            raise numeric_error(
                "source f0 interval is inverted",
                slice_id="<unknown>",
                band_index=0,
                frame_index=0,
            )
        for band in range(config.band_count):
            low = edges[band]
            high = edges[band + 1]
            if high < source.f0_min_hz:
                distance = source.f0_min_hz - high
            elif low > source.f0_max_hz:
                distance = low - source.f0_max_hz
            else:
                distance = 0.0
            scale = max(30.0, 0.55 * band_width, 0.75 * (source.f0_max_hz - source.f0_min_hz))
            prior = float(np.exp(-0.5 * (distance / scale) ** 2))
            prior = round(prior, 15)
            if prior < 1e-6:
                prior = 0.0
            if not source.visible:
                prior *= 0.55
            priors[source_index, band] = prior
    return priors


def _hash_projection(
    sources: Sequence[SourceTruth],
    frame_indices: np.ndarray,
    band_indices: np.ndarray,
    config: SeparationConfig,
) -> np.ndarray:
    projection = np.empty((len(sources), len(frame_indices), len(band_indices)), dtype=np.float64)
    for source_pos, source in enumerate(sources):
        for frame_pos, frame_index in enumerate(frame_indices.tolist()):
            for band_pos, band_index in enumerate(band_indices.tolist()):
                unit = fixed_hash_unit(config.projection_seed, source.source_id, int(frame_index), int(band_index))
                projection[source_pos, frame_pos, band_pos] = 0.18 + 0.82 * unit
    return projection


def _base_scores(
    features: np.ndarray,
    sources: Sequence[SourceTruth],
    config: SeparationConfig,
    *,
    global_frame_indices: np.ndarray,
    band_indices: np.ndarray,
) -> np.ndarray:
    priors = source_priors(sources, config)[:, band_indices]
    projection = _hash_projection(sources, global_frame_indices, band_indices, config)
    all_log_energy = features[:, :, 0]
    e_min = np.min(all_log_energy, axis=1, keepdims=True)
    e_max = np.max(all_log_energy, axis=1, keepdims=True)
    energy_rel_all = (all_log_energy - e_min) / (e_max - e_min + 1e-12)
    energy_rel = energy_rel_all[:, band_indices]
    energy_factor = 0.22 + 0.78 * energy_rel
    flatness = features[:, band_indices, 1]
    flat_factor = 0.55 + 0.45 * np.clip(flatness, 0.0, 1.0)
    scores = (
        priors[:, None, :]
        * projection
        * energy_factor[None, :, :]
        * flat_factor[None, :, :]
    )
    scores[~np.isfinite(scores)] = 0.0
    return np.maximum(scores, 0.0)


def normalize_scores(scores: np.ndarray) -> np.ndarray:
    total = np.sum(scores, axis=0, keepdims=True)
    fallback = np.full_like(scores, 1.0 / max(1, scores.shape[0]))
    return np.divide(scores, total, out=fallback, where=total > EPS)


def initialize_masks(
    features: BandFeatures,
    sources: Sequence[SourceTruth],
    config: SeparationConfig,
    *,
    slice_id: str,
    global_frame_indices: np.ndarray | None = None,
    band_indices: Iterable[int] | None = None,
    existing: np.ndarray | None = None,
) -> MaskState:
    data = np.asarray(features.features)
    if data.ndim != 3 or data.shape[1] != config.band_count or data.shape[2] != 2:
        raise numeric_error(
            "feature matrix dimensions do not match band_count",
            slice_id=slice_id,
            band_index=0,
            frame_index=0,
        )
    if len(sources) > config.max_sources:
        raise numeric_error(
            "source count exceeds max_sources",
            slice_id=slice_id,
            band_index=0,
            frame_index=0,
            source_count=len(sources),
            max_sources=config.max_sources,
        )
    frames = (
        np.arange(data.shape[0], dtype=np.int64)
        if global_frame_indices is None
        else np.asarray(global_frame_indices, dtype=np.int64)
    )
    bands = (
        np.arange(config.band_count, dtype=np.int64)
        if band_indices is None
        else np.asarray(list(band_indices), dtype=np.int64)
    )
    if len(frames) != data.shape[0]:
        raise numeric_error(
            "frame index vector does not match feature matrix",
            slice_id=slice_id,
            band_index=0,
            frame_index=0,
        )
    scores = _base_scores(data, sources, config, global_frame_indices=frames, band_indices=bands)
    new_band_masks = normalize_scores(scores)
    if existing is None:
        masks = np.empty((len(sources), data.shape[0], config.band_count), dtype=np.float64)
        masks[:, :, bands] = new_band_masks
    else:
        masks = np.array(existing, dtype=np.float64, copy=True)
        if masks.shape != (len(sources), data.shape[0], config.band_count):
            raise numeric_error(
                "existing mask shape is incompatible with feature matrix",
                slice_id=slice_id,
                band_index=0,
                frame_index=0,
            )
        masks[:, :, bands] = new_band_masks
    return MaskState(masks=masks, digest=mask_digest(masks))


def refine_round(
    masks: np.ndarray,
    features: np.ndarray,
    sources: Sequence[SourceTruth],
    config: SeparationConfig,
    *,
    slice_id: str,
    global_frame_indices: np.ndarray,
    band_indices: Iterable[int] | None = None,
) -> tuple[np.ndarray, np.ndarray]:
    """Run one deterministic source-ordered refinement round on a frame block."""
    current = np.asarray(masks)
    data = np.asarray(features)
    if current.ndim != 3 or current.shape[0] != len(sources) or current.shape[1] != data.shape[0]:
        raise numeric_error(
            "mask shape does not match feature/source dimensions",
            slice_id=slice_id,
            band_index=0,
            frame_index=0,
        )
    if data.ndim != 3 or data.shape[1] != config.band_count or data.shape[2] != 2:
        raise numeric_error(
            "feature matrix dimensions do not match band_count",
            slice_id=slice_id,
            band_index=0,
            frame_index=0,
        )
    bands = (
        np.arange(config.band_count, dtype=np.int64)
        if band_indices is None
        else np.asarray(list(band_indices), dtype=np.int64)
    )
    before = current[:, :, bands].copy()
    base = _base_scores(
        data,
        sources,
        config,
        global_frame_indices=np.asarray(global_frame_indices, dtype=np.int64),
        band_indices=bands,
    )
    working = current[:, :, bands].copy()
    for source_pos in range(len(sources)):
        others = np.sum(working, axis=0, keepdims=True) - working[source_pos : source_pos + 1]
        suppression = 0.52 + 0.48 * (1.0 - np.clip(others, 0.0, 1.0))
        working[source_pos : source_pos + 1] = base[source_pos : source_pos + 1] * suppression
        working = normalize_scores(working)
    current[:, :, bands] = working
    change = np.sum(np.abs(current[:, :, bands] - before), axis=(0, 1))
    return current, change


def mask_digest(masks: np.ndarray) -> str:
    return digest_json({"masks": digest_array(np.asarray(masks))})


def masks_input_fingerprint(features_input_fp: str, sources: Sequence[SourceTruth], config: SeparationConfig) -> str:
    source_payload = [
        {
            "source_id": int(source.source_id),
            "f0_min_hz": float(source.f0_min_hz),
            "f0_max_hz": float(source.f0_max_hz),
            "visible": bool(source.visible),
        }
        for source in sources
    ]
    return digest_json(
        {
            "features_input_fingerprint": features_input_fp,
            "sources": source_payload,
            "stage_config": config.stage_fingerprint("mask_init"),
        }
    )


def refinement_input_fingerprint(masks_input_fp: str, config: SeparationConfig) -> str:
    return digest_json(
        {
            "masks_input_fingerprint": masks_input_fp,
            "stage_config": config.stage_fingerprint("mask_refine"),
        }
    )


def changed_bands_for_sources(
    old_sources: Sequence[SourceTruth],
    new_sources: Sequence[SourceTruth],
    config: SeparationConfig,
) -> list[int]:
    if len(old_sources) != len(new_sources):
        return list(range(config.band_count))
    old_priors = source_priors(old_sources, config)
    new_priors = source_priors(new_sources, config)
    changed = np.any(old_priors != new_priors, axis=0)
    return np.flatnonzero(changed).astype(int).tolist()

