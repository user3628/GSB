"""Stage 5: per-block inverse STFT contributions and overlap-add reconstruction."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Sequence

import numpy as np

from .config import SeparationConfig
from .errors import numeric_error
from .features import frequency_bin_to_band
from .masks import mask_digest
from .stft import hann_window
from .util import digest_array, digest_json


@dataclass(frozen=True)
class ProcessBlock:
    index: int
    frame_start: int
    frame_end: int

    @property
    def frame_count(self) -> int:
        return self.frame_end - self.frame_start

    def to_dict(self) -> dict[str, int]:
        return {
            "index": self.index,
            "frame_start": self.frame_start,
            "frame_end": self.frame_end,
            "frame_count": self.frame_count,
        }


@dataclass(frozen=True)
class ReconstructionBlock:
    index: int
    core_frame_start: int
    core_frame_end: int
    frame_start: int
    frame_end: int
    sample_offset: int
    local_length: int
    frame_weights: np.ndarray

    def to_dict(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "core_frame_start": self.core_frame_start,
            "core_frame_end": self.core_frame_end,
            "frame_start": self.frame_start,
            "frame_end": self.frame_end,
            "sample_offset": self.sample_offset,
            "local_length": self.local_length,
        }


@dataclass
class ReconstructionResult:
    float_waveforms: np.ndarray
    int16_waveforms: np.ndarray
    norm: np.ndarray
    clipping: dict[str, Any]
    digest: str

    def summary(self) -> dict[str, Any]:
        return {
            "shape": list(self.int16_waveforms.shape),
            "reconstruction_digest": self.digest,
            "clipping": self.clipping,
        }


def make_process_blocks(n_frames: int, block_frames: int) -> list[ProcessBlock]:
    if n_frames <= 0 or block_frames <= 0:
        return []
    blocks: list[ProcessBlock] = []
    start = 0
    while start < n_frames:
        end = min(n_frames, start + block_frames)
        blocks.append(ProcessBlock(len(blocks), start, end))
        start = end
    return blocks


def make_reconstruction_blocks(
    process_blocks: Sequence[ProcessBlock],
    n_frames: int,
    n_samples: int,
    config: SeparationConfig,
) -> list[ReconstructionBlock]:
    if not process_blocks:
        return []
    halo = int(round(config.block_frames * config.block_overlap_percent / 100.0))
    halo = min(halo, max(0, config.block_frames - 1))
    spans: list[tuple[int, int]] = []
    for block in process_blocks:
        start = max(0, block.frame_start - halo)
        end = min(n_frames, block.frame_end + halo)
        spans.append((start, end))
    counts = np.zeros(n_frames, dtype=np.int64)
    for start, end in spans:
        counts[start:end] += 1
    blocks: list[ReconstructionBlock] = []
    for process_block, (start, end) in zip(process_blocks, spans):
        weight = 1.0 / counts[start:end].astype(np.float64)
        offset = start * config.hop_length
        local_length = min(n_samples - offset, (end - start - 1) * config.hop_length + config.frame_length)
        local_length = max(0, int(local_length))
        blocks.append(
            ReconstructionBlock(
                index=process_block.index,
                core_frame_start=process_block.frame_start,
                core_frame_end=process_block.frame_end,
                frame_start=start,
                frame_end=end,
                sample_offset=int(offset),
                local_length=local_length,
                frame_weights=weight,
            )
        )
    return blocks


def compute_ola_norm(
    recon_blocks: Sequence[ReconstructionBlock],
    n_samples: int,
    config: SeparationConfig,
) -> np.ndarray:
    window = hann_window(config.frame_length)
    squared = window * window
    norm = np.zeros(n_samples, dtype=np.float64)
    for block in recon_blocks:
        for local_frame, frame_index in enumerate(range(block.frame_start, block.frame_end)):
            start = int(frame_index * config.hop_length)
            stop = min(n_samples, start + config.frame_length)
            if stop <= start:
                continue
            norm[start:stop] += block.frame_weights[local_frame] * squared[: stop - start]
    return norm


def reconstruct_block(
    block: ReconstructionBlock,
    spectra: np.ndarray,
    masks: np.ndarray,
    *,
    slice_id: str,
    config: SeparationConfig,
) -> np.ndarray:
    """Return source-by-local-sample float contributions for one block."""
    n_sources = int(masks.shape[0])
    contribution = np.zeros((n_sources, block.local_length), dtype=np.float64)
    if block.local_length == 0:
        return contribution
    window = hann_window(config.frame_length)
    mapper = frequency_bin_to_band(config)
    for local_frame, frame_index in enumerate(range(block.frame_start, block.frame_end)):
        frame_start = int(frame_index * config.hop_length)
        local_start = frame_start - block.sample_offset
        available = min(config.frame_length, block.local_length - local_start)
        if available <= 0:
            continue
        weight = float(block.frame_weights[local_frame])
        frame_spectrum = spectra[frame_index]
        for source_index in range(n_sources):
            mask_per_bin = masks[source_index, frame_index, mapper]
            time_frame = np.fft.irfft(frame_spectrum * mask_per_bin, n=config.frame_length)
            contribution[source_index, local_start : local_start + available] += (
                time_frame[:available] * window[:available] * weight
            )
    return contribution


def reconstruct_block_digest(contribution: np.ndarray, block: ReconstructionBlock) -> str:
    return digest_json(
        {
            "contribution": digest_array(contribution),
            "sample_offset": block.sample_offset,
            "local_length": block.local_length,
        }
    )


def assemble_waveforms(
    recon_blocks: Sequence[ReconstructionBlock],
    contributions: Sequence[np.ndarray],
    norm: np.ndarray,
    n_samples: int,
    *,
    slice_id: str,
) -> tuple[np.ndarray, np.ndarray, dict[str, Any], str]:
    if len(recon_blocks) != len(contributions):
        raise numeric_error(
            "reconstruction block/contribution count mismatch",
            slice_id=slice_id,
            band_index=0,
            frame_index=0,
        )
    if not contributions:
        raise numeric_error(
            "no reconstruction contributions are available",
            slice_id=slice_id,
            band_index=0,
            frame_index=0,
        )
    n_sources = int(contributions[0].shape[0])
    accumulated = np.zeros((n_sources, n_samples), dtype=np.float64)
    for block, contribution in zip(recon_blocks, contributions):
        if contribution.shape != (n_sources, block.local_length):
            raise numeric_error(
                "contribution shape does not match its block",
                slice_id=slice_id,
                band_index=0,
                frame_index=block.frame_start,
            )
        stop = min(n_samples, block.sample_offset + block.local_length)
        length = stop - block.sample_offset
        accumulated[:, block.sample_offset:stop] += contribution[:, :length]
    valid = norm > 1e-12
    float_waveforms = np.zeros_like(accumulated)
    float_waveforms[:, valid] = accumulated[:, valid] / norm[valid]
    scaled = np.rint(float_waveforms * 32767.0)
    clipped = scaled < -32768.0
    clipped |= scaled > 32767.0
    int16_waveforms = np.clip(scaled, -32768.0, 32767.0).astype(np.int16)
    per_source = [int(np.count_nonzero(clipped[index])) for index in range(n_sources)]
    clipping = {
        "total_clipped_samples": int(np.count_nonzero(clipped)),
        "per_source": per_source,
        "clamped_range": [-32768, 32767],
        "normalization_floor": 1e-12,
    }
    digest = reconstruction_digest(int16_waveforms, norm, clipping)
    return float_waveforms, int16_waveforms, clipping, digest


def reconstruction_digest(
    int16_waveforms: np.ndarray,
    norm: np.ndarray,
    clipping: dict[str, Any],
) -> str:
    return digest_json(
        {
            "waveforms": digest_array(int16_waveforms),
            "norm": digest_array(norm),
            "clipping": clipping,
        }
    )


def reconstruction_input_fingerprint(refinement_input_fp: str, config: SeparationConfig) -> str:
    return digest_json(
        {
            "refinement_input_fingerprint": refinement_input_fp,
            "stage_config": config.stage_fingerprint("reconstruct"),
            "block_frames": config.block_frames,
            "block_overlap_percent": config.block_overlap_percent,
        }
    )


def round_changes_digest(round_changes: np.ndarray) -> str:
    return digest_json({"round_changes": digest_array(round_changes)})


def full_result_digest(
    int16_waveforms: np.ndarray,
    masks: np.ndarray,
    round_changes: np.ndarray,
    clipping: dict[str, Any],
) -> str:
    return digest_json(
        {
            "waveforms": digest_array(int16_waveforms),
            "masks": mask_digest(masks),
            "round_changes": round_changes_digest(round_changes),
            "clipping": clipping,
        }
    )

