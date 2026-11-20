"""Deterministic acoustic-to-PCM waveform synthesis."""

from __future__ import annotations

import io
import math
import wave
from dataclasses import dataclass
from typing import Any

import numpy as np

from .acoustic import AcousticResult, assert_finite
from .config import EngineConfig
from .utils import sha256_bytes


_MASK64 = (1 << 64) - 1


def _splitmix64(value: int) -> int:
    value = (value + 0x9E3779B97F4A7C15) & _MASK64
    value = (value ^ (value >> 30)) * 0xBF58476D1CE4E5B9 & _MASK64
    value = (value ^ (value >> 27)) * 0x94D049BB133111EB & _MASK64
    return (value ^ (value >> 31)) & _MASK64


def _noise_unit(seed: int, sample_index: int, channel: int) -> float:
    value = _splitmix64(seed ^ ((sample_index + 1) * 0x100000001B3) ^ (channel * 0x9E3779B1))
    return (value / float(_MASK64)) * 2.0 - 1.0


@dataclass(frozen=True)
class SynthesisResult:
    utt_id: str
    frame_start: int
    frame_end: int
    sample_start: int
    sample_end: int
    channels: int
    bit_depth: int
    sample_rate: int
    pcm: bytes
    clip_count: int

    @property
    def sample_count_per_channel(self) -> int:
        return self.sample_end - self.sample_start

    @property
    def digest(self) -> str:
        return sha256_bytes(self.pcm)

    def to_trace(self) -> dict[str, Any]:
        return {
            "frame_start": self.frame_start,
            "frame_end": self.frame_end,
            "sample_start": self.sample_start,
            "sample_end": self.sample_end,
            "sample_count_per_channel": self.sample_count_per_channel,
            "pcm_digest": self.digest,
            "clip_count": self.clip_count,
        }


def wav_bytes(*, pcm: bytes, config: EngineConfig) -> bytes:
    buffer = io.BytesIO()
    with wave.open(buffer, "wb") as wav:
        wav.setnchannels(config.output.channels)
        wav.setsampwidth(config.output.bytes_per_sample)
        wav.setframerate(config.output.sample_rate)
        wav.writeframes(pcm)
    return buffer.getvalue()


def write_wav(path, *, pcm: bytes, config: EngineConfig) -> None:
    from .utils import atomic_write_bytes

    atomic_write_bytes(path, wav_bytes(pcm=pcm, config=config))


def synth_chunk(
    *,
    utt_id: str,
    acoustic: AcousticResult,
    config: EngineConfig,
) -> SynthesisResult:
    assert_finite(utt_id=utt_id, values=acoustic.values, frame_offset=acoustic.frame_start)
    values = acoustic.values
    n_frames, n_columns = values.shape
    n_bands = n_columns - 2
    hop = config.hop_samples
    channels = config.output.channels
    sample_start = acoustic.frame_start * hop
    sample_end = acoustic.frame_end * hop
    total_samples = n_frames * hop
    output = np.empty((total_samples, channels), dtype=np.int16)
    clip_count = 0
    seed = config.synthesis.phase_seed
    noise_level = config.synthesis.noise_level

    for local_frame in range(n_frames):
        bands = values[local_frame, :n_bands]
        f0 = float(values[local_frame, n_bands])
        envelope = float(values[local_frame, n_bands + 1])
        if not (math.isfinite(f0) and math.isfinite(envelope)):
            assert_finite(utt_id=utt_id, values=values[local_frame : local_frame + 1], frame_offset=acoustic.frame_start + local_frame)
        amplitude_sum = float(sum(abs(float(x)) for x in bands))
        if amplitude_sum <= 1e-12:
            amplitude_sum = 1.0
        global_frame = acoustic.frame_start + local_frame
        base_sample = global_frame * hop
        for within in range(hop):
            global_sample = base_sample + within
            for channel in range(channels):
                phase_offset = 0.0 if channel == 0 else 0.37
                acc = 0.0
                for band_index in range(n_bands):
                    frequency = f0 * float(band_index + 1)
                    phase = (
                        2.0
                        * math.pi
                        * frequency
                        * float(global_sample)
                        / float(config.output.sample_rate)
                        + phase_offset
                    )
                    acc += float(bands[band_index]) * math.sin(phase)
                signal = envelope * acc / amplitude_sum
                signal += noise_level * envelope * _noise_unit(seed, global_sample, channel)
                if signal > 1.0:
                    signal = 1.0
                    clip_count += 1
                elif signal < -1.0:
                    signal = -1.0
                    clip_count += 1
                if signal >= 1.0:
                    quantized = 32767
                elif signal <= -1.0:
                    quantized = -32768
                else:
                    quantized = int(round(signal * 32767.0))
                output[local_frame * hop + within, channel] = quantized
    pcm = output.astype("<i2", copy=False).tobytes(order="C")
    return SynthesisResult(
        utt_id=utt_id,
        frame_start=acoustic.frame_start,
        frame_end=acoustic.frame_end,
        sample_start=sample_start,
        sample_end=sample_end,
        channels=channels,
        bit_depth=config.output.bit_depth,
        sample_rate=config.output.sample_rate,
        pcm=pcm,
        clip_count=clip_count,
    )


def synth_full(*, acoustic: AcousticResult, config: EngineConfig) -> SynthesisResult:
    return synth_chunk(utt_id=acoustic.utt_id, acoustic=acoustic, config=config)


def pcm_summary(
    *, pcm: bytes, sample_count: int, channels: int, clip_count: int, markers: list[str] | tuple[str, ...]
) -> dict[str, Any]:
    return {
        "pcm_digest": sha256_bytes(pcm),
        "pcm_bytes": len(pcm),
        "sample_count_per_channel": sample_count,
        "channels": channels,
        "clip_count": clip_count,
        "markers_digest": sha256_bytes("\n".join(markers).encode("utf-8")),
        "marker_count": len(markers),
    }
