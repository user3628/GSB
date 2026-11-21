from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Iterable

import numpy as np

from .profiles import SpeakerProfile


@dataclass(frozen=True)
class DSPConfig:
    sample_rate: int = 16000
    frame_length: int = 400
    fft_size: int = 512
    mel_bands: int = 32
    f0_min_hz: float = 70.0
    f0_max_hz: float = 450.0
    f0_peak_threshold: float = 0.28
    # Low-energy syllable boundaries are acoustically ambiguous. They are
    # deliberately reported as UNVOICED instead of being filled with a
    # confident (often octave-shifted) pitch estimate.
    silence_rms: float = 1.5e-2

    def to_dict(self) -> dict[str, Any]:
        return {
            "sample_rate": self.sample_rate,
            "frame_length": self.frame_length,
            "fft_size": self.fft_size,
            "mel_bands": self.mel_bands,
            "f0_min_hz": self.f0_min_hz,
            "f0_max_hz": self.f0_max_hz,
            "f0_peak_threshold": self.f0_peak_threshold,
            "silence_rms": self.silence_rms,
            "window": "hann",
            "mel_scale": "htk",
            "phase_projection": "blake2b-u64-v1",
            "synthesis": "hann-ola-normalized-v1",
        }


DEFAULT_DSP = DSPConfig()


def r10(value: float) -> float:
    """Stable JSON rounding, including conversion of negative zero to +0.0."""

    rounded = round(float(value), 10)
    if abs(rounded) < 5.0e-11:
        return 0.0
    return rounded


def _window(config: DSPConfig = DEFAULT_DSP) -> np.ndarray:
    return np.hanning(config.frame_length).astype(np.float64)


@lru_cache(maxsize=8)
def mel_filterbank(
    sample_rate: int = 16000,
    fft_size: int = 512,
    mel_bands: int = 32,
) -> np.ndarray:
    def hz_to_mel(freq: np.ndarray | float) -> np.ndarray:
        return 2595.0 * np.log10(1.0 + np.asarray(freq, dtype=np.float64) / 700.0)

    def mel_to_hz(mel: np.ndarray | float) -> np.ndarray:
        return 700.0 * (10.0 ** (np.asarray(mel, dtype=np.float64) / 2595.0) - 1.0)

    bins = np.linspace(0.0, sample_rate / 2.0, fft_size // 2 + 1, dtype=np.float64)
    mel_edges = mel_to_hz(np.linspace(hz_to_mel(0.0), hz_to_mel(sample_rate / 2.0), mel_bands + 2))
    filters = np.zeros((mel_bands, bins.size), dtype=np.float64)
    for band in range(mel_bands):
        left, center, right = mel_edges[band : band + 3]
        rising = (bins - left) / max(center - left, 1.0e-12)
        falling = (right - bins) / max(right - center, 1.0e-12)
        filters[band] = np.maximum(0.0, np.minimum(rising, falling))
    return filters


def frame_starts(sample_count: int, hop: int, frame_length: int = DEFAULT_DSP.frame_length) -> int:
    if sample_count <= frame_length:
        return 1
    return 1 + int(math.ceil((sample_count - frame_length) / hop))


def extract_frame(samples: np.ndarray, start: int, frame_length: int) -> np.ndarray:
    frame = np.zeros(frame_length, dtype=np.float64)
    if start >= samples.size:
        return frame
    available = min(frame_length, samples.size - start)
    frame[:available] = samples[start : start + available]
    return frame


def estimate_f0(frame: np.ndarray, config: DSPConfig = DEFAULT_DSP) -> float | None:
    """Autocorrelation F0 estimate. Silence/unreliable frames return None."""

    centered = frame - float(np.mean(frame))
    rms = float(np.sqrt(np.mean(centered * centered)))
    if not math.isfinite(rms) or rms < config.silence_rms:
        return None
    windowed = centered * _window(config)
    autocorr = np.correlate(windowed, windowed, mode="full")[config.frame_length - 1 :]
    r0 = float(autocorr[0])
    if not math.isfinite(r0) or r0 <= 0.0:
        return None
    lag_min = max(1, int(math.floor(config.sample_rate / config.f0_max_hz)))
    lag_max = min(
        config.frame_length - 2,
        int(math.ceil(config.sample_rate / config.f0_min_hz)),
    )
    if lag_max <= lag_min:
        return None
    region = autocorr[lag_min : lag_max + 1] / r0
    peak_offset = int(np.argmax(region))
    peak_value = float(region[peak_offset])
    if not math.isfinite(peak_value) or peak_value < config.f0_peak_threshold:
        return None
    peak = peak_offset + lag_min
    # Amplitude-modulated speech can make the two-period autocorrelation peak
    # stronger than the one-period peak. Prefer the half-lag peak when it is a
    # credible local maximum; this removes the common octave-down failure.
    if peak > 2 * lag_min:
        half = int(round(peak / 2.0))
        search_lo = max(lag_min, half - 2)
        search_hi = min(lag_max, half + 2)
        if search_hi >= search_lo:
            half_offset = int(np.argmax(autocorr[search_lo : search_hi + 1] / r0))
            half_peak = search_lo + half_offset
            half_value = float(autocorr[half_peak] / r0)
            if (
                half_value >= config.f0_peak_threshold
                and half_value >= 0.55 * peak_value
            ):
                peak = half_peak
                peak_value = half_value
    lag = float(peak)
    if 0 < peak < autocorr.size - 1:
        a = float(autocorr[peak - 1] / r0)
        b = float(autocorr[peak] / r0)
        c = float(autocorr[peak + 1] / r0)
        denom = a - 2.0 * b + c
        if abs(denom) > 1.0e-12:
            delta = 0.5 * (a - c) / denom
            if -0.5 <= delta <= 0.5:
                lag = peak + delta
    if lag <= 0.0:
        return None
    f0 = config.sample_rate / lag
    if not math.isfinite(f0) or not (config.f0_min_hz <= f0 <= config.f0_max_hz):
        return None
    return r10(f0)


def mel_log_energy(frame: np.ndarray, config: DSPConfig = DEFAULT_DSP) -> np.ndarray:
    centered = frame - float(np.mean(frame))
    windowed = centered * _window(config)
    spectrum = np.fft.rfft(windowed, n=config.fft_size)
    power = (spectrum.real * spectrum.real + spectrum.imag * spectrum.imag) / float(
        config.fft_size
    )
    bank = mel_filterbank(config.sample_rate, config.fft_size, config.mel_bands)
    energies = bank @ power
    logged = np.log(np.maximum(energies, 1.0e-12))
    return np.asarray([r10(float(v)) for v in logged], dtype=np.float64)


def analyze_frame(
    samples: np.ndarray,
    frame_index: int,
    hop: int,
    config: DSPConfig = DEFAULT_DSP,
) -> dict[str, Any]:
    start = frame_index * hop
    frame = extract_frame(samples, start, config.frame_length)
    f0 = estimate_f0(frame, config)
    mel = mel_log_energy(frame, config)
    rms = r10(float(np.sqrt(np.mean(frame * frame))) if frame.size else 0.0)
    return {
        "frame_index": frame_index,
        "start_sample": start,
        "end_sample": start + config.frame_length,
        "f0_hz": f0,
        "voiced": f0 is not None,
        "rms": rms,
        "mel_log_energy": [float(v) for v in mel],
    }


def warp_mel(log_energy: Iterable[float], warp: float, bands: int = DEFAULT_DSP.mel_bands) -> np.ndarray:
    """Warp linear mel-band energy and return the log-domain representation."""

    source = np.asarray(list(log_energy), dtype=np.float64)
    if source.size != bands:
        raise ValueError(f"expected {bands} mel bands, got {source.size}")
    energy = np.exp(np.clip(source, -40.0, 20.0))
    positions = (np.arange(bands, dtype=np.float64) - bands / 2.0) / float(warp) + bands / 2.0
    warped = np.interp(positions, np.arange(bands, dtype=np.float64), energy)
    logged = np.log(np.maximum(warped, 1.0e-12))
    return np.asarray([r10(float(v)) for v in logged], dtype=np.float64)


def hash_projection(seed: str, count: int) -> np.ndarray:
    """Deterministic uniform projection without depending on PRNG implementation."""

    needed = count * 8
    chunks: list[bytes] = []
    counter = 0
    while sum(len(c) for c in chunks) < needed:
        chunks.append(
            hashlib.blake2b(
                f"{seed}|{counter}".encode("utf-8"),
                digest_size=64,
            ).digest()
        )
        counter += 1
    raw = b"".join(chunks)[:needed]
    ints = np.frombuffer(raw, dtype="<u8", count=count)
    return ints.astype(np.float64) / float(2**64)


def reconstruct_frame(
    mapped_frame: dict[str, Any],
    *,
    shard_id: str,
    target_speaker: str,
    profile: SpeakerProfile,
    hop: int,
    config: DSPConfig = DEFAULT_DSP,
) -> np.ndarray:
    frame_index = int(mapped_frame["frame_index"])
    logged = np.asarray(mapped_frame["mel_log_energy"], dtype=np.float64)
    energy = np.exp(np.clip(logged, -40.0, 20.0))
    bank = mel_filterbank(config.sample_rate, config.fft_size, config.mel_bands)
    row_sums = np.maximum(bank.sum(axis=1, keepdims=True), 1.0e-12)
    power = ((energy[:, None] / row_sums) * bank).sum(axis=0)
    magnitude = np.sqrt(np.maximum(power, 0.0))

    n_bins = config.fft_size // 2 + 1
    seed = f"{shard_id}|{target_speaker}|{frame_index}|phase-projection"
    uniform = hash_projection(seed, n_bins + 2)
    phase = 2.0 * np.pi * uniform[:n_bins]
    spectrum = magnitude * np.exp(1j * phase)
    waveform = np.fft.irfft(spectrum, n=config.fft_size)[: config.frame_length]

    target_f0 = mapped_frame.get("target_f0_hz")
    global_samples = frame_index * hop + np.arange(config.frame_length, dtype=np.float64)
    if target_f0 is None:
        angle = 2.0 * np.pi * (
            uniform[n_bins] + global_samples / float(max(config.frame_length * 7, 1))
        )
        phase_envelope = 1.0 + 0.06 * np.cos(angle)
    else:
        angle = 2.0 * np.pi * (
            float(target_f0) / config.sample_rate * global_samples
            + profile.phase_cycles
            + uniform[n_bins + 1]
        )
        phase_envelope = 0.82 + 0.18 * np.cos(angle)
    waveform = waveform * phase_envelope

    current_rms = float(np.sqrt(np.mean(waveform * waveform))) if waveform.size else 0.0
    target_rms = float(mapped_frame.get("rms", 0.0)) * profile.gain
    if current_rms > 1.0e-12 and target_rms > 0.0:
        waveform = waveform * (target_rms / current_rms)
    elif target_rms <= 0.0:
        waveform = np.zeros_like(waveform)
    return np.asarray(waveform, dtype=np.float64)


def quantize_float_to_pcm16(segment: np.ndarray, quant_bits: int) -> tuple[np.ndarray, int]:
    if quant_bits < 8 or quant_bits > 16:
        raise ValueError("quant_bits must be between 8 and 16")
    values = np.asarray(segment, dtype=np.float64)
    clipped = int(np.count_nonzero((values > 1.0) | (values < -1.0)))
    full_scale = float((1 << (quant_bits - 1)) - 1)
    codes = np.rint(np.clip(values, -1.0, 1.0) * full_scale).astype(np.int64)
    if quant_bits < 16:
        codes = codes << (16 - quant_bits)
    return codes.astype("<i2"), clipped


class OverlapAddSynthesizer:
    """Frame-independent OLA state used identically by full and streaming paths."""

    def __init__(
        self,
        *,
        target_samples: int,
        hop: int,
        quant_bits: int,
        config: DSPConfig = DEFAULT_DSP,
    ) -> None:
        if target_samples < 0:
            raise ValueError("target_samples must be non-negative")
        if hop <= 0:
            raise ValueError("hop must be positive")
        self.target_samples = int(target_samples)
        self.hop = int(hop)
        self.quant_bits = int(quant_bits)
        self.config = config
        extra = config.frame_length + hop
        self._length = self.target_samples + extra
        self._ola = np.zeros(self._length, dtype=np.float64)
        self._norm = np.zeros(self._length, dtype=np.float64)
        self._window = _window(config)
        self._cursor = 0
        self._finalized = 0
        self.clip_count = 0
        self.pcm_parts: list[np.ndarray] = []

    @property
    def emitted_samples(self) -> int:
        return self._cursor

    def add_frame(self, frame_index: int, waveform: np.ndarray) -> None:
        start = frame_index * self.hop
        if start >= self.target_samples:
            self._finalized = min(self.target_samples, max(self._finalized, (frame_index + 1) * self.hop))
            return
        stop = min(start + self.config.frame_length, self._length)
        local = waveform[: stop - start]
        windowed = local * self._window[: local.size]
        self._ola[start:stop] += windowed
        self._norm[start:stop] += self._window[: local.size] ** 2
        self._finalized = min(self.target_samples, max(self._finalized, (frame_index + 1) * self.hop))

    def drain(self) -> np.ndarray:
        end = min(self._finalized, self.target_samples)
        if end <= self._cursor:
            return np.zeros(0, dtype="<i2")
        segment = self._ola[self._cursor : end].copy()
        norm = self._norm[self._cursor : end]
        valid = norm > 1.0e-10
        segment[valid] = segment[valid] / norm[valid]
        segment[~valid] = 0.0
        pcm, clipped = quantize_float_to_pcm16(segment, self.quant_bits)
        self.clip_count += clipped
        self._cursor = end
        if pcm.size:
            self.pcm_parts.append(pcm)
        return pcm

    def finish(self) -> np.ndarray:
        self._finalized = self.target_samples
        return self.drain()

    def pcm_bytes(self) -> bytes:
        return np.concatenate(self.pcm_parts).astype("<i2", copy=False).tobytes() if self.pcm_parts else b""


def collect_pcm(parts: Iterable[np.ndarray]) -> np.ndarray:
    arrays = [np.asarray(part, dtype="<i2") for part in parts if np.asarray(part).size]
    if not arrays:
        return np.zeros(0, dtype="<i2")
    return np.concatenate(arrays).astype("<i2", copy=False)
