"""Deterministic 16 kHz mono PCM audio primitives.

Everything here is pure NumPy plus the standard library ``wave`` module.
The WAV layout is fixed: 16 kHz, mono, 16-bit little-endian PCM, which is the
only layout the workbench produces and accepts.
"""
from __future__ import annotations

import hashlib
import wave
from dataclasses import dataclass
from typing import Tuple

import numpy as np

SAMPLE_RATE = 16000
SAMPLE_WIDTH = 2
INT16_SCALE = 32768.0
INT16_MAX = 32767
INT16_MIN = -32768


def read_wav(path: str) -> np.ndarray:
    """Return mono PCM samples as a float64 array in [-1, 1)."""
    with wave.open(path, "rb") as wf:
        n_channels = wf.getnchannels()
        sampwidth = wf.getsampwidth()
        rate = wf.getframerate()
        raw = wf.readframes(wf.getnframes())
    if n_channels != 1 or sampwidth != SAMPLE_WIDTH or rate != SAMPLE_RATE:
        raise ValueError(
            f"unsupported WAV layout: ch={n_channels} width={sampwidth} rate={rate}")
    ints = np.frombuffer(raw, dtype="<i2").astype(np.float64)
    return ints / INT16_SCALE


def write_wav(path: str, samples: np.ndarray) -> None:
    """Write a float array as 16-bit mono PCM (deterministic rounding)."""
    clipped = np.clip(samples, -1.0, 1.0)
    ints = np.floor(clipped * INT16_SCALE + 0.5).astype(np.int16)
    ints[clipped >= 1.0] = INT16_MAX
    with wave.open(path, "wb") as wf:
        wf.setnchannels(1)
        wf.setsampwidth(SAMPLE_WIDTH)
        wf.setframerate(SAMPLE_RATE)
        wf.writeframes(ints.astype("<i2").tobytes())


def quantize_int16(samples: np.ndarray) -> np.ndarray:
    """Deterministic float -> int16 quantization used for separation output."""
    clipped = np.clip(samples, -1.0, 1.0)
    ints = np.floor(clipped * INT16_SCALE + 0.5).astype(np.int16)
    ints[clipped >= 1.0] = INT16_MAX
    ints[clipped <= -1.0] = INT16_MIN
    return ints


def file_fingerprint(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 16), b""):
            h.update(block)
    return "wav-" + h.hexdigest()[:16]


def bytes_fingerprint(data: bytes, prefix: str = "blob") -> str:
    return prefix + "-" + hashlib.sha256(data).hexdigest()[:16]


def hann_window(n: int) -> np.ndarray:
    return np.hanning(n).astype(np.float64)


def frame_signal(x: np.ndarray, frame_size: int, hop_size: int) -> Tuple[np.ndarray, int]:
    """Slice ``x`` into symmetrically zero-padded overlapping frames.

    A full frame of zeros on both sides keeps the Hann-window normalization
    well defined at the signal boundaries, so reconstruction is accurate up to
    the first/last sample instead of only in the interior.
    """
    n = len(x)
    n_frames = 1 + (n + 2 * frame_size - frame_size - 1) // hop_size
    padded_len = (n_frames - 1) * hop_size + frame_size
    padded = np.zeros(padded_len, dtype=np.float64)
    padded[frame_size: frame_size + n] = x
    starts = np.arange(n_frames)[:, None] * hop_size
    offsets = np.arange(frame_size)[None, :]
    return padded[starts + offsets], n_frames


def stft(x: np.ndarray, frame_size: int, hop_size: int) -> np.ndarray:
    """Hann-windowed one-sided STFT, shape (n_frames, n_bins)."""
    frames, _ = frame_signal(x, frame_size, hop_size)
    window = hann_window(frame_size)
    return np.fft.rfft(frames * window[None, :], n=frame_size, axis=1)


def istft(spectrum: np.ndarray, frame_size: int, hop_size: int,
          out_length: int) -> np.ndarray:
    """Overlap-add inverse STFT normalized by the squared-window sum.

    Mirrors the symmetric padding of :func:`frame_signal`: the recovered
    signal starts ``frame_size`` samples into the overlap-add buffer.
    """
    n_frames = spectrum.shape[0]
    window = hann_window(frame_size)
    out_len = (n_frames - 1) * hop_size + frame_size
    output = np.zeros(out_len, dtype=np.float64)
    norm = np.zeros(out_len, dtype=np.float64)
    w2 = window * window
    for t in range(n_frames):
        frame = np.fft.irfft(spectrum[t], n=frame_size) * window
        start = t * hop_size
        output[start:start + frame_size] += frame
        norm[start:start + frame_size] += w2
    np.divide(output, norm, out=output, where=norm > 1e-12)
    return output[frame_size: frame_size + out_length]


def hz_to_mel(f: np.ndarray) -> np.ndarray:
    return 2595.0 * np.log10(1.0 + f / 700.0)


def mel_to_hz(m: np.ndarray) -> np.ndarray:
    return 700.0 * (10.0 ** (m / 2595.0) - 1.0)


def mel_filterbank(sample_rate: int, frame_size: int, n_mels: int) -> np.ndarray:
    """Build a deterministic triangular mel filterbank (n_mels, n_bins)."""
    n_bins = frame_size // 2 + 1
    fft_freqs = np.linspace(0, sample_rate / 2, n_bins)
    mel_points = np.linspace(hz_to_mel(np.array(0.0)),
                             hz_to_mel(np.array(sample_rate / 2.0)),
                             n_mels + 2)
    hz_points = mel_to_hz(mel_points)
    bank = np.zeros((n_mels, n_bins), dtype=np.float64)
    for m in range(n_mels):
        left, center, right = hz_points[m], hz_points[m + 1], hz_points[m + 2]
        rising = (fft_freqs - left) / max(center - left, 1e-12)
        falling = (right - fft_freqs) / max(right - center, 1e-12)
        bank[m] = np.maximum(0.0, np.minimum(rising, falling))
    return bank


@dataclass
class FrameClock:
    """Convert between STFT frame indices and millisecond timestamps."""

    sample_rate: int
    hop_size: int

    def frame_to_ms(self, frame: int) -> int:
        return int(round(frame * self.hop_size * 1000.0 / self.sample_rate))

    def ms_to_frame(self, ms: int) -> int:
        sample = ms * self.sample_rate / 1000.0
        return int(round(sample / self.hop_size))
