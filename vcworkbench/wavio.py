from __future__ import annotations

import io
import wave
from dataclasses import dataclass
from pathlib import Path

import numpy as np

from .canonical import atomic_write_bytes
from .errors import fail


@dataclass(frozen=True)
class WavInfo:
    path: Path
    channels: int
    sample_rate: int
    sample_width: int
    frame_count: int
    raw_sha256: str


def read_pcm16_wav(path: Path) -> tuple[WavInfo, np.ndarray, bytes]:
    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        fail(
            "E_MANIFEST_MISMATCH",
            f"WAV file does not exist: {path}",
            stage="analysis",
            path=str(path),
        )
    try:
        with wave.open(io.BytesIO(raw), "rb") as wav:
            channels = wav.getnchannels()
            sample_rate = wav.getframerate()
            sample_width = wav.getsampwidth()
            comp_type = wav.getcomptype()
            frame_count = wav.getnframes()
            frames = wav.readframes(frame_count)
    except Exception as exc:
        fail(
            "E_MANIFEST_MISMATCH",
            f"cannot decode WAV: {exc}",
            stage="analysis",
            path=str(path),
        )
    if comp_type != "NONE":
        fail(
            "E_MANIFEST_MISMATCH",
            "compressed WAV is not supported",
            stage="analysis",
            path=str(path),
            compression=comp_type,
        )
    if sample_width != 2:
        fail(
            "E_INVALID_PARAM",
            "only 16-bit PCM WAV is supported",
            stage="analysis",
            path=str(path),
            sample_width_bits=sample_width * 8,
        )
    expected_bytes = frame_count * channels * 2
    if len(frames) != expected_bytes:
        fail(
            "E_MANIFEST_MISMATCH",
            "truncated WAV sample payload",
            stage="analysis",
            path=str(path),
            declared_bytes=expected_bytes,
            actual_bytes=len(frames),
        )
    import hashlib

    info = WavInfo(
        path=path,
        channels=channels,
        sample_rate=sample_rate,
        sample_width=sample_width,
        frame_count=frame_count,
        raw_sha256=hashlib.sha256(raw).hexdigest(),
    )
    pcm = np.frombuffer(frames, dtype="<i2")
    return info, pcm.reshape(frame_count, channels), raw


def wav_bytes_from_pcm16(pcm: np.ndarray, sample_rate: int = 16000) -> bytes:
    data = np.asarray(pcm, dtype="<i2").tobytes(order="C")
    buf = io.BytesIO()
    with wave.open(buf, "wb") as wav:
        wav.setnchannels(1)
        wav.setsampwidth(2)
        wav.setframerate(sample_rate)
        wav.writeframes(data)
    return buf.getvalue()


def write_pcm16_wav(path: Path, pcm: np.ndarray, sample_rate: int = 16000) -> bytes:
    data = wav_bytes_from_pcm16(pcm, sample_rate)
    atomic_write_bytes(path, data)
    return data
