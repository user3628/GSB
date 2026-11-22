"""Minimal, deterministic RIFF/WAVE PCM parser.

Only the contract used by the component is accepted: one mono channel,
16-bit little-endian PCM, and a 16 kHz sample rate.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np

from .errors import E_INVALID_WAV, FingerprintError
from .jsonutil import sha256_file


EXPECTED_SAMPLE_RATE = 16_000
EXPECTED_CHANNELS = 1
EXPECTED_BITS_PER_SAMPLE = 16


@dataclass(frozen=True)
class WavData:
    path: str
    sample_rate: int
    channels: int
    bits_per_sample: int
    sample_count: int
    samples: np.ndarray
    content_digest: str

    @property
    def duration_seconds(self) -> float:
        return self.sample_count / self.sample_rate


def _wav_error(path: str, message: str, shard_id: str | None = None) -> FingerprintError:
    return FingerprintError(
        code=E_INVALID_WAV,
        message=message,
        shard_id=shard_id,
        stage="input",
        details={"path_basename": Path(path).name},
    )


def read_wav(path: str, shard_id: str | None = None) -> WavData:
    source = Path(path)
    try:
        raw = source.read_bytes()
    except OSError as exc:
        raise _wav_error(path, f"cannot read WAV: {exc.strerror}", shard_id) from exc

    if len(raw) < 12 or raw[0:4] != b"RIFF" or raw[8:12] != b"WAVE":
        raise _wav_error(path, "not a RIFF/WAVE file", shard_id)

    position = 12
    fmt: tuple[int, int, int, int, int, int, int] | None = None
    pcm: bytes | None = None
    while position + 8 <= len(raw):
        chunk_id = raw[position : position + 4]
        chunk_size = int.from_bytes(raw[position + 4 : position + 8], "little")
        payload_start = position + 8
        payload_end = payload_start + chunk_size
        if payload_end > len(raw):
            raise _wav_error(path, "truncated WAV chunk", shard_id)
        payload = raw[payload_start:payload_end]
        if chunk_id == b"fmt ":
            if chunk_size < 16:
                raise _wav_error(path, "fmt chunk is too short", shard_id)
            audio_format = int.from_bytes(payload[0:2], "little")
            channels = int.from_bytes(payload[2:4], "little")
            sample_rate = int.from_bytes(payload[4:8], "little")
            byte_rate = int.from_bytes(payload[8:12], "little")
            block_align = int.from_bytes(payload[12:14], "little")
            bits = int.from_bytes(payload[14:16], "little")
            fmt = (audio_format, channels, sample_rate, byte_rate, block_align, bits, chunk_size)
        elif chunk_id == b"data" and pcm is None:
            pcm = payload
        position = payload_end + (chunk_size & 1)

    if fmt is None:
        raise _wav_error(path, "missing fmt chunk", shard_id)
    if pcm is None:
        raise _wav_error(path, "missing data chunk", shard_id)
    audio_format, channels, sample_rate, byte_rate, block_align, bits, _ = fmt
    if audio_format != 1:
        raise _wav_error(path, "only uncompressed PCM is supported", shard_id)
    if channels != EXPECTED_CHANNELS:
        raise _wav_error(path, "only mono audio is supported", shard_id)
    if sample_rate != EXPECTED_SAMPLE_RATE:
        raise _wav_error(path, "only 16000 Hz audio is supported", shard_id)
    if bits != EXPECTED_BITS_PER_SAMPLE:
        raise _wav_error(path, "only 16-bit PCM is supported", shard_id)
    if block_align != 2 or byte_rate != EXPECTED_SAMPLE_RATE * 2:
        raise _wav_error(path, "inconsistent PCM byte rate or block alignment", shard_id)
    if len(pcm) % 2:
        raise _wav_error(path, "odd-length 16-bit PCM data", shard_id)

    samples = np.frombuffer(pcm, dtype="<i2").astype(np.float64) / 32768.0
    return WavData(
        path=str(source),
        sample_rate=sample_rate,
        channels=channels,
        bits_per_sample=bits,
        sample_count=int(samples.size),
        samples=samples,
        content_digest=sha256_file(source),
    )

