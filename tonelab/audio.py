"""WAV I/O while preserving declared sample rate, channels, and PCM bit depth."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import soundfile as sf

from .errors import ToneLabError


SUBTYPE_BITS = {
    "PCM_S8": 8,
    "PCM_U8": 8,
    "PCM_16": 16,
    "PCM_24": 24,
    "PCM_32": 32,
    "FLOAT": 32,
    "DOUBLE": 64,
}


def subtype_for_bits(bit_depth: int) -> str:
    if int(bit_depth) == 16:
        return "PCM_16"
    raise ToneLabError(
        "E_FORMAT_MISMATCH",
        f"unsupported PCM bit depth {bit_depth}; deterministic export requires 16",
        context={"declared": {"bit_depth": int(bit_depth)}, "actual": {"bit_depth": 16}},
    )


def inspect_wav(path: str | Path) -> dict[str, object]:
    try:
        info = sf.info(str(path))
    except FileNotFoundError as exc:
        raise ToneLabError(
            "E_NOT_FOUND",
            "WAV file does not exist",
            context={"path": str(path)},
        ) from exc
    except Exception as exc:
        raise ToneLabError(
            "E_FORMAT_MISMATCH",
            "WAV file cannot be opened or is not a valid audio container",
            context={"path": str(path), "reason": str(exc)},
        ) from exc
    return {
        "sample_rate": int(info.samplerate),
        "channels": int(info.channels),
        "frames": int(info.frames),
        "format": str(info.format),
        "subtype": str(info.subtype),
        "bit_depth": int(SUBTYPE_BITS.get(str(info.subtype), 0)),
    }


def read_wav(
    path: str | Path,
    *,
    shard_id: str | None = None,
    expected_sample_rate: int | None = None,
    expected_channels: int | None = None,
    expected_bit_depth: int | None = None,
) -> tuple[np.ndarray, dict[str, object]]:
    info = inspect_wav(path)
    mismatches: dict[str, object] = {}
    declared: dict[str, object] = {}
    actual: dict[str, object] = {}
    if expected_sample_rate is not None and int(info["sample_rate"]) != int(expected_sample_rate):
        mismatches["sample_rate"] = True
    if expected_channels is not None and int(info["channels"]) != int(expected_channels):
        mismatches["channels"] = True
    if expected_bit_depth is not None and int(info["bit_depth"]) != int(expected_bit_depth):
        mismatches["bit_depth"] = True
    if mismatches:
        if expected_sample_rate is not None:
            declared["sample_rate"] = int(expected_sample_rate)
        if expected_channels is not None:
            declared["channels"] = int(expected_channels)
        if expected_bit_depth is not None:
            declared["bit_depth"] = int(expected_bit_depth)
        actual = {
            "sample_rate": int(info["sample_rate"]),
            "channels": int(info["channels"]),
            "bit_depth": int(info["bit_depth"]),
        }
        raise ToneLabError(
            "E_FORMAT_MISMATCH",
            "WAV format does not match manifest declaration",
            shard_id=shard_id,
            context={
                "declared": declared,
                "actual": actual,
                "watched_fields": sorted(mismatches),
                "subtype": info["subtype"],
            },
        )
    if int(info["bit_depth"]) != 16 or int(info["channels"]) != 1:
        raise ToneLabError(
            "E_FORMAT_MISMATCH",
            "workbench accepts only mono 16-bit PCM WAV input",
            shard_id=shard_id,
            context={"actual": info},
        )
    data, sample_rate = sf.read(str(path), dtype="int16", always_2d=True)
    if int(sample_rate) != int(info["sample_rate"]):
        raise ToneLabError(
            "E_FORMAT_MISMATCH",
            "sample rate changed while reading WAV",
            shard_id=shard_id,
            context={
                "declared": {"sample_rate": int(info["sample_rate"])},
                "actual": {"sample_rate": int(sample_rate)},
            },
        )
    mono = np.asarray(data[:, 0], dtype=np.int16).copy()
    return mono, info


def write_wav(
    path: str | Path,
    pcm: np.ndarray,
    *,
    sample_rate: int,
    channels: int = 1,
    bit_depth: int = 16,
    shard_id: str | None = None,
) -> dict[str, object]:
    subtype = subtype_for_bits(bit_depth)
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    samples = np.asarray(pcm, dtype=np.int16)
    if channels != 1:
        raise ToneLabError(
            "E_FORMAT_MISMATCH",
            "only mono WAV export is supported",
            shard_id=shard_id,
            context={
                "declared": {"channels": int(channels)},
                "actual": {"channels": 1},
            },
        )
    sf.write(str(target), samples, int(sample_rate), subtype=subtype, format="WAV")
    info = inspect_wav(target)
    if (
        int(info["sample_rate"]) != int(sample_rate)
        or int(info["channels"]) != int(channels)
        or int(info["bit_depth"]) != int(bit_depth)
    ):
        raise ToneLabError(
            "E_FORMAT_MISMATCH",
            "exported WAV does not preserve declared format",
            shard_id=shard_id,
            context={
                "declared": {
                    "sample_rate": int(sample_rate),
                    "channels": int(channels),
                    "bit_depth": int(bit_depth),
                },
                "actual": info,
            },
        )
    return info

