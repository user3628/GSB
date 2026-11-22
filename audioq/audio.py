"""Deterministic WAV loading, Hann framing and local quality metrics."""

from __future__ import annotations

import math
import wave
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import numpy as np

from .canonical import finite_float, rounded_float, sha256_bytes
from .errors import AudioQError
from .models import FrameConfig, MetricsConfig


SILENCE_AMPLITUDE = 1e-7


@dataclass
class AudioData:
    path: Path
    samples: np.ndarray
    pcm_bytes: bytes
    sample_rate: int
    sample_count: int
    channels: int
    sample_width: int

    @property
    def content_sha256(self) -> str:
        return sha256_bytes(self.pcm_bytes)


def read_pcm_wav(path: Path | str, *, shard_id: str | None = None) -> AudioData:
    path = Path(path)
    try:
        if path.stat().st_size == 0:
            raise AudioQError(
                "E_EMPTY_AUDIO",
                "audio file is zero bytes",
                shard_id=shard_id,
                stage="frame",
                observed={"path": str(path), "size_bytes": 0},
            )
    except FileNotFoundError as exc:
        raise AudioQError(
            "E_EMPTY_AUDIO",
            "audio file does not exist",
            shard_id=shard_id,
            stage="frame",
            observed={"path": str(path), "reason": "missing"},
        ) from exc
    except OSError as exc:
        raise AudioQError(
            "E_IO",
            "audio file cannot be inspected",
            shard_id=shard_id,
            stage="frame",
            observed={"path": str(path), "reason": str(exc)},
        ) from exc
    try:
        with wave.open(str(path), "rb") as wav:
            channels = wav.getnchannels()
            sample_width = wav.getsampwidth()
            sample_rate = wav.getframerate()
            frame_count = wav.getnframes()
            compression = wav.getcomptype()
            raw = wav.readframes(frame_count)
    except (wave.Error, EOFError) as exc:
        raise AudioQError(
            "E_AUDIO_FORMAT",
            "file is not a readable PCM WAV",
            shard_id=shard_id,
            stage="frame",
            observed={"path": str(path), "reason": str(exc)},
        ) from exc
    if channels != 1 or sample_width != 2 or compression != "NONE":
        raise AudioQError(
            "E_AUDIO_FORMAT",
            "audio must be mono 16-bit uncompressed PCM WAV",
            shard_id=shard_id,
            stage="frame",
            observed={
                "channels": channels,
                "sample_width": sample_width,
                "compression": compression,
            },
        )
    if sample_rate <= 0:
        raise AudioQError(
            "E_AUDIO_FORMAT",
            "audio has an invalid sample rate",
            shard_id=shard_id,
            stage="frame",
            observed={"sample_rate": sample_rate},
        )
    pcm = np.frombuffer(raw, dtype="<i2")
    samples = pcm.astype(np.float64) / 32768.0
    return AudioData(
        path=path,
        samples=samples,
        pcm_bytes=raw,
        sample_rate=sample_rate,
        sample_count=int(pcm.size),
        channels=channels,
        sample_width=sample_width,
    )


def frame_audio(audio: AudioData, frame_config: FrameConfig, *, shard_id: str | None = None) -> dict[str, Any]:
    """Return a serializable frame artifact. All-silent input is an error."""

    if audio.sample_count == 0:
        raise AudioQError(
            "E_EMPTY_AUDIO",
            "audio contains no samples",
            shard_id=shard_id,
            stage="frame",
            observed={"sample_count": 0, "frame_length": frame_config.length},
        )
    if frame_config.length > audio.sample_count:
        raise AudioQError(
            "E_EMPTY_AUDIO",
            "frame length exceeds the number of audio samples",
            shard_id=shard_id,
            stage="frame",
            observed={"sample_count": audio.sample_count, "frame_length": frame_config.length},
        )
    max_amplitude = float(np.max(np.abs(audio.samples))) if audio.sample_count else 0.0
    if max_amplitude < SILENCE_AMPLITUDE:
        raise AudioQError(
            "E_EMPTY_AUDIO",
            "audio is entirely silent",
            shard_id=shard_id,
            stage="frame",
            observed={"sample_count": audio.sample_count, "max_amplitude": max_amplitude},
        )

    starts = np.arange(0, audio.sample_count - frame_config.length + 1, frame_config.hop, dtype=np.int64)
    if starts.size == 0:
        raise AudioQError(
            "E_EMPTY_AUDIO",
            "no complete frame could be produced",
            shard_id=shard_id,
            stage="frame",
            observed={"sample_count": audio.sample_count, "frame_length": frame_config.length, "hop": frame_config.hop},
        )
    indices = starts[:, None] + np.arange(frame_config.length, dtype=np.int64)[None, :]
    frames = audio.samples[indices]
    window = np.hanning(frame_config.length)
    windowed = frames * window[None, :]
    energies = np.sum(windowed * windowed, axis=1)
    peaks = np.max(np.abs(frames), axis=1)
    if not np.all(np.isfinite(energies)) or not np.all(np.isfinite(peaks)):
        raise AudioQError(
            "E_NUMERIC",
            "framing produced a non-finite value",
            shard_id=shard_id,
            stage="frame",
            observed={"frame_count": int(starts.size)},
        )
    return {
        "status": "ok",
        "shard_id": shard_id,
        "sample_count": audio.sample_count,
        "sample_rate": audio.sample_rate,
        "duration_seconds": rounded_float(audio.sample_count / audio.sample_rate, 12),
        "content_sha256": audio.content_sha256,
        "frame": frame_config.to_dict(),
        "frame_count": int(starts.size),
        "frame_starts": [int(value) for value in starts],
        "frame_energies": [rounded_float(value, 15) for value in energies],
        "frame_peak_amplitudes": [rounded_float(value, 12) for value in peaks],
        "errors": [],
    }


def evaluate_metrics(
    audio: AudioData,
    frame_artifact: Mapping[str, Any],
    config: MetricsConfig,
    *,
    decimals: int = 12,
    shard_id: str | None = None,
) -> dict[str, Any]:
    """Calculate SNR, spectral distortion and clipping metrics.

    Clipping is deliberately represented as a metric error while retaining the
    observed score, so a diagnostics run can continue without masking it.
    """

    errors: list[dict[str, Any]] = []
    frame_count = int(frame_artifact.get("frame_count", 0))
    energies = np.asarray(frame_artifact.get("frame_energies", []), dtype=np.float64)
    if frame_count <= 0 or energies.size != frame_count:
        raise AudioQError(
            "E_INVALID_STAGE",
            "frame artifact has no usable frame energies",
            shard_id=shard_id,
            stage="metrics",
            observed={"frame_count": frame_count, "energy_count": int(energies.size)},
        )
    if not np.all(np.isfinite(energies)):
        raise AudioQError(
            "E_NUMERIC",
            "frame energies contain NaN or infinity",
            shard_id=shard_id,
            stage="metrics",
            observed={"frame_count": frame_count},
        )

    frame_config = FrameConfig(**frame_artifact["frame"])
    starts = np.asarray(frame_artifact["frame_starts"], dtype=np.int64)
    indices = starts[:, None] + np.arange(frame_config.length, dtype=np.int64)[None, :]
    frames = audio.samples[indices]
    window = np.hanning(frame_config.length)
    windowed = frames * window[None, :]

    tiny = 1e-15
    noise_floor = float(np.percentile(energies, 10))
    signal_level = float(np.percentile(energies, 90))
    noise_power = max(noise_floor, tiny)
    signal_power = max(signal_level, tiny)
    snr_ratio = max(signal_power - noise_power, tiny) / noise_power
    snr_db = float(np.clip(10.0 * math.log10(snr_ratio), -20.0, 60.0))
    frame_ratios = np.maximum(energies - noise_power, tiny) / noise_power
    frame_snr_db = np.clip(10.0 * np.log10(frame_ratios), -20.0, 60.0)
    snr_score = float(np.clip(2.5 + 0.25 * (snr_db - config.snr_threshold_db), 0.0, 5.0))

    spectra = np.abs(np.fft.rfft(windowed, axis=1))
    spectra = spectra / np.maximum(np.sum(spectra, axis=1, keepdims=True), tiny)
    active = energies > max(noise_power * 1.25, tiny)
    if not np.any(active):
        active = np.ones_like(energies, dtype=bool)
    reference_spectrum = np.median(spectra[active], axis=0)
    reference_spectrum = np.maximum(reference_spectrum, config.spectral_epsilon)
    frame_spectral_db = np.sqrt(
        np.mean(
            np.square(20.0 * np.log10(np.maximum(spectra, config.spectral_epsilon)) - 20.0 * np.log10(reference_spectrum[None, :])),
            axis=1,
        )
    )
    power = np.square(spectra)
    spectral_flatness = np.exp(np.mean(np.log(np.maximum(power, 1e-15)), axis=1)) / np.maximum(
        np.mean(power, axis=1), 1e-15
    )
    flatness_median = float(np.median(spectral_flatness[active])) if np.any(active) else 0.0
    flatness_penalty_db = 20.0 * math.log10(1.0 + 100.0 * max(flatness_median, 0.0))
    reference_distortion_db = (
        float(np.median(frame_spectral_db[active])) if np.any(active) else 0.0
    )
    spectral_distortion_db = reference_distortion_db + flatness_penalty_db
    spectral_score = float(np.clip(5.0 * math.exp(-spectral_distortion_db / config.spectral_scale_db), 0.0, 5.0))

    frame_clipping_ratios = np.mean(np.abs(frames) >= config.clipping_level, axis=1)
    clipping_count = int(np.count_nonzero(np.abs(audio.samples) >= config.clipping_level))
    clipping_ratio = clipping_count / audio.sample_count if audio.sample_count else 0.0
    clipping_score = float(
        np.clip(5.0 * (1.0 - min(1.0, clipping_ratio / config.clipping_scale_ratio)), 0.0, 5.0)
    )

    score_values = {"snr": snr_score, "spectral": spectral_score, "clipping": clipping_score}
    for name, value in score_values.items():
        if not math.isfinite(float(value)):
            errors.append(
                AudioQError(
                    "E_NUMERIC",
                    "metric score is NaN or infinite",
                    shard_id=shard_id,
                    stage="metrics",
                    observed={"metric": name, "value": repr(value)},
                ).to_dict()
            )
            score_values[name] = None

    if clipping_ratio > config.clipping_ratio_limit:
        errors.append(
            AudioQError(
                "E_CLIPPED_AUDIO",
                "clipping ratio exceeds the configured limit",
                shard_id=shard_id,
                stage="metrics",
                observed={
                    "clipping_ratio": rounded_float(clipping_ratio, 12),
                    "clipping_level": config.clipping_level,
                    "clipping_ratio_limit": config.clipping_ratio_limit,
                    "clipping_sample_count": clipping_count,
                },
            ).to_dict()
        )

    return {
        "status": "error" if errors else "ok",
        "shard_id": shard_id,
        "frame_count": frame_count,
        "scores": {
            name: (rounded_float(value, decimals, name=f"{name}_score") if value is not None else None)
            for name, value in score_values.items()
        },
        "snr": {
            "score": rounded_float(snr_score, decimals, name="snr_score") if score_values["snr"] is not None else None,
            "snr_db": rounded_float(snr_db, decimals),
            "noise_floor_energy": rounded_float(noise_floor, 15),
            "signal_energy": rounded_float(signal_level, 15),
            "threshold_db": config.snr_threshold_db,
            "frame_snr_db": [rounded_float(value, 9) for value in frame_snr_db],
        },
        "spectral": {
            "score": rounded_float(spectral_score, decimals, name="spectral_score") if score_values["spectral"] is not None else None,
            "distortion_db": rounded_float(spectral_distortion_db, decimals),
            "reference_distortion_db": rounded_float(reference_distortion_db, decimals),
            "flatness_median": rounded_float(flatness_median, 12),
            "flatness_penalty_db": rounded_float(flatness_penalty_db, decimals),
            "active_frame_count": int(np.count_nonzero(active)),
            "frame_distortion_db": [rounded_float(value, 9) for value in frame_spectral_db],
        },
        "clipping": {
            "score": rounded_float(clipping_score, decimals, name="clipping_score") if score_values["clipping"] is not None else None,
            "clipping_ratio": rounded_float(clipping_ratio, 12),
            "clipping_sample_count": clipping_count,
            "clipping_level": config.clipping_level,
            "clipping_ratio_limit": config.clipping_ratio_limit,
            "frame_clipping_ratios": [rounded_float(value, 12) for value in frame_clipping_ratios],
        },
        "errors": errors,
    }


def validate_metric_scores(scores: Mapping[str, Any]) -> dict[str, float | None]:
    """Validate a metrics artifact before fusion; NaN/Inf raises E_NUMERIC."""

    result: dict[str, float | None] = {}
    for name in ("snr", "spectral", "clipping"):
        value = scores.get(name) if isinstance(scores, Mapping) else None
        if value is None:
            result[name] = None
            continue
        number = finite_float(value, name=f"{name}_score")
        if number < 0.0 or number > 5.0:
            raise AudioQError(
                "E_NUMERIC",
                "metric score lies outside the MOS-like [0, 5] range",
                observed={"metric": name, "value": number},
            )
        result[name] = number
    return result
