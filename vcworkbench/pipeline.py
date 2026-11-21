from __future__ import annotations

import csv
import base64
import io
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

import numpy as np

from .canonical import (
    atomic_write_bytes,
    canonical_json_bytes,
    digest_obj,
    read_json,
    sha256_bytes,
)
from .dsp import (
    DEFAULT_DSP,
    DSPConfig,
    OverlapAddSynthesizer,
    analyze_frame,
    collect_pcm,
    reconstruct_frame,
    r10,
    warp_mel,
)
from .errors import OperationStopped, WorkbenchError, fail
from .manifest import Manifest, ReferencePitch, ShardSpec
from .profiles import SpeakerProfile, target_profile
from .storage import STAGES, Artifact, CheckpointStore
from .version import CONFIG_VERSION
from .wavio import WavInfo, read_pcm16_wav, wav_bytes_from_pcm16
from .workspace import Workspace


INJECTION_ALIASES = {
    "channels": "channels",
    "channel_count": "channels",
    "declared_sample_rate": "declared_sample_rate",
    "sample_rate_declared": "declared_sample_rate",
    "nan_index": "nan_index",
    "nan_indices": "nan_index",
    "sample_count_delta": "sample_count_delta",
    "manifest_sample_count": "manifest_sample_count",
    "declared_sample_count": "manifest_sample_count",
    "pitch_shift": "pitch_shift",
    "pitch_offset": "pitch_shift",
    "mel_warp": "mel_warp",
    "warp": "mel_warp",
    "spectral_warp": "mel_warp",
    "quant_bits": "quant_bits",
    "frame_shift": "frame_shift",
    "target_speaker": "target_speaker",
    "reference_pitch_hz": "reference_pitch_hz",
    "reference_min_hz": "reference_min_hz",
    "reference_max_hz": "reference_max_hz",
    "reference_tolerance_hz": "reference_tolerance_hz",
    "tolerance_hz": "reference_tolerance_hz",
    "remove_reference": "remove_reference",
    "expected_output_samples": "expected_output_samples",
}

DEFAULT_PITCH_TOLERANCE_HZ = 5.0


@dataclass
class EffectiveParams:
    target_speaker: str
    pitch_shift: float
    mel_warp: float
    frame_shift: int
    quant_bits: int
    expected_output_samples: int
    reference_pitch: ReferencePitch | None
    reference_tolerance_hz: float
    injection: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "target_speaker": self.target_speaker,
            "pitch_shift": self.pitch_shift,
            "mel_warp": self.mel_warp,
            "frame_shift": self.frame_shift,
            "quant_bits": self.quant_bits,
            "expected_output_samples": self.expected_output_samples,
            "reference_pitch_hz": None
            if self.reference_pitch is None
            else self.reference_pitch.to_dict(),
            "reference_tolerance_hz": self.reference_tolerance_hz,
        }


@dataclass
class SourceAudio:
    info: WavInfo
    samples: np.ndarray
    raw_bytes: bytes
    effective: EffectiveParams


def normalize_injection(injection: dict[str, Any] | None) -> dict[str, Any]:
    if injection is None:
        return {}
    if not isinstance(injection, dict):
        fail("E_INVALID_PARAM", "injection must be a JSON object", stage="ingest")
    result: dict[str, Any] = {}
    for raw_key, value in injection.items():
        key = INJECTION_ALIASES.get(raw_key)
        if key is None:
            fail(
                "E_INVALID_PARAM",
                f"unsupported injection parameter: {raw_key}",
                stage="ingest",
                parameter=raw_key,
                allowed=sorted(set(INJECTION_ALIASES.values())),
            )
        if key in result and result[key] != value:
            fail(
                "E_INVALID_PARAM",
                f"conflicting aliases for injection parameter: {key}",
                stage="ingest",
                parameter=key,
            )
        result[key] = value
    return result


def _as_int(value: Any, name: str, *, stage: str, shard_id: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        fail(
            "E_INVALID_PARAM",
            f"{name} must be an integer",
            shard_id=shard_id,
            stage=stage,
            parameter=name,
        )
    if isinstance(value, float) and not value.is_integer():
        fail(
            "E_INVALID_PARAM",
            f"{name} must be an integer",
            shard_id=shard_id,
            stage=stage,
            parameter=name,
        )
    return int(value)


def _as_float(value: Any, name: str, *, stage: str, shard_id: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        fail(
            "E_INVALID_PARAM",
            f"{name} must be numeric",
            shard_id=shard_id,
            stage=stage,
            parameter=name,
        )
    result = float(value)
    if not math.isfinite(result):
        fail(
            "E_INVALID_PARAM",
            f"{name} must be finite",
            shard_id=shard_id,
            stage=stage,
            parameter=name,
        )
    return result


def effective_params(spec: ShardSpec, injection: dict[str, Any] | None) -> EffectiveParams:
    inj = normalize_injection(injection)
    shard_id = spec.shard_id
    target = inj.get("target_speaker", spec.target_speaker)
    if not isinstance(target, str) or not target:
        fail("E_INVALID_PARAM", "target_speaker must be a non-empty string", shard_id=shard_id, stage="mapping")
    pitch_shift = _as_float(inj.get("pitch_shift", spec.pitch_shift), "pitch_shift", stage="mapping", shard_id=shard_id)
    mel_warp = _as_float(inj.get("mel_warp", spec.mel_warp), "mel_warp", stage="mapping", shard_id=shard_id)
    frame_shift = _as_int(inj.get("frame_shift", spec.frame_shift), "frame_shift", stage="analysis", shard_id=shard_id)
    quant_bits = _as_int(inj.get("quant_bits", spec.quant_bits), "quant_bits", stage="reconstruction", shard_id=shard_id)
    expected = _as_int(
        inj.get("expected_output_samples", spec.expected_output_samples),
        "expected_output_samples",
        stage="reconstruction",
        shard_id=shard_id,
    )
    tolerance = _as_float(
        inj.get(
            "reference_tolerance_hz",
            spec.reference_tolerance_hz
            if spec.reference_tolerance_hz is not None
            else DEFAULT_PITCH_TOLERANCE_HZ,
        ),
        "reference_tolerance_hz",
        stage="validation",
        shard_id=shard_id,
    )
    reference = spec.reference_pitch
    if inj.get("remove_reference"):
        reference = None
    if "reference_pitch_hz" in inj:
        raw_ref = inj["reference_pitch_hz"]
        if raw_ref is not None:
            if not isinstance(raw_ref, dict) or "min_hz" not in raw_ref or "max_hz" not in raw_ref:
                fail(
                    "E_INVALID_PARAM",
                    "reference_pitch_hz injection must include min_hz and max_hz",
                    shard_id=shard_id,
                    stage="validation",
                )
            reference = ReferencePitch(
                _as_float(raw_ref["min_hz"], "reference_pitch_hz.min_hz", stage="validation", shard_id=shard_id),
                _as_float(raw_ref["max_hz"], "reference_pitch_hz.max_hz", stage="validation", shard_id=shard_id),
            )
        else:
            reference = None
    if "reference_min_hz" in inj or "reference_max_hz" in inj:
        base = reference or spec.reference_pitch
        if base is None:
            fail(
                "E_REFERENCE_MISSING",
                "reference interval cannot be extended because no reference exists",
                shard_id=shard_id,
                stage="validation",
            )
        reference = ReferencePitch(
            _as_float(inj.get("reference_min_hz", base.min_hz), "reference_min_hz", stage="validation", shard_id=shard_id),
            _as_float(inj.get("reference_max_hz", base.max_hz), "reference_max_hz", stage="validation", shard_id=shard_id),
        )
    if reference is not None and reference.max_hz < reference.min_hz:
        fail(
            "E_INVALID_PARAM",
            "reference pitch interval is reversed",
            shard_id=shard_id,
            stage="validation",
        )
    if frame_shift <= 0:
        fail(
            "E_INVALID_PARAM",
            "frame_shift must be positive",
            shard_id=shard_id,
            stage="analysis",
            value=frame_shift,
        )
    if pitch_shift <= 0.0:
        fail(
            "E_INVALID_PARAM",
            "pitch_shift must be greater than zero",
            shard_id=shard_id,
            stage="mapping",
            value=pitch_shift,
        )
    if mel_warp <= 0.0:
        fail(
            "E_INVALID_PARAM",
            "mel_warp must be greater than zero",
            shard_id=shard_id,
            stage="mapping",
            value=mel_warp,
        )
    if quant_bits < 8 or quant_bits > 16:
        fail(
            "E_INVALID_PARAM",
            "quant_bits must be between 8 and 16",
            shard_id=shard_id,
            stage="reconstruction",
            value=quant_bits,
        )
    if expected <= 0:
        fail(
            "E_INVALID_PARAM",
            "expected_output_samples must be positive",
            shard_id=shard_id,
            stage="reconstruction",
            value=expected,
        )
    if tolerance < 0.0:
        fail(
            "E_INVALID_PARAM",
            "reference_tolerance_hz must be non-negative",
            shard_id=shard_id,
            stage="validation",
            value=tolerance,
        )
    return EffectiveParams(
        target_speaker=target,
        pitch_shift=pitch_shift,
        mel_warp=mel_warp,
        frame_shift=frame_shift,
        quant_bits=quant_bits,
        expected_output_samples=expected,
        reference_pitch=reference,
        reference_tolerance_hz=tolerance,
        injection=inj,
    )


def load_source(workspace: Workspace, spec: ShardSpec, injection: dict[str, Any] | None) -> SourceAudio:
    effective = effective_params(spec, injection)
    inj = effective.injection
    wav_path = spec.resolve_wav(workspace.root)
    info, pcm, raw = read_pcm16_wav(wav_path)
    injected_channels = _as_int(
        inj.get("channels", info.channels), "channels", stage="analysis", shard_id=spec.shard_id
    )
    if info.channels != 1 or spec.channels != 1 or injected_channels != 1:
        fail(
            "E_CHANNEL_UNSUPPORTED",
            "only mono WAV input is supported",
            shard_id=spec.shard_id,
            stage="analysis",
            actual_channels=info.channels,
            manifest_channels=spec.channels,
            injected_channels=injected_channels,
        )
    declared_rate = _as_int(
        inj.get("declared_sample_rate", spec.sample_rate),
        "declared_sample_rate",
        stage="analysis",
        shard_id=spec.shard_id,
    )
    if info.sample_rate != 16000 or declared_rate != info.sample_rate:
        fail(
            "E_SAMPLE_RATE_MISMATCH",
            "WAV sample rate conflicts with the manifest or 16 kHz contract",
            shard_id=spec.shard_id,
            stage="analysis",
            actual_sample_rate=info.sample_rate,
            declared_sample_rate=declared_rate,
        )
    injected_count = inj.get("manifest_sample_count")
    if injected_count is None:
        delta = _as_int(
            inj.get("sample_count_delta", 0),
            "sample_count_delta",
            stage="analysis",
            shard_id=spec.shard_id,
        )
        injected_count = spec.sample_count + delta
    declared_count = _as_int(
        injected_count,
        "manifest_sample_count",
        stage="analysis",
        shard_id=spec.shard_id,
    )
    if declared_count != info.frame_count:
        fail(
            "E_MANIFEST_MISMATCH",
            "manifest sample_count does not match the WAV payload",
            shard_id=spec.shard_id,
            stage="analysis",
            manifest_sample_count=declared_count,
            actual_sample_count=info.frame_count,
        )
    samples = pcm[:, 0].astype(np.float64) / 32768.0
    if "nan_index" in inj:
        raw_indices = inj["nan_index"]
        indices = raw_indices if isinstance(raw_indices, list) else [raw_indices]
        for value in indices:
            index = _as_int(value, "nan_index", stage="analysis", shard_id=spec.shard_id)
            if index < 0 or index >= samples.size:
                fail(
                    "E_INVALID_PARAM",
                    "nan_index is outside the sample range",
                    shard_id=spec.shard_id,
                    stage="analysis",
                    nan_index=index,
                    sample_count=int(samples.size),
                )
            samples[index] = np.nan
    finite = np.isfinite(samples)
    if not bool(np.all(finite)):
        first_index = int(np.flatnonzero(~finite)[0])
        fail(
            "E_NUMERIC",
            "source samples contain NaN or infinity",
            shard_id=spec.shard_id,
            stage="analysis",
            first_invalid_sample=first_index,
            invalid_sample_count=int(np.count_nonzero(~finite)),
        )
    return SourceAudio(info=info, samples=samples, raw_bytes=raw, effective=effective)


def _analysis_fingerprint(source: SourceAudio, config: DSPConfig) -> str:
    return digest_obj(
        {
            "schema": "vc-analysis-input-v1",
            "config_version": CONFIG_VERSION,
            "raw_wav_sha256": source.info.raw_sha256,
            "declared_sample_count": source.info.frame_count,
            "frame_shift": source.effective.frame_shift,
            "dsp": config.to_dict(),
        }
    )


def _mapping_fingerprint(analysis_digest: str, effective: EffectiveParams) -> str:
    return digest_obj(
        {
            "schema": "vc-mapping-input-v1",
            "config_version": CONFIG_VERSION,
            "analysis_digest": analysis_digest,
            "target_speaker": effective.target_speaker,
            "pitch_shift": effective.pitch_shift,
            "mel_warp": effective.mel_warp,
            "profiles": "target-profiles-v1",
        }
    )


def _reconstruction_fingerprint(mapping_digest: str, effective: EffectiveParams) -> str:
    return digest_obj(
        {
            "schema": "vc-reconstruction-input-v1",
            "config_version": CONFIG_VERSION,
            "mapping_digest": mapping_digest,
            "quant_bits": effective.quant_bits,
            "expected_output_samples": effective.expected_output_samples,
            "sample_rate": DEFAULT_DSP.sample_rate,
            "phase_projection": "blake2b-u64-v1",
            "synthesis": "hann-ola-normalized-v1",
        }
    )


def _validation_fingerprint(
    reconstruction_digest: str,
    effective: EffectiveParams,
    *,
    verify_stream: bool,
) -> str:
    return digest_obj(
        {
            "schema": "vc-validation-input-v1",
            "config_version": CONFIG_VERSION,
            "reconstruction_digest": reconstruction_digest,
            "reference_pitch_hz": None
            if effective.reference_pitch is None
            else effective.reference_pitch.to_dict(),
            "reference_tolerance_hz": effective.reference_tolerance_hz,
            "verify_stream": bool(verify_stream),
            "diagnostic_schema": "pitch-alignment-v1",
        }
    )


def effective_from_artifacts(
    analysis: dict[str, Any],
    mapping: dict[str, Any],
    reconstruction: dict[str, Any],
    diagnostics: dict[str, Any],
) -> EffectiveParams:
    raw_reference = diagnostics.get("reference_pitch_hz")
    reference = None
    if isinstance(raw_reference, dict):
        reference = ReferencePitch(
            float(raw_reference["min_hz"]), float(raw_reference["max_hz"])
        )
    return EffectiveParams(
        target_speaker=str(mapping["target_speaker"]),
        pitch_shift=float(mapping["pitch_shift"]),
        mel_warp=float(mapping["mel_warp"]),
        frame_shift=int(analysis["frame_shift"]),
        quant_bits=int(reconstruction["quant_bits"]),
        expected_output_samples=int(reconstruction["expected_output_samples"]),
        reference_pitch=reference,
        reference_tolerance_hz=float(diagnostics["reference_tolerance_hz"]),
    )


def _analysis_files(payload: dict[str, Any]) -> dict[str, bytes]:
    output = io.StringIO()
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(
        ["frame_index", "start_sample", "f0_hz", "voiced", "rms", "mel_log_energy_digest"]
    )
    for frame in payload["frames"]:
        writer.writerow(
            [
                frame["frame_index"],
                frame["start_sample"],
                "UNDEFINED" if frame["f0_hz"] is None else frame["f0_hz"],
                "true" if frame["voiced"] else "false",
                frame["rms"],
                digest_obj(frame["mel_log_energy"]),
            ]
        )
    return {
        "analysis.json": canonical_json_bytes(payload),
        "frames.csv": output.getvalue().encode("utf-8"),
    }


def build_analysis_payload(
    spec: ShardSpec,
    source: SourceAudio,
    *,
    config: DSPConfig,
    block_frames: int,
    after_block: Callable[[str, int], None] | None = None,
) -> dict[str, Any]:
    n_frames = __import__("vcworkbench.dsp", fromlist=["frame_starts"]).frame_starts(
        source.samples.size, source.effective.frame_shift, config.frame_length
    )
    frames: list[dict[str, Any]] = []
    block_count = 0
    for start in range(0, n_frames, block_frames):
        stop = min(n_frames, start + block_frames)
        for frame_index in range(start, stop):
            frames.append(analyze_frame(source.samples, frame_index, source.effective.frame_shift, config))
        block_count += 1
        if after_block is not None:
            after_block("analysis", block_count)
    voiced = [float(frame["f0_hz"]) for frame in frames if frame["f0_hz"] is not None]
    mel_values = [frame["mel_log_energy"] for frame in frames]
    summary = {
        "frame_count": n_frames,
        "voiced_frames": len(voiced),
        "unvoiced_frames": n_frames - len(voiced),
        "f0_min_hz": r10(min(voiced)) if voiced else None,
        "f0_max_hz": r10(max(voiced)) if voiced else None,
        "f0_mean_hz": r10(float(np.mean(voiced))) if voiced else None,
        "mel_energy_digest": digest_obj(mel_values),
    }
    return {
        "schema": "vc-analysis-v1",
        "shard_id": spec.shard_id,
        "source_speaker": spec.source_speaker,
        "transcript": spec.transcript,
        "source_audio_sha256": source.info.raw_sha256,
        "sample_rate": config.sample_rate,
        "frame_length": config.frame_length,
        "frame_shift": source.effective.frame_shift,
        "f0_min_hz": config.f0_min_hz,
        "f0_max_hz": config.f0_max_hz,
        "mel_bands": config.mel_bands,
        "dsp": config.to_dict(),
        "frames": frames,
        "summary": summary,
    }


def build_mapping_payload(
    spec: ShardSpec,
    analysis: dict[str, Any],
    effective: EffectiveParams,
    *,
    analysis_digest: str,
    config: DSPConfig,
    block_frames: int,
    after_block: Callable[[str, int], None] | None = None,
) -> dict[str, Any]:
    profile = target_profile(effective.target_speaker)
    if profile is None:
        fail(
            "E_INVALID_PARAM",
            f"unknown target speaker: {effective.target_speaker}",
            shard_id=spec.shard_id,
            stage="mapping",
            known_target_speakers=sorted(
                __import__("vcworkbench.profiles", fromlist=["TARGET_SPEAKERS"]).TARGET_SPEAKERS
            ),
        )
    assert profile is not None
    frames: list[dict[str, Any]] = []
    block_count = 0
    mapped_voiced: list[float] = []
    source_frames = analysis["frames"]
    for start in range(0, len(source_frames), block_frames):
        stop = min(len(source_frames), start + block_frames)
        for frame in source_frames[start:stop]:
            warped = warp_mel(frame["mel_log_energy"], effective.mel_warp, config.mel_bands)
            source_f0 = frame["f0_hz"]
            target_f0 = None
            if source_f0 is not None:
                target_f0 = r10(
                    float(source_f0) * profile.pitch_factor * effective.pitch_shift
                )
                mapped_voiced.append(float(target_f0))
            frames.append(
                {
                    "frame_index": frame["frame_index"],
                    "source_f0_hz": source_f0,
                    "target_f0_hz": target_f0,
                    "voiced": target_f0 is not None,
                    "rms": frame["rms"],
                    "mel_log_energy": [float(v) for v in warped],
                }
            )
        block_count += 1
        if after_block is not None:
            after_block("mapping", block_count)
    summary = {
        "frame_count": len(frames),
        "voiced_frames": len(mapped_voiced),
        "mapped_f0_min_hz": r10(min(mapped_voiced)) if mapped_voiced else None,
        "mapped_f0_max_hz": r10(max(mapped_voiced)) if mapped_voiced else None,
        "mapped_f0_mean_hz": r10(float(np.mean(mapped_voiced))) if mapped_voiced else None,
        "mel_energy_digest": digest_obj([frame["mel_log_energy"] for frame in frames]),
    }
    return {
        "schema": "vc-mapping-v1",
        "shard_id": spec.shard_id,
        "source_speaker": spec.source_speaker,
        "target_speaker": effective.target_speaker,
        "target_profile": {
            "pitch_factor": profile.pitch_factor,
            "gain": profile.gain,
            "phase_cycles": profile.phase_cycles,
        },
        "pitch_shift": effective.pitch_shift,
        "effective_pitch_factor": r10(profile.pitch_factor * effective.pitch_shift),
        "mel_warp": effective.mel_warp,
        "analysis_digest": analysis_digest,
        "frames": frames,
        "summary": summary,
    }


def synthesize_frames(
    spec: ShardSpec,
    mapping: dict[str, Any],
    effective: EffectiveParams,
    *,
    profile: SpeakerProfile,
    block_frames: int,
    config: DSPConfig,
    after_block: Callable[[str, int], None] | None = None,
    incremental_pcm: bool = False,
) -> tuple[np.ndarray, int, list[dict[str, Any]]]:
    synth = OverlapAddSynthesizer(
        target_samples=effective.expected_output_samples,
        hop=effective.frame_shift,
        quant_bits=effective.quant_bits,
        config=config,
    )
    chunks: list[dict[str, Any]] = []
    frames = mapping["frames"]
    block_count = 0
    emitted_before = 0
    for start in range(0, len(frames), block_frames):
        stop = min(len(frames), start + block_frames)
        for frame in frames[start:stop]:
            waveform = reconstruct_frame(
                frame,
                shard_id=spec.shard_id,
                target_speaker=effective.target_speaker,
                profile=profile,
                hop=effective.frame_shift,
                config=config,
            )
            synth.add_frame(int(frame["frame_index"]), waveform)
        block_count += 1
        pcm_chunk = synth.drain() if incremental_pcm else np.zeros(0, dtype="<i2")
        if after_block is not None:
            after_block("reconstruction", block_count)
        if pcm_chunk.size:
            chunks.append(
                {
                    "chunk_index": len(chunks),
                    "block_index": block_count - 1,
                    "frame_start": start,
                    "frame_count": stop - start,
                    "sample_start": emitted_before,
                    "sample_count": int(pcm_chunk.size),
                    "pcm_sha256": sha256_bytes(pcm_chunk.tobytes()),
                }
            )
            emitted_before += int(pcm_chunk.size)
    tail = synth.finish()
    if tail.size:
        chunks.append(
            {
                "chunk_index": len(chunks),
                "block_index": block_count,
                "frame_start": len(frames),
                "frame_count": 0,
                "sample_start": emitted_before,
                "sample_count": int(tail.size),
                "pcm_sha256": sha256_bytes(tail.tobytes()),
            }
        )
        emitted_before += int(tail.size)
    pcm = collect_pcm(synth.pcm_parts)
    return pcm, synth.clip_count, chunks


def build_reconstruction_files(
    spec: ShardSpec,
    mapping: dict[str, Any],
    effective: EffectiveParams,
    *,
    mapping_digest: str,
    block_frames: int,
    config: DSPConfig,
    after_block: Callable[[str, int], None] | None = None,
) -> tuple[dict[str, Any], dict[str, bytes]]:
    profile = target_profile(effective.target_speaker)
    if profile is None:
        fail(
            "E_INVALID_PARAM",
            f"unknown target speaker: {effective.target_speaker}",
            shard_id=spec.shard_id,
            stage="mapping",
        )
    assert profile is not None
    pcm, clip_count, chunks = synthesize_frames(
        spec,
        mapping,
        effective,
        profile=profile,
        block_frames=block_frames,
        config=config,
        after_block=after_block,
    )
    if int(pcm.size) != effective.expected_output_samples:
        fail(
            "E_STREAM_MISMATCH",
            "reconstructed sample count does not equal the declared upper bound",
            shard_id=spec.shard_id,
            stage="reconstruction",
            expected_samples=effective.expected_output_samples,
            actual_samples=int(pcm.size),
        )
    pcm_data = pcm.astype("<i2", copy=False).tobytes()
    wav_data = wav_bytes_from_pcm16(pcm, config.sample_rate)
    payload = {
        "schema": "vc-reconstruction-v1",
        "shard_id": spec.shard_id,
        "mapping_digest": mapping_digest,
        "sample_rate": config.sample_rate,
        "quant_bits": effective.quant_bits,
        "expected_output_samples": effective.expected_output_samples,
        "pcm_sample_count": int(pcm.size),
        "clip_count": clip_count,
        "pcm_sha256": sha256_bytes(pcm_data),
        "wav_sha256": sha256_bytes(wav_data),
        "synthesis": "hann-ola-normalized-v1",
        "phase_projection": "blake2b-u64-v1",
    }
    return payload, {
        "reconstruction.json": canonical_json_bytes(payload),
        "output.pcm": pcm_data,
        "output.wav": wav_data,
    }


def build_diagnostics(
    spec: ShardSpec,
    mapping: dict[str, Any],
    effective: EffectiveParams,
    *,
    reconstruction_digest: str,
    stream_consistency: dict[str, Any],
) -> dict[str, Any]:
    if effective.reference_pitch is None:
        fail(
            "E_REFERENCE_MISSING",
            "manifest reference pitch interval is required for truth alignment",
            shard_id=spec.shard_id,
            stage="validation",
        )
    reference = effective.reference_pitch
    center = (reference.min_hz + reference.max_hz) / 2.0
    tolerance = effective.reference_tolerance_hz
    frames_out: list[dict[str, Any]] = []
    voiced_values: list[float] = []
    outside_indices: list[int] = []
    out_of_tolerance: list[int] = []
    interpolation_count = 0
    extrapolation_count = 0
    unvoiced_count = 0
    deviations: list[float] = []
    abs_deviations: list[float] = []
    for frame in mapping["frames"]:
        f0 = frame["target_f0_hz"]
        if f0 is None:
            unvoiced_count += 1
            frames_out.append(
                {
                    "frame_index": frame["frame_index"],
                    "target_f0_hz": None,
                    "deviation_from_center_hz": None,
                    "outside_interval_hz": None,
                    "in_reference_interval": False,
                    "alignment": "unvoiced",
                }
            )
            continue
        f0 = float(f0)
        voiced_values.append(f0)
        inside = reference.min_hz <= f0 <= reference.max_hz
        outside = max(reference.min_hz - f0, f0 - reference.max_hz, 0.0)
        deviation = f0 - center
        deviation_r = r10(deviation)
        outside_r = r10(outside)
        deviations.append(deviation)
        abs_deviations.append(abs(deviation))
        if inside:
            interpolation_count += 1
            alignment = "interpolation"
        else:
            extrapolation_count += 1
            alignment = "extrapolation"
            outside_indices.append(int(frame["frame_index"]))
        if outside > tolerance:
            out_of_tolerance.append(int(frame["frame_index"]))
        frames_out.append(
            {
                "frame_index": frame["frame_index"],
                "target_f0_hz": r10(f0),
                "deviation_from_center_hz": deviation_r,
                "outside_interval_hz": outside_r,
                "in_reference_interval": inside,
                "alignment": alignment,
            }
        )
    voiced_count = len(voiced_values)
    payload = {
        "schema": "vc-diagnostics-v1",
        "shard_id": spec.shard_id,
        "reconstruction_digest": reconstruction_digest,
        "reference_pitch_hz": reference.to_dict(),
        "reference_center_hz": r10(center),
        "reference_tolerance_hz": r10(tolerance),
        "voiced_frame_count": voiced_count,
        "unvoiced_frame_count": unvoiced_count,
        "in_range_frame_count": interpolation_count,
        "outside_interval_frame_count": extrapolation_count,
        "in_range_ratio": r10(interpolation_count / voiced_count) if voiced_count else 0.0,
        "interpolation_frames": interpolation_count,
        "extrapolation_frames": extrapolation_count,
        "mean_target_f0_hz": r10(float(np.mean(voiced_values))) if voiced_values else None,
        "mean_deviation_hz": r10(float(np.mean(deviations))) if deviations else None,
        "mean_abs_deviation_hz": r10(float(np.mean(abs_deviations))) if abs_deviations else None,
        "outside_interval_frame_indices": outside_indices,
        "out_of_tolerance_frame_indices": out_of_tolerance,
        "max_outside_interval_hz": r10(max(
            (float(frame["outside_interval_hz"]) for frame in frames_out if frame["outside_interval_hz"] is not None),
            default=0.0,
        )),
        "frames": frames_out,
        "stream_consistency": stream_consistency,
    }
    if out_of_tolerance:
        fail(
            "E_PITCH_OUT_OF_RANGE",
            "converted pitch deviates beyond the configured reference tolerance",
            shard_id=spec.shard_id,
            stage="validation",
            frame_indices=out_of_tolerance,
            first_frame_index=out_of_tolerance[0],
            reference_pitch_hz=reference.to_dict(),
            reference_tolerance_hz=r10(tolerance),
            outside_interval_frame_indices=outside_indices,
        )
    return payload


def verify_streaming(
    spec: ShardSpec,
    source: SourceAudio,
    analysis: dict[str, Any],
    mapping: dict[str, Any],
    expected_pcm: bytes,
    *,
    block_frames: int,
    config: DSPConfig,
    after_block: Callable[[str, int], None] | None = None,
) -> dict[str, Any]:
    effective = source.effective
    profile = target_profile(effective.target_speaker)
    if profile is None:
        fail(
            "E_INVALID_PARAM",
            f"unknown target speaker: {effective.target_speaker}",
            shard_id=spec.shard_id,
            stage="mapping",
        )
    assert profile is not None
    synth = OverlapAddSynthesizer(
        target_samples=effective.expected_output_samples,
        hop=effective.frame_shift,
        quant_bits=effective.quant_bits,
        config=config,
    )
    reference_frames = analysis["frames"]
    reference_mapping = mapping["frames"]
    chunks: list[dict[str, Any]] = []
    block_count = 0
    emitted = 0
    for start in range(0, len(reference_frames), block_frames):
        stop = min(len(reference_frames), start + block_frames)
        for frame_index in range(start, stop):
            streamed_analysis = analyze_frame(
                source.samples, frame_index, effective.frame_shift, config
            )
            expected_analysis = reference_frames[frame_index]
            if streamed_analysis["f0_hz"] != expected_analysis["f0_hz"]:
                fail(
                    "E_STREAM_MISMATCH",
                    "streaming and full analysis F0 sequences differ",
                    shard_id=spec.shard_id,
                    stage="validation",
                    first_mismatch_frame=frame_index,
                    kind="f0",
                )
            if streamed_analysis["mel_log_energy"] != expected_analysis["mel_log_energy"]:
                fail(
                    "E_STREAM_MISMATCH",
                    "streaming and full mel-energy summaries differ",
                    shard_id=spec.shard_id,
                    stage="validation",
                    first_mismatch_frame=frame_index,
                    kind="mel_log_energy",
                )
            warped = warp_mel(
                streamed_analysis["mel_log_energy"],
                effective.mel_warp,
                config.mel_bands,
            )
            target_f0 = None
            if streamed_analysis["f0_hz"] is not None:
                target_f0 = r10(
                    float(streamed_analysis["f0_hz"])
                    * profile.pitch_factor
                    * effective.pitch_shift
                )
            expected_mapped = reference_mapping[frame_index]
            if target_f0 != expected_mapped["target_f0_hz"]:
                fail(
                    "E_STREAM_MISMATCH",
                    "streaming and full mapped-F0 sequences differ",
                    shard_id=spec.shard_id,
                    stage="validation",
                    first_mismatch_frame=frame_index,
                    kind="mapped_f0",
                )
            if [float(v) for v in warped] != expected_mapped["mel_log_energy"]:
                fail(
                    "E_STREAM_MISMATCH",
                    "streaming and full mapped mel energies differ",
                    shard_id=spec.shard_id,
                    stage="validation",
                    first_mismatch_frame=frame_index,
                    kind="mapped_mel",
                )
            mapped_frame = {
                "frame_index": frame_index,
                "target_f0_hz": target_f0,
                "voiced": target_f0 is not None,
                "rms": streamed_analysis["rms"],
                "mel_log_energy": [float(v) for v in warped],
            }
            waveform = reconstruct_frame(
                mapped_frame,
                shard_id=spec.shard_id,
                target_speaker=effective.target_speaker,
                profile=profile,
                hop=effective.frame_shift,
                config=config,
            )
            synth.add_frame(frame_index, waveform)
        block_count += 1
        chunk = synth.drain()
        if after_block is not None:
            after_block("validation", block_count)
        if chunk.size:
            chunks.append(
                {
                    "chunk_index": len(chunks),
                    "block_index": block_count - 1,
                    "frame_start": start,
                    "frame_count": stop - start,
                    "sample_start": emitted,
                    "sample_count": int(chunk.size),
                    "pcm_sha256": sha256_bytes(chunk.tobytes()),
                    "pcm_base64": base64.b64encode(chunk.tobytes()).decode("ascii"),
                }
            )
            emitted += int(chunk.size)
    tail = synth.finish()
    if tail.size:
        chunks.append(
            {
                "chunk_index": len(chunks),
                "block_index": block_count,
                "frame_start": len(reference_frames),
                "frame_count": 0,
                "sample_start": emitted,
                "sample_count": int(tail.size),
                "pcm_sha256": sha256_bytes(tail.tobytes()),
                "pcm_base64": base64.b64encode(tail.tobytes()).decode("ascii"),
            }
        )
        emitted += int(tail.size)
    streamed_pcm = collect_pcm(synth.pcm_parts).astype("<i2", copy=False).tobytes()
    if len(streamed_pcm) != len(expected_pcm):
        common_samples = min(len(streamed_pcm), len(expected_pcm)) // 2
        first = min(
            max(0, len(reference_frames) - 1),
            common_samples // max(1, effective.frame_shift),
        )
        fail(
            "E_STREAM_MISMATCH",
            "streaming and full PCM sample counts differ",
            shard_id=spec.shard_id,
            stage="validation",
            first_mismatch_frame=first,
            expected_bytes=len(expected_pcm),
            actual_bytes=len(streamed_pcm),
        )
    if streamed_pcm != expected_pcm:
        mismatch_byte = next(
            index for index, (left, right) in enumerate(zip(streamed_pcm, expected_pcm)) if left != right
        )
        mismatch_sample = mismatch_byte // 2
        first = min(len(reference_frames) - 1, mismatch_sample // effective.frame_shift)
        fail(
            "E_STREAM_MISMATCH",
            "streaming and full PCM bytes differ",
            shard_id=spec.shard_id,
            stage="validation",
            first_mismatch_frame=max(0, first),
            first_mismatch_sample=mismatch_sample,
            expected_pcm_sha256=sha256_bytes(expected_pcm),
            actual_pcm_sha256=sha256_bytes(streamed_pcm),
        )
    return {
        "verified": True,
        "sample_count": emitted,
        "pcm_sha256": sha256_bytes(streamed_pcm),
        "f0_sequence_digest": digest_obj([frame["f0_hz"] for frame in reference_frames]),
        "mel_energy_digest": analysis["summary"]["mel_energy_digest"],
        "chunk_count": len(chunks),
        "chunks": chunks,
    }


class PipelineRunner:
    def __init__(
        self,
        workspace: Workspace,
        *,
        dsp_config: DSPConfig = DEFAULT_DSP,
        stop_after_blocks: int | None = None,
    ) -> None:
        self.workspace = workspace
        self.dsp = dsp_config
        self.store = CheckpointStore(workspace.state_dir)
        self.stop_after_blocks = stop_after_blocks
        if stop_after_blocks is not None and stop_after_blocks <= 0:
            fail("E_INVALID_PARAM", "stop_after_blocks must be positive", stage="ingest")

    def _block_hook(self, shard_id: str) -> Callable[[str, int], None]:
        def hook(stage: str, block_count: int) -> None:
            if (
                self.stop_after_blocks is not None
                and stage == "reconstruction"
                and block_count >= self.stop_after_blocks
            ):
                self.workspace.request_stop(reason="simulated stop after frame block")
            if self.workspace.should_stop(shard_id):
                raise OperationStopped(shard_id, stage, block_count)

        return hook

    def _read_artifact_json(self, artifact: Artifact, name: str) -> dict[str, Any]:
        path = artifact.path(name)
        try:
            return read_json(path)
        except Exception as exc:
            raise WorkbenchError(
                "E_STALE_CHECKPOINT",
                f"cannot read artifact payload: {exc}",
                shard_id=artifact.shard_id,
                stage=artifact.stage,
                details={"path": str(path)},
            ) from exc

    def _current_match(self, shard_id: str, stage: str, fingerprint: str) -> Artifact | None:
        record = self.store.current(shard_id, stage, verify_artifact=False)
        if record is None:
            return None
        artifact = self.store.artifacts.load(
            self.workspace.state_dir / record["artifact_relpath"],
            expected_shard=shard_id,
            strict=True,
        )
        if artifact.artifact_digest != record["artifact_digest"]:
            fail(
                "E_STALE_CHECKPOINT",
                "checkpoint artifact digest mismatch",
                shard_id=shard_id,
                stage=stage,
            )
        if record["input_fingerprint"] != fingerprint:
            return None
        return artifact

    def _artifact_or_build(
        self,
        *,
        shard_id: str,
        stage: str,
        fingerprint: str,
        build: Callable[[], tuple[dict[str, Any], dict[str, bytes]]],
        force_build: bool = False,
    ) -> tuple[Artifact, dict[str, Any], bool]:
        directory = self.store.artifacts.directory(shard_id, stage, fingerprint)
        cached = None if force_build else self.store.artifacts.load_if_valid(directory, strict=False)
        if cached is not None and cached.input_fingerprint == fingerprint:
            payload_name = {
                "analysis": "analysis.json",
                "mapping": "mapping.json",
                "reconstruction": "reconstruction.json",
                "validation": "diagnostics.json",
            }[stage]
            return cached, self._read_artifact_json(cached, payload_name), False
        payload, files = build()
        artifact = self.store.artifacts.put(
            shard_id=shard_id,
            stage=stage,
            input_fingerprint=fingerprint,
            files=files,
        )
        return artifact, payload, True

    def _build_stage_files(
        self,
        stage: str,
        payload: dict[str, Any],
        *,
        extra_files: dict[str, bytes] | None = None,
    ) -> tuple[dict[str, Any], dict[str, bytes]]:
        if stage == "analysis":
            return payload, _analysis_files(payload)
        if stage == "mapping":
            return payload, {"mapping.json": canonical_json_bytes(payload)}
        if stage == "reconstruction":
            assert extra_files is not None
            return payload, extra_files
        if stage == "validation":
            return payload, {"diagnostics.json": canonical_json_bytes(payload)}
        fail("E_INVALID_STAGE", f"unknown stage {stage}")
        raise AssertionError("unreachable")

    def run_shard(
        self,
        manifest: Manifest,
        shard_id: str,
        *,
        block_frames: int = 8,
        verify_stream: bool = True,
        injection: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if isinstance(block_frames, bool) or not isinstance(block_frames, int) or block_frames <= 0:
            fail("E_INVALID_PARAM", "block_frames must be a positive integer", shard_id=shard_id, stage="ingest")
        spec = manifest.by_id(shard_id)
        if self.workspace.should_stop(shard_id):
            raise OperationStopped(shard_id, "preflight", 0)
        source = load_source(self.workspace, spec, injection)
        effective = source.effective
        if target_profile(effective.target_speaker) is None:
            fail(
                "E_INVALID_PARAM",
                f"unknown target speaker: {effective.target_speaker}",
                shard_id=shard_id,
                stage="mapping",
            )

        analysis_fp = _analysis_fingerprint(source, self.dsp)
        reused: dict[str, Artifact] = {}
        earliest: str | None = None
        analysis_artifact = self._current_match(shard_id, "analysis", analysis_fp)
        if analysis_artifact is not None:
            reused["analysis"] = analysis_artifact
        else:
            earliest = "analysis"
        if earliest is None:
            mapping_fp = _mapping_fingerprint(reused["analysis"].artifact_digest, effective)
            mapping_artifact = self._current_match(shard_id, "mapping", mapping_fp)
            if mapping_artifact is not None:
                reused["mapping"] = mapping_artifact
            else:
                earliest = "mapping"
        if earliest is None:
            reconstruction_fp = _reconstruction_fingerprint(
                reused["mapping"].artifact_digest, effective
            )
            reconstruction_artifact = self._current_match(
                shard_id, "reconstruction", reconstruction_fp
            )
            if reconstruction_artifact is not None:
                reused["reconstruction"] = reconstruction_artifact
            else:
                earliest = "reconstruction"
        if earliest is None:
            validation_fp = _validation_fingerprint(
                reused["reconstruction"].artifact_digest,
                effective,
                verify_stream=verify_stream,
            )
            validation_artifact = self._current_match(shard_id, "validation", validation_fp)
            if validation_artifact is not None:
                reused["validation"] = validation_artifact
            else:
                earliest = "validation"

        ledger_before = self.store.read_ledger(shard_id)
        rebuild_info = None
        next_seq = 0
        epoch = 0
        if earliest is not None:
            rebuild_info = self.store.begin_rebuild(shard_id, earliest)
            next_seq = rebuild_info["next_seq"]
            epoch = rebuild_info["epoch"]
        elif ledger_before is None:
            earliest = "analysis"
            rebuild_info = self.store.begin_rebuild(shard_id, earliest)
            next_seq = rebuild_info["next_seq"]
            epoch = rebuild_info["epoch"]

        stage_results: list[dict[str, Any]] = []
        current_artifacts: dict[str, Artifact] = dict(reused)
        fingerprints: dict[str, str] = {}
        payloads: dict[str, dict[str, Any]] = {}
        start_index = len(STAGES) if earliest is None else STAGES.index(earliest)

        for stage in STAGES[:start_index]:
            artifact = current_artifacts[stage]
            record = self.store.current(shard_id, stage, verify_artifact=True)
            assert record is not None
            fingerprints[stage] = record["input_fingerprint"]
            payloads[stage] = self._read_artifact_json(
                artifact,
                {
                    "analysis": "analysis.json",
                    "mapping": "mapping.json",
                    "reconstruction": "reconstruction.json",
                    "validation": "diagnostics.json",
                }[stage],
            )
            stage_results.append(
                {
                    "stage": stage,
                    "status": "reused",
                    "input_fingerprint": record["input_fingerprint"],
                    "artifact_digest": record["artifact_digest"],
                    "seq": record["seq"],
                }
            )

        for stage in STAGES[start_index:]:
            hook = self._block_hook(shard_id)
            built = False
            if stage == "analysis":
                fingerprint = analysis_fp

                def build_analysis(stage=stage):
                    return self._build_stage_files(
                        stage,
                        build_analysis_payload(
                            spec,
                            source,
                            config=self.dsp,
                            block_frames=block_frames,
                            after_block=hook,
                        ),
                    )

                artifact, payload, built = self._artifact_or_build(
                    shard_id=shard_id,
                    stage=stage,
                    fingerprint=fingerprint,
                    build=build_analysis,
                )
            elif stage == "mapping":
                analysis_digest = current_artifacts["analysis"].artifact_digest
                fingerprint = _mapping_fingerprint(analysis_digest, effective)
                analysis_payload = self._read_artifact_json(
                    current_artifacts["analysis"], "analysis.json"
                )

                def build_mapping(stage=stage):
                    return self._build_stage_files(
                        stage,
                        build_mapping_payload(
                            spec,
                            analysis_payload,
                            effective,
                            analysis_digest=analysis_digest,
                            config=self.dsp,
                            block_frames=block_frames,
                            after_block=hook,
                        ),
                    )

                artifact, payload, built = self._artifact_or_build(
                    shard_id=shard_id,
                    stage=stage,
                    fingerprint=fingerprint,
                    build=build_mapping,
                )
            elif stage == "reconstruction":
                mapping_digest = current_artifacts["mapping"].artifact_digest
                fingerprint = _reconstruction_fingerprint(mapping_digest, effective)
                mapping_payload = self._read_artifact_json(
                    current_artifacts["mapping"], "mapping.json"
                )

                def build_reconstruction(stage=stage):
                    payload, files = build_reconstruction_files(
                        spec,
                        mapping_payload,
                        effective,
                        mapping_digest=mapping_digest,
                        block_frames=block_frames,
                        config=self.dsp,
                        after_block=hook,
                    )
                    return self._build_stage_files(stage, payload, extra_files=files)

                artifact, payload, built = self._artifact_or_build(
                    shard_id=shard_id,
                    stage=stage,
                    fingerprint=fingerprint,
                    build=build_reconstruction,
                    force_build=self.stop_after_blocks is not None,
                )
            else:
                reconstruction_digest = current_artifacts["reconstruction"].artifact_digest
                fingerprint = _validation_fingerprint(
                    reconstruction_digest, effective, verify_stream=verify_stream
                )
                analysis_payload = self._read_artifact_json(
                    current_artifacts["analysis"], "analysis.json"
                )
                mapping_payload = self._read_artifact_json(
                    current_artifacts["mapping"], "mapping.json"
                )
                reconstruction_artifact = current_artifacts["reconstruction"]
                expected_pcm = reconstruction_artifact.path("output.pcm").read_bytes()

                def build_validation(stage=stage):
                    if verify_stream:
                        stream = verify_streaming(
                            spec,
                            source,
                            analysis_payload,
                            mapping_payload,
                            expected_pcm,
                            block_frames=block_frames,
                            config=self.dsp,
                            after_block=hook,
                        )
                        # Runtime chunk boundaries are intentionally excluded: the
                        # validation fingerprint and final artifact must not change
                        # when only block_frames changes.
                        stream_summary = {
                            "verified": True,
                            "sample_count": stream["sample_count"],
                            "pcm_sha256": stream["pcm_sha256"],
                            "f0_sequence_digest": stream["f0_sequence_digest"],
                            "mel_energy_digest": stream["mel_energy_digest"],
                        }
                    else:
                        stream_summary = {
                            "verified": False,
                            "reason": "full-only mode requested",
                        }
                    payload = build_diagnostics(
                        spec,
                        mapping_payload,
                        effective,
                        reconstruction_digest=reconstruction_digest,
                        stream_consistency=stream_summary,
                    )
                    return self._build_stage_files(stage, payload)

                artifact, payload, built = self._artifact_or_build(
                    shard_id=shard_id,
                    stage=stage,
                    fingerprint=fingerprint,
                    build=build_validation,
                )
            current_artifacts[stage] = artifact
            fingerprints[stage] = fingerprint
            payloads[stage] = payload
            prerequisite = None
            if STAGES.index(stage) > 0:
                prerequisite = current_artifacts[STAGES[STAGES.index(stage) - 1]].artifact_digest
            if built:
                record = self.store.publish(
                    shard_id=shard_id,
                    stage=stage,
                    expected_seq=next_seq,
                    expected_epoch=epoch,
                    input_fingerprint=fingerprint,
                    artifact=artifact,
                    prerequisite_digest=prerequisite,
                )
                next_seq += 1
                stage_results.append(
                    {
                        "stage": stage,
                        "status": "built",
                        "input_fingerprint": fingerprint,
                        "artifact_digest": artifact.artifact_digest,
                        "seq": record["seq"],
                    }
                )
            else:
                # The content-addressed cache survived but the checkpoint pointer did
                # not. Republish it so continuity and crash recovery remain explicit.
                record = self.store.publish(
                    shard_id=shard_id,
                    stage=stage,
                    expected_seq=next_seq,
                    expected_epoch=epoch,
                    input_fingerprint=fingerprint,
                    artifact=artifact,
                    prerequisite_digest=prerequisite,
                )
                next_seq += 1
                stage_results.append(
                    {
                        "stage": stage,
                        "status": "reused_cache",
                        "input_fingerprint": fingerprint,
                        "artifact_digest": artifact.artifact_digest,
                        "seq": record["seq"],
                    }
                )

        if earliest is None:
            # All stages matched. Populate payloads from current artifacts for the report.
            for stage in STAGES:
                artifact = current_artifacts[stage]
                payloads[stage] = self._read_artifact_json(
                    artifact,
                    {
                        "analysis": "analysis.json",
                        "mapping": "mapping.json",
                        "reconstruction": "reconstruction.json",
                        "validation": "diagnostics.json",
                    }[stage],
                )
                fingerprints[stage] = self.store.current(
                    shard_id, stage, verify_artifact=True
                )["input_fingerprint"]

        reconstruction = payloads["reconstruction"]
        diagnostics = payloads["validation"]
        stream_consistency = diagnostics.get("stream_consistency", {"verified": False})
        result = {
            "ok": True,
            "status": "complete",
            "shard_id": shard_id,
            "transcript": spec.transcript,
            "source_speaker": spec.source_speaker,
            "target_speaker": effective.target_speaker,
            "effective_parameters": effective.to_dict(),
            "block_frames": block_frames,
            "verify_stream": bool(verify_stream),
            "earliest_invalidated_stage": earliest,
            "rebuild": rebuild_info,
            "stages": stage_results,
            "version_fingerprints": fingerprints,
            "output": {
                "sample_count": reconstruction["pcm_sample_count"],
                "expected_output_samples": reconstruction["expected_output_samples"],
                "quant_bits": reconstruction["quant_bits"],
                "clip_count": reconstruction["clip_count"],
                "pcm_sha256": reconstruction["pcm_sha256"],
                "wav_sha256": reconstruction["wav_sha256"],
                "wav_artifact": current_artifacts["reconstruction"].relpath + "/output.wav",
                "pcm_artifact": current_artifacts["reconstruction"].relpath + "/output.pcm",
            },
            "diagnostics": {
                "reference_pitch_hz": diagnostics["reference_pitch_hz"],
                "in_range_ratio": diagnostics["in_range_ratio"],
                "mean_target_f0_hz": diagnostics["mean_target_f0_hz"],
                "mean_deviation_hz": diagnostics["mean_deviation_hz"],
                "outside_interval_frame_indices": diagnostics[
                    "outside_interval_frame_indices"
                ],
                "interpolation_frames": diagnostics["interpolation_frames"],
                "extrapolation_frames": diagnostics["extrapolation_frames"],
            },
            "stream_consistency": stream_consistency,
        }
        return result

    def convert(
        self,
        *,
        shard_ids: list[str] | None = None,
        block_frames: int = 8,
        verify_stream: bool = True,
        injections: dict[str, dict[str, Any]] | None = None,
        clear_stop: bool = False,
        progress: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        if clear_stop:
            self.workspace.clear_stop()
        self.workspace.ensure()
        manifest = self.workspace.require_manifest()
        selected = shard_ids or [spec.shard_id for spec in manifest.shards]
        known = {spec.shard_id for spec in manifest.shards}
        for shard_id in selected:
            if shard_id not in known:
                fail(
                    "E_INVALID_PARAM",
                    f"unknown shard id: {shard_id}",
                    shard_id=shard_id,
                    stage="ingest",
                )
        results: list[dict[str, Any]] = []
        for index, shard_id in enumerate(selected):
            injection = (injections or {}).get(shard_id, {})
            if progress is not None:
                progress(
                    {
                        "status": "running",
                        "shard_id": shard_id,
                        "index": index,
                        "total": len(selected),
                    }
                )
            result = self.run_shard(
                manifest,
                shard_id,
                block_frames=block_frames,
                verify_stream=verify_stream,
                injection=injection,
            )
            results.append(result)
            if progress is not None:
                progress(
                    {
                        "status": "shard_complete",
                        "shard_id": shard_id,
                        "index": index + 1,
                        "total": len(selected),
                        "result": result,
                    }
                )
        return {
            "ok": True,
            "status": "complete",
            "manifest_digest": manifest.digest(),
            "shard_ids": selected,
            "results": results,
        }

    def _load_complete_artifacts(self, shard_id: str) -> dict[str, dict[str, Any]]:
        manifest = self.workspace.require_manifest()
        manifest.by_id(shard_id)
        payloads: dict[str, dict[str, Any]] = {}
        names = {
            "analysis": "analysis.json",
            "mapping": "mapping.json",
            "reconstruction": "reconstruction.json",
            "validation": "diagnostics.json",
        }
        for stage in STAGES:
            artifact = self.store.load_current_artifact(shard_id, stage)
            if artifact is None:
                fail(
                    "E_INVALID_STAGE",
                    f"stage artifact is not available: {stage}",
                    shard_id=shard_id,
                    stage=stage,
                )
            payloads[stage] = self._read_artifact_json(artifact, names[stage])
        return payloads

    def diagnostics(self, shard_id: str) -> dict[str, Any]:
        manifest = self.workspace.require_manifest()
        spec = manifest.by_id(shard_id)
        payloads = self._load_complete_artifacts(shard_id)
        diagnostics = payloads["validation"]
        return {
            "ok": True,
            "shard_id": shard_id,
            "source_speaker": spec.source_speaker,
            "target_speaker": payloads["mapping"]["target_speaker"],
            "transcript": spec.transcript,
            "summary": {
                key: diagnostics[key]
                for key in (
                    "reference_pitch_hz",
                    "reference_center_hz",
                    "reference_tolerance_hz",
                    "voiced_frame_count",
                    "unvoiced_frame_count",
                    "in_range_frame_count",
                    "outside_interval_frame_count",
                    "in_range_ratio",
                    "interpolation_frames",
                    "extrapolation_frames",
                    "mean_target_f0_hz",
                    "mean_deviation_hz",
                    "mean_abs_deviation_hz",
                    "outside_interval_frame_indices",
                    "out_of_tolerance_frame_indices",
                    "max_outside_interval_hz",
                    "stream_consistency",
                )
            },
            "frames": diagnostics["frames"],
        }

    def stream_check(self, shard_id: str, *, block_frames: int = 8) -> dict[str, Any]:
        if isinstance(block_frames, bool) or not isinstance(block_frames, int) or block_frames <= 0:
            fail(
                "E_INVALID_PARAM",
                "block_frames must be a positive integer",
                shard_id=shard_id,
                stage="validation",
            )
        manifest = self.workspace.require_manifest()
        spec = manifest.by_id(shard_id)
        source = load_source(self.workspace, spec, None)
        payloads = self._load_complete_artifacts(shard_id)
        source.effective = effective_from_artifacts(
            payloads["analysis"],
            payloads["mapping"],
            payloads["reconstruction"],
            payloads["validation"],
        )
        artifact = self.store.load_current_artifact(shard_id, "reconstruction")
        assert artifact is not None
        expected_pcm = artifact.path("output.pcm").read_bytes()
        result = verify_streaming(
            spec,
            source,
            payloads["analysis"],
            payloads["mapping"],
            expected_pcm,
            block_frames=block_frames,
            config=self.dsp,
        )
        result.update(
            {
                "ok": True,
                "shard_id": shard_id,
                "block_frames": block_frames,
                "concatenated_pcm_bytes": sum(
                    int(chunk["sample_count"]) * 2 for chunk in result["chunks"]
                ),
                "full_pcm_sha256": sha256_bytes(expected_pcm),
            }
        )
        return result

    def diagnose_frame(self, shard_id: str, frame_index: int) -> dict[str, Any]:
        if isinstance(frame_index, bool) or not isinstance(frame_index, int) or frame_index < 0:
            fail(
                "E_INVALID_PARAM",
                "frame_index must be a non-negative integer",
                shard_id=shard_id,
                stage="diagnostics",
                frame_index=frame_index,
            )
        payloads = self._load_complete_artifacts(shard_id)
        analysis = payloads["analysis"]
        mapping = payloads["mapping"]
        diagnostics = payloads["validation"]
        if frame_index >= len(analysis["frames"]):
            fail(
                "E_INVALID_PARAM",
                "frame_index is outside the shard frame range",
                shard_id=shard_id,
                stage="diagnostics",
                frame_index=frame_index,
                frame_count=len(analysis["frames"]),
            )
        return {
            "ok": True,
            "shard_id": shard_id,
            "frame_index": frame_index,
            "analysis_frame": analysis["frames"][frame_index],
            "mapping_frame": mapping["frames"][frame_index],
            "diagnostic_frame": diagnostics["frames"][frame_index],
            "reference_pitch_hz": diagnostics["reference_pitch_hz"],
            "reference_center_hz": diagnostics["reference_center_hz"],
            "reference_tolerance_hz": diagnostics["reference_tolerance_hz"],
            "alignment_definition": {
                "interpolation": "mapped F0 lies inside the manifest mean interval",
                "extrapolation": "mapped F0 lies outside the manifest mean interval",
                "unvoiced": "no reliable source F0; excluded from ratio denominator",
            },
        }

    def report(self, shard_id: str) -> dict[str, Any]:
        manifest = self.workspace.require_manifest()
        spec = manifest.by_id(shard_id)
        payloads = self._load_complete_artifacts(shard_id)
        ledger = self.store.read_ledger(shard_id)
        status = self.store.status(shard_id, verify_artifacts=True)
        reconstruction = payloads["reconstruction"]
        diagnostics = payloads["validation"]
        current = {} if ledger is None else ledger["current"]
        return {
            "ok": True,
            "schema": "vc-conversion-report-v1",
            "config_version": CONFIG_VERSION,
            "manifest_digest": manifest.digest(),
            "shard": spec.to_dict(),
            "transcript": spec.transcript,
            "target_speaker": payloads["mapping"]["target_speaker"],
            "effective_pitch_factor": payloads["mapping"]["effective_pitch_factor"],
            "version_fingerprints": {
                stage: (current.get(stage) or {}).get("input_fingerprint")
                for stage in STAGES
            },
            "checkpoints": {
                "seq": status.get("seq", 0),
                "epoch": status.get("epoch", 0),
                "stages": {
                    stage: {
                        "status": status.get("stage_status", {}).get(stage),
                        "seq": (current.get(stage) or {}).get("seq"),
                        "artifact_digest": (current.get(stage) or {}).get("artifact_digest"),
                        "record_digest": (current.get(stage) or {}).get("record_digest"),
                    }
                    for stage in STAGES
                },
            },
            "analysis_summary": payloads["analysis"]["summary"],
            "mapping_summary": payloads["mapping"]["summary"],
            "output": {
                "sample_count": reconstruction["pcm_sample_count"],
                "expected_output_samples": reconstruction["expected_output_samples"],
                "quant_bits": reconstruction["quant_bits"],
                "clip_count": reconstruction["clip_count"],
                "pcm_sha256": reconstruction["pcm_sha256"],
                "wav_sha256": reconstruction["wav_sha256"],
            },
            "pitch_diagnostics": {
                key: diagnostics[key]
                for key in (
                    "reference_pitch_hz",
                    "reference_center_hz",
                    "reference_tolerance_hz",
                    "in_range_ratio",
                    "interpolation_frames",
                    "extrapolation_frames",
                    "outside_interval_frame_indices",
                    "out_of_tolerance_frame_indices",
                    "mean_target_f0_hz",
                    "mean_deviation_hz",
                    "mean_abs_deviation_hz",
                    "stream_consistency",
                )
            },
            "frames": diagnostics["frames"],
        }

    def export_wav(self, shard_id: str, output_path: Path) -> dict[str, Any]:
        artifact = self.store.load_current_artifact(shard_id, "reconstruction")
        if artifact is None:
            fail(
                "E_INVALID_STAGE",
                "reconstruction artifact is not available for export",
                shard_id=shard_id,
                stage="reconstruction",
            )
        data = artifact.path("output.wav").read_bytes()
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_bytes(output_path, data)
        return {
            "ok": True,
            "shard_id": shard_id,
            "path": str(output_path),
            "bytes": len(data),
            "sha256": sha256_bytes(data),
        }

    def export_report(self, shard_id: str, output_path: Path | None = None) -> dict[str, Any]:
        report = self.report(shard_id)
        path = output_path or (self.workspace.exports_dir / f"{shard_id}.report.json")
        path.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_bytes(path, canonical_json_bytes(report) + b"\n")
        return {
            "ok": True,
            "shard_id": shard_id,
            "path": str(path),
            "sha256": sha256_bytes(canonical_json_bytes(report) + b"\n"),
            "report": report,
        }
