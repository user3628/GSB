"""Deterministic demo assets and an end-to-end acceptance run."""

from __future__ import annotations

import json
import math
import shutil
import wave
from pathlib import Path
from typing import Any

import numpy as np

from .canonical import atomic_write_json, read_json, sha256_bytes, sha256_json
from .errors import AudioQError
from .models import decode_config, encode_config, load_config_file
from .pipeline import EvaluationWorkbench


SAMPLE_RATE = 16000
DURATION_SECONDS = 2.0
SAMPLE_COUNT = int(SAMPLE_RATE * DURATION_SECONDS)


def _envelope(t: np.ndarray) -> np.ndarray:
    envelope = np.zeros_like(t)
    for start, end in ((0.08, 0.62), (0.74, 1.18), (1.30, 1.92)):
        active = (t >= start) & (t <= end)
        local = np.zeros_like(t)
        local[active] = np.sin(np.pi * (t[active] - start) / (end - start)) ** 0.55
        envelope = np.maximum(envelope, local)
    return envelope


def _speech_like(seed: int) -> np.ndarray:
    rng = np.random.default_rng(seed)
    t = np.arange(SAMPLE_COUNT, dtype=np.float64) / SAMPLE_RATE
    f0 = 128.0 + 18.0 * np.sin(2.0 * np.pi * 1.15 * t) + 7.0 * np.sin(2.0 * np.pi * 2.6 * t)
    phase = 2.0 * np.pi * np.cumsum(f0) / SAMPLE_RATE
    voiced = (
        0.58 * np.sin(phase)
        + 0.25 * np.sin(2.0 * phase + 0.15)
        + 0.12 * np.sin(3.0 * phase + 0.41)
        + 0.05 * np.sin(5.0 * phase + 0.73)
    )
    formant_texture = 0.08 * np.sin(2.0 * np.pi * 410.0 * t) * np.sin(2.0 * np.pi * 83.0 * t)
    speech = (voiced + formant_texture) * _envelope(t)
    speech += rng.normal(0.0, 0.0008, SAMPLE_COUNT)
    return speech / max(1e-12, float(np.max(np.abs(speech))))


def _write_wav(path: Path, samples: np.ndarray) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pcm = np.asarray(samples, dtype=np.float64)
    pcm = np.clip(pcm, -1.0, 1.0)
    ints = np.round(pcm * 32767.0).astype("<i2")
    with wave.open(str(path), "wb") as wav:
        wav.setnchannels(1)
        wav.setsampwidth(2)
        wav.setframerate(SAMPLE_RATE)
        wav.writeframes(ints.tobytes())


def _config_v2() -> dict[str, Any]:
    return {
        "schema_version": 2,
        "evaluation_id": "demo-quality-v2",
        "frame": {"length": 400, "hop": 160, "window": "hann"},
        "metrics": {
            "snr_threshold_db": 8.0,
            "spectral_epsilon": 1e-8,
            "spectral_scale_db": 20.0,
            "clipping_level": 0.98,
            "clipping_ratio_limit": 0.001,
            "clipping_scale_ratio": 0.08,
        },
        "weights": {"snr": 0.4, "spectral": 0.35, "clipping": 0.25},
        "fusion": {"domain": "log", "epsilon": 1e-6, "decimals": 12, "high_precision": 50},
        "default_reference": {"min_mos": 0.0, "max_mos": 5.0, "target_mos": 4.0},
    }


def _config_v1_legacy() -> dict[str, Any]:
    # Legacy v1 intentionally has no SNR threshold field and uses integers.
    return {
        "schema_version": 1,
        "evaluation_id": "demo-legacy-v1",
        "frame": {"length": 400, "hop": 160, "window": "hann"},
        "metrics": {
            "spectral_epsilon": 1e-8,
            "spectral_scale_db": 20.0,
            "clipping_level": 0.98,
            "clipping_ratio_limit": 0.001,
            "clipping_scale_ratio": 0.08,
        },
        "weights_percent": {"snr": 40, "spectral": 35, "clipping": 25},
        "fusion": {"domain": "log", "epsilon": 1e-6, "decimals": 12, "high_precision": 50},
        "default_reference": {"min_mos": 0.0, "max_mos": 5.0, "target_mos": 4.0},
    }


def generate_demo_assets(base_dir: Path | str = "demo", *, root: Path | str | None = None) -> dict[str, Any]:
    root_path = Path(root).resolve() if root is not None else Path.cwd().resolve()
    requested_base = Path(base_dir)
    base = requested_base if requested_base.is_absolute() else root_path / requested_base
    samples_dir = base / "samples"
    configs_dir = base / "configs"
    manifests_dir = base / "manifests"
    for directory in (samples_dir, configs_dir, manifests_dir):
        directory.mkdir(parents=True, exist_ok=True)

    t = np.arange(SAMPLE_COUNT, dtype=np.float64) / SAMPLE_RATE
    clean_base = _speech_like(3701)
    clean = 0.42 * clean_base

    rng = np.random.default_rng(3702)
    noisy = 0.32 * clean_base + rng.normal(0.0, 0.095, SAMPLE_COUNT)
    burst_start = int(1.03 * SAMPLE_RATE)
    burst_end = burst_start + int(0.035 * SAMPLE_RATE)
    burst_rng = np.random.default_rng(3703)
    noisy[burst_start:burst_end] += burst_rng.normal(0.0, 0.48, burst_end - burst_start)
    noisy = np.clip(noisy, -1.0, 1.0)

    clipped_tone = (
        1.45 * np.sin(2.0 * np.pi * 190.0 * t)
        + 0.65 * np.sin(2.0 * np.pi * 570.0 * t + 0.3)
        + 0.22 * np.sin(2.0 * np.pi * 1130.0 * t + 0.8)
    ) * _envelope(t)
    clipped = np.clip(clipped_tone, -1.0, 1.0)

    silent = np.zeros(SAMPLE_COUNT, dtype=np.float64)
    waveforms = {
        "clean_speech.wav": clean,
        "low_snr_burst.wav": noisy,
        "clipped_distortion.wav": clipped,
        "all_silent.wav": silent,
    }
    for filename, waveform in waveforms.items():
        _write_wav(samples_dir / filename, waveform)

    shard_rows = [
        ("clean", "clean_speech.wav", "zh-CN", 1, 3.80, 5.00, 4.40),
        ("low_snr", "low_snr_burst.wav", "zh-CN", 2, 0.80, 2.80, 2.10),
        ("clipped", "clipped_distortion.wav", "en-US", 3, 0.50, 1.50, 1.00),
        ("silent", "all_silent.wav", "zh-CN", 4, 0.00, 0.50, 0.20),
    ]
    manifest = {
        "manifest_version": 1,
        "evaluation_id": "demo-quality-v2",
        "shards": [
            {
                "shard_id": shard_id,
                "path": (samples_dir / filename).relative_to(root_path).as_posix(),
                "num_samples": SAMPLE_COUNT,
                "sample_rate": SAMPLE_RATE,
                "language": language,
                "order": order,
                "reference": {"min_mos": low, "max_mos": high, "target_mos": target},
            }
            for shard_id, filename, language, order, low, high, target in shard_rows
        ],
    }
    atomic_write_json(base / "manifest.json", manifest, pretty=True)
    atomic_write_json(configs_dir / "config_v2.json", _config_v2(), pretty=True)
    atomic_write_json(configs_dir / "config_v1_legacy.json", _config_v1_legacy(), pretty=True)

    return {
        "base": base,
        "manifest": base / "manifest.json",
        "config_v2": configs_dir / "config_v2.json",
        "config_v1": configs_dir / "config_v1_legacy.json",
        "samples": [samples_dir / filename for filename in waveforms],
    }


def _check_config_roundtrip(assets: dict[str, Any], root: Path) -> dict[str, Any]:
    legacy = load_config_file(assets["config_v1"], snr_threshold_rule="fixed", default_snr_threshold_db=8.0)
    exported_v1 = encode_config(legacy, 1)
    exported_v1_path = assets["base"] / "configs" / "config_v1_roundtrip.json"
    atomic_write_json(exported_v1_path, exported_v1, pretty=True)
    reimported_v1 = decode_config(exported_v1)
    semantic_equal = legacy.semantic_dict() == reimported_v1.semantic_dict()

    exported_v2 = encode_config(legacy, 2)
    exported_v2_path = assets["base"] / "configs" / "config_from_v1_v2.json"
    atomic_write_json(exported_v2_path, exported_v2, pretty=True)
    reimported_v2 = decode_config(exported_v2)
    v2_equal = all(
        reimported_v2.semantic_dict()[key] == legacy.semantic_dict()[key]
        for key in ("evaluation_id", "frame", "metrics", "weights", "fusion", "default_reference")
    )
    return {
        "ok": semantic_equal and v2_equal,
        "derived_snr_threshold_db": legacy.metrics.snr_threshold_db,
        "weights_percent": exported_v1["weights_percent"],
        "downgrade_items": exported_v1["downgrade_items"],
        "downgrade_restore": exported_v1["downgrade_restore"],
        "v1_roundtrip_semantic_equal": semantic_equal,
        "v1_to_v2_semantic_equal": v2_equal,
        "v1_export_path": exported_v1_path.relative_to(root).as_posix(),
        "v2_from_v1_path": exported_v2_path.relative_to(root).as_posix(),
    }


def _check_invalid_weight(base: Path, config_v2: dict[str, Any]) -> dict[str, Any]:
    bad = json.loads(json.dumps(config_v2))
    bad["weights"] = {"snr": 0, "spectral": 0, "clipping": 0}
    path = base / "configs" / "invalid_weight.json"
    atomic_write_json(path, bad, pretty=True)
    try:
        load_config_file(path)
    except AudioQError as exc:
        return {"ok": exc.code == "E_INVALID_WEIGHT", "expected_code": "E_INVALID_WEIGHT", "error": exc.to_dict()}
    return {"ok": False, "expected_code": "E_INVALID_WEIGHT", "error": None}


def _check_length_mismatch(
    root: Path,
    assets: dict[str, Any],
    state_dir: Path,
) -> dict[str, Any]:
    manifest = read_json(assets["manifest"])
    manifest["shards"][0]["num_samples"] += 1
    bad_manifest = assets["base"] / "manifests" / "length_mismatch.json"
    atomic_write_json(bad_manifest, manifest, pretty=True)
    if state_dir.exists():
        shutil.rmtree(state_dir)
    workbench = EvaluationWorkbench(root, bad_manifest, assets["config_v2"], state_dir=state_dir)
    result = workbench.run()
    report = result["report"]
    clean = next(shard for shard in report["shards"] if shard["shard_id"] == "clean")
    codes = [error["code"] for error in clean["errors"]]
    return {
        "ok": "E_LENGTH_MISMATCH" in codes,
        "expected_code": "E_LENGTH_MISMATCH",
        "observed_codes": codes,
        "manifest": bad_manifest.relative_to(root).as_posix(),
    }


def run_demo(root: Path | str = ".", *, force: bool = True) -> dict[str, Any]:
    root_path = Path(root).resolve()
    base = root_path / "demo"
    assets = generate_demo_assets(base, root=root_path)
    full_state = base / "state_full"
    resume_state = base / "state_resume"
    mismatch_state = base / "state_length_mismatch"
    if force:
        for state in (full_state, resume_state, mismatch_state):
            if state.exists():
                shutil.rmtree(state)

    manifest_rel = assets["manifest"].relative_to(root_path)
    config_rel = assets["config_v2"].relative_to(root_path)

    full_workbench = EvaluationWorkbench(root_path, manifest_rel, config_rel, state_dir=full_state.relative_to(root_path))
    full_result = full_workbench.run()
    full_report_path = full_state / "report.json"
    full_report_bytes = full_report_path.read_bytes()

    resume_workbench = EvaluationWorkbench(
        root_path, manifest_rel, config_rel, state_dir=resume_state.relative_to(root_path)
    )
    stopped_result = resume_workbench.run(stop_after_shards=2, write_report=False)
    resume_workbench.request_stop(reason="demo_after_two_shards")
    resumed_result = resume_workbench.resume()
    resumed_report_path = resume_state / "report.json"
    resumed_report_bytes = resumed_report_path.read_bytes()
    byte_identical = full_report_bytes == resumed_report_bytes

    report = full_result["report"]
    config_roundtrip = _check_config_roundtrip(assets, root_path)
    invalid_weight = _check_invalid_weight(base, _config_v2())
    length_mismatch = _check_length_mismatch(root_path, assets, mismatch_state)

    observed_codes = sorted({code for shard in report["shards"] for code in [e["code"] for e in shard["errors"]]})
    summary = {
        "demo_version": 1,
        "deterministic_seed": 3701,
        "assets": {
            "manifest": assets["manifest"].relative_to(root_path).as_posix(),
            "config_v2": assets["config_v2"].relative_to(root_path).as_posix(),
            "config_v1_legacy": assets["config_v1"].relative_to(root_path).as_posix(),
            "samples": [path.relative_to(root_path).as_posix() for path in assets["samples"]],
        },
        "full_run": {
            "status": full_result["status"],
            "report_path": full_report_path.relative_to(root_path).as_posix(),
            "report_sha256": sha256_bytes(full_report_bytes),
            "report_fingerprint": report["report_fingerprint"],
            "summary": report["summary"],
            "shard_scores": {
                shard["shard_id"]: shard["fusion"].get("fused_score") for shard in report["shards"]
            },
        },
        "stop_resume": {
            "stopped_after_shards": stopped_result["completed_count"],
            "stopped_status": stopped_result["status"],
            "recovered": resumed_result["status"] == "complete",
            "byte_identical": byte_identical,
            "recovered_report_path": resumed_report_path.relative_to(root_path).as_posix(),
            "recovered_report_sha256": sha256_bytes(resumed_report_bytes),
        },
        "config_roundtrip": config_roundtrip,
        "error_checks": {
            "invalid_weight": invalid_weight,
            "length_mismatch": length_mismatch,
            "observed_report_codes": observed_codes,
        },
    }
    summary["acceptance_ok"] = bool(
        full_result["status"] == "complete"
        and stopped_result["status"] == "stopped"
        and byte_identical
        and config_roundtrip["ok"]
        and invalid_weight["ok"]
        and length_mismatch["ok"]
        and "E_EMPTY_AUDIO" in observed_codes
        and "E_CLIPPED_AUDIO" in observed_codes
    )
    summary["summary_sha256"] = sha256_json(summary)
    summary_path = base / "demo_summary.json"
    atomic_write_json(summary_path, summary, pretty=True)
    summary["summary_path"] = summary_path.relative_to(root_path).as_posix()
    return summary
