"""Deterministic local demo generation and controlled source mutation."""

from __future__ import annotations

import json
import math
import random
import shutil
from pathlib import Path
from typing import Any

import yaml

from .canonical import atomic_write_json, atomic_write_text, sha256_file
from .errors import TelemetryError


DEMO_SEED = 20261122
DEMO_ROOT_MARKER = ".telemetry-align-demo"

SOURCE_SPECS: dict[str, dict[str, Any]] = {
    "sensor_a": {
        "interval_ms": 100,
        "end_ms": 3000,
        "gaps_ms": [[1000, 1200], [2200, 2400]],
        "phase": 0.15,
        "bias": 0.0,
        "seed": 11,
    },
    "sensor_b": {
        "interval_ms": 150,
        "end_ms": 3000,
        "gaps_ms": [[1500, 1650], [2200, 2400]],
        "phase": 0.85,
        "bias": 0.7,
        "seed": 23,
    },
    "sensor_c": {
        "interval_ms": 200,
        "end_ms": 2800,
        "gaps_ms": [[2200, 2400]],
        "phase": 1.45,
        "bias": -0.5,
        "seed": 37,
    },
}

GENERATED_DIRS = ("data", "state", "artifacts", "out", "reports", "imports")


def _channel_labels() -> dict[str, dict[str, str]]:
    return {
        "temperature_c": {"label": "温度 Temperature (°C)", "unit": "°C"},
        "humidity_pct": {"label": "湿度 Humidity (%)", "unit": "%"},
    }


def _source_jsonl(sensor_id: str) -> str:
    spec = SOURCE_SPECS[sensor_id]
    rng = random.Random(DEMO_SEED + int(spec["seed"]))
    records: list[dict[str, Any]] = []
    for timestamp_ms in range(0, int(spec["end_ms"]) + 1, int(spec["interval_ms"])):
        if any(
            gap_start <= timestamp_ms <= gap_end
            for gap_start, gap_end in spec["gaps_ms"]
        ):
            continue
        temperature = (
            21.5
            + 3.4 * math.sin((timestamp_ms / 1000.0) + spec["phase"])
            + spec["bias"]
            + rng.uniform(-0.35, 0.35)
        )
        humidity = (
            48.0
            + 8.5 * math.cos((timestamp_ms / 1275.0) + spec["phase"])
            + rng.uniform(-1.2, 1.2)
        )
        quality = "good"
        if sensor_id == "sensor_a" and timestamp_ms == 400:
            quality = "suspect"
        if sensor_id == "sensor_b" and timestamp_ms == 900:
            quality = "bad"
        # A deliberately out-of-range but syntactically valid value demonstrates cleaning.
        if sensor_id == "sensor_a" and timestamp_ms == 500:
            temperature = 999.0
        records.append(
            {
                "sensor_id": sensor_id,
                "timestamp_ms": timestamp_ms,
                "channel_values": {
                    "temp_raw": round(temperature, 6),
                    "rh_raw": round(humidity, 6),
                },
                "quality": quality,
                "channel_labels": {
                    "temp_raw": "温度 Temp / raw",
                    "rh_raw": "湿度 Humidity / raw",
                },
                "sample_note": "本地确定性样本 / deterministic local sample",
            }
        )
    lines = [json.dumps(record, ensure_ascii=False, sort_keys=True) for record in records]
    return "\n".join(lines) + "\n"


def _v2_config() -> dict[str, Any]:
    sources: dict[str, Any] = {}
    priorities = {"sensor_a": 30, "sensor_b": 20, "sensor_c": 20}
    for sensor_id in sorted(SOURCE_SPECS):
        sources[sensor_id] = {
            "priority": priorities[sensor_id],
            "channel_mapping": {
                "temp_raw": "temperature_c",
                "rh_raw": "humidity_pct",
            },
            "valid_ranges": {
                "temperature_c": {"min": -40.0, "max": 125.0},
                "humidity_pct": {"min": 0.0, "max": 100.0},
            },
            "quality_accept": ["good", "fair"],
        }
    return {
        "schema_version": 2,
        "workspace_id": "deterministic-telemetry-demo",
        "grid": {"step_ms": 100, "start_ms": 0, "end_ms": 3000},
        "tolerance_ms": 200,
        "resample": {"max_gap_ms": 250, "allow_extrapolation": True},
        "interpolation": {
            "default": "linear",
            "per_source": {"sensor_b": "ffill", "sensor_c": "nearest"},
        },
        "channels": _channel_labels(),
        "sources": sources,
        "fusion": {
            "mode": "weighted_mean",
            "tie_break": "source_id_lexicographic",
            "weights": {"sensor_a": 0.5, "sensor_b": 0.3, "sensor_c": 0.2},
        },
        "export": {
            "title": "多源遥测增量对齐摘要 / Multi-source Telemetry Alignment Summary",
            "include_points": True,
        },
    }


def _v1_config() -> dict[str, Any]:
    v2 = _v2_config()
    sources: dict[str, Any] = {}
    for sensor_id, source in v2["sources"].items():
        sources[sensor_id] = {
            "priority": source["priority"],
            "valid_ranges": source["valid_ranges"],
            "quality_accept": source["quality_accept"],
        }
    return {
        "schema_version": 1,
        "workspace_id": v2["workspace_id"],
        "grid": v2["grid"],
        "tolerance_percent": 20,
        "resample": v2["resample"],
        "interpolation": v2["interpolation"],
        "channels": v2["channels"],
        "sources": sources,
        "fusion": v2["fusion"],
        "export": v2["export"],
        "compatibility": {
            "tolerance_basis_ms": 1000,
            "tolerance_rounding": "half_up",
            "default_channel_mapping": {
                "temp_raw": "temperature_c",
                "rh_raw": "humidity_pct",
            },
        },
    }


def reset_generated_workspace(root: Path | str) -> Path:
    workspace = Path(root).resolve()
    workspace.mkdir(parents=True, exist_ok=True)
    marker = workspace / DEMO_ROOT_MARKER
    if marker.exists():
        for dirname in GENERATED_DIRS:
            target = workspace / dirname
            if target.is_dir():
                shutil.rmtree(target)
            elif target.exists():
                target.unlink()
        manifest = workspace / "workspace.json"
        if manifest.exists():
            manifest.unlink()
    return workspace


def generate_demo(root: Path | str, *, reset: bool = True) -> dict[str, Any]:
    workspace = Path(root).resolve()
    if reset:
        reset_generated_workspace(workspace)
    else:
        workspace.mkdir(parents=True, exist_ok=True)
    atomic_write_text(workspace / DEMO_ROOT_MARKER, "telemetry-align-demo-v1\n")

    source_paths: dict[str, str] = {}
    for sensor_id in sorted(SOURCE_SPECS):
        path = workspace / "data" / "sources" / f"{sensor_id}.jsonl"
        atomic_write_text(path, _source_jsonl(sensor_id))
        source_paths[sensor_id] = path.relative_to(workspace).as_posix()

    v2_path = workspace / "data" / "configs" / "config_v2.yaml"
    v1_path = workspace / "data" / "configs" / "config_v1.yaml"
    atomic_write_text(
        v2_path,
        yaml.safe_dump(
            _v2_config(),
            sort_keys=False,
            allow_unicode=True,
            default_flow_style=False,
            width=1000,
        ),
    )
    atomic_write_text(
        v1_path,
        yaml.safe_dump(
            _v1_config(),
            sort_keys=False,
            allow_unicode=True,
            default_flow_style=False,
            width=1000,
        ),
    )

    manifest = {
        "schema_version": 1,
        "workspace_id": "deterministic-telemetry-demo",
        "sources": source_paths,
        "configs": {
            "v1": v1_path.relative_to(workspace).as_posix(),
            "v2": v2_path.relative_to(workspace).as_posix(),
        },
        "active_config": "v2",
        "generated_by": "telemetry_align.samples.generate_demo",
        "seed": DEMO_SEED,
        "source_digests": {
            sensor_id: sha256_file(workspace / path) for sensor_id, path in source_paths.items()
        },
    }
    atomic_write_json(workspace / "workspace.json", manifest)
    return manifest


def mutate_source(
    root: Path | str,
    sensor_id: str = "sensor_a",
    *,
    timestamp_ms: int = 300,
    delta: float = 0.37,
) -> dict[str, Any]:
    """Mutate one numeric sample while retaining a valid monotonic shard."""

    workspace = Path(root).resolve()
    manifest_path = workspace / "workspace.json"
    if not manifest_path.exists():
        raise TelemetryError(
            "E_NOT_FOUND", "工作区尚未初始化 / workspace is not initialised", {"root": str(workspace)}
        )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if sensor_id not in manifest.get("sources", {}):
        raise TelemetryError(
            "E_NOT_FOUND", "源分片不存在 / source shard not found", {"sensor_id": sensor_id}
        )
    path = workspace / manifest["sources"][sensor_id]
    lines = path.read_text(encoding="utf-8").splitlines()
    records: list[dict[str, Any]] = [json.loads(line) for line in lines if line.strip()]
    target = None
    for record in records:
        if int(record["timestamp_ms"]) == timestamp_ms and record.get("quality") == "good":
            target = record
            break
    if target is None:
        for record in records:
            if record.get("quality") == "good":
                target = record
                break
    if target is None:
        raise TelemetryError(
            "E_SOURCE_INVALID", "没有可修改的有效样本 / no mutable sample", {"sensor_id": sensor_id}
        )
    values = target["channel_values"]
    old_value = float(values["temp_raw"])
    values["temp_raw"] = round(old_value + delta, 6)
    target["revision_note"] = "incremental demo mutation / 增量演示变更"
    atomic_write_text(
        path,
        "\n".join(json.dumps(record, ensure_ascii=False, sort_keys=True) for record in records) + "\n",
    )
    manifest["source_digests"][sensor_id] = sha256_file(path)
    atomic_write_json(manifest_path, manifest)
    return {
        "sensor_id": sensor_id,
        "timestamp_ms": int(target["timestamp_ms"]),
        "old_value": old_value,
        "new_value": float(values["temp_raw"]),
        "path": path.relative_to(workspace).as_posix(),
        "new_digest": manifest["source_digests"][sensor_id],
    }

