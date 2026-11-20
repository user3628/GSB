"""Parameterized anomaly injection entry points."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .audio import load_dataset, read_wav
from .config import VADConfig
from .errors import VadError
from .stream import detect_samples


@dataclass(frozen=True)
class AnomalyExample:
    kind: str
    chunk_id: str
    path: Path | None
    raw: dict[str, Any]


def load_anomalies(data_dir: str | Path) -> dict[str, AnomalyExample]:
    root = Path(data_dir)
    manifest_path = root / "anomalies" / "manifest.json"
    try:
        body = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise VadError(
            "E_INVALID_CONFIG",
            f"unable to load anomaly manifest: {exc}",
            sample_position=0,
        ) from exc
    examples: dict[str, AnomalyExample] = {}
    for item in body.get("items", []):
        kind = str(item.get("kind", ""))
        relative = item.get("path")
        examples[kind] = AnomalyExample(
            kind=kind,
            chunk_id=str(item.get("chunk_id", kind)),
            path=(root / relative) if relative else None,
            raw=item,
        )
    return examples


def execute_injection(
    kind: str,
    data_dir: str | Path,
    *,
    config: VADConfig | None = None,
) -> None:
    """Execute one injected fault. Success means the injection was ineffective."""

    root = Path(data_dir)
    examples = load_anomalies(root)
    if kind not in examples:
        raise VadError(
            "E_INVALID_CONFIG",
            "unknown injection kind",
            details={"kind": kind, "available": sorted(examples)},
        )
    example = examples[kind]
    active_config = (config or VADConfig()).validate()

    if kind == "silent_input":
        assert example.path is not None
        samples, _ = read_wav(example.path, chunk_id=example.chunk_id)
        detect_samples(samples, active_config, chunk_id=example.chunk_id)
        return
    if kind == "clipped_peak":
        assert example.path is not None
        samples, _ = read_wav(example.path, chunk_id=example.chunk_id)
        detect_samples(samples, active_config, chunk_id=example.chunk_id)
        return
    if kind == "length_mismatch":
        manifest_path = example.raw.get("manifest_path")
        if manifest_path:
            load_dataset(root / str(manifest_path))
        else:
            assert example.path is not None
            declared = int(example.raw["declared_sample_count"])
            read_wav(example.path, chunk_id=example.chunk_id, expected_sample_count=declared)
        return
    if kind == "invalid_threshold":
        high = float(example.raw["high_threshold"])
        low = float(example.raw["low_threshold"])
        VADConfig(
            high_threshold=high,
            low_threshold=low,
            energy_weight=active_config.energy_weight,
            flatness_weight=active_config.flatness_weight,
        ).validate()
        return
    if kind == "feature_dimension":
        dataset = load_dataset(root / "manifest.json")
        chunk = dataset.chunks[0]
        detect_samples(
            chunk.samples,
            active_config,
            chunk_id=chunk.chunk_id,
            expected_sample_count=chunk.sample_count,
            inject_feature_dimensions=int(example.raw["injected_dimensions"]),
        )
        return
    raise VadError(
        "E_INVALID_CONFIG",
        "injection kind is declared but not implemented",
        details={"kind": kind},
    )

