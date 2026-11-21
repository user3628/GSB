#!/usr/bin/env python3
"""Regenerate the three checked-in configs with deterministic local CCA alignment."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shardseek.config import BASE_FEATURE_CONFIG, calibrated_projection_matrices, write_configs  # noqa: E402
from shardseek.features import extract_text_features, extract_visual_features  # noqa: E402


def main() -> None:
    manifest = ROOT / "samples" / "manifest_strict.jsonl"
    rows = [json.loads(line) for line in manifest.read_text(encoding="utf-8").splitlines() if line.strip()]
    visual_vectors = []
    text_vectors = []
    for row in rows:
        visual_vectors.append(
            extract_visual_features(
                ROOT / "samples" / row["image_file"],
                BASE_FEATURE_CONFIG,
                item_id=row["item_id"],
            )["vector"]
        )
        text_vectors.append(
            extract_text_features(
                row["caption"],
                BASE_FEATURE_CONFIG,
                item_id=row["item_id"],
            )["vector"]
        )
    calibration = calibrated_projection_matrices(
        np.vstack(visual_vectors),
        np.vstack(text_vectors),
    )
    hashes = write_configs(ROOT / "configs", calibration=calibration)
    print(json.dumps(hashes, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

