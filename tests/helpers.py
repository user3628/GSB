from __future__ import annotations

import io
import tempfile
from pathlib import Path
from typing import Any

import numpy as np

from dimcluster.config import load_config
from dimcluster.preprocessing import PreprocessedData


def workspace() -> Path:
    return Path(tempfile.mkdtemp(prefix="dimcluster-test-"))


def small_config(manifest: Path, tiers: list[str] | None = None) -> dict[str, Any]:
    config = load_config()
    config["sample_manifest"] = str(manifest.resolve())
    if tiers is not None:
        config["tiers"] = tiers
    return config


def preprocessed_small(manifest: Path) -> PreprocessedData:
    from dimcluster.preprocessing import compute_preprocess

    config = small_config(manifest, ["small"])
    bundle = compute_preprocess(manifest, "small", config)
    X = np.load(io.BytesIO(bundle.artifacts["X.npy"]), allow_pickle=False)
    return PreprocessedData(
        X=X,
        feature_names=bundle.metadata["feature_names"],
        metadata=bundle.metadata,
    )
