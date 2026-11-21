"""Shared fixtures: small deterministic datasets and workspaces."""

from __future__ import annotations

from pathlib import Path

from voxel_engine.engine import Engine
from voxel_engine.generator import generate_dataset

SMALL_CONFIG = {
    "ray_step": 0.5,
    "truncation_distance": 0.75,
    "voxel_resolution": 0.5,
    "fusion_radius": 1.5,
    "sparsity_threshold": 0.02,
}


def make_dataset(directory, seed=123, views=4, config_overrides=None) -> Path:
    directory = Path(directory)
    overrides = dict(SMALL_CONFIG)
    if config_overrides:
        overrides.update(config_overrides)
    generate_dataset(
        directory,
        seed=seed,
        num_views=views,
        width=48,
        height=36,
        config_overrides=overrides,
    )
    return directory


def make_engine(workspace, dataset_dir) -> Engine:
    engine = Engine(Path(workspace))
    engine.import_dataset(Path(dataset_dir) / "manifest.json")
    return engine
