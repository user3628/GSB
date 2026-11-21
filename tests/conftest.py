from __future__ import annotations

from pathlib import Path

import pytest

from chunk_engine.engine import Engine, EngineConfig
from chunk_engine.samples import generate_suite
from chunk_engine.snapshot import SnapshotManager


@pytest.fixture(scope="session")
def sample_suite(tmp_path_factory: pytest.TempPathFactory) -> dict:
    root = tmp_path_factory.mktemp("deterministic-samples")
    return generate_suite(
        root,
        small_rows=2000,
        large_rows=20000,
        corrupt_rows=2000,
        small_block_size=512,
        large_block_size=4096,
        corrupt_block_size=512,
        seed=20261122,
    )


@pytest.fixture
def engine_config() -> EngineConfig:
    return EngineConfig(
        total_memory_budget_bytes=2 * 1024 * 1024,
        single_task_budget_bytes=16 * 1024,
        max_parallel_blocks=4,
        max_keys=1000,
        max_page_size=50,
        default_page_size=10,
    )


def make_engine(
    tmp_path: Path,
    sample_suite: dict,
    versions: tuple[int, ...],
    config: EngineConfig,
) -> Engine:
    engine = Engine(tmp_path / "runtime", config=config, snapshot_manager=SnapshotManager())
    engine.recover()
    mapping = {1: "v1_small", 2: "v2_large", 3: "v3_corrupt"}
    for version in versions:
        engine.load_snapshot(sample_suite[mapping[version]]["root"])
    return engine


@pytest.fixture
def engine_factory(tmp_path: Path, sample_suite: dict, engine_config: EngineConfig):
    counter = {"value": 0}

    def factory(versions: tuple[int, ...] = (1,), config: EngineConfig | None = None) -> Engine:
        counter["value"] += 1
        return make_engine(
            tmp_path / f"engine-{counter['value']}",
            sample_suite,
            versions,
            config or engine_config,
        )

    return factory
