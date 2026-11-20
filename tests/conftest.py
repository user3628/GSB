from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from app.config import EngineConfig, load_config
from app.crash import CrashController
from app.engine import WindowEngine


ROOT = Path(__file__).resolve().parents[1]
SAMPLE_CONFIG = ROOT / "samples" / "config.json"


@pytest.fixture()
def config() -> EngineConfig:
    return load_config(SAMPLE_CONFIG)


@pytest.fixture()
def make_engine(tmp_path: Path, config: EngineConfig):
    created: list[WindowEngine] = []

    def factory(*, runtime_name: str = "runtime", **overrides) -> WindowEngine:
        selected = replace(config, **overrides) if overrides else config
        engine = WindowEngine(
            config=selected,
            runtime_dir=tmp_path / runtime_name,
            crash=CrashController(),
        )
        created.append(engine)
        return engine

    return factory


@pytest.fixture()
def engine(make_engine) -> WindowEngine:
    return make_engine()
