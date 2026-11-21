from __future__ import annotations

from pathlib import Path

import pytest

from factmerge.config import load_config
from factmerge.pipeline import FactMergeEngine, load_corpus, load_expected


ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture()
def root() -> Path:
    return ROOT


@pytest.fixture()
def config() -> dict:
    return load_config(ROOT / "configs" / "default.json")


@pytest.fixture()
def rows() -> list[dict]:
    return load_corpus(ROOT / "samples" / "corpus")


@pytest.fixture()
def expected() -> dict:
    return load_expected(ROOT / "samples" / "expected.json")


@pytest.fixture()
def engine_factory(config, rows, expected):
    def factory(state_dir=None, *, config_override=None, corpus=None, expected_override=None):
        return FactMergeEngine(
            config_override or config,
            corpus if corpus is not None else rows,
            state_dir=state_dir,
            expected=expected if expected_override is None else expected_override,
        )

    return factory
