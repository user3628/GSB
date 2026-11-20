from __future__ import annotations

from pathlib import Path

import pytest

from dialogue_engine.generator import generate_demo_dataset


@pytest.fixture(scope="session")
def demo_data(tmp_path_factory: pytest.TempPathFactory) -> Path:
    path = tmp_path_factory.mktemp("demo-data")
    generate_demo_dataset(path, force=True)
    return path

