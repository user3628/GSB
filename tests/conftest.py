from __future__ import annotations

from pathlib import Path

import pytest

from mmbackfill.demo import generate_demo_dataset
from mmbackfill.engine import Engine
from mmbackfill.workspace import Workspace


@pytest.fixture
def demo_dataset(tmp_path: Path) -> Path:
    destination = tmp_path / "dataset"
    generate_demo_dataset(destination, seed=42)
    return destination


def make_engine(root: Path, dataset: Path, layout_path: Path | None = None, texts_path: Path | None = None) -> Engine:
    from mmbackfill.canonical import read_json, read_jsonl

    engine = Engine(Workspace(root))
    engine.workspace.ensure()
    engine.import_inputs(
        read_jsonl(dataset / "images.jsonl"),
        read_jsonl(texts_path or dataset / "texts.jsonl"),
        image_source_dir=dataset / "images",
    )
    engine.install_layout(read_json(layout_path or dataset / "layout.json"))
    return engine
