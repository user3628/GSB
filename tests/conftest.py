from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from transcript_workbench.demo import generate_demo_data
from transcript_workbench.workspace import Workspace


@pytest.fixture()
def demo_root(tmp_path: Path) -> Path:
    root = tmp_path / "project"
    root.mkdir()
    generate_demo_data(root, clean=True)
    return root


@pytest.fixture()
def demo_data(demo_root: Path) -> dict[str, Any]:
    manifest_path = demo_root / "demo_data" / "manifest.json"
    return {
        "root": demo_root,
        "manifest_path": manifest_path,
        "legacy_path": demo_root / "demo_data" / "legacy_transcript.json",
        "invalid_path": demo_root / "demo_data" / "invalid_timeline.json",
    }


@pytest.fixture()
def boot_workspace(demo_data: dict[str, Any]) -> Workspace:
    workspace = Workspace(demo_data["root"] / "artifacts" / "workspace")
    workspace.bootstrap(demo_data["manifest_path"])
    return workspace


def first_word(state: Any, chunk_id: str = "chunk-01") -> Any:
    return min(
        (word for word in state.words if word.chunk_id == chunk_id),
        key=lambda word: (word.start_ms, word.entry_id),
    )
