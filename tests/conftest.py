from __future__ import annotations

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from checkpoint_recovery.samples import generate_samples, load_sample_payload  # noqa: E402


@pytest.fixture(scope="session")
def sample_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("fixed-samples")
    generate_samples(root, rounds=8)
    return root


@pytest.fixture
def payload_factory(sample_root: Path):
    def build(round_number: int, request_id: str | None = None) -> dict:
        metadata_path = sample_root / f"round-{round_number:04d}" / "metadata.json"
        return load_sample_payload(metadata_path, request_id or f"test-r{round_number}")

    return build

