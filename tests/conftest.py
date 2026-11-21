from __future__ import annotations

import copy
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rag_orchestrator.config import load_config  # noqa: E402
from rag_orchestrator.samplegen import generate_samples  # noqa: E402
from rag_orchestrator.util import read_jsonl  # noqa: E402


@dataclass
class SampleEnv:
    root: Path
    data_dir: Path
    config_path: Path
    docs_path: Path
    requests_path: Path

    def load(self) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
        return (
            load_config(self.config_path),
            read_jsonl(self.docs_path),
            read_jsonl(self.requests_path),
        )

    def config(self) -> dict[str, Any]:
        return load_config(self.config_path)

    def documents(self) -> list[dict[str, Any]]:
        return read_jsonl(self.docs_path)

    def requests(self) -> list[dict[str, Any]]:
        return read_jsonl(self.requests_path)

    def request(self, query_id: str) -> dict[str, Any]:
        return next(item for item in self.requests() if item["query_id"] == query_id)


@pytest.fixture()
def sample_env(tmp_path: Path) -> SampleEnv:
    data_dir = tmp_path / "data"
    generate_samples(data_dir)
    return SampleEnv(
        root=tmp_path,
        data_dir=data_dir,
        config_path=data_dir / "config.yaml",
        docs_path=data_dir / "docs.jsonl",
        requests_path=data_dir / "requests.jsonl",
    )


def clone(value: Any) -> Any:
    return copy.deepcopy(value)
