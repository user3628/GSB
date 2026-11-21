from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
SAMPLES = ROOT / "samples"


@pytest.fixture
def sample_root() -> Path:
    return ROOT


@pytest.fixture
def samples_dir() -> Path:
    return SAMPLES


@pytest.fixture
def sample_recipes() -> list[dict]:
    return json.loads((SAMPLES / "recipes.json").read_text(encoding="utf-8"))


@pytest.fixture
def sample_config():
    from matgen.config import load_config

    return load_config(SAMPLES / "config.json")


@pytest.fixture
def fast_config():
    from matgen.config import normalize_config

    return normalize_config({"sample_count": 6, "max_threads": 4})


def write_recipes(path: Path, recipes: list[dict]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(recipes, ensure_ascii=False, allow_nan=True), encoding="utf-8")
    return path


def tree_digest(directory: Path, *, exclude: set[str] | None = None) -> str:
    excluded = exclude or set()
    digest = hashlib.sha256()
    for path in sorted(directory.rglob("*")):
        relative = path.relative_to(directory).as_posix()
        if path.is_file() and relative not in excluded:
            digest.update(relative.encode("utf-8"))
            digest.update(b"\0")
            digest.update(path.read_bytes())
    return digest.hexdigest()

