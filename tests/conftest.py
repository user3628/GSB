from __future__ import annotations

import copy
import shutil
from pathlib import Path
from typing import Any, Callable

import pytest
import yaml

from mmcoop.config import Config, load_config
from mmcoop.dataio import Dataset
from mmcoop.service import WorkspaceService


PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def config_path(tmp_path: Path) -> Path:
    target = tmp_path / "config"
    target.mkdir()
    shutil.copyfile(PROJECT_ROOT / "config" / "default.yaml", target / "default.yaml")
    shutil.copyfile(PROJECT_ROOT / "config" / "projection_matrix.json", target / "projection_matrix.json")
    return target / "default.yaml"


@pytest.fixture
def service(tmp_path: Path, config_path: Path) -> WorkspaceService:
    value = WorkspaceService(config_path, tmp_path / "data", tmp_path / "workspace")
    value.generate_demo(seed=20261123, fresh_workspace=True)
    return value


@pytest.fixture
def built_service(service: WorkspaceService) -> WorkspaceService:
    service.build(verify_incremental=True)
    return service


@pytest.fixture
def demo_dataset(service: WorkspaceService) -> Dataset:
    return service._load()[1]


def load_test_config(path: Path) -> Config:
    return load_config(path)


def mutate_config(path: Path, mutator: Callable[[dict[str, Any]], None]) -> None:
    value = yaml.safe_load(path.read_text(encoding="utf-8"))
    mutator(value)
    path.write_text(yaml.safe_dump(value, sort_keys=False, allow_unicode=True), encoding="utf-8")


def config_with(path: Path, mutator: Callable[[dict[str, Any]], None]) -> Config:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    mutator(raw)
    matrix_file = raw.get("projection", {}).get("matrix_file")
    if matrix_file:
        candidate = Path(str(matrix_file))
        if not candidate.is_absolute():
            candidate = path.parent / candidate
        matrix = yaml.safe_load(candidate.read_text(encoding="utf-8")) if candidate.suffix in {".yaml", ".yml"} else __import__("json").loads(candidate.read_text(encoding="utf-8"))
    else:
        matrix = raw.get("projection", {}).get("matrix", [])
    from mmcoop.canonical import digest_json

    return Config(path=path, raw=raw, matrix=matrix, digest=digest_json({"raw": raw, "projection_matrix": matrix}))


def clone_dataset(
    dataset: Dataset,
    *,
    texts: list[dict[str, Any]] | None = None,
    images: list[dict[str, Any]] | None = None,
) -> Dataset:
    return Dataset(
        texts=tuple(copy.deepcopy(texts if texts is not None else list(dataset.texts))),
        images=tuple(copy.deepcopy(images if images is not None else list(dataset.images))),
    )
