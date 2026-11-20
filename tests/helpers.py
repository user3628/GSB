from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any, Callable

import yaml

from sparse_index.config import IndexConfig


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
BASE_CONFIG = DATA_DIR / "index.yaml"


def variant_config(
    directory: Path,
    mutate: Callable[[dict[str, Any]], None] | None = None,
    *,
    filename: str = "variant.yaml",
) -> IndexConfig:
    raw = yaml.safe_load(BASE_CONFIG.read_text(encoding="utf-8"))
    raw = copy.deepcopy(raw)
    raw["documents"]["glob"] = str((DATA_DIR / "shards" / "*.jsonl").resolve())
    raw["queries"] = str((DATA_DIR / "queries.jsonl").resolve())
    if mutate is not None:
        mutate(raw)
    path = directory / filename
    path.write_text(yaml.safe_dump(raw, allow_unicode=True, sort_keys=True), encoding="utf-8")
    return IndexConfig.load(path)


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def artifact_bytes(workspace: Path) -> dict[str, bytes]:
    paths = [
        workspace / "artifacts" / "stage1" / "terms.json",
        workspace / "artifacts" / "stage2" / "statistics.json",
        workspace / "artifacts" / "stage3" / "postings.bin",
        workspace / "artifacts" / "stage3" / "postings.json",
        workspace / "artifacts" / "stage4" / "query_results.json",
    ]
    return {path.relative_to(workspace).as_posix(): path.read_bytes() for path in paths}


def base_query_list() -> list[dict[str, Any]]:
    queries = []
    with (DATA_DIR / "queries.jsonl").open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                queries.append(json.loads(line))
    return queries
