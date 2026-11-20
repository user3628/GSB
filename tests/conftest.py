from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from join_planner.config import PlannerConfig
from join_planner.query import QueryCatalog
from join_planner.snapshot import SnapshotStore


ROOT = Path(__file__).resolve().parent.parent
SNAPSHOT_DIR = ROOT / "data" / "snapshots"
QUERY_CATALOG = ROOT / "data" / "queries" / "queries.json"


@pytest.fixture
def config() -> PlannerConfig:
    return PlannerConfig(fast_candidate_budget=2)


@pytest.fixture
def snapshot_dir(tmp_path: Path) -> Path:
    destination = tmp_path / "snapshots"
    shutil.copytree(SNAPSHOT_DIR, destination)
    return destination


@pytest.fixture
def snapshot_store(snapshot_dir: Path) -> SnapshotStore:
    store = SnapshotStore(snapshot_dir)
    store.load_file("snapshot_v1.json")
    return store


@pytest.fixture
def catalog() -> QueryCatalog:
    return QueryCatalog(QUERY_CATALOG)


@pytest.fixture
def extended_catalog(tmp_path: Path, catalog: QueryCatalog) -> QueryCatalog:
    raw = json.loads(QUERY_CATALOG.read_text(encoding="utf-8"))
    relations = [{"alias": f"c{index}", "table": "customers"} for index in range(9)]
    predicates = [
        {
            "id": f"chain_{index}",
            "type": "equality",
            "left": {"relation": f"c{index}", "column": "customer_id"},
            "right": {"relation": f"c{index + 1}", "column": "customer_id"},
        }
        for index in range(8)
    ]
    raw["queries"].append(
        {
            "query_id": "q_too_large",
            "relations": relations,
            "predicates": predicates,
        }
    )
    path = tmp_path / "extended_queries.json"
    path.write_text(json.dumps(raw), encoding="utf-8")
    return QueryCatalog(path)

