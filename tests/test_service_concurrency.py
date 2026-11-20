from __future__ import annotations

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from join_planner.config import PlannerConfig
from join_planner.errors import PlannerError
from join_planner.journal import IntentJournal
from join_planner.optimizer import PlanOptimizer
from join_planner.query import QueryCatalog
from join_planner.service import PlanService
from join_planner.snapshot import SnapshotStore


class _SerialProbeOptimizer(PlanOptimizer):
    def __init__(self, config: PlannerConfig) -> None:
        super().__init__(config)
        self._state_lock = threading.Lock()
        self.active = 0
        self.max_active = 0

    def plan(self, snapshot, query, mode):
        with self._state_lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        try:
            time.sleep(0.08)
            return super().plan(snapshot, query, mode)
        finally:
            with self._state_lock:
                self.active -= 1


def _service(tmp_path: Path, snapshots: SnapshotStore, catalog: QueryCatalog, delay: float = 0.0) -> PlanService:
    config = PlannerConfig(build_delay_seconds=delay, fast_candidate_budget=2)
    journal = IntentJournal(tmp_path / "runtime")
    return PlanService(snapshots, catalog, journal, PlanOptimizer(config), config)


def test_duplicate_submission_builds_once_and_reuses_result(
    tmp_path: Path,
    snapshot_store: SnapshotStore,
    catalog: QueryCatalog,
) -> None:
    service = _service(tmp_path, snapshot_store, catalog, delay=0.15)
    barrier = threading.Barrier(2)
    outcomes: list[tuple[dict, str]] = []
    errors: list[BaseException] = []

    def submit() -> None:
        try:
            barrier.wait()
            outcomes.append(service.submit("q_product_priority", 1, "exact"))
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=submit) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)
    assert not errors
    assert len(outcomes) == 2
    assert {status for _, status in outcomes} == {"miss", "hit"}
    assert outcomes[0][0] == outcomes[1][0]
    intents = list((tmp_path / "runtime" / "intents").glob("*.json"))
    assert len(intents) == 1
    assert json.loads(intents[0].read_text(encoding="utf-8"))["state"] == "completed"


def test_build_keeps_submission_snapshot_when_version_changes(
    tmp_path: Path,
    snapshot_store: SnapshotStore,
    catalog: QueryCatalog,
) -> None:
    service = _service(tmp_path, snapshot_store, catalog, delay=0.4)
    result: list[dict] = []
    errors: list[BaseException] = []

    def submit() -> None:
        try:
            result.append(service.submit("q_customer_revenue", 1, "exact")[0])
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=submit)
    thread.start()
    deadline = time.monotonic() + 5
    intent_path = None
    while time.monotonic() < deadline:
        paths = list((tmp_path / "runtime" / "intents").glob("*.json"))
        if paths:
            intent_path = paths[0]
            break
        time.sleep(0.01)
    assert intent_path is not None
    v2 = snapshot_store.load_file("snapshot_v2.json")
    thread.join(timeout=5)
    assert not errors
    plan = result[0]
    assert plan["snapshot"]["version"] == 1
    assert plan["snapshot_binding"]["captured_checksum"] == plan["snapshot"]["checksum"]
    assert plan["snapshot_binding"]["retired_during_build"] is True
    assert snapshot_store.current is v2
    with pytest.raises(PlannerError) as exc_info:
        service.submit("q_customer_revenue", 1, "exact")
    assert exc_info.value.code == "E_VERSION_RETIRED"


def test_materialized_plan_supports_concurrent_reads(
    tmp_path: Path,
    snapshot_store: SnapshotStore,
    catalog: QueryCatalog,
) -> None:
    service = _service(tmp_path, snapshot_store, catalog)
    plan, _ = service.submit("q_customer_revenue", 1, "exact")

    def read(_: int) -> dict:
        return service.get_materialized("q_customer_revenue", "exact", 1)

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(read, range(32)))
    assert all(item == plan for item in results)


def test_distinct_plan_builds_are_serialized(
    tmp_path: Path,
    snapshot_store: SnapshotStore,
    catalog: QueryCatalog,
) -> None:
    config = PlannerConfig(fast_candidate_budget=2)
    optimizer = _SerialProbeOptimizer(config)
    journal = IntentJournal(tmp_path / "runtime")
    service = PlanService(snapshot_store, catalog, journal, optimizer, config)
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(service.submit, "q_customer_revenue", 1, "exact"),
            executor.submit(service.submit, "q_employee_manager", 1, "exact"),
        ]
        for future in futures:
            future.result(timeout=5)
    assert optimizer.max_active == 1

