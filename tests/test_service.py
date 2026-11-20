from __future__ import annotations

import copy
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from app.db import connect, get_current_version, get_snapshot_row, snapshot_is_complete
from app.errors import BusinessError
from app.service import ConsistencyService
from app.snapshot import attach_checksum


def table_count(db_path: Path, table: str, where: str = "1=1", parameters: tuple = ()) -> int:
    connection = connect(db_path)
    try:
        row = connection.execute(f"SELECT COUNT(*) AS count FROM {table} WHERE {where}", parameters).fetchone()
        return int(row["count"])
    finally:
        connection.close()


def test_version_switch_and_historical_queries_are_complete(
    service: ConsistencyService, baseline_payload: dict, adjusted_payload: dict
) -> None:
    first = service.commit_snapshot(baseline_payload, base_version=0)
    assert first["current_version"] == 1
    assert first["status"] == "CONSISTENT"
    assert service.get_metrics(1)["current"] is True

    second = service.commit_snapshot(adjusted_payload, base_version=1)
    assert second["current_version"] == 2
    assert second["weighted_consistency"] == "0.914142"
    assert service.get_current()["current_version"] == 2
    assert service.get_metrics(1)["current"] is False
    assert service.get_metrics(2)["current"] is True
    assert service.get_consistency(1)["weighted_consistency"]["value"] == "1.000000"
    assert service.get_consistency(2)["weighted_consistency"]["value"] == "0.914142"


def test_stale_base_changes_no_state(service: ConsistencyService, baseline_payload: dict, adjusted_payload: dict) -> None:
    service.commit_snapshot(baseline_payload, base_version=0)
    before = {
        "snapshots": table_count(service.db_path, "snapshots"),
        "intents": table_count(service.db_path, "intents"),
        "current": service.get_current()["current_version"],
    }
    with pytest.raises(BusinessError) as caught:
        service.commit_snapshot(adjusted_payload, base_version=0)
    assert caught.value.code == "E_STALE_BASE"
    after = {
        "snapshots": table_count(service.db_path, "snapshots"),
        "intents": table_count(service.db_path, "intents"),
        "current": service.get_current()["current_version"],
    }
    assert after == before


def test_duplicate_version_is_idempotent_only_for_identical_checksum(
    service: ConsistencyService, baseline_payload: dict, reseal
) -> None:
    first = service.commit_snapshot(baseline_payload, base_version=0)
    replay = service.commit_snapshot(baseline_payload, base_version=1)
    assert first["reused"] is False
    assert replay["reused"] is True
    assert replay["snapshot_version"] == 1
    assert replay["base_version"] == 1

    conflicting = copy.deepcopy(baseline_payload)
    conflicting["components"]["api"]["quota"] = 401
    conflicting = reseal(conflicting)
    with pytest.raises(BusinessError) as caught:
        service.commit_snapshot(conflicting)
    assert caught.value.code == "E_DUPLICATE_VERSION"
    assert service.get_current()["current_version"] == 1


def test_concurrent_duplicate_commits_execute_once(service: ConsistencyService, baseline_payload: dict) -> None:
    barrier = threading.Barrier(6)

    def submit() -> dict:
        barrier.wait(timeout=5)
        return service.commit_snapshot(baseline_payload)

    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(lambda _: submit(), range(6)))

    assert all(result["snapshot_version"] == 1 for result in results)
    assert sum(1 for result in results if not result["reused"]) == 1
    assert table_count(service.db_path, "intents", "status = 'committed'") == 1
    assert table_count(service.db_path, "intents", "status IN ('aborted', 'recovered_aborted')") == 0
    assert service.get_metrics(1)["conclusion"]["status"] == "CONSISTENT"


def test_queries_do_not_observe_partial_cascade(
    service: ConsistencyService, baseline_payload: dict, adjusted_payload: dict
) -> None:
    service.commit_snapshot(baseline_payload, base_version=0)
    entered = threading.Event()

    def hook(point: str) -> None:
        if point == "after_intent_committed":
            entered.set()
            time.sleep(0.35)

    service.fault_hook = hook
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(service.commit_snapshot, adjusted_payload, base_version=1)
        assert entered.wait(timeout=5)
        assert service.get_current()["current_version"] == 1
        with pytest.raises(BusinessError) as caught:
            service.get_metrics(2)
        assert caught.value.code == "E_VERSION_NOT_FOUND"
        result = future.result(timeout=10)
    service.fault_hook = None

    assert result["current_version"] == 2
    assert service.get_current()["current_version"] == 2
    assert service.get_metrics(2)["current"] is True
    assert service.get_metrics(2)["components"]["api"]["quota_share"]["value"] == "0.393939"


def test_failed_recompute_is_marked_aborted(
    service: ConsistencyService, baseline_payload: dict, adjusted_payload: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    service.commit_snapshot(baseline_payload, base_version=0)

    import app.service as service_module

    def explode(_: dict) -> dict:
        raise RuntimeError("synthetic recompute failure")

    monkeypatch.setattr(service_module, "calculate_metrics", explode)
    with pytest.raises(RuntimeError):
        service.commit_snapshot(adjusted_payload, base_version=1)
    monkeypatch.undo()

    assert service.get_current()["current_version"] == 1
    assert table_count(service.db_path, "intents", "status = 'aborted'") == 1
    connection = connect(service.db_path)
    try:
        assert get_snapshot_row(connection, 2) is None
        assert snapshot_is_complete(connection, 1)
        assert get_current_version(connection) == 1
    finally:
        connection.close()
