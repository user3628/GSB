from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path

import pytest

from slo_budget.engine import CalculationCoordinator, SLIEngine
from slo_budget.errors import BusinessError

from .conftest import event, window_events

BASE = 1_767_002_400_000


def test_minimum_sample_error_contains_count(engine: SLIEngine) -> None:
    engine.ingest(window_events(5, BASE), batch_id="small")
    metric = engine.window_metrics("checkout", "availability", "5m")
    assert metric["status"] == "E_INSUFFICIENT_DATA"
    assert metric["sample_count"] == 5
    assert metric["minimum_sample_count"] == 20
    assert metric["sli"] is None


def test_exact_window_and_budget(engine: SLIEngine) -> None:
    events = window_events(100, BASE, bad_every=10)
    response = engine.ingest(events, batch_id="exact")
    assert response["accepted_events"] == 100
    metric = engine.window_metrics("checkout", "availability", "5m")
    assert metric["sampling"]["used"] is False
    assert metric["sli"] == pytest.approx(0.9)
    assert metric["error_budget"]["observed_bad_events"] == 10
    assert metric["error_budget"]["consumed_fraction"] == pytest.approx(100.0)
    assert metric["window_end"].endswith("Z")


def test_sampling_is_deterministic_and_ci_contains_exact(engine: SLIEngine) -> None:
    events = window_events(1000, BASE, bad_every=100)
    engine.ingest(events, batch_id="sample")
    first = engine.window_metrics("checkout", "availability", "5m")
    second = engine.window_metrics("checkout", "availability", "5m")
    assert first == second
    assert first["sampling"]["used"] is True
    assert first["sampling"]["method"] == "deterministic_reservoir_blake2b_256"
    assert first["sampling"]["sample_rate"] == pytest.approx(0.5)
    exact_sli = 990 / 1000
    lower, upper = first["sli_confidence_interval"]
    assert lower <= exact_sli <= upper
    assert first["error_budget"]["consumed_fraction_confidence_interval"][0] <= 10.0
    assert first["error_budget"]["consumed_fraction_confidence_interval"][1] >= 10.0


def test_budget_ledger_is_additive_across_windows(
    tmp_path: Path,
) -> None:
    from slo_budget.config import Settings

    settings = Settings(
        base_dir=tmp_path,
        data_dir=tmp_path / "data",
        db_path=tmp_path / "data" / "budget.db",
        windows=(300, 1800, 3600),
        minimum_samples=20,
        exact_event_limit=10_000,
        late_tolerance_seconds=10_000,
    )
    engine = SLIEngine(settings)
    events = []
    for minute in range(120):
        for offset in range(50):
            index = minute * 50 + offset
            timestamp = BASE + minute * 60_000 + offset * 900
            events.append(event(timestamp, good=index % 100 != 0))
    engine.ingest(events, batch_id="two-hours")
    summary = engine.budget_summary("checkout")
    assert summary["consistency_checks"]["ledger_bad_events_equal_total"]
    assert summary["consistency_checks"]["all_tumbling_windows_equal_service_total"]
    total_bad = summary["service_total"]["consumed_error_budget"]
    for label in ("5m", "30m", "1h"):
        item = summary["tumbling_windows"][label]
        assert item["sum_consumed_error_budget"] == total_bad
        assert item["equals_service_total"] is True
        assert sum(slice_["bad_events"] for slice_ in item["slices"]) == total_bad
    assert summary["service_total"]["total_events"] == 6000
    first_hour = engine.budget_summary("checkout", end=BASE + 3_600_000)
    assert first_hour["service_total"]["total_events"] == 3000
    assert first_hour["consistency_checks"][
        "all_tumbling_windows_equal_service_total"
    ]


def test_late_event_does_not_mutate_closed_bucket(
    engine: SLIEngine, settings
) -> None:
    first_batch = [event(BASE + minute * 60_000, good=True) for minute in range(5)]
    engine.ingest(first_batch, batch_id="open-window")
    engine.ingest([event(BASE + 15 * 60_000, good=True)], batch_id="advance")
    before = engine.health()
    with pytest.raises(BusinessError) as raised:
        engine.ingest(
            [
                event(BASE, good=False),
                event(BASE + 15 * 60_000 + 1_000, good=True),
            ],
            batch_id="late-batch",
        )
    assert raised.value.code == "E_LATE_EVENT"
    assert raised.value.details["bucket_counts_unchanged"] is True
    after = engine.health()
    assert after["version"] == before["version"]
    assert after["event_count"] == before["event_count"]
    connection = sqlite3.connect(settings.db_path)
    try:
        closed = connection.execute(
            "SELECT COUNT(*) FROM closed_budget_snapshots"
        ).fetchone()[0]
        rejected = connection.execute("SELECT COUNT(*) FROM rejected_events").fetchone()[0]
    finally:
        connection.close()
    assert closed > 0
    assert rejected == 1
    summary = engine.budget_summary("checkout")
    assert summary["service_total"]["total_events"] == 6


def test_multiwindow_alert_is_critical(engine: SLIEngine) -> None:
    events = []
    for minute in range(120):
        for offset in range(50):
            index = minute * 50 + offset
            events.append(
                event(
                    BASE + minute * 60_000 + offset * 900,
                    good=index % 20 != 0,
                )
            )
    for chunk_index in range(0, len(events), 250):
        engine.ingest(
            events[chunk_index : chunk_index + 250],
            batch_id=f"alert-data-{chunk_index}",
        )
    alerts = engine.alerts(service="checkout")["alerts"]
    assert len(alerts) == 1
    alert = alerts[0]
    assert alert["status"] == "CRITICAL"
    assert alert["short_burn_rate"] >= 6
    assert alert["long_burn_rate"] >= 6
    assert alert["details"]["windows"]["5m"]["status"] == "OK"


def test_idempotent_batch(engine: SLIEngine) -> None:
    events = window_events(30, BASE)
    first = engine.ingest(events, batch_id="same")
    second = engine.ingest(events, batch_id="same")
    assert second["replayed"] is True
    assert second["version"] == first["version"]
    assert engine.health()["event_count"] == 30


def test_calculation_coordinator_single_flight() -> None:
    coordinator = CalculationCoordinator()
    calls = 0

    def compute() -> dict:
        nonlocal calls
        calls += 1
        time.sleep(0.05)
        return {"value": 42}

    results: list[dict] = []

    def worker() -> None:
        results.append(coordinator.get_or_compute(("same",), compute))

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=2)
    assert calls <= 1
    assert results == [{"value": 42}] * 8


def test_fixed_version_query_is_stable_after_new_commit(engine: SLIEngine) -> None:
    engine.ingest(window_events(100, BASE, bad_every=10), batch_id="snapshot-one")
    first = engine.window_metrics("checkout", "availability", "5m", version=1)
    engine.ingest(
        [event(BASE + 6 * 60_000, good=False)], batch_id="snapshot-two"
    )
    replay = engine.window_metrics("checkout", "availability", "5m", version=1)
    assert replay == first
    current = engine.window_metrics("checkout", "availability", "5m")
    assert current["version"] == 2


def test_recovery_marks_unfinished_intent(tmp_path: Path) -> None:
    from slo_budget.config import Settings

    settings = Settings(
        base_dir=tmp_path,
        data_dir=tmp_path / "data",
        db_path=tmp_path / "data" / "recovery.db",
        minimum_samples=10,
        exact_event_limit=100,
        late_tolerance_seconds=600,
    )
    first = SLIEngine(settings)
    first.recover()
    first.ingest(window_events(20, BASE), batch_id="baseline")
    version = first.health()["version"]
    first._start_intent(
        intent_id="intent-crash",
        kind="INGEST",
        service="checkout",
        batch_id="crash",
        payload_sha256="abc",
        target_version=version + 1,
        accepted_events=10,
        created_at="2025-12-30T20:00:00.000Z",
        details={"event_count": 10},
    )
    restarted = SLIEngine(settings)
    report = restarted.recover(reason="unit-restart")
    assert report["last_complete_version"] == version
    assert report["intents_scanned"] == 1
    assert report["intents_abandoned"] == 1
    assert report["unfinished_intents"][0]["intent_id"] == "intent-crash"
    assert restarted.health()["version"] == version
