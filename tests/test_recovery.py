from __future__ import annotations

import pytest

from .conftest import full_snapshot, incremental_snapshot, make_service, write


def build_merge_scenario(service):
    full_snapshot(service, "s0", "c0")
    write(service, 0, b"child-data", "w0")
    incremental_snapshot(service, "s1", "s0", "c1")


def test_merge_forced_stop_is_rolled_back_and_reported(crash_settings, monkeypatch):
    service = make_service(crash_settings)
    build_merge_scenario(service)

    def forced_exit(code):
        raise SystemExit(code)

    monkeypatch.setattr("app.service.os._exit", forced_exit)
    with pytest.raises(SystemExit):
        service.merge_snapshots("vol0", "s1", "s0", "crash-merge", crash_after_begin=True)

    raw = service.get_chain("vol0")
    s1 = next(item for item in raw["snapshots"] if item["snapshot_id"] == "s1")
    assert s1["state"] == "MERGING"
    assert raw["state_counts"]["MERGING"] == 1

    restarted = make_service(crash_settings)
    chain = restarted.get_chain("vol0")
    s1 = next(item for item in chain["snapshots"] if item["snapshot_id"] == "s1")
    assert s1["state"] == "COMMITTED"
    assert chain["state_counts"]["MERGING"] == 0

    report = restarted.get_recovery_report("vol0")
    assert report["status"] == "RECOVERED"
    assert report["durable_only"] is True
    assert report["summary"]["pending_intents_scanned"] == 1
    assert report["summary"]["intents_aborted"] == 1
    assert report["summary"]["requests_aborted"] == 1
    assert report["snapshot_repairs"] == [
        {"snapshot_id": "s1", "from_state": "MERGING", "to_state": "COMMITTED"}
    ]
    assert report["consistency"]["ok"] is True

    retried = restarted.merge_snapshots("vol0", "s1", "s0", "retry-merge")
    assert retried["source_state_after"] == "MERGED"


def test_rollback_forced_stop_leaves_complete_previous_view(crash_settings, monkeypatch):
    service = make_service(crash_settings)
    full_snapshot(service, "s0", "c0")
    write(service, 0, b"newer", "w0")
    before = service.get_current_view("vol0")

    def forced_exit(code):
        raise SystemExit(code)

    monkeypatch.setattr("app.service.os._exit", forced_exit)
    with pytest.raises(SystemExit):
        service.rollback_snapshot(
            "vol0", "s0", "crash-rollback", crash_before_commit=True
        )

    assert service.get_current_view("vol0") == before
    restarted = make_service(crash_settings)
    assert restarted.get_current_view("vol0") == before
    report = restarted.get_recovery_report("vol0")
    assert report["status"] == "RECOVERED"
    assert report["summary"]["pending_intents_scanned"] == 1
    assert report["summary"]["requests_aborted"] == 1
    assert report["consistency"]["ok"] is True
    assert report["durable_state"]["consistency_seq"] == before["consistency_seq"]


def test_interrupted_request_is_durably_marked_aborted_and_not_replayed(crash_settings, monkeypatch):
    service = make_service(crash_settings)
    build_merge_scenario(service)

    def forced_exit(code):
        raise SystemExit(code)

    monkeypatch.setattr("app.service.os._exit", forced_exit)
    with pytest.raises(SystemExit):
        service.merge_snapshots("vol0", "s1", "s0", "once-only", crash_after_begin=True)

    restarted = make_service(crash_settings)
    with pytest.raises(Exception) as exc:
        restarted.merge_snapshots("vol0", "s1", "s0", "once-only", crash_after_begin=False)
    assert getattr(exc.value, "code", None) in {"E_CONFLICTING_REQUEST", "E_OPERATION_ABORTED"}

