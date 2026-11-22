from __future__ import annotations

import threading
from pathlib import Path

import pytest

from gitops_convergence.engine import Engine
from gitops_convergence.errors import BusinessError

from conftest import app, increment, snapshot


def test_stale_base_does_not_change_committed_state(engine: Engine) -> None:
    engine.submit_snapshot(snapshot([app("a")]))
    with pytest.raises(BusinessError) as captured:
        engine.submit_increment(
            999,
            increment(
                version=1000,
                base_version=999,
                changes=[{"app_id": "a", "desired_version": "v2"}],
            ),
        )
    assert captured.value.code == "E_STALE_BASE"
    assert engine.current_version() == 1
    assert engine.get_snapshot_summary(1)["application_count"] == 1


def test_concurrent_same_base_increment_allows_exactly_one(tmp_path: Path) -> None:
    engine = Engine(tmp_path / "increment-race.db")
    engine.submit_snapshot(snapshot([app("a"), app("b")]))
    barrier = threading.Barrier(3)
    outcomes: list[tuple[str, str]] = []
    lock = threading.Lock()

    def worker(app_id: str) -> None:
        payload = increment(
            version=2,
            base_version=1,
            changes=[{"app_id": app_id, "desired_version": "v2"}],
        )
        barrier.wait()
        try:
            engine.submit_increment(1, payload)
            outcome = ("ok", app_id)
        except BusinessError as exc:
            outcome = (exc.code, app_id)
        with lock:
            outcomes.append(outcome)

    threads = [threading.Thread(target=worker, args=(item,)) for item in ("a", "b")]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join(timeout=10)

    assert sorted(code for code, _ in outcomes) == ["E_STALE_BASE", "ok"]
    assert engine.current_version() == 2
    versions = engine.db.connect().execute(
        "SELECT version FROM snapshot_headers ORDER BY version"
    ).fetchall()
    assert [row["version"] for row in versions] == [1, 2]


def test_increment_marks_downstream_for_reevaluation(engine: Engine) -> None:
    engine.submit_snapshot(
        snapshot(
            [
                app("a", dependencies=["b"]),
                app("b"),
                app("c", dependencies=["a"]),
                app("unrelated"),
            ]
        )
    )
    result = engine.submit_increment(
        1,
        increment(
            version=2,
            base_version=1,
            changes=[{"app_id": "b", "desired_version": "v2"}],
        ),
    )
    assert result["affected_app_ids"] == ["a", "b", "c"]
    assert result["reevaluation_app_ids"] == ["a", "c"]
    assert engine.get_app(2, "a")["requires_reevaluation"] is True
    assert engine.get_app(2, "c")["requires_reevaluation"] is True
    assert engine.get_app(2, "unrelated")["requires_reevaluation"] is False
    assert engine.get_app(2, "b")["state"] == "PENDING"
