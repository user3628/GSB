from __future__ import annotations

import pytest

from gitops_convergence.engine import Engine
from gitops_convergence.errors import BusinessError

from conftest import app, increment, snapshot


def test_rollback_cascade_is_atomic_and_downstream_first(engine: Engine) -> None:
    engine.submit_snapshot(
        snapshot(
            [
                app("a", dependencies=["b"]),
                app("b", dependencies=["c"]),
                app("c"),
                app("d", dependencies=["a"]),
                app("outside"),
            ]
        )
    )
    result = engine.submit_increment(
        1,
        increment(
            version=2,
            base_version=1,
            changes=[{"app_id": "b", "desired_version": "v0", "rollback": True}],
        ),
    )
    cascade = result["rollback_cascade"]
    assert cascade["affected_app_ids"] == ["a", "b", "d"]
    assert cascade["ordered_app_ids"] == ["d", "a", "b"]
    states = {
        item["app_id"]: item["state"]
        for item in engine.list_apps(2)["applications"]
    }
    assert states == {
        "a": "ROLLING_BACK",
        "b": "ROLLING_BACK",
        "c": "CONVERGED",
        "d": "ROLLING_BACK",
        "outside": "CONVERGED",
    }
    assert result["state_counts"] == {"CONVERGED": 2, "ROLLING_BACK": 3}


def test_cycle_reports_cycle_and_outside_affected_without_commit(engine: Engine) -> None:
    engine.submit_snapshot(
        snapshot(
            [
                app("x", dependencies=["y"]),
                app("y", dependencies=["x"]),
                app("z", dependencies=["y"]),
                app("w", dependencies=["z"]),
            ]
        )
    )
    with pytest.raises(BusinessError) as captured:
        engine.submit_increment(
            1,
            increment(
                version=2,
                base_version=1,
                changes=[{"app_id": "x", "desired_version": "v0", "rollback": True}],
            ),
        )
    assert captured.value.code == "E_DEPENDENCY_CYCLE"
    assert captured.value.context["cycle_app_ids"] == ["x", "y"]
    assert captured.value.context["outside_cycle_app_ids"] == ["w", "z"]
    assert captured.value.context["affected_app_ids"] == ["w", "x", "y", "z"]
    assert engine.current_version() == 1
    with pytest.raises(BusinessError) as missing:
        engine.get_snapshot_summary(2)
    assert missing.value.code == "E_NOT_FOUND"


def test_cycle_query_keeps_contract_and_does_not_mutate(engine: Engine) -> None:
    engine.submit_snapshot(
        snapshot([app("x", dependencies=["y"]), app("y", dependencies=["x"])])
    )
    before = engine.get_snapshot_summary(1)["state_counts"]
    with pytest.raises(BusinessError) as captured:
        engine.compute_cascade(1, ["x"])
    assert captured.value.code == "E_DEPENDENCY_CYCLE"
    assert captured.value.context["cycle_app_ids"] == ["x", "y"]
    assert engine.get_snapshot_summary(1)["state_counts"] == before
