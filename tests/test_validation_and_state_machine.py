from __future__ import annotations

import pytest

from gitops_convergence.engine import Engine
from gitops_convergence.errors import BusinessError
from gitops_convergence.models import MAX_SQLITE_INT

from conftest import app, snapshot


def assert_spec_error(engine: Engine, payload: dict, fragment: str) -> None:
    with pytest.raises(BusinessError) as captured:
        engine.submit_snapshot(payload)
    assert captured.value.code == "E_SPEC_INVALID"
    assert fragment in captured.value.message
    assert engine.current_version() is None


def test_rejects_invalid_specs_without_committing(engine: Engine) -> None:
    assert_spec_error(engine, snapshot([app("a"), app("a")]), "unique")
    assert_spec_error(engine, snapshot([app("a", dependencies=["missing"])]), "missing")
    assert_spec_error(engine, snapshot([app("a", replicas=-1)]), ">= 0")
    assert_spec_error(engine, snapshot([app("a", replicas=MAX_SQLITE_INT + 1)]), "overflow")
    payload = snapshot([app("a")])
    payload["checksum"] = "0" * 64
    assert_spec_error(engine, payload, "checksum")


def test_initial_states_and_invalid_transition_leave_state_unchanged(engine: Engine) -> None:
    engine.submit_snapshot(
        snapshot(
            [
                app("same", desired="v1", effective="v1"),
                app("drift", desired="v2", effective="v1"),
            ]
        )
    )
    assert engine.get_app(1, "same")["state"] == "CONVERGED"
    assert engine.get_app(1, "drift")["state"] == "PENDING"

    engine.transition_app(1, "drift", "ABORTED", "test terminal state")
    with pytest.raises(BusinessError) as captured:
        engine.transition_app(1, "drift", "CONVERGED", "must fail")
    assert captured.value.code == "E_INVALID_TRANSITION"
    assert engine.get_app(1, "drift")["state"] == "ABORTED"


def test_recovery_recomputes_from_persisted_initial_state(tmp_path) -> None:
    first = Engine(tmp_path / "restart.db")
    first.submit_snapshot(snapshot([app("a", desired="v2", effective="v1")]))
    second = Engine(tmp_path / "restart.db")
    assert second.current_version() == 1
    assert second.get_app(1, "a")["state"] == "PENDING"
    result = second.trigger_convergence(1)
    assert result["final_state_counts"] == {"CONVERGED": 1}
