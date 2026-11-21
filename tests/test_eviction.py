from __future__ import annotations

import pytest

from pdb_engine.errors import BusinessError


def test_single_pod_blocked_by_min_available(service) -> None:
    with pytest.raises(BusinessError) as exc:
        service.check_eviction("ledger-0", "single-blocked")
    assert exc.value.code == "E_PDB_BLOCKED"
    decision = exc.value.details["decision"]
    assert decision["decision"] == "BLOCKED"
    reason = decision["blocking_reasons"][0]
    assert reason["pdb"] == "ledger-full-availability"
    assert reason["current_available"] == 2
    assert reason["required_available"] == 2
    assert reason["gap"] == 1


def test_allowed_single_pod_has_placement(service) -> None:
    decision = service.check_eviction("web-a-alpha", "single-allowed")
    assert decision["decision"] == "ALLOWED"
    assert decision["placement"]["target_node"] == "worker-b"
    assert decision["affected_pdbs"][0]["after_available"] == 2
    assert decision["affected_pdbs"][0]["required_available"] == 2


def test_no_feasible_node_downgrades_eviction(service) -> None:
    with pytest.raises(BusinessError) as exc:
        service.check_eviction("web-b-offsite", "single-no-node")
    assert exc.value.code == "E_NO_FEASIBLE_NODE"
    decision = exc.value.details["decision"]
    assert decision["decision"] == "BLOCKED"
    assert decision["blocking_reasons"][0]["code"] == "E_NO_FEASIBLE_NODE"


def test_multiple_pdbs_must_all_allow(service) -> None:
    service.drain_node("worker-a", "drain-before-shared-check")
    with pytest.raises(BusinessError) as exc:
        service.check_eviction("web-a-shared", "shared-check")
    assert exc.value.code == "E_PDB_BLOCKED"
    reasons = exc.value.details["decision"]["blocking_reasons"]
    assert {reason["pdb"] for reason in reasons} == {
        "web-a-budget",
        "web-critical-budget",
    }
    assert all(reason["gap"] == 1 for reason in reasons)

