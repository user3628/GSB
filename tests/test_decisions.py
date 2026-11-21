from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest

from app.errors import BusinessError


def test_full_release_persists_state_transition(seeded_service):
    result = seeded_service.apply_decision(
        "B-PASS", {"mode": "FULL", "decision_time_ms": 1_763_900_070_000}
    )
    assert result["applied"] is True
    assert result["action"] == "RELEASE"
    assert result["batch"]["status"] == "PASSED"
    trajectory = seeded_service.get_batch("B-PASS")["transitions"]
    assert [(item["from_status"], item["to_status"]) for item in trajectory] == [
        (None, "OBSERVING"),
        ("OBSERVING", "PASSED"),
    ]
    assert trajectory[-1]["policy_version"] == result["risk"]["policy_version"]
    assert trajectory[-1]["window_version"] == result["risk"]["window_versions"]["window_version"]

    with pytest.raises(BusinessError) as error:
        seeded_service.apply_decision(
            "B-PASS", {"mode": "FULL", "decision_time_ms": 1_763_900_080_000}
        )
    assert error.value.code == "E_INVALID_TRANSITION"
    assert seeded_service.get_batch("B-PASS")["status"] == "PASSED"


def test_auto_rollback_and_cooldown_are_one_atomic_fact(seeded_service):
    first = seeded_service.apply_decision(
        "B-ROLLBACK",
        {"mode": "FULL", "decision_time_ms": 1_763_910_070_000},
    )
    assert first["action"] == "AUTO_ROLLBACK"
    assert first["batch"]["status"] == "ROLLED_BACK"
    assert first["cooldown"]["active"] is True
    assert first["cooldown"]["cooldown_until_ms"] == 1_763_910_370_000

    batch = seeded_service.get_batch("B-ROLLBACK")
    assert batch["status"] == "ROLLED_BACK"
    assert batch["cooldown"]["source_batch_id"] == "B-ROLLBACK"
    assert [
        (item["from_status"], item["to_status"], item["action"])
        for item in batch["transitions"][-2:]
    ] == [
        ("OBSERVING", "ROLLING_BACK", "AUTO_ROLLBACK_START"),
        ("ROLLING_BACK", "ROLLED_BACK", "AUTO_ROLLBACK_COMPLETE"),
    ]

    with pytest.raises(BusinessError) as error:
        seeded_service.apply_decision(
            "B-ROLLBACK",
            {"mode": "FULL", "decision_time_ms": 1_763_910_100_000},
        )
    assert error.value.code == "E_ROLLBACK_COOLDOWN"
    assert error.value.context["remaining_ms"] == 270_000
    assert seeded_service.get_batch("B-ROLLBACK")["status"] == "ROLLED_BACK"


def test_concurrent_rollback_attempts_produce_one_winner(tmp_path):
    from app.seed import seed_workbench
    from app.service import WorkbenchService

    service = WorkbenchService(tmp_path / "concurrent.sqlite3")
    seed_workbench(service, __import__("pathlib").Path(__file__).resolve().parent.parent / "samples")

    def attempt() -> tuple[str, object]:
        try:
            result = service.apply_decision(
                "B-ROLLBACK",
                {"mode": "FULL", "decision_time_ms": 1_763_910_070_000},
            )
            return "ok", result["batch"]["status"]
        except BusinessError as error:
            return "error", error.code

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(lambda _: attempt(), range(2)))
    assert sum(1 for status, value in outcomes if status == "ok" and value == "ROLLED_BACK") == 1
    assert sum(1 for status, value in outcomes if status == "error" and value == "E_ROLLBACK_COOLDOWN") == 1
    batch = service.get_batch("B-ROLLBACK")
    assert batch["status"] == "ROLLED_BACK"
    assert batch["cooldown"]["cooldown_until_ms"] == 1_763_910_370_000


def test_full_decision_under_new_policy_binds_same_window(seeded_service):
    result = seeded_service.apply_decision(
        "B-COMPARE",
        {
            "mode": "FULL",
            "policy_version": 2,
            "decision_time_ms": 1_763_920_070_000,
        },
    )
    assert result["policy_version"] == 2
    assert result["action"] == "AUTO_ROLLBACK"
    assert result["batch"]["status"] == "ROLLED_BACK"
    assert result["window_versions"]["policy_version"] == 2
    decision = seeded_service.list_decisions("B-COMPARE")[0]
    assert decision["policy_version"] == 2
    assert decision["window_version"] == result["risk"]["window_versions"]["window_version"]
