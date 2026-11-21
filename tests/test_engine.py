import threading

import pytest

from fiw.errors import BizError, E_INVALID_TRANSITION, E_ROUND_INVALID
from fiw.models import (
    OUTCOME_INJECTED,
    OUTCOME_SKIPPED,
    OUTCOME_TIMEOUT,
    OUTCOME_UNSUPPORTED,
    STATUS_COMPLETED,
    STATUS_PLANNED,
    STATUS_RUNNING,
)


def _outcomes(rnd):
    return {t["item_id"]: t for t in rnd["trajectory"]}


def test_round_requires_running_state(loaded_engine):
    with pytest.raises(BizError) as exc:
        loaded_engine.run_round("plan-test", 1)
    assert exc.value.code == E_INVALID_TRANSITION
    assert loaded_engine.get_plan("plan-test")["status"] == STATUS_PLANNED


def test_round_out_of_range(loaded_engine):
    loaded_engine.transition("plan-test", STATUS_RUNNING)
    with pytest.raises(BizError) as exc:
        loaded_engine.run_round("plan-test", 99)
    assert exc.value.code == E_ROUND_INVALID


def test_full_round_execution(loaded_engine):
    engine = loaded_engine
    engine.transition("plan-test", STATUS_RUNNING)
    rnd = engine.run_round("plan-test", 1)
    assert rnd["reused"] is False

    outcomes = _outcomes(rnd)
    assert outcomes["it-1"]["outcome"] == OUTCOME_INJECTED
    assert outcomes["it-2"]["outcome"] == OUTCOME_TIMEOUT
    assert outcomes["it-2"]["code"] == "E_INJECTION_TIMEOUT"
    assert outcomes["it-3"]["outcome"] == OUTCOME_INJECTED
    assert outcomes["it-4"]["outcome"] == OUTCOME_UNSUPPORTED
    assert outcomes["it-4"]["code"] == "E_FAULT_UNSUPPORTED"
    assert "listening" in outcomes["it-4"]["reason"]

    obs = rnd["observations"]
    assert obs["ep-cache-1"]["conclusion"] == "UNREACHABLE"
    assert obs["ep-db-1"]["conclusion"] == "UNREACHABLE"
    assert obs["ep-web-1"]["conclusion"] == "UNSTABLE"
    assert obs["ep-web-2"]["conclusion"] == "NORMAL"
    assert obs["ep-api-1"]["conclusion"] == "UNREACHABLE"
    assert obs["ep-api-1"]["propagation"]

    budget = rnd["budget"]
    assert budget["limit_ms"] == 5000
    assert budget["injection_quota"] == 4
    assert budget["concurrency_limit"] == 2
    assert budget["used_ms"] > 0
    assert budget["remaining_ms"] == pytest.approx(
        budget["limit_ms"] - budget["used_ms"])


def test_round_idempotent_retrigger(loaded_engine):
    engine = loaded_engine
    engine.transition("plan-test", STATUS_RUNNING)
    first = engine.run_round("plan-test", 1)
    second = engine.run_round("plan-test", 1)
    assert second["reused"] is True
    assert second["committed_at"] == first["committed_at"]
    assert second["trajectory"] == first["trajectory"]


def test_concurrent_duplicate_trigger_shares_result(loaded_engine):
    engine = loaded_engine
    engine.transition("plan-test", STATUS_RUNNING)
    results = []
    threads = [threading.Thread(
        target=lambda: results.append(engine.run_round("plan-test", 1)))
        for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    committed = {r["committed_at"] for r in results}
    assert len(committed) == 1
    assert sum(1 for r in results if not r["reused"]) == 1


def test_disabled_fault_switch_skips_item(env, engine):
    plan = env.plan_dict()
    plan["fault_switches"]["corrupt_body"] = False
    env.write_plan(plan)
    engine.load_plan(str(env.endpoints_path), str(env.plan_path))
    engine.transition("plan-test", STATUS_RUNNING)
    rnd = engine.run_round("plan-test", 1)
    skipped = {s["item_id"]: s for s in rnd["skipped"]}
    assert skipped["it-3"]["outcome"] == OUTCOME_SKIPPED
    assert skipped["it-3"]["reason"] == "fault_type_disabled"


def test_low_budget_sacrifices_coverage(env, engine):
    plan = env.plan_dict()
    plan["scheduler"]["round_time_budget_ms"] = 25  # only one refused fits
    env.write_plan(plan)
    engine.load_plan(str(env.endpoints_path), str(env.plan_path))
    engine.transition("plan-test", STATUS_RUNNING)
    rnd = engine.run_round("plan-test", 1)
    outcomes = _outcomes(rnd)
    assert set(outcomes) == {"it-1"}  # highest priority, cheapest
    skipped_reasons = {s["item_id"]: s["reason"] for s in rnd["skipped"]}
    assert skipped_reasons["it-2"] == "round_time_budget_exhausted"
    assert skipped_reasons["it-4"] == "round_time_budget_exhausted"
    assert skipped_reasons["it-3"] == "round_time_budget_exhausted"
    assert rnd["budget"]["injection_quota"] == 1


def test_plan_completes_after_final_round(loaded_engine):
    engine = loaded_engine
    engine.transition("plan-test", STATUS_RUNNING)
    engine.run_round("plan-test", 1)
    rnd2 = engine.run_round("plan-test", 2)
    outcomes = _outcomes(rnd2)
    assert set(outcomes) == {"it-1", "it-2"}  # duration_rounds=2 items only
    assert engine.get_plan("plan-test")["status"] == STATUS_COMPLETED
    # round 1 already committed -> idempotent reuse even after completion
    assert engine.run_round("plan-test", 1)["reused"] is True
