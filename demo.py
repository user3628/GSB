#!/usr/bin/env python3
"""Offline repeatable demonstration of the TLS handshake workbench.

Runs entirely against local deterministic samples and a local SQLite file.
Scenarios:
  normal        successful five-stage handshake with fixed-point diagnostics
  over-budget   E_BUDGET_EXCEEDED adjudication with triggering stage
  clock-skew    expired chain with injected clock offset
  crash-recovery forced-termination recovery from a PENDING intent
  numeric       naive vs Kahan summation error audit and score determinism
  all           every scenario in order (default)
"""

from __future__ import annotations

import argparse
import json
import os
import sys

from tls_workbench import budget, scoring, states, timeutil
from tls_workbench.certificates import load_chain
from tls_workbench.engine import HandshakeConfig, HandshakeEngine, SampleStore
from tls_workbench.persistence import Store
from tls_workbench.samplegen import generate_all

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "data", "demo_workbench.sqlite3")
SAMPLES_PATH = os.path.join(BASE_DIR, "samples", "handshake_samples.json")


def emit(title: str, payload) -> None:
    print("=" * 78)
    print(title)
    print("=" * 78)
    print(json.dumps(payload, indent=2, default=str, sort_keys=True))
    print()


def load_params(sample_id: str) -> dict:
    with open(SAMPLES_PATH, encoding="utf-8") as handle:
        samples = json.load(handle)["samples"]
    for sample in samples:
        if sample["sample_id"] == sample_id:
            return dict(sample)
    raise KeyError(sample_id)


def fresh_store(path: str = DB_PATH) -> tuple[Store, SampleStore, HandshakeEngine]:
    if os.path.exists(path):
        os.remove(path)
    for suffix in ("-wal", "-shm"):
        sidecar = path + suffix
        if os.path.exists(sidecar):
            os.remove(sidecar)
    store = Store(path)
    samples = SampleStore(BASE_DIR)
    return store, samples, HandshakeEngine(store, samples)


def scenario_normal() -> dict:
    store, samples, engine = fresh_store()
    params = load_params("normal_handshake")
    session_id = "demo-normal"
    engine.create_session(session_id, params["chain_id"], params)
    result = engine.get_or_advance(session_id, params)
    summary = {
        "session_id": session_id,
        "session_status": result["session_status"],
        "stages": [row["stage"] + ":" + row["status"] for row in result["stage_states"]],
        "verification_instant": result["verification_instant"],
        "leaf_margin_ms": _leaf_margin(result),
        "total_score": result["score_report"]["total_score"],
        "total_budget_ms": result["budget_report"]["total_budget_ms"],
        "share_sum_percent": result["budget_report"]["share_sum_percent"],
    }
    emit("SCENARIO 1: normal handshake", summary)
    return result


def _leaf_margin(result: dict):
    for stage in result["stage_states"]:
        detail = json.loads(stage["detail_json"])
        if "path" in detail:
            return next(
                item["margin_after_ms"]
                for item in detail["path"]["margins"]
                if item["role"] == "leaf"
            )
    return None


def scenario_over_budget() -> None:
    from tls_workbench.errors import WorkbenchError
    store, samples, engine = fresh_store()
    params = load_params("over_budget_handshake")
    session_id = "demo-over-budget"
    engine.create_session(session_id, params["chain_id"], params)
    try:
        result = engine.get_or_advance(session_id, params)
        failure = result["failures"][-1]
        audit = result["numeric_error_audit"]
        status = result["session_status"]
    except WorkbenchError as exc:
        failure = {"code": exc.code, "details": exc.details}
        audit = exc.details["diagnostic"]["numeric_error_audit"]
        status = engine.store.get_session(session_id)["status"]
    emit(
        "SCENARIO 2: budget exceeded",
        {
            "session_status": status,
            "error_code": failure["code"],
            "trigger_stage": failure["details"]["trigger_stage"],
            "actual_ms_fixed": failure["details"]["actual_ms_fixed"],
            "limit_ms_fixed": failure["details"]["limit_ms_fixed"],
            "numeric_audit": audit,
        },
    )


def scenario_clock_skew() -> None:
    store, samples, engine = fresh_store()
    params = load_params("clock_skewed_handshake")
    session_id = "demo-clock-skew"
    engine.create_session(session_id, params["chain_id"], params)
    result = engine.get_or_advance(
        session_id, params, HandshakeConfig(continue_on_failure=True)
    )
    emit(
        "SCENARIO 3: clock skew with expired leaf (continue diagnostics)",
        {
            "verification_instant": result["verification_instant"],
            "failures": result["failures"],
            "certificate_stage_detail": json.loads(
                result["stage_states"][2]["detail_json"]
            ),
            "session_status": result["session_status"],
        },
    )


def scenario_crash_recovery() -> None:
    store, samples, engine = fresh_store()
    params = load_params("normal_handshake")
    session_id = "demo-crash"
    engine.create_session(session_id, params["chain_id"], params)
    # Manually persist the first two complete stage boundaries, then simulate
    # a hard kill: the third stage's PENDING intent is logged but its boundary
    # commit never happens.
    for stage in states.STAGES[:2]:
        detail, status = engine._execute_stage(
            stage,
            samples.load(params["chain_id"]),
            timeutil.build_verification_instant(
                params["verification_wall_ms"], params["clock_offset_ms"]
            ),
            params,
            engine._build_estimates(params, []),
            [],
            {},
        )
        store.append_stage(session_id, stage, status, detail)
    crashed_stage = states.STAGES[2]
    store._write_intent(
        session_id, "ADVANCE_STAGE", crashed_stage,
        {"simulated": "forced termination before boundary commit"},
    )
    pending_before = store.get_pending_intents()
    store.close()
    # Reopen the database exactly as a restarted process would.
    restarted = Store(DB_PATH)
    report = restarted.recover_pending()
    session = restarted.get_session(session_id)
    emit(
        "SCENARIO 4: forced-termination recovery",
        {
            "crashed_during_stage": crashed_stage,
            "pending_intents_before_restart": len(pending_before),
            "recovery_report": report,
            "session_after_recovery": {
                "status": session["status"],
                "current_stage": session["current_stage"],
                "version": session["version"],
            },
            "note": "report is based only on committed facts; no synthetic "
                    "Finished report is produced after a hard kill",
        },
    )
    restarted.close()


def scenario_numeric() -> None:
    params = load_params("normal_handshake")
    estimates = [
        budget.stage_estimate(
            stage,
            params["stage_costs_ms_fixed"][stage],
            params["chain_overhead_ms_fixed"][stage],
        )
        for stage in states.STAGES
    ]
    audit = budget.naive_vs_compensated_audit(estimates)
    samples = SampleStore(BASE_DIR)
    instant = timeutil.build_verification_instant(
        params["verification_wall_ms"], params["clock_offset_ms"]
    )
    score_runs = []
    chain = samples.load("rsa2048_chain")
    from tls_workbench import chain as chain_mod
    path = chain_mod.validate_chain(chain, instant)
    first = scoring.score_chain(chain, path)
    for _ in range(3):
        score_runs.append(scoring.score_chain(chain, path)["total_score_fixed"])
    chain_b = samples.load("rsa3072_chain")
    path_b = chain_mod.validate_chain(chain_b, instant)
    comparison = scoring.compare_chains(
        first, scoring.score_chain(chain_b, path_b)
    )
    emit(
        "SCENARIO 5: numeric precision and error propagation",
        {
            "summation_audit": audit,
            "score_runs_identical": len(set(score_runs)) == 1,
            "score_runs": score_runs,
            "chain_comparison": comparison,
            "fixed_point_contract": {
                "scale": 1_000_000_000,
                "rounding": "ROUND_HALF_EVEN",
                "storage": "arbitrary-precision Python integers",
            },
        },
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="TLS workbench offline demo")
    parser.add_argument(
        "scenario",
        nargs="?",
        default="all",
        choices=["all", "normal", "over-budget", "clock-skew",
                 "crash-recovery", "numeric"],
    )
    args = parser.parse_args()
    generate_all(BASE_DIR)
    runners = {
        "normal": [scenario_normal],
        "over-budget": [scenario_over_budget],
        "clock-skew": [scenario_clock_skew],
        "crash-recovery": [scenario_crash_recovery],
        "numeric": [scenario_numeric],
    }
    chosen = (
        [scenario_normal, scenario_over_budget, scenario_clock_skew,
         scenario_crash_recovery, scenario_numeric]
        if args.scenario == "all" else runners[args.scenario]
    )
    for runner in chosen:
        runner()
    return 0


if __name__ == "__main__":
    sys.exit(main())
