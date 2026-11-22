"""Single-writer capacity run orchestration and up/down-shift decisions."""

from __future__ import annotations

import threading
from typing import Any

from .errors import AppError, Conflict, InvalidTransition, RoundAborted, RoundActive
from .models import downshift_concurrency, stage_for_retest
from .repository import Database
from .sampler import Sampler
from .verdict import evaluate_round


class CapacityEngine:
    def __init__(self, database: Database, *, sampler_class: type[Sampler] = Sampler) -> None:
        self.db = database
        self.sampler_class = sampler_class
        self._events: dict[str, threading.Event] = {}
        self._events_lock = threading.Lock()
        self._running_runs: set[str] = set()
        self._runs_lock = threading.Lock()

    def create_and_execute_run(
        self,
        plan_id: str,
        version: int | None = None,
        *,
        mode: str = "FULL",
    ) -> dict[str, Any]:
        run = self.db.create_run(plan_id, version, mode=mode)
        return self.execute_run(run["run_id"])

    def execute_run(self, run_id: str) -> dict[str, Any]:
        with self._runs_lock:
            if run_id in self._running_runs:
                raise RoundActive(
                    "this run is already being executed",
                    details={"run_id": run_id},
                )
            self._running_runs.add(run_id)
        error_code: str | None = None
        try:
            run = self.db.get_run(run_id)
            if run["status"] in {"CONVERGED", "ABORTED"}:
                return run
            plan = self.db.get_plan(run["plan_id"], run["plan_version"])
            stages = plan["stages"]
            rules = plan["decision_rules"]
            passed: list[dict[str, Any]] = []
            retests: list[dict[str, Any]] = []
            first_rejected: dict[str, Any] | None = None
            last_rejected_round: dict[str, Any] | None = None
            retest_count = 0

            for stage in stages:
                round_result = self.execute_stage(
                    run_id=run_id,
                    stage=stage,
                    stage_key=f"base:{stage['stage_index']}",
                    kind="BASE",
                    attempt_no=0,
                    plan=plan,
                )
                verdict = round_result["conclusion"]["verdict"]
                if verdict == "PASS":
                    passed.append(_stage_evidence(stage, round_result))
                    continue

                first_rejected = _stage_evidence(stage, round_result)
                last_rejected_round = round_result
                break

            if first_rejected is None:
                inflection = (
                    {
                        "kind": "SEQUENCE_END",
                        "stage_index": passed[-1]["stage_index"],
                        "concurrency": passed[-1]["concurrency"],
                        "round_id": passed[-1]["round_id"],
                    }
                    if passed
                    else None
                )
                decision = {
                    "rule": rules,
                    "outcome": "ALL_STAGES_PASSED",
                    "passed_stages": passed,
                    "retests": [],
                    "rationale": "完整档位序列均通过；未发现否决档位。",
                }
                return self.db.conclude_run(
                    run_id=run_id,
                    status="CONVERGED",
                    inflection=inflection,
                    decision_basis=decision,
                )

            decision_mode = rules["on_reject"]
            if decision_mode == "STEP_DOWN_RETEST":
                while (
                    passed
                    and retest_count < rules["max_downshift_retests"]
                    and first_rejected is not None
                ):
                    previous = passed[-1]
                    candidate = downshift_concurrency(
                        previous["concurrency"],
                        first_rejected["concurrency"],
                        rules["downshift_fraction_ppm"],
                    )
                    if candidate <= 0:
                        break
                    retest_index = first_rejected["stage_index"]
                    retest_stage = stage_for_retest(
                        stages[retest_index],
                        concurrency=candidate,
                        stage_index=retest_index,
                        name=f"downshift-{candidate}",
                    )
                    attempt_no = retest_count + 1
                    retest_round = self.execute_stage(
                        run_id=run_id,
                        stage=retest_stage,
                        stage_key=f"retest:{retest_index}:{attempt_no}",
                        kind="DOWNSHIFT_RETEST",
                        attempt_no=attempt_no,
                        plan=plan,
                    )
                    retest_count += 1
                    evidence = _stage_evidence(retest_stage, retest_round)
                    retests.append(evidence)
                    last_rejected_round = retest_round
                    if retest_round["conclusion"]["verdict"] == "PASS":
                        inflection = {
                            "kind": "BETWEEN_PASS_AND_REJECT",
                            "stage_index": retest_stage["stage_index"],
                            "concurrency": candidate,
                            "round_id": retest_round["round_id"],
                            "attempt_no": attempt_no,
                        }
                        decision = {
                            "rule": rules,
                            "outcome": "DOWNSHIFT_RETEST_PASSED",
                            "last_passed_stage": previous,
                            "rejected_stage": first_rejected,
                            "retests": retests,
                            "inflection": inflection,
                            "rationale": (
                                f"原档位 {first_rejected['concurrency']} 否决，"
                                f"在上一通过档 {previous['concurrency']} 与否决档之间"
                                f"按 {rules['downshift_fraction_ppm']} ppm 比例降档重测至 "
                                f"{candidate}；重测通过，容量边界位于该重测档。"
                            ),
                        }
                        return self.db.conclude_run(
                            run_id=run_id,
                            status="CONVERGED",
                            inflection=inflection,
                            decision_basis=decision,
                            rejected_stage=first_rejected,
                        )
                    break

            if decision_mode == "RETURN_LAST_PASS" and passed:
                inflection = {
                    "kind": "LAST_PASSED_STAGE",
                    "stage_index": passed[-1]["stage_index"],
                    "concurrency": passed[-1]["concurrency"],
                    "round_id": passed[-1]["round_id"],
                }
            else:
                inflection = {
                    "kind": "FIRST_REJECTED_STAGE",
                    "stage_index": first_rejected["stage_index"],
                    "concurrency": first_rejected["concurrency"],
                    "round_id": first_rejected["round_id"],
                }
            decision = {
                "rule": rules,
                "outcome": (
                    "DOWNSHIFT_RETEST_REJECTED"
                    if retests
                    else "REJECTED_STAGE_STOP"
                ),
                "last_passed_stage": passed[-1] if passed else None,
                "rejected_stage": first_rejected,
                "last_retest": retests[-1] if retests else None,
                "retests": retests,
                "inflection": inflection,
                "rationale": (
                    f"档位 {first_rejected['concurrency']} 未通过阈值；规则 "
                    f"{decision_mode} 停止推进，"
                    + (
                        "以最后通过档作为容量边界。"
                        if passed
                        else "首个档位即否决，无通过容量点。"
                    )
                ),
            }
            return self.db.conclude_run(
                run_id=run_id,
                status="CONVERGED",
                inflection=inflection,
                decision_basis=decision,
                rejected_stage=first_rejected,
            )
        except AppError as exc:
            error_code = exc.code
            try:
                current = self.db.get_run(run_id)
                if current["status"] in {"PENDING", "SAMPLING"}:
                    self.db.conclude_run(
                        run_id=run_id,
                        status="ABORTED",
                        inflection=None,
                        decision_basis={
                            "outcome": "ABORTED",
                            "error_code": exc.code,
                            "message": exc.message,
                        },
                        error_code=exc.code,
                    )
            except Exception:
                pass
            raise
        except Exception as exc:
            error_code = "E_INTERNAL"
            try:
                current = self.db.get_run(run_id)
                if current["status"] in {"PENDING", "SAMPLING"}:
                    self.db.conclude_run(
                        run_id=run_id,
                        status="ABORTED",
                        inflection=None,
                        decision_basis={
                            "outcome": "ABORTED",
                            "error_code": "E_INTERNAL",
                            "message": type(exc).__name__,
                        },
                        error_code="E_INTERNAL",
                    )
            except Exception:
                pass
            raise AppError(
                "E_INTERNAL", "capacity run failed", http_status=500, details={"type": type(exc).__name__}
            ) from exc
        finally:
            with self._runs_lock:
                self._running_runs.discard(run_id)

    def execute_stage(
        self,
        *,
        run_id: str,
        stage: dict[str, Any],
        stage_key: str,
        kind: str,
        attempt_no: int,
        plan: dict[str, Any],
    ) -> dict[str, Any]:
        event_key = f"{run_id}|{stage_key}"
        with self._events_lock:
            existing_event = self._events.get(event_key)
            owner = existing_event is None
            if owner:
                event = threading.Event()
                self._events[event_key] = event
            else:
                event = existing_event
        if not owner:
            if not event.wait(timeout=900):
                raise RoundActive(
                    "timed out waiting for the in-flight stage result",
                    details={"run_id": run_id, "stage_key": stage_key},
                )
            duplicate = self.db.find_round(run_id, stage_key)
            if duplicate is None:
                raise Conflict(
                    "duplicate stage waiter found no persisted result",
                    details={"run_id": run_id, "stage_key": stage_key},
                )
            if duplicate["status"] == "ABORTED":
                raise RoundAborted(
                    "duplicate stage was aborted",
                    details={"round_id": duplicate["round_id"], "stage_key": stage_key},
                )
            return self.db.get_round(duplicate["round_id"], include_samples=True)

        intent_id: str | None = None
        round_id: str | None = None
        try:
            existing = self.db.find_round(run_id, stage_key)
            if existing is not None:
                if existing["status"] == "CONVERGED":
                    return existing
                if existing["status"] == "ABORTED":
                    raise RoundAborted(
                        "stage has already been aborted",
                        details={"round_id": existing["round_id"], "stage_key": stage_key},
                    )
                raise RoundActive(
                    "stage is already sampling and has no in-process waiter",
                    details={"round_id": existing["round_id"], "stage_key": stage_key},
                )
            round_result, created = self.db.trigger_round(
                run_id=run_id,
                stage_key=stage_key,
                stage=stage,
                kind=kind,
                attempt_no=attempt_no,
                quantile_method=plan["numeric_contract"]["quantile_method"],
                plan_fingerprint_value=plan["fingerprint"],
            )
            round_id = round_result["round_id"]
            intent_id = round_result.get("_intent_id")
            if not created:
                if round_result["status"] == "CONVERGED":
                    return self.db.get_round(round_id, include_samples=True)
                raise RoundActive(
                    "round was created by another writer",
                    details={"round_id": round_id},
                )
            minimum = max(plan["numeric_contract"]["min_samples"], 100)
            if stage["sample_size"] < minimum:
                from .errors import InsufficientSamples

                raise InsufficientSamples(
                    "sample_size is below the minimum required for the requested P99",
                    details={
                        "sample_size": stage["sample_size"],
                        "required_samples": minimum,
                        "stage_index": stage["stage_index"],
                    },
                )
            stream_id = (
                f"plan:{plan['plan_id']}:v{plan['version']}:{stage_key}:c{stage['concurrency']}"
            )
            sampler = self.sampler_class(plan=plan, stage=stage, stream_id=stream_id)
            samples, diagnostics = sampler.sample()
            evaluation = evaluate_round(
                plan,
                stage,
                samples,
                wall_seconds=diagnostics["wall_seconds"],
            )
            evaluation["sample_payload"]["aggregate"]["sampler_diagnostics"] = diagnostics
            return self.db.finalize_round(
                round_id=round_id,
                intent_id=intent_id or "",
                sample_payload=evaluation["sample_payload"],
                conclusion_payload=evaluation,
            )
        except AppError as exc:
            if round_id and intent_id:
                self.db.abort_round(
                    round_id=round_id,
                    intent_id=intent_id,
                    error_code=exc.code,
                    message=exc.message,
                )
            raise
        except Exception as exc:
            if round_id and intent_id:
                self.db.abort_round(
                    round_id=round_id,
                    intent_id=intent_id,
                    error_code="E_SAMPLING_FAILED",
                    message=type(exc).__name__,
                )
            raise AppError(
                "E_SAMPLING_FAILED",
                "sampling or evaluation failed",
                http_status=500,
                details={"type": type(exc).__name__},
            ) from exc
        finally:
            with self._events_lock:
                current = self._events.get(event_key)
                if current is event:
                    self._events.pop(event_key, None)
            event.set()

    def trigger_manual_stage(
        self,
        plan_id: str,
        stage_index: int,
        *,
        version: int | None = None,
        run_id: str | None = None,
    ) -> dict[str, Any]:
        plan = self.db.get_plan(plan_id, version)
        if stage_index < 0 or stage_index >= len(plan["stages"]):
            from .errors import BadRequest

            raise BadRequest(
                "stage index is out of range",
                details={"stage_index": stage_index, "stage_count": len(plan["stages"])},
            )
        stage = plan["stages"][stage_index]
        if run_id is None:
            latest = self.db.latest_run_for_plan(plan_id, plan["version"])
            if latest is not None:
                existing = self.db.find_round(latest["run_id"], f"base:{stage_index}")
                if existing is not None or latest["status"] in {"PENDING", "SAMPLING"}:
                    run_id = latest["run_id"]
            if run_id is None:
                run_id = self.db.create_run(plan_id, plan["version"], mode="MANUAL")["run_id"]
        run = self.db.get_run(run_id)
        if run["plan_id"] != plan_id or run["plan_version"] != plan["version"]:
            raise InvalidTransition(
                "run does not belong to the requested plan version",
                details={"run_id": run_id, "plan_id": plan_id, "plan_version": plan["version"]},
            )
        try:
            result = self.execute_stage(
                run_id=run_id,
                stage=stage,
                stage_key=f"base:{stage_index}",
                kind="BASE",
                attempt_no=0,
                plan=plan,
            )
        except AppError as exc:
            current = self.db.get_run(run_id)
            if current["mode"] == "MANUAL" and current["status"] in {"PENDING", "SAMPLING"}:
                self.db.conclude_run(
                    run_id=run_id,
                    status="ABORTED",
                    inflection=None,
                    decision_basis={
                        "outcome": "MANUAL_STAGE_ABORTED",
                        "error_code": exc.code,
                        "message": exc.message,
                    },
                    error_code=exc.code,
                )
            raise
        current = self.db.get_run(run_id)
        if current["mode"] == "MANUAL" and current["status"] in {"PENDING", "SAMPLING"}:
            verdict = result["conclusion"]["verdict"]
            inflection = {
                "kind": "MANUAL_STAGE",
                "stage_index": stage["stage_index"],
                "concurrency": stage["concurrency"],
                "round_id": result["round_id"],
            }
            self.db.conclude_run(
                run_id=run_id,
                status="CONVERGED",
                inflection=inflection,
                decision_basis={
                    "outcome": "MANUAL_STAGE_CONCLUDED",
                    "verdict": verdict,
                    "round": _stage_evidence(stage, result),
                    "rationale": "手工触发的单档位轮次已完成；结论按轮次原子提交。",
                },
                rejected_stage=_stage_evidence(stage, result)
                if verdict == "REJECT"
                else None,
            )
        return result


def _stage_evidence(stage: dict[str, Any], round_result: dict[str, Any]) -> dict[str, Any]:
    conclusion = round_result["conclusion"]
    return {
        "round_id": round_result["round_id"],
        "stage_index": stage["stage_index"],
        "concurrency": stage["concurrency"],
        "kind": round_result.get("kind"),
        "attempt_no": round_result.get("attempt_no"),
        "verdict": conclusion["verdict"],
        "conclusion_version": conclusion["conclusion_version"],
        "quantile_method": conclusion["quantile_method"],
        "computation_digest": conclusion["computation_digest"],
        "failed_thresholds": conclusion["reason"]["failed_thresholds"],
    }

