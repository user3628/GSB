from __future__ import annotations

import asyncio
from typing import Any

from .db import Store
from .errors import InsufficientSamples, RoundActive, StageOrderError, WorkbenchError
from .models import PlanRecord, RoundRecord, StageSpec
from .sampling import DeterministicSampler, aggregate_samples, evaluate_round


class WorkbenchService:
    def __init__(
        self, store: Store, sampler: DeterministicSampler | None = None
    ) -> None:
        self.store = store
        self.sampler = sampler or DeterministicSampler()
        self._stage_locks: dict[tuple[str, int], asyncio.Lock] = {}
        self._run_guard = asyncio.Lock()
        self._running_plans: set[str] = set()
        self.recovery_report: dict[str, Any] | None = None

    def initialize(self) -> None:
        self.store.init_schema()
        self.recovery_report = self.store.recover_incomplete()

    async def recover(self) -> dict[str, Any] | None:
        self.recovery_report = self.store.recover_incomplete()
        return self.recovery_report

    def create_plan(self, payload: dict[str, Any]) -> dict[str, Any]:
        plan = self.store.insert_plan(payload)
        return self.store.get_plan_report(plan.id)

    def report(self, plan_id: str) -> dict[str, Any]:
        return self.store.get_plan_report(plan_id)

    def round_detail(self, round_id: str) -> dict[str, Any]:
        return self.store.get_round_detail(round_id)

    async def execute_stage(self, plan_id: str, stage_index: int) -> dict[str, Any]:
        plan = self.store.get_plan(plan_id)
        if stage_index < 0 or stage_index >= plan.stage_count:
            raise WorkbenchError(
                "E_STAGE_OUT_OF_RANGE",
                "stage_index is outside the plan sequence",
                http_status=422,
                details={"stage_index": stage_index, "stage_count": plan.stage_count},
            )

        # A concurrent duplicate may arrive before it can acquire the per-stage
        # lock. Remember the in-flight round so the later caller reuses that
        # exact result even when it ends in ABORTED.
        latest_before_lock = self.store.latest_round(plan_id, stage_index)
        waiting_round_id = (
            latest_before_lock.id
            if latest_before_lock is not None
            and latest_before_lock.status in {"PENDING", "SAMPLING"}
            else None
        )
        lock = self._stage_lock(plan_id, stage_index)
        async with lock:
            if waiting_round_id is not None:
                waiting = self.store.get_round_detail(waiting_round_id)
                if waiting["status"] in {"CONVERGED", "ABORTED"}:
                    return waiting
                if waiting["status"] == "PENDING":
                    round_record = self.store.get_round(waiting_round_id)
                    return await self._run_existing_round(plan, round_record)
                return await self._wait_for_terminal(waiting_round_id)

            active = self.store.get_active_round_for_plan(plan_id)
            if active is not None:
                if int(active["stage_index"]) == stage_index:
                    return await self._wait_for_terminal(str(active["id"]))
                raise RoundActive(plan_id, str(active["id"]))

            latest = self.store.latest_round(plan_id, stage_index)
            if latest is not None:
                if latest.status == "CONVERGED":
                    return self.store.get_round_detail(latest.id)
                if latest.status == "PENDING":
                    return await self._run_existing_round(plan, latest)
                if latest.status == "ABORTED":
                    stage = plan.stages[stage_index]
                    return await self._create_and_execute(
                        plan=plan,
                        stage_index=stage_index,
                        stage=stage,
                        attempt=latest.attempt + 1,
                        concurrency=latest.concurrency,
                        origin="retry_after_abort",
                        retry_of_round_id=latest.retry_of_round_id or latest.id,
                    )

            self._ensure_previous_stages_passed(plan, stage_index)
            stage = plan.stages[stage_index]
            return await self._create_and_execute(
                plan=plan,
                stage_index=stage_index,
                stage=stage,
                attempt=1,
                concurrency=stage.concurrency,
                origin="sequence",
                retry_of_round_id=None,
            )

    async def run_plan(self, plan_id: str) -> dict[str, Any]:
        async with self._run_guard:
            if plan_id in self._running_plans:
                raise RoundActive(plan_id)
            self._running_plans.add(plan_id)
        try:
            plan = self.store.get_plan(plan_id)
            if plan.status == "COMPLETED":
                return self.store.get_plan_report(plan_id)
            self.store.mark_plan_running(plan_id)

            last_detail: dict[str, Any] | None = None
            for stage_index in range(plan.stage_count):
                detail = await self.execute_stage(plan_id, stage_index)
                last_detail = detail
                if detail["status"] != "CONVERGED" or detail["judgment"] is None:
                    raise WorkbenchError(
                        "E_ROUND_NOT_CONVERGED",
                        "Round did not produce a conclusion",
                        http_status=409,
                        details={"round_id": detail["id"], "status": detail["status"]},
                    )
                if not detail["judgment"]["passed"]:
                    retry_context = self._retry_context(plan_id, detail)
                    if retry_context is not None:
                        original_failed, _ = retry_context
                        retries_used = self._step_down_retries_used(
                            plan_id, detail["stage_index"], original_failed["id"]
                        )
                        return await self._handle_rejection_async(
                            plan,
                            detail,
                            retries_used=retries_used,
                            original_failed=original_failed,
                        )
                    return await self._handle_rejection_async(plan, detail)

                # A passed retry after an earlier rejected stage is a capacity
                # conclusion, not permission to continue up the original ladder.
                retry_context = self._retry_context(plan_id, detail)
                if retry_context is not None:
                    original_failed, accepted = retry_context
                    conclusion = self._knee_conclusion(
                        plan, original_failed, accepted
                    )
                    conclusion["reason"] = (
                        "Recovered a step-down retry pass; the lower concurrency is the accepted capacity point."
                    )
                    conclusion["retry_evidence"] = {
                        "attempt": accepted["attempt"],
                        "round_key": accepted["round_key"],
                        "concurrency": accepted["concurrency"],
                    }
                    self.store.finalize_plan(
                        plan_id,
                        conclusion,
                        action="step_down_retry_passed",
                        stage_index=accepted["stage_index"],
                        round_id=accepted["id"],
                    )
                    return self.store.get_plan_report(plan_id)

            if last_detail is not None:
                conclusion = {
                    "outcome": "SEQUENCE_EXHAUSTED",
                    "capacity": {
                        "stage_index": last_detail["stage_index"],
                        "concurrency": last_detail["concurrency"],
                        "round_id": last_detail["id"],
                        "round_key": last_detail["round_key"],
                    },
                    "knee": None,
                    "reason": "Every configured stage passed; the sequence upper bound was reached.",
                }
                self.store.finalize_plan(
                    plan_id,
                    conclusion,
                    action="sequence_exhausted",
                    stage_index=last_detail["stage_index"],
                    round_id=last_detail["id"],
                )
            return self.store.get_plan_report(plan_id)
        finally:
            async with self._run_guard:
                self._running_plans.discard(plan_id)

    def _handle_rejection(
        self, plan: PlanRecord, failed_detail: dict[str, Any]
    ) -> dict[str, Any]:
        rule = plan.decision_rule
        if rule.on_reject in {"stop", "fallback"}:
            action = "stop_sequence" if rule.on_reject == "stop" else "fallback_to_last_passed"
            conclusion = self._knee_conclusion(plan, failed_detail, None)
            conclusion["reason"] = (
                "Rejected stage stopped the sequence."
                if rule.on_reject == "stop"
                else "Rejected stage fell back to the last accepted concurrency."
            )
            self.store.finalize_plan(
                plan.id,
                conclusion,
                action=action,
                stage_index=failed_detail["stage_index"],
                round_id=failed_detail["id"],
            )
            return self.store.get_plan_report(plan.id)

        raise RuntimeError("step_down_retry requires async rejection handling")

    async def _handle_rejection_async(
        self,
        plan: PlanRecord,
        failed_detail: dict[str, Any],
        *,
        retries_used: int = 0,
        original_failed: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        rule = plan.decision_rule
        if rule.on_reject in {"stop", "fallback"}:
            return self._handle_rejection(plan, failed_detail)

        original_failed = original_failed or failed_detail
        current_failed = failed_detail
        while retries_used < rule.max_retries:
            last_pass = self._last_passed_round(
                plan.id, below=current_failed["concurrency"]
            )
            candidate = self._step_down_candidate(
                current_failed["concurrency"],
                rule.step_down_factor,
                last_pass["concurrency"] if last_pass else None,
            )
            if candidate is None:
                break
            stage_index = int(current_failed["stage_index"])
            stage = plan.stages[stage_index]
            attempt = self.store.attempt_count(plan.id, stage_index) + 1
            lock = self._stage_lock(plan.id, stage_index)
            async with lock:
                active = self.store.get_active_round_for_plan(plan.id)
                if active is not None:
                    raise RoundActive(plan.id, str(active["id"]))
                self.store.record_decision(
                    plan.id,
                    action="step_down_retry_started",
                    stage_index=stage_index,
                    round_id=current_failed["id"],
                    details={
                        "from_concurrency": current_failed["concurrency"],
                        "to_concurrency": candidate,
                        "retry_attempt": attempt,
                    },
                )
                current_failed = await self._create_and_execute(
                    plan=plan,
                    stage_index=stage_index,
                    stage=stage,
                    attempt=attempt,
                    concurrency=candidate,
                    origin="step_down_retry",
                    retry_of_round_id=current_failed["id"],
                )
            retries_used += 1
            if current_failed["judgment"] and current_failed["judgment"]["passed"]:
                conclusion = self._knee_conclusion(
                    plan, original_failed, current_failed
                )
                conclusion["reason"] = (
                    "The original stage was rejected and a lower concurrency retry passed."
                )
                conclusion["retry_evidence"] = {
                    "attempt": current_failed["attempt"],
                    "round_key": current_failed["round_key"],
                    "concurrency": current_failed["concurrency"],
                }
                self.store.finalize_plan(
                    plan.id,
                    conclusion,
                    action="step_down_retry_passed",
                    stage_index=stage_index,
                    round_id=current_failed["id"],
                )
                return self.store.get_plan_report(plan.id)

        conclusion = self._knee_conclusion(plan, original_failed, None)
        conclusion["reason"] = (
            "The stage was rejected and step-down retries were exhausted without a pass."
        )
        self.store.finalize_plan(
            plan.id,
            conclusion,
            action="step_down_exhausted",
            stage_index=original_failed["stage_index"],
            round_id=current_failed["id"],
        )
        return self.store.get_plan_report(plan.id)

    def _retry_context(
        self, plan_id: str, detail: dict[str, Any]
    ) -> tuple[dict[str, Any], dict[str, Any]] | None:
        """Return (original rejected stage, retry round) for a step-down chain."""

        if detail.get("retry_of_round_id") is None:
            return None
        parent = self.store.get_round_detail(str(detail["retry_of_round_id"]))
        root = parent
        seen: set[str] = set()
        while (
            root.get("retry_of_round_id") is not None
            and root["origin"] in {"step_down_retry", "retry_after_abort"}
            and root["id"] not in seen
        ):
            seen.add(root["id"])
            root = self.store.get_round_detail(str(root["retry_of_round_id"]))
        if (
            root["status"] == "CONVERGED"
            and root["judgment"] is not None
            and not root["judgment"]["passed"]
        ):
            return root, detail
        return None

    def _step_down_retries_used(
        self, plan_id: str, stage_index: int, original_failed_id: str
    ) -> int:
        used = 0
        for detail in self.store.list_rounds(plan_id):
            if detail["stage_index"] != stage_index:
                continue
            if detail["status"] != "CONVERGED" or detail["judgment"] is None:
                continue
            context = self._retry_context(plan_id, detail)
            if context is not None and context[0]["id"] == original_failed_id:
                used += 1
        return used

    def _knee_conclusion(
        self,
        plan: PlanRecord,
        rejected: dict[str, Any],
        accepted: dict[str, Any] | None,
    ) -> dict[str, Any]:
        last_pass = accepted
        if last_pass is None:
            last_pass = self._last_passed_round(
                plan.id, below=rejected["concurrency"]
            )
        capacity: dict[str, Any] | None = None
        if last_pass is not None:
            capacity = {
                "stage_index": last_pass["stage_index"],
                "concurrency": last_pass["concurrency"],
                "round_id": last_pass["id"],
                "round_key": last_pass["round_key"],
                "basis": (
                    "step_down_retry_pass"
                    if last_pass.get("origin") == "step_down_retry"
                    else "last_passed_stage"
                ),
            }
        triggered = rejected["judgment"]["triggered"] if rejected["judgment"] else []
        return {
            "outcome": "KNEE_FOUND",
            "capacity": capacity,
            "knee": {
                "stage_index": rejected["stage_index"],
                "base_concurrency": plan.stages[rejected["stage_index"]].concurrency,
                "rejected_concurrency": rejected["concurrency"],
                "round_id": rejected["id"],
                "round_key": rejected["round_key"],
                "attempt": rejected["attempt"],
                "triggered": triggered,
                "sample_size": rejected["judgment"]["sample_size"]
                if rejected["judgment"]
                else None,
            },
        }

    def _last_passed_round(
        self, plan_id: str, *, below: int
    ) -> dict[str, Any] | None:
        candidates: list[dict[str, Any]] = []
        for detail in self.store.list_rounds(plan_id):
            if (
                detail["status"] == "CONVERGED"
                and detail["judgment"]
                and detail["judgment"]["passed"]
                and detail["concurrency"] < below
            ):
                candidates.append(detail)
        if not candidates:
            return None
        return max(candidates, key=lambda item: (item["concurrency"], item["stage_index"]))

    @staticmethod
    def _step_down_candidate(
        failed_concurrency: int, factor: float, last_passed: int | None
    ) -> int | None:
        candidate = max(1, int(failed_concurrency * factor))
        if candidate >= failed_concurrency:
            candidate = failed_concurrency - 1
        if last_passed is not None and candidate <= last_passed:
            return None
        if candidate <= 0:
            return None
        return candidate

    def _ensure_previous_stages_passed(
        self, plan: PlanRecord, stage_index: int
    ) -> None:
        for previous_index in range(stage_index):
            latest = self.store.latest_round(plan.id, previous_index)
            detail = (
                self.store.get_round_detail(latest.id) if latest is not None else None
            )
            if (
                detail is None
                or detail["status"] != "CONVERGED"
                or detail["judgment"] is None
                or not detail["judgment"]["passed"]
            ):
                raise StageOrderError(stage_index, previous_index)

    def _stage_lock(self, plan_id: str, stage_index: int) -> asyncio.Lock:
        key = (plan_id, stage_index)
        lock = self._stage_locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._stage_locks[key] = lock
        return lock

    async def _create_and_execute(
        self,
        *,
        plan: PlanRecord,
        stage_index: int,
        stage: StageSpec,
        attempt: int,
        concurrency: int,
        origin: str,
        retry_of_round_id: str | None,
    ) -> dict[str, Any]:
        round_record = self.store.create_round(
            plan_id=plan.id,
            stage_index=stage_index,
            attempt=attempt,
            concurrency=concurrency,
            duration_seconds=stage.duration_seconds,
            origin=origin,
            retry_of_round_id=retry_of_round_id,
        )
        return await self._run_existing_round(plan, round_record)

    async def _run_existing_round(
        self, plan: PlanRecord, round_record: RoundRecord
    ) -> dict[str, Any]:
        try:
            self.store.start_round(round_record.id)
        except RoundActive:
            raise
        try:
            output = await self.sampler.sample(plan, round_record)
            aggregate = aggregate_samples(plan, output)
            judgment = evaluate_round(plan, aggregate)
        except InsufficientSamples as exc:
            self.store.abort_round(round_record.id, f"insufficient_samples:{exc.message}")
            raise
        except asyncio.CancelledError:
            self.store.abort_round(round_record.id, "sampling_cancelled")
            raise
        except Exception as exc:
            if isinstance(exc, WorkbenchError):
                reason = f"sampling_failed:{exc.code}"
            else:
                reason = f"sampling_failed:{exc.__class__.__name__}"
            try:
                self.store.abort_round(round_record.id, reason)
            except Exception:
                pass
            raise

        try:
            return self.store.persist_round_result(
                round_id=round_record.id,
                samples=[sample.as_row() for sample in output.samples],
                aggregate=aggregate,
                judgment=judgment,
            )
        except Exception:
            try:
                self.store.abort_round(round_record.id, "result_commit_failed")
            except Exception:
                pass
            raise

    async def _wait_for_terminal(self, round_id: str) -> dict[str, Any]:
        while True:
            detail = self.store.get_round_detail(round_id)
            if detail["status"] in {"CONVERGED", "ABORTED"}:
                return detail
            await asyncio.sleep(0.05)

