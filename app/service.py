from __future__ import annotations

import copy
import hashlib
import json
import uuid
from pathlib import Path
from typing import Any, Callable

from app.compare import BatchComputation, compute_batch, compute_run_quantiles
from app.config import PRECISION_TOLERANCE, SAMPLES_DIR
from app.errors import (
    BatchNotFoundError,
    InsufficientSamplesError,
    PrecisionDriftError,
    SpecInvalidError,
    TraceNotFoundError,
)
from app.fixedpoint import QuantileValue
from app.scheduler import ScheduleResult, schedule_trace
from app.spec import NormalizedTrace, parse_trace_payload
from app.store import Store
from app.units import format_bytes, format_fixed_ms, round_half_up_div, us_to_ms_x100

SAMPLE_FILES = {
    "normal": {
        "trace": "trace_normal.json",
        "description": "正常批次组织：5 个批次、优先级隔离、延迟上限拆分与真值核验",
        "kind": "trace",
    },
    "fragment": {
        "trace": "trace_fragment.json",
        "description": "碎片降批：连续区域不足时拆分样本并保留位图变化，同时记录混批延迟拒绝",
        "kind": "trace",
    },
    "precision-drift": {
        "trace": "trace_precision_drift.json",
        "description": "1us 真值经 0.01ms 输出量化后越过相对偏差上界，返回 E_PRECISION_DRIFT",
        "kind": "trace",
    },
    "dimensions-invalid": {
        "trace": "dimensions_invalid.json",
        "description": "量纲异常记录：负数、未知后缀与 64 位溢出，整份提交拒绝",
        "kind": "invalid",
    },
}


def _new_run_id() -> str:
    return f"run-{uuid.uuid4().hex}"


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _canonical_digest(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _decode_json(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return default
    return value


def _quantile_view(quantile: QuantileValue | dict[str, Any]) -> dict[str, Any]:
    if isinstance(quantile, QuantileValue):
        raw = quantile.raw_us
        output = quantile.output_ms_x100
        data = quantile.as_dict()
    else:
        raw = int(quantile["raw_us"])
        output = int(quantile["output_ms_x100"])
        data = dict(quantile)
        data.setdefault("method", "nearest_rank")
        data.setdefault("output_unit", "0.01ms")
        data["rounding_bit"] = int(data["rounding_error_us"]) != 0
    data["latency_ms"] = format_fixed_ms(output)
    data["raw_latency_ms"] = format_fixed_ms(us_to_ms_x100(raw))
    return data


class WorkbenchService:
    def __init__(
        self,
        store: Store,
        *,
        allow_crash: bool = False,
        terminate: Callable[[str], None] | None = None,
    ) -> None:
        self.store = store
        self.allow_crash = allow_crash
        self.terminate = terminate

    def health(self) -> dict[str, Any]:
        return {
            "status": "ok",
            "bind_scope": "loopback-only deployment",
            "database": str(self.store.path),
            "intent_stats": self.store.intent_stats(),
            "precision_tolerance": PRECISION_TOLERANCE,
        }

    def submit_trace(self, payload: Any) -> dict[str, Any]:
        trace = parse_trace_payload(payload)
        saved = self.store.submit_trace(trace)
        return {
            "inserted": saved["inserted"],
            "trace_id": trace.trace_id,
            "payload_digest": trace.payload_digest,
            "request_count": len(trace.requests),
            "model_count": len(trace.models),
            "normalized": trace.as_dict(),
            "persisted_at": saved["trace"]["created_at"],
        }

    def get_trace(self, trace_id: str) -> dict[str, Any]:
        trace = self.store.get_trace(trace_id)
        if trace is None:
            raise TraceNotFoundError("trace not found", details={"trace_id": trace_id})
        return trace

    def _load_normalized_trace(self, trace_id: str) -> NormalizedTrace:
        trace_row = self.store.get_trace(trace_id)
        if trace_row is None:
            raise TraceNotFoundError("trace not found", details={"trace_id": trace_id})
        return parse_trace_payload(trace_row["normalized"])

    def organize(
        self,
        trace_id: str,
        *,
        crash_at: str | None = None,
    ) -> dict[str, Any]:
        if crash_at not in (None, "mid_quantiles"):
            raise SpecInvalidError(
                "unsupported crash checkpoint",
                details={"crash_at": crash_at, "allowed": ["mid_quantiles"]},
            )
        if crash_at and not self.allow_crash:
            raise SpecInvalidError(
                "demo crash injection is disabled",
                details={"hint": "start the server with WORKBENCH_ALLOW_CRASH=1"},
            )

        trace = self._load_normalized_trace(trace_id)
        run_id = _new_run_id()
        schedule = schedule_trace(trace)
        intent_id = self.store.create_run_intent(
            trace_id=trace.trace_id, run_id=run_id, crash_at=crash_at
        )
        persisted = False
        try:
            self.store.persist_run_skeleton(
                intent_id=intent_id,
                run_id=run_id,
                trace=trace,
                schedule=schedule,
            )
            persisted = True
            if crash_at == "mid_quantiles":
                if self.terminate is None:
                    raise RuntimeError("no termination callback configured")
                self.terminate("mid_quantiles")
                raise RuntimeError("termination callback returned unexpectedly")

            computations = [compute_batch(batch) for batch in schedule.batches]
            run_quantiles = compute_run_quantiles(schedule.batches)
            deterministic_digest = self._deterministic_result_digest(
                schedule, computations, run_quantiles
            )
            commit = self.store.finalize_run(
                intent_id=intent_id,
                run_id=run_id,
                computations=computations,
                run_quantiles=run_quantiles,
                deterministic_digest=deterministic_digest,
            )
            response = self.get_run(run_id)
            if commit["error_code"] == "E_PRECISION_DRIFT":
                failed = [
                    {
                        "batch_uid": f"{run_id}:{item.batch.batch_id}",
                        "batch_id": item.batch.batch_id,
                        "failed_fields": item.failed_fields,
                    }
                    for item in computations
                    if not item.passed
                ]
                raise PrecisionDriftError(
                    "one or more batches exceeded the declared precision tolerance",
                    details={
                        "run_id": run_id,
                        "failed_batches": failed,
                        "tolerance": PRECISION_TOLERANCE,
                        "persisted": True,
                    },
                )
            return response
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            self.store.fail_intent(intent_id, exc, run_id=run_id if persisted else None)
            raise

    @staticmethod
    def _deterministic_result_digest(
        schedule: ScheduleResult,
        computations: list[BatchComputation],
        run_quantiles: list[QuantileValue],
    ) -> str:
        payload = {
            "algorithm_version": schedule.algorithm_version,
            "schedule_digest": schedule.deterministic_digest,
            "batches": [
                {
                    "ordinal": computation.batch.ordinal,
                    "memory_bytes": computation.batch.memory_bytes,
                    "latency_us": computation.batch.latency_us,
                    "quantiles": [
                        {
                            "label": quantile.label,
                            "raw_us": quantile.raw_us,
                            "output_ms_x100": quantile.output_ms_x100,
                            "rank": quantile.rank,
                            "sample_count": quantile.sample_count,
                        }
                        for quantile in computation.quantiles
                    ],
                    "checks": [
                        {
                            "scope": check.scope,
                            "subject_id": check.subject_id,
                            "metric": check.metric,
                            "truth": check.truth,
                            "estimate": check.estimate,
                            "absolute_deviation": check.absolute_deviation,
                            "relative_deviation_ppm": check.relative_deviation_ppm,
                            "passed": check.passed,
                        }
                        for check in computation.checks
                    ],
                }
                for computation in computations
            ],
            "run_quantiles": [
                {
                    "label": quantile.label,
                    "raw_us": quantile.raw_us,
                    "rank": quantile.rank,
                    "sample_count": quantile.sample_count,
                }
                for quantile in run_quantiles
            ],
        }
        return _canonical_digest(payload)

    def get_run(self, run_id: str) -> dict[str, Any]:
        run = self.store.require_run(run_id)
        return self._run_view(run)

    def _run_view(self, run: dict[str, Any]) -> dict[str, Any]:
        batches = []
        for batch in run["batches"]:
            batches.append(
                {
                    "batch_uid": batch["batch_uid"],
                    "batch_id": batch["batch_id"],
                    "ordinal": batch["ordinal"],
                    "state": batch["state"],
                    "model_id": batch["model_id"],
                    "priority": batch["priority"],
                    "memory_bytes": batch["memory_bytes"],
                    "memory_display": format_bytes(batch["memory_bytes"]),
                    "latency_us": batch["latency_us"],
                    "latency_ms": format_fixed_ms(us_to_ms_x100(batch["latency_us"])),
                    "sample_count": batch["sample_count"],
                    "member_count": batch["member_count"],
                    "first_arrival_us": batch["first_arrival_us"],
                    "last_arrival_us": batch["last_arrival_us"],
                    "timeline_start_us": batch["timeline_start_us"],
                    "timeline_end_us": batch["timeline_end_us"],
                    "timeline_start_ms": format_fixed_ms(us_to_ms_x100(batch["timeline_start_us"])),
                    "timeline_end_ms": format_fixed_ms(us_to_ms_x100(batch["timeline_end_us"])),
                    "region_start_bytes": batch["region_start"],
                    "region_size_bytes": batch["region_size"],
                    "split_reasons": _decode_json(batch["split_reasons_json"], []),
                    "split_notes": _decode_json(batch["split_notes_json"], []),
                    "bitmap": _decode_json(batch["bitmap_json"], {}),
                    "validation_state": batch["validation_state"],
                    "failed_fields": _decode_json(batch["failed_fields_json"], []),
                    "fragmentation_event_id": batch["fragmentation_event_id"],
                }
            )
        schedule = run["schedule"]
        return {
            "run_id": run["run_id"],
            "trace_id": run["trace_id"],
            "state": run["state"],
            "error_code": run["error_code"],
            "algorithm_version": run["algorithm_version"],
            "schedule_digest": run["schedule_digest"],
            "deterministic_digest": run["deterministic_digest"],
            "created_at": run["created_at"],
            "committed_at": run["committed_at"],
            "node": run["node"],
            "memory_plan": {
                "node_total_bytes": schedule["node_total_bytes"],
                "node_total_display": format_bytes(schedule["node_total_bytes"]),
                "node_initial_free_bytes": schedule["node_initial_free_bytes"],
                "node_initial_free_display": format_bytes(schedule["node_initial_free_bytes"]),
                "node_final_free_bytes": schedule["node_final_free_bytes"],
                "node_final_free_display": format_bytes(schedule["node_final_free_bytes"]),
                "initial_bitmap": schedule["initial_bitmap"],
                "final_bitmap": schedule["final_bitmap"],
                "initial_arena": schedule["initial_arena"],
                "final_arena": schedule["final_arena"],
                "fragmentation_events": schedule["fragmentation_events"],
                "mixed_rejections": schedule["mixed_rejections"],
            },
            "quantiles": [_quantile_view(item) for item in run["quantiles"]],
            "batches": batches,
            "precision_tolerance": PRECISION_TOLERANCE,
        }

    def list_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        return self.store.list_runs(limit)

    def _require_batch(self, batch_uid: str) -> dict[str, Any]:
        bundle = self.store.get_batch_bundle(batch_uid)
        if bundle is None:
            raise BatchNotFoundError("batch not found", details={"batch_uid": batch_uid})
        return bundle

    def batch_metrics(self, batch_uid: str) -> dict[str, Any]:
        batch = self._require_batch(batch_uid)
        if batch["state"] == "ABORTED" or not batch["quantiles"]:
            raise InsufficientSamplesError(
                "batch has no committed quantile observations",
                details={
                    "batch_uid": batch_uid,
                    "state": batch["state"],
                    "sample_count": 0,
                    "method": "nearest_rank",
                },
            )
        quantiles = [_quantile_view(item) for item in batch["quantiles"]]
        return {
            "batch_uid": batch["batch_uid"],
            "batch_id": batch["batch_id"],
            "run_id": batch["run_id"],
            "state": batch["state"],
            "validation_state": batch["validation_state"],
            "model_id": batch["model_id"],
            "priority": batch["priority"],
            "memory_budget": {
                "batch_memory_bytes": batch["memory_bytes"],
                "batch_memory_display": format_bytes(batch["memory_bytes"]),
                "region_start_bytes": batch["region_start"],
                "region_end_bytes": batch["region_start"] + batch["region_size"],
                "bitmap": batch["bitmap_json"],
                "per_member": [
                    {
                        "request_id": member["request_id"],
                        "samples": member["samples"],
                        "part_index": member["part_index"],
                        "part_count": member["part_count"],
                        "member_memory_bytes": member["memory_bytes"],
                        "member_memory_display": format_bytes(member["memory_bytes"]),
                    }
                    for member in batch["members"]
                ],
            },
            "latency": {
                "batch_raw_us": batch["latency_us"],
                "batch_ms": format_fixed_ms(us_to_ms_x100(batch["latency_us"])),
                "quantile_sample_count": batch["sample_count"],
                "member_observation_count": batch["member_count"],
                "observation_contract": "each batch member contributes samples * declared unit cost",
                "quantiles": quantiles,
            },
            "rounding": batch["rounding_json"],
            "fixedpoint_trace": batch["engine_trace_json"],
            "members": batch["members"],
        }

    def batch_errors(self, batch_uid: str) -> dict[str, Any]:
        batch = self._require_batch(batch_uid)
        return {
            "batch_uid": batch["batch_uid"],
            "batch_id": batch["batch_id"],
            "run_id": batch["run_id"],
            "state": batch["state"],
            "validation_state": batch["validation_state"],
            "failed_fields": batch["failed_fields_json"] or [],
            "error_chain": batch["error_chain_json"],
            "rounding": batch["rounding_json"],
            "precision_checks": batch["checks"],
            "explanation": (
                "Fixed-point products and compensated accumulation are integer-exact. "
                "The only numeric residue is the declared half-up conversion from microseconds "
                "to 0.01ms; it is bounded at 5us per emitted value."
                if batch["error_chain_json"]
                else "The batch was interrupted before its quantile transaction committed. "
                "No error chain is fabricated; inspect the recovery report."
            ),
        }

    def batch_validation(self, batch_uid: str) -> dict[str, Any]:
        batch = self._require_batch(batch_uid)
        failed = [check for check in batch["checks"] if not check["passed"]]
        if batch["state"] == "ABORTED":
            conclusion = "NOT_AVAILABLE"
        elif failed:
            conclusion = "FAIL"
        else:
            conclusion = "PASS"
        return {
            "batch_uid": batch_uid,
            "batch_id": batch["batch_id"],
            "state": batch["state"],
            "conclusion": conclusion,
            "tolerance": PRECISION_TOLERANCE,
            "checks": batch["checks"],
            "failed_count": len(failed),
        }

    def run_validation(self, run_id: str) -> dict[str, Any]:
        run = self.store.require_run(run_id)
        checks: list[dict[str, Any]] = []
        for batch in run["batches"]:
            bundle = self.store.get_batch_bundle(batch["batch_uid"])
            if bundle is not None:
                checks.extend(bundle["checks"])
        failed = [check for check in checks if not check["passed"]]
        passed_count = len(checks) - len(failed)
        hit_rate_ppm = (
            round_half_up_div(passed_count * 1_000_000, len(checks)) if checks else 0
        )
        if run["state"] == "ABORTED" or not checks:
            conclusion = "NOT_AVAILABLE"
        elif failed:
            conclusion = "FAIL"
        else:
            conclusion = "PASS"
        return {
            "run_id": run_id,
            "state": run["state"],
            "error_code": run["error_code"],
            "conclusion": conclusion,
            "deterministic_digest": run["deterministic_digest"],
            "tolerance": PRECISION_TOLERANCE,
            "check_count": len(checks),
            "passed_count": passed_count,
            "failed_count": len(failed),
            "precision_hit_rate_ppm": hit_rate_ppm,
            "precision_hit_rate": f"{hit_rate_ppm / 10_000:.4f}%",
            "failed_fields": [
                {
                    "batch_uid": check["batch_uid"],
                    "scope": check["scope"],
                    "subject_id": check["subject_id"],
                    "metric": check["metric"],
                    "absolute_deviation": check["absolute_deviation"],
                    "relative_deviation_ppm": check["relative_deviation_ppm"],
                }
                for check in failed
            ],
            "checks": checks,
        }

    def run_quantiles(self, run_id: str) -> dict[str, Any]:
        run = self.store.require_run(run_id)
        if not run["quantiles"]:
            raise InsufficientSamplesError(
                "run has no committed batch latency observations",
                details={
                    "run_id": run_id,
                    "state": run["state"],
                    "sample_count": 0,
                    "method": "nearest_rank",
                },
            )
        return {
            "run_id": run_id,
            "state": run["state"],
            "sample_count": len(run["batches"]),
            "method": "nearest_rank",
            "observation_unit": "committed batch total latency",
            "quantiles": [_quantile_view(item) for item in run["quantiles"]],
        }

    def recovery_report(self) -> dict[str, Any]:
        report = self.store.latest_recovery_report()
        if report is None:
            self.store.recover_pending_intents()
            report = self.store.latest_recovery_report()
        if report is None:
            raise InsufficientSamplesError("recovery scan did not persist a report")
        return report["body"]

    def list_samples(self) -> list[dict[str, Any]]:
        result = []
        for name, meta in SAMPLE_FILES.items():
            path = SAMPLES_DIR / meta["trace"]
            result.append(
                {
                    "name": name,
                    "kind": meta["kind"],
                    "description": meta["description"],
                    "file": str(path.relative_to(SAMPLES_DIR.parent)),
                    "available": path.exists(),
                }
            )
        return result

    def sample_payload(self, name: str) -> dict[str, Any]:
        meta = SAMPLE_FILES.get(name)
        if meta is None:
            raise SpecInvalidError(
                "fixed sample not found",
                details={"sample": name, "available": sorted(SAMPLE_FILES)},
            )
        path = SAMPLES_DIR / meta["trace"]
        document = _read_json(path)
        if meta["kind"] == "invalid":
            return {
                "name": name,
                "kind": "invalid",
                "description": meta["description"],
                "cases": document["cases"],
                "payload": document["reject_payload"],
            }
        catalog = _read_json(SAMPLES_DIR / "models.json")
        node = document.get("node", catalog.get("node"))
        combined = {
            "schema_version": catalog.get("schema_version", "1"),
            "models": copy.deepcopy(catalog["models"]),
            "node": copy.deepcopy(node),
            "trace": copy.deepcopy(document),
            "sample_name": name,
            "sample_description": meta["description"],
        }
        combined["trace"].pop("node", None)
        return {
            "name": name,
            "kind": "trace",
            "description": meta["description"],
            "payload": combined,
        }

    def intent_log(self, limit: int = 100) -> list[dict[str, Any]]:
        return self.store.intent_log(limit)
