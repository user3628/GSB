from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal, localcontext
from pathlib import Path
from typing import Any, Callable, Iterator

from .db import Database
from .errors import (
    BusinessError,
    conflicting_round,
    progress_out_of_range,
    report_invalid,
    version_mismatch,
)
from .models import (
    NORMALIZATION_MODES,
    ReplicaGroup,
    format_timestamp,
    parse_aggregate_request,
    parse_group,
    parse_report_batch,
    parse_round,
    parse_timestamp,
)
from .numerics import (
    DECIMAL_PRECISION,
    PROGRESS_SCALE,
    ROUNDING_MODE,
    WEIGHT_SCALE,
    ZERO,
    compensated_sum_diagnostics,
    convergence_decision,
    decimal_string,
    digest_payload,
    fixed_string,
    normalize_weights,
    quantize_decimal,
)


NowProvider = Callable[[], datetime]
SleepProvider = Callable[[float], None]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class RepairService:
    """Coordinator for report ingestion, deterministic aggregation and recovery."""

    def __init__(
        self,
        db_path: str | Path,
        *,
        now_fn: NowProvider = utc_now,
        sleep_fn: SleepProvider = time.sleep,
        aggregation_delay_ms: int = 0,
        auto_recover: bool = True,
    ) -> None:
        self.db = Database(db_path)
        self.now_fn = now_fn
        self.sleep_fn = sleep_fn
        self.aggregation_delay_ms = max(0, int(aggregation_delay_ms))
        self._locks_guard = threading.Lock()
        self._group_locks: dict[str, threading.RLock] = {}
        self.startup_recovery_report: dict[str, Any] | None = None
        if auto_recover:
            self.startup_recovery_report = self.db.recover_incomplete_intents(
                now=self.now_fn()
            )

    @contextmanager
    def _group_lock(self, group_id: str) -> Iterator[None]:
        with self._locks_guard:
            lock = self._group_locks.setdefault(group_id, threading.RLock())
        with lock:
            yield

    def health(self) -> dict[str, Any]:
        return {
            "ok": True,
            "code": "OK",
            "service": "replica-repair-domain",
            "db_path": self.db.path,
            "now": format_timestamp(self.now_fn()),
        }

    def create_group(self, payload: Any) -> dict[str, Any]:
        group = parse_group(payload)
        stored, reused = self.db.insert_group(group, now=self.now_fn())
        return {
            "ok": True,
            "code": "OK",
            "reused": reused,
            "group": stored.as_dict(),
        }

    def get_group(self, group_id: str) -> dict[str, Any]:
        group = self.db.get_group(group_id)
        return {"ok": True, "code": "OK", "group": group.as_dict()}

    def create_round(self, payload: Any) -> dict[str, Any]:
        spec = parse_round(payload)
        with self._group_lock(spec.group_id):
            round_row = self.db.create_round(spec, now=self.now_fn())
        return {
            "ok": True,
            "code": "OK",
            "round": round_row,
            "adopted_late_report_ids": round_row.get("adopted_late_report_ids", []),
        }

    def submit_reports(self, round_id: str, payload: Any) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise report_invalid("上报请求必须是 JSON 对象")
        round_row = self.db.get_round(round_id)
        group_id = payload.get("group_id")
        if group_id != round_row["group_id"]:
            raise report_invalid(
                "上报请求的 group_id 与轮次不一致",
                {
                    "round_group_id": round_row["group_id"],
                    "request_group_id": group_id,
                },
            )
        body_version = payload.get("group_version")
        if (
            isinstance(body_version, bool)
            or not isinstance(body_version, int)
            or body_version != round_row["group_version"]
        ):
            raise version_mismatch(
                "上报请求的 group_version 与轮次快照不一致",
                {
                    "round_group_version": round_row["group_version"],
                    "request_group_version": body_version,
                },
            )

        group = self.db.get_group(round_row["group_id"])
        if group.version != round_row["group_version"]:
            raise version_mismatch(
                "副本组当前版本已偏离轮次快照",
                {
                    "group_id": group.group_id,
                    "current_version": group.version,
                    "round_group_version": round_row["group_version"],
                },
            )
        reports = parse_report_batch(payload, group=group)
        with self._group_lock(group.group_id):
            result = self.db.accept_reports(
                round_row,
                group,
                reports,
                now=self.now_fn(),
            )
        result["ok"] = True
        result["code"] = "OK_LATE_QUEUED" if result["late_report_ids"] else "OK"
        result["http_status"] = 202 if result["late_report_ids"] else 200
        return result

    def get_round_status(self, round_id: str) -> dict[str, Any]:
        return {
            "ok": True,
            "code": "OK",
            "round": self.db.round_status_payload(round_id),
        }

    def _existing_conclusion(self, round_id: str) -> dict[str, Any] | None:
        return self.db.get_conclusion(round_id)

    def _assert_reusable(
        self,
        existing: dict[str, Any],
        *,
        normalization: str,
        round_id: str,
    ) -> None:
        committed_mode = existing["conclusion"]["normalization"]["mode"]
        if committed_mode != normalization:
            raise conflicting_round(
                "同一轮次已使用不同归一化方案提交结论",
                {
                    "round_id": round_id,
                    "committed_normalization": committed_mode,
                    "requested_normalization": normalization,
                },
            )

    def _strict_wait(
        self,
        round_row: dict[str, Any],
        request: dict[str, Any],
    ) -> dict[str, Any]:
        started = self.now_fn()
        sources: list[str] = []
        if not request["strict_wait"] or round_row["sealed_at"] is not None:
            return {
                "strict_wait": request["strict_wait"],
                "requested_grace_ms": request["grace_ms"],
                "max_wait_ms": request["max_wait_ms"],
                "delay_sources": sources,
                "delay_ms": 0,
                "wait_started_at": format_timestamp(started),
                "wait_finished_at": format_timestamp(started),
            }

        deadline = parse_timestamp(
            round_row["deadline"], field="deadline", code="E_ROUND_INVALID"
        )
        current = self.now_fn()
        planned_ms = 0
        if current < deadline:
            planned_ms = max(0, int((deadline - current).total_seconds() * 1000))
        if request["grace_ms"] > 0:
            eligible = self.db.eligible_reports(round_row["round_id"])
            present = {row["replica_id"] for row in eligible}
            group = self.db.get_group(round_row["group_id"])
            missing = [replica for replica in group.weights if replica not in present]
            if missing:
                planned_ms += request["grace_ms"]
        if planned_ms > request["max_wait_ms"]:
            raise BusinessError(
                "E_WAIT_TOO_LONG",
                "严格等待时长超过请求允许的 max_wait_ms",
                http_status=409,
                details={
                    "planned_wait_ms": planned_ms,
                    "max_wait_ms": request["max_wait_ms"],
                },
            )

        current = self.now_fn()
        if current < deadline:
            seconds = (deadline - current).total_seconds()
            if seconds > 0:
                self.sleep_fn(seconds)
                sources.append("WAIT_FOR_DEADLINE")

        if request["grace_ms"] > 0:
            eligible = self.db.eligible_reports(round_row["round_id"])
            present = {row["replica_id"] for row in eligible}
            group = self.db.get_group(round_row["group_id"])
            missing = [replica for replica in group.weights if replica not in present]
            if missing:
                self.sleep_fn(request["grace_ms"] / 1000.0)
                sources.append("WAIT_FOR_MISSING_REPLICAS")

        finished = self.now_fn()
        delay_ms = max(0, int((finished - started).total_seconds() * 1000))
        return {
            "strict_wait": True,
            "requested_grace_ms": request["grace_ms"],
            "max_wait_ms": request["max_wait_ms"],
            "delay_sources": sources,
            "delay_ms": delay_ms,
            "wait_started_at": format_timestamp(started),
            "wait_finished_at": format_timestamp(finished),
        }

    def _fingerprint(
        self,
        round_row: dict[str, Any],
        group: ReplicaGroup,
        normalization: str,
        eligible_reports: list[Any],
        late_rows: list[Any],
    ) -> str:
        payload = {
            "schema": "replica-repair-aggregation-v1",
            "round": {
                "round_id": round_row["round_id"],
                "group_id": round_row["group_id"],
                "group_version": round_row["group_version"],
                "deadline": round_row["deadline"],
                "target_progress_raw": round_row["target_progress_raw"],
                "target_progress": round_row["target_progress"],
                "relative_tolerance": round_row["relative_tolerance"],
            },
            "group": {
                "group_id": group.group_id,
                "version": group.version,
                "replica_count": group.replica_count,
                "block_size_bytes": group.block_size_bytes,
                "weights": {
                    replica_id: str(weight)
                    for replica_id, weight in sorted(group.weights.items())
                },
            },
            "normalization": normalization,
            "eligible_reports": sorted(
                (
                    {
                        "report_id": row["report_id"],
                        "replica_id": row["replica_id"],
                        "digest": row["report_digest"],
                        "source_round_id": row["submission_round_id"],
                        "late": bool(row["late"]),
                    }
                    for row in eligible_reports
                ),
                key=lambda item: (item["replica_id"], item["report_id"]),
            ),
            "late_reports_at_seal": sorted(
                (
                    {
                        "report_id": row["report_id"],
                        "replica_id": row["replica_id"],
                        "digest": row["report_digest"],
                        "reason": row["late_reason"],
                    }
                    for row in late_rows
                ),
                key=lambda item: (item["replica_id"], item["report_id"]),
            ),
        }
        return digest_payload(payload)

    def aggregate(self, round_id: str, payload: Any) -> dict[str, Any]:
        request = parse_aggregate_request(payload)
        round_row = self.db.get_round(round_id)
        group = self.db.get_group(round_row["group_id"])
        if group.version != round_row["group_version"]:
            raise version_mismatch(
                "副本组当前版本已偏离轮次快照",
                {
                    "group_id": group.group_id,
                    "current_version": group.version,
                    "round_group_version": round_row["group_version"],
                },
            )

        existing = self._existing_conclusion(round_id)
        if existing is not None:
            self._assert_reusable(
                existing,
                normalization=request["normalization"],
                round_id=round_id,
            )
            return self._aggregate_response(existing, reused=True)

        timing = self._strict_wait(round_row, request)

        with self._group_lock(round_row["group_id"]):
            existing = self._existing_conclusion(round_id)
            if existing is not None:
                self._assert_reusable(
                    existing,
                    normalization=request["normalization"],
                    round_id=round_id,
                )
                return self._aggregate_response(existing, reused=True)

            current_round = self.db.get_round(round_id)
            eligible_reports = self.db.eligible_reports(round_id)
            all_late_rows = self.db.late_reports_for_submission(round_id)
            late_rows: list[Any] = []
            if all_late_rows:
                sealed_upper_bound = current_round["sealed_at"]
                if sealed_upper_bound is None:
                    sealed_upper_bound = format_timestamp(self.now_fn())
                late_rows = [
                    row
                    for row in all_late_rows
                    if row["received_at"] <= sealed_upper_bound
                ]

            fingerprint = self._fingerprint(
                current_round,
                group,
                request["normalization"],
                eligible_reports,
                late_rows,
            )
            intent = self.db.get_intent(round_id)
            if intent is not None and intent["input_fingerprint"] != fingerprint:
                raise conflicting_round(
                    "同一轮次推导出的聚合输入与既有意图不一致",
                    {
                        "round_id": round_id,
                        "existing_fingerprint": intent["input_fingerprint"],
                        "requested_fingerprint": fingerprint,
                    },
                )

            intent_result = self.db.begin_intent(
                current_round,
                fingerprint=fingerprint,
                now=self.now_fn(),
            )
            if self.aggregation_delay_ms:
                # Deliberate pre-commit pause used by the SIGKILL recovery demo.
                self.sleep_fn(self.aggregation_delay_ms / 1000.0)

            try:
                conclusion = self._build_conclusion(
                    round_row=current_round,
                    group=group,
                    normalization=request["normalization"],
                    eligible_reports=eligible_reports,
                    late_rows=late_rows,
                    fingerprint=fingerprint,
                    attempt=intent_result["attempt"],
                    sealed_at=intent_result["sealed_at"],
                    closed_early=intent_result["closed_early"],
                    timing=timing,
                )
            except BusinessError as exc:
                self.db.abort_intent(round_id, now=self.now_fn(), error=exc)
                raise
            except Exception as exc:
                failure = BusinessError(
                    "E_AGGREGATION_FAILED",
                    f"聚合失败: {exc}",
                    http_status=500,
                )
                self.db.abort_intent(round_id, now=self.now_fn(), error=failure)
                raise

            committed = self.db.commit_conclusion(
                round_id,
                group_id=group.group_id,
                group_version=group.version,
                fingerprint=fingerprint,
                conclusion=conclusion,
                now=self.now_fn(),
                late_count=len(late_rows),
            )
            if committed["reused"]:
                self._assert_reusable(
                    committed,
                    normalization=request["normalization"],
                    round_id=round_id,
                )
            return {
                "ok": True,
                "code": "OK",
                "reused": bool(committed["reused"]),
                "round_id": round_id,
                "result_version": committed["result_version"],
                "conclusion_digest": committed["conclusion_digest"],
                "conclusion": committed["conclusion"],
            }

    def _aggregate_response(
        self,
        existing: dict[str, Any],
        *,
        reused: bool,
    ) -> dict[str, Any]:
        return {
            "ok": True,
            "code": "OK",
            "reused": reused,
            "round_id": existing["conclusion"]["round"]["round_id"],
            "result_version": existing["result_version"],
            "conclusion_digest": existing["conclusion_digest"],
            "conclusion": existing["conclusion"],
        }

    def _build_conclusion(
        self,
        *,
        round_row: dict[str, Any],
        group: ReplicaGroup,
        normalization: str,
        eligible_reports: list[Any],
        late_rows: list[Any],
        fingerprint: str,
        attempt: int,
        sealed_at: str,
        closed_early: bool,
        timing: dict[str, Any],
    ) -> dict[str, Any]:
        if normalization not in NORMALIZATION_MODES:
            raise BusinessError(
                "E_NORMALIZATION_UNKNOWN",
                f"未知归一化方案: {normalization!r}",
                http_status=400,
            )

        present = sorted(row["replica_id"] for row in eligible_reports)
        weights = normalize_weights(group.weights, present, normalization)
        report_by_replica = {row["replica_id"]: row for row in eligible_reports}
        normalized_decimal = weights["normalized_weight_decimal"]

        with localcontext() as ctx:
            ctx.prec = DECIMAL_PRECISION
            block_terms = [
                (
                    replica_id,
                    normalized_decimal[replica_id]
                    * Decimal(int(report_by_replica[replica_id]["block_count"])),
                )
                for replica_id in sorted(report_by_replica)
            ]
            synced_terms = [
                (
                    replica_id,
                    normalized_decimal[replica_id]
                    * Decimal(int(report_by_replica[replica_id]["synced_bytes"])),
                )
                for replica_id in sorted(report_by_replica)
            ]
            weighted_blocks, block_diagnostics = compensated_sum_diagnostics(
                block_terms, label="weighted_block_count"
            )
            weighted_synced, synced_diagnostics = compensated_sum_diagnostics(
                synced_terms, label="weighted_synced_bytes"
            )

            estimated_total_raw = weighted_blocks * Decimal(group.block_size_bytes)
            estimated_total, estimated_total_error = quantize_decimal(
                estimated_total_raw, 0
            )
            estimated_synced, estimated_synced_error = quantize_decimal(
                weighted_synced, 0
            )
            pending_repair_bytes = estimated_total - estimated_synced

            if estimated_total == 0:
                if estimated_synced == 0:
                    progress_raw = ZERO
                else:
                    raise progress_out_of_range(
                        "组级估算字节为 0 但已同步字节为正，进度无定义",
                        {
                            "estimated_total_bytes": int(estimated_total),
                            "estimated_synced_bytes": int(estimated_synced),
                            "rule": "NO_NAN_NO_SILENT_CLAMP",
                        },
                    )
            else:
                progress_raw = estimated_synced / estimated_total
            if progress_raw < 0 or progress_raw > 1:
                raise progress_out_of_range(
                    "进度比例超出 [0,1]，拒绝截断或 NaN",
                    {
                        "progress_raw": decimal_string(progress_raw),
                        "estimated_total_bytes": int(estimated_total),
                        "estimated_synced_bytes": int(estimated_synced),
                        "pending_repair_bytes": int(pending_repair_bytes),
                        "rule": "REJECT_OUT_OF_RANGE",
                    },
                )
            progress, progress_error = quantize_decimal(
                progress_raw, PROGRESS_SCALE
            )
            target_raw = Decimal(round_row["target_progress_raw"])
            target = Decimal(round_row["target_progress"])
            target_rounding_error = Decimal(round_row["target_rounding_error"])
            tolerance = Decimal(round_row["relative_tolerance"])

            canonical_order = [
                f"{row['replica_id']}:{row['report_id']}"
                for row in sorted(
                    eligible_reports,
                    key=lambda item: (item["replica_id"], item["report_id"]),
                )
            ]
            decision = convergence_decision(
                progress,
                target,
                tolerance,
                canonical_order=canonical_order,
            )

            normalization_error = Decimal(
                weights["rounding"]["cumulative_error"]
            )
            if estimated_total_raw == 0:
                byte_relative_error = (
                    ZERO
                    if estimated_synced_error == 0
                    else Decimal(1)
                )
            else:
                byte_relative_error = (
                    abs(estimated_total_error) + abs(estimated_synced_error)
                ) / abs(estimated_total_raw)
            progress_relative_error = abs(progress_error)
            cumulative_relative_error = (
                abs(normalization_error)
                + byte_relative_error
                + abs(target_rounding_error)
                + progress_relative_error
            )

            eligible_output = [
                {
                    "report_id": row["report_id"],
                    "replica_id": row["replica_id"],
                    "source_round_id": row["submission_round_id"],
                    "reported_at": row["reported_at"],
                    "received_at": row["received_at"],
                    "block_count": int(row["block_count"]),
                    "synced_bytes": int(row["synced_bytes"]),
                    "late": bool(row["late"]),
                    "late_reason": row["late_reason"],
                    "digest": row["report_digest"],
                }
                for row in sorted(
                    eligible_reports,
                    key=lambda item: (item["replica_id"], item["report_id"]),
                )
            ]
            late_output = [
                {
                    "report_id": row["report_id"],
                    "replica_id": row["replica_id"],
                    "reported_at": row["reported_at"],
                    "received_at": row["received_at"],
                    "late_reason": row["late_reason"],
                    "digest": row["report_digest"],
                }
                for row in sorted(
                    late_rows,
                    key=lambda item: (item["replica_id"], item["report_id"]),
                )
            ]
            mixed_magnitude = _mixed_magnitude(block_terms) or _mixed_magnitude(
                synced_terms
            )

            conclusion = {
                "schema_version": "replica-repair-conclusion-v1",
                "computed_at": format_timestamp(self.now_fn()),
                "round": {
                    "round_id": round_row["round_id"],
                    "group_id": round_row["group_id"],
                    "group_version": round_row["group_version"],
                    "deadline": round_row["deadline"],
                    "sealed_at": sealed_at,
                    "closed_early": closed_early,
                    "target_progress_raw": decimal_string(target_raw),
                    "target_progress": fixed_string(target, PROGRESS_SCALE),
                    "relative_tolerance": decimal_string(tolerance),
                },
                "normalization": {
                    "mode": weights["mode"],
                    "denominator": weights["denominator"],
                    "present_replicas": weights["present_replicas"],
                    "missing_replicas": weights["missing_replicas"],
                    "raw_weights": weights["raw_weights"],
                    "normalized_weights": weights["normalized_weights"],
                    "present_weight_sum": weights["present_weight_sum"],
                    "target_present_sum": weights["target_present_sum"],
                    "rounding": weights["rounding"],
                },
                "reports": {
                    "eligible_count": len(eligible_output),
                    "eligible_reports": eligible_output,
                    "late_report_count_at_seal": len(late_output),
                    "late_reports_at_seal": late_output,
                    "participating_late_report_ids": [
                        item["report_id"]
                        for item in eligible_output
                        if item["late"]
                    ],
                },
                "estimates": {
                    "block_size_bytes": group.block_size_bytes,
                    "weighted_block_count_raw": decimal_string(weighted_blocks),
                    "estimated_total_bytes_raw": decimal_string(estimated_total_raw),
                    "estimated_total_bytes": int(estimated_total),
                    "estimated_synced_bytes_raw": decimal_string(weighted_synced),
                    "estimated_synced_bytes": int(estimated_synced),
                    "pending_repair_bytes": int(pending_repair_bytes),
                    "progress_raw": decimal_string(progress_raw),
                    "progress": fixed_string(progress, PROGRESS_SCALE),
                    "fixed_point": {
                        "bytes_scale": 0,
                        "progress_scale": PROGRESS_SCALE,
                        "bytes_representation": "integer bytes",
                        "progress_representation": f"fixed {PROGRESS_SCALE} decimal places",
                    },
                },
                "rounding": {
                    "mode": ROUNDING_MODE,
                    "steps": [
                        {
                            "step": "normalize_weights",
                            "mode": weights["rounding"]["mode"],
                            "scale": WEIGHT_SCALE,
                            "error": weights["rounding"]["cumulative_error"],
                        },
                        {
                            "step": "target_progress",
                            "mode": ROUNDING_MODE,
                            "scale": PROGRESS_SCALE,
                            "input": decimal_string(target_raw),
                            "output": fixed_string(target, PROGRESS_SCALE),
                            "error": decimal_string(target_rounding_error),
                        },
                        {
                            "step": "estimated_total_bytes",
                            "mode": ROUNDING_MODE,
                            "scale": 0,
                            "input": decimal_string(estimated_total_raw),
                            "output": str(int(estimated_total)),
                            "error": decimal_string(estimated_total_error),
                        },
                        {
                            "step": "estimated_synced_bytes",
                            "mode": ROUNDING_MODE,
                            "scale": 0,
                            "input": decimal_string(weighted_synced),
                            "output": str(int(estimated_synced)),
                            "error": decimal_string(estimated_synced_error),
                        },
                        {
                            "step": "pending_repair_bytes",
                            "mode": "SUBTRACT_AFTER_ROUNDING",
                            "scale": 0,
                            "input": (
                                f"{int(estimated_total)} - {int(estimated_synced)}"
                            ),
                            "output": str(int(pending_repair_bytes)),
                            "error": "0",
                        },
                        {
                            "step": "progress",
                            "mode": ROUNDING_MODE,
                            "scale": PROGRESS_SCALE,
                            "input": decimal_string(progress_raw),
                            "output": fixed_string(progress, PROGRESS_SCALE),
                            "error": decimal_string(progress_error),
                        },
                    ],
                    "cumulative_relative_error_bound": decimal_string(
                        cumulative_relative_error
                    ),
                    "cumulative_components": {
                        "normalization_abs_error": decimal_string(
                            abs(normalization_error)
                        ),
                        "byte_rounding_relative_error": decimal_string(
                            byte_relative_error
                        ),
                        "target_progress_rounding_error": decimal_string(
                            target_rounding_error
                        ),
                        "progress_rounding_error": decimal_string(progress_error),
                    },
                },
                "sum_diagnostics": {
                    "canonical_sum_method": (
                        "Decimal precision 80; report terms sorted by replica_id"
                    ),
                    "mixed_magnitude_sample": mixed_magnitude,
                    "weighted_block_count": block_diagnostics,
                    "weighted_synced_bytes": synced_diagnostics,
                },
                "convergence": decision,
                "timing": {
                    **timing,
                    "closed_early": closed_early,
                    "sealed_at": sealed_at,
                },
                "aggregation": {
                    "input_fingerprint": fingerprint,
                    "execution_attempt": attempt,
                    "single_committed_execution": True,
                },
            }
            return conclusion

    def get_conclusion(self, round_id: str) -> dict[str, Any]:
        round_row = self.db.get_round(round_id)
        existing = self.db.get_conclusion(round_id)
        if existing is None:
            if round_row["status"] == "ABORTED":
                raise BusinessError(
                    "E_ROUND_ABORTED",
                    "轮次已中止，尚无完整结论",
                    http_status=409,
                    details={
                        "round_id": round_id,
                        "status": round_row["status"],
                    },
                )
            raise BusinessError(
                "E_ROUND_NOT_READY",
                "轮次尚无完整提交结论",
                http_status=409,
                details={
                    "round_id": round_id,
                    "status": round_row["status"],
                },
            )
        return {
            "ok": True,
            "code": "OK",
            "round_id": round_id,
            "result_version": existing["result_version"],
            "conclusion_digest": existing["conclusion_digest"],
            "conclusion": existing["conclusion"],
        }

    def latest_recovery_report(self) -> dict[str, Any]:
        report = self.db.latest_recovery_report()
        if report is None:
            raise BusinessError(
                "E_NO_RECOVERY_REPORT",
                "没有由未完成意图生成的恢复报告",
                http_status=404,
            )
        return {"ok": True, "code": "OK", "report": report}


def _mixed_magnitude(terms: list[tuple[str, Decimal]]) -> bool:
    nonzero = [abs(value) for _, value in terms if value != 0]
    if len(nonzero) < 2:
        return False
    return max(nonzero) / min(nonzero) >= Decimal("1000000000000")

