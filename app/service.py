from __future__ import annotations

import hashlib
import json
import math
import os
import re
import threading
import time
import uuid
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Sequence

from .db import Database
from .errors import BusinessError
from .policy import PolicySnapshot, decimal_text, to_decimal
from .risk import (
    BaselineStats,
    Observation,
    calculate_risk,
    compute_stats,
    decision_for_fast,
    decision_for_full,
    fixed_micros,
    q6,
    systematic_sample,
    bootstrap_interval,
)


TERMINAL_STATUSES = {"PASSED", "ROLLED_BACK", "ESCALATED"}
CHANGE_TYPES = {
    "CONFIG": "CONFIG_APPLY",
    "CONFIG_APPLY": "CONFIG_APPLY",
    "CONFIG_DEPLOY": "CONFIG_APPLY",
    "配置下发": "CONFIG_APPLY",
    "IMAGE": "IMAGE_UPGRADE",
    "IMAGE_UPGRADE": "IMAGE_UPGRADE",
    "镜像升级": "IMAGE_UPGRADE",
    "SCALE": "SCALE",
    "SCALING": "SCALE",
    "扩缩容": "SCALE",
}
BATCH_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{1,63}$")


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return decimal_text(value)
    raise TypeError(f"not JSON serializable: {type(value)!r}")


def json_dumps(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=json_default,
    )


def rows_to_dicts(rows: Sequence[Any]) -> list[dict[str, Any]]:
    return [dict(row) for row in rows]


def _require_int(
    value: Any, field: str, code: str, minimum: int, maximum: int | None = None
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise BusinessError(code, "字段必须是整数", {"field": field, "value": value})
    if value < minimum or (maximum is not None and value > maximum):
        raise BusinessError(
            code,
            "整数字段超出范围",
            {"field": field, "value": value, "minimum": minimum, "maximum": maximum},
        )
    return value


def _require_text(value: Any, field: str, code: str, maximum: int = 128) -> str:
    if not isinstance(value, str) or not value.strip() or len(value.strip()) > maximum:
        raise BusinessError(
            code,
            "字段必须是非空字符串且不超过长度限制",
            {"field": field, "value": value, "maximum": maximum},
        )
    return value.strip()


def _content_signature(
    *, timestamp_ms: int, metric_type: str, latency_micros: int, success: bool
) -> dict[str, Any]:
    return {
        "timestamp_ms": timestamp_ms,
        "metric_type": metric_type,
        "latency_micros": latency_micros,
        "success": bool(success),
    }


@dataclass
class FaultInjector:
    enabled: bool = False
    _armed: dict[str, Any] | None = None
    _lock: threading.Lock = threading.Lock()

    def arm(self, operation: str, batch_id: str | None = None) -> dict[str, Any]:
        if not self.enabled:
            raise BusinessError(
                "E_FAULT_INJECTION_DISABLED",
                "故障注入未启用；请以 RISKLAB_FAULT_INJECTION=1 启动服务",
                {"environment": "RISKLAB_FAULT_INJECTION"},
            )
        with self._lock:
            self._armed = {"operation": operation, "batch_id": batch_id, "armed_at_ms": now_ms()}
            return dict(self._armed)

    def disarm(self) -> None:
        with self._lock:
            self._armed = None

    def status(self) -> dict[str, Any] | None:
        with self._lock:
            return dict(self._armed) if self._armed else None

    def maybe_crash(self, operation: str, batch_id: str) -> None:
        if not self.enabled:
            return
        with self._lock:
            armed = dict(self._armed) if self._armed else None
            if (
                armed
                and armed["operation"] == operation
                and (armed.get("batch_id") in (None, batch_id))
            ):
                self._armed = None
        if armed:
            # A true process-level termination. The caller deliberately calls
            # this after all writes in the SQLite transaction but before COMMIT.
            os._exit(137)


class WorkbenchService:
    def __init__(self, db_path: str | Path, *, fault_injection: bool = False):
        self.db = Database(db_path)
        self.db.initialize()
        self.faults = FaultInjector(enabled=fault_injection)

    # ------------------------------------------------------------------
    # Intent handling
    # ------------------------------------------------------------------
    def _create_intent(self, operation: str, payload: dict[str, Any]) -> str:
        intent_id = f"int_{uuid.uuid4().hex}"
        at = now_ms()
        with self.db.transaction() as connection:
            connection.execute(
                "INSERT INTO intents(intent_id, operation, status, payload_json, created_at_ms, updated_at_ms) "
                "VALUES (?, ?, 'PENDING', ?, ?, ?)",
                (intent_id, operation, json_dumps(payload), at, at),
            )
        return intent_id

    def _mark_intent_committed(self, connection: Any, intent_id: str) -> None:
        at = now_ms()
        connection.execute(
            "UPDATE intents SET status='COMMITTED', updated_at_ms=?, completed_at_ms=? "
            "WHERE intent_id=? AND status='PENDING'",
            (at, at, intent_id),
        )

    def _mark_intent_aborted(self, intent_id: str, error: BaseException | None = None) -> None:
        at = now_ms()
        error_json = None
        if isinstance(error, BusinessError):
            error_json = json_dumps(
                {"code": error.code, "message": error.message, "context": error.context}
            )
        elif error is not None:
            error_json = json_dumps({"code": "E_INTERNAL", "message": str(error)})
        try:
            with self.db.transaction() as connection:
                connection.execute(
                    "UPDATE intents SET status='ABORTED', updated_at_ms=?, completed_at_ms=?, error_json=? "
                    "WHERE intent_id=? AND status='PENDING'",
                    (at, at, error_json, intent_id),
                )
        except Exception:
            # Recovery will still find the pending intent after restart.
            pass

    # ------------------------------------------------------------------
    # Row helpers
    # ------------------------------------------------------------------
    def _get_policy_row(self, connection: Any, version: int) -> Any:
        row = connection.execute(
            "SELECT * FROM policies WHERE version=?", (version,)
        ).fetchone()
        if row is None:
            raise BusinessError(
                "E_POLICY_NOT_FOUND",
                "策略版本不存在",
                {"policy_version": version},
            )
        return row

    def _get_batch_row(self, connection: Any, batch_id: str) -> Any:
        row = connection.execute(
            "SELECT * FROM batches WHERE batch_id=?", (batch_id,)
        ).fetchone()
        if row is None:
            raise BusinessError(
                "E_BATCH_NOT_FOUND", "变更批次不存在", {"batch_id": batch_id}
            )
        return row

    def _resolve_policy(
        self, connection: Any, batch: Any, requested_version: int | None
    ) -> PolicySnapshot:
        version = requested_version if requested_version is not None else batch["policy_version"]
        policy = PolicySnapshot.from_storage(self._get_policy_row(connection, version))
        if (
            policy.window_ms != batch["window_ms"]
            or policy.late_tolerance_ms != batch["late_tolerance_ms"]
        ):
            raise BusinessError(
                "E_POLICY_WINDOW_INCOMPATIBLE",
                "策略版本的窗口几何与批次已固化窗口不兼容",
                {
                    "batch_id": batch["batch_id"],
                    "batch_window_ms": batch["window_ms"],
                    "batch_late_tolerance_ms": batch["late_tolerance_ms"],
                    "policy_version": policy.version,
                    "policy_window_ms": policy.window_ms,
                    "policy_late_tolerance_ms": policy.late_tolerance_ms,
                },
            )
        return policy

    def _resolve_window(
        self, connection: Any, batch_id: str, window_index: int | None
    ) -> Any:
        if window_index is None:
            row = connection.execute(
                "SELECT * FROM windows WHERE batch_id=? ORDER BY window_index DESC LIMIT 1",
                (batch_id,),
            ).fetchone()
        else:
            row = connection.execute(
                "SELECT * FROM windows WHERE batch_id=? AND window_index=?",
                (batch_id, window_index),
            ).fetchone()
        if row is None:
            code = "E_WINDOW_NOT_FOUND" if window_index is not None else "E_INSUFFICIENT_SAMPLES"
            raise BusinessError(
                code,
                "没有可用观察窗口",
                {"batch_id": batch_id, "window_index": window_index},
            )
        return row

    def _load_observations(
        self, connection: Any, batch_id: str, window_index: int
    ) -> list[Observation]:
        rows = connection.execute(
            "SELECT sequence, timestamp_ms, latency_micros, success, metric_type "
            "FROM observations WHERE batch_id=? AND window_index=? ORDER BY sequence",
            (batch_id, window_index),
        ).fetchall()
        return [
            Observation(
                sequence=row["sequence"],
                timestamp_ms=row["timestamp_ms"],
                latency_micros=row["latency_micros"],
                success=bool(row["success"]),
                metric_type=row["metric_type"],
            )
            for row in rows
        ]

    def _window_dict(
        self, window: Any, late_tolerance_ms: int | None = None
    ) -> dict[str, Any]:
        count = window["sample_count"]
        successes = window["success_count"]
        return {
            "window_index": window["window_index"],
            "version": window["version"],
            "state": window["state"],
            "start_ms": window["start_ms"],
            "end_ms": window["end_ms"],
            "close_boundary_ms": (
                window["end_ms"] + late_tolerance_ms
                if late_tolerance_ms is not None
                else None
            ),
            "sample_count": count,
            "success_count": successes,
            "error_rate": decimal_text(q6(Decimal(count - successes) / Decimal(count)))
            if count
            else None,
            "success_rate": decimal_text(q6(Decimal(successes) / Decimal(count)))
            if count
            else None,
            "watermark_ms": window["watermark_ms"],
            "closed_at_ms": window["closed_at_ms"],
            "created_policy_version": window["policy_version"],
        }

    def _batch_dict(self, row: Any) -> dict[str, Any]:
        baseline = json.loads(row["baseline_json"])
        return {
            "batch_id": row["batch_id"],
            "service": row["service"],
            "change_type": row["change_type"],
            "batch_size": row["batch_size"],
            "baseline": baseline,
            "started_at_ms": row["started_at_ms"],
            "policy_version": row["policy_version"],
            "window_ms": row["window_ms"],
            "late_tolerance_ms": row["late_tolerance_ms"],
            "status": row["status"],
            "terminal": row["status"] in TERMINAL_STATUSES,
            "window_epoch": row["window_epoch"],
            "watermark_ms": row["watermark_ms"],
            "row_version": row["row_version"],
            "created_at_ms": row["created_at_ms"],
            "updated_at_ms": row["updated_at_ms"],
        }

    # ------------------------------------------------------------------
    # Policies and batches
    # ------------------------------------------------------------------
    def submit_policy(self, raw_policy: Any) -> dict[str, Any]:
        policy = PolicySnapshot.from_mapping(raw_policy)
        pre_state: dict[str, Any] = {}
        with self.db.read_snapshot() as connection:
            latest = connection.execute("SELECT MAX(version) AS v FROM policies").fetchone()[0]
            pre_state = {"latest_policy_version": latest}
        intent_id = self._create_intent(
            "POLICY_SUBMIT",
            {
                "batch_id": None,
                "policy_version": policy.version,
                "digest": policy.digest,
                "pre_state": pre_state,
            },
        )
        try:
            with self.db.transaction() as connection:
                latest = connection.execute(
                    "SELECT MAX(version) AS v FROM policies"
                ).fetchone()[0]
                if latest is not None and policy.version <= latest:
                    raise BusinessError(
                        "E_STALE_POLICY",
                        "策略版本必须严格大于当前已提交版本",
                        {
                            "submitted_version": policy.version,
                            "current_version": latest,
                        },
                    )
                connection.execute(
                    "INSERT INTO policies(version, generated_at_ms, digest, canonical_json, created_at_ms) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (
                        policy.version,
                        policy.generated_at_ms,
                        policy.digest,
                        policy.canonical_json,
                        now_ms(),
                    ),
                )
                self._mark_intent_committed(connection, intent_id)
            return {
                "policy": policy.as_dict(),
                "version": policy.version,
                "digest": policy.digest,
                "intent_id": intent_id,
            }
        except BaseException as error:
            self._mark_intent_aborted(intent_id, error)
            raise

    def list_policies(self) -> list[dict[str, Any]]:
        with self.db.read_snapshot() as connection:
            rows = connection.execute(
                "SELECT version, generated_at_ms, digest, canonical_json, created_at_ms "
                "FROM policies ORDER BY version DESC"
            ).fetchall()
        result = []
        for row in rows:
            item = json.loads(row["canonical_json"])
            item["digest"] = row["digest"]
            item["created_at_ms"] = row["created_at_ms"]
            result.append(item)
        return result

    def latest_policy(self) -> dict[str, Any] | None:
        with self.db.read_snapshot() as connection:
            row = connection.execute(
                "SELECT version, digest, canonical_json, created_at_ms FROM policies "
                "ORDER BY version DESC LIMIT 1"
            ).fetchone()
        if row is None:
            return None
        data = json.loads(row["canonical_json"])
        data["digest"] = row["digest"]
        data["created_at_ms"] = row["created_at_ms"]
        return data

    def _normalize_baseline(self, raw: Any) -> tuple[dict[str, Any], int, int, int]:
        code = "E_BATCH_INVALID"
        if not isinstance(raw, dict):
            raise BusinessError(code, "基线必须是对象", {"field": "baseline"})
        sample_count = _require_int(
            raw.get("sample_count"), "baseline.sample_count", code, 1
        )
        success_count = _require_int(
            raw.get("success_count"),
            "baseline.success_count",
            code,
            0,
            sample_count,
        )
        p90_ms = to_decimal(
            raw.get("latency_p90_ms"), "baseline.latency_p90_ms", code
        )
        if p90_ms <= 0 or p90_ms > Decimal("3600000"):
            raise BusinessError(
                code,
                "基线 P90 延迟必须位于 (0, 3600000] 毫秒",
                {"field": "baseline.latency_p90_ms", "value": decimal_text(p90_ms)},
            )
        p90_micros = int(
            (p90_ms * Decimal(1000)).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
        )
        derived_error = q6(Decimal(sample_count - success_count) / Decimal(sample_count))
        derived_success = q6(Decimal(success_count) / Decimal(sample_count))
        for name, derived in (
            ("error_rate", derived_error),
            ("success_rate", derived_success),
        ):
            if name in raw:
                supplied = to_decimal(raw[name], f"baseline.{name}", code)
                if q6(supplied) != derived:
                    raise BusinessError(
                        code,
                        "基线汇总字段与样本计数不一致",
                        {
                            "field": f"baseline.{name}",
                            "supplied": decimal_text(supplied),
                            "derived": decimal_text(derived),
                        },
                    )
        normalized = {
            "sample_count": sample_count,
            "success_count": success_count,
            "error_rate": decimal_text(derived_error),
            "success_rate": decimal_text(derived_success),
            "latency_p90_ms": decimal_text(
                (Decimal(p90_micros) / Decimal(1000)).quantize(
                    Decimal("0.001"), rounding=ROUND_HALF_UP
                )
            ),
            "latency_p90_micros": p90_micros,
        }
        return normalized, sample_count, success_count, p90_micros

    def submit_batch(self, raw: Any) -> dict[str, Any]:
        code = "E_BATCH_INVALID"
        if not isinstance(raw, dict):
            raise BusinessError(code, "批次请求必须是对象", {"field": "$"})
        batch_id = _require_text(raw.get("batch_id"), "batch_id", code, 64)
        if not BATCH_ID_PATTERN.match(batch_id):
            raise BusinessError(
                code,
                "batch_id 格式非法",
                {
                    "field": "batch_id",
                    "value": batch_id,
                    "pattern": BATCH_ID_PATTERN.pattern,
                },
            )
        service = _require_text(raw.get("service"), "service", code, 96)
        change_type_input = _require_text(raw.get("change_type"), "change_type", code, 64)
        change_type = CHANGE_TYPES.get(change_type_input.upper()) or CHANGE_TYPES.get(
            change_type_input
        )
        if change_type is None:
            raise BusinessError(
                code,
                "change_type 非法",
                {
                    "field": "change_type",
                    "value": change_type_input,
                    "allowed": ["CONFIG_APPLY", "IMAGE_UPGRADE", "SCALE"],
                },
            )
        batch_size = _require_int(raw.get("batch_size"), "batch_size", code, 1, 10_000_000)
        started_at_ms = _require_int(raw.get("started_at_ms"), "started_at_ms", code, 0)
        baseline, baseline_count, baseline_success, baseline_p90 = self._normalize_baseline(
            raw.get("baseline")
        )
        requested_policy = raw.get("policy_version")
        if requested_policy is not None:
            requested_policy = _require_int(
                requested_policy, "policy_version", code, 1
            )

        with self.db.read_snapshot() as connection:
            if requested_policy is None:
                latest = connection.execute(
                    "SELECT MAX(version) AS v FROM policies"
                ).fetchone()[0]
                if latest is None:
                    raise BusinessError(
                        "E_POLICY_NOT_FOUND",
                        "尚未提交任何策略，无法创建批次",
                        {"batch_id": batch_id},
                    )
                requested_policy = latest
            policy = PolicySnapshot.from_storage(
                self._get_policy_row(connection, requested_policy)
            )
            pre_state = {"batch_id": batch_id, "exists": False}

        intent_id = self._create_intent(
            "BATCH_SUBMIT",
            {
                "batch_id": batch_id,
                "policy_version": requested_policy,
                "pre_state": pre_state,
            },
        )
        try:
            at = now_ms()
            with self.db.transaction() as connection:
                existing = connection.execute(
                    "SELECT 1 FROM batches WHERE batch_id=?", (batch_id,)
                ).fetchone()
                if existing is not None:
                    raise BusinessError(
                        "E_BATCH_EXISTS",
                        "变更批次已存在",
                        {"batch_id": batch_id},
                    )
                connection.execute(
                    "INSERT INTO batches(batch_id, service, change_type, batch_size, baseline_json, "
                    "baseline_sample_count, baseline_success_count, baseline_p90_micros, started_at_ms, "
                    "policy_version, window_ms, late_tolerance_ms, status, window_epoch, watermark_ms, "
                    "row_version, created_at_ms, updated_at_ms) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'OBSERVING', 0, NULL, 1, ?, ?)",
                    (
                        batch_id,
                        service,
                        change_type,
                        batch_size,
                        json_dumps(baseline),
                        baseline_count,
                        baseline_success,
                        baseline_p90,
                        started_at_ms,
                        policy.version,
                        policy.window_ms,
                        policy.late_tolerance_ms,
                        at,
                        at,
                    ),
                )
                connection.execute(
                    "INSERT INTO batch_transitions(batch_id, from_status, to_status, action, "
                    "policy_version, window_index, window_version, window_epoch, risk_score, decision_id, at_ms) "
                    "VALUES (?, NULL, 'OBSERVING', 'BATCH_CREATED', ?, NULL, NULL, 0, '0', NULL, ?)",
                    (batch_id, policy.version, at),
                )
                self._mark_intent_committed(connection, intent_id)
                row = self._get_batch_row(connection, batch_id)
                result = self._batch_dict(row)
            return {"batch": result, "intent_id": intent_id}
        except BaseException as error:
            self._mark_intent_aborted(intent_id, error)
            raise

    def list_batches(self) -> list[dict[str, Any]]:
        with self.db.read_snapshot() as connection:
            rows = connection.execute(
                "SELECT * FROM batches ORDER BY created_at_ms ASC, batch_id ASC"
            ).fetchall()
            result = []
            for row in rows:
                item = self._batch_dict(row)
                windows = connection.execute(
                    "SELECT * FROM windows WHERE batch_id=? ORDER BY window_index",
                    (row["batch_id"],),
                ).fetchall()
                item["windows"] = [
                    self._window_dict(window, row["late_tolerance_ms"])
                    for window in windows
                ]
                latest_decision = connection.execute(
                    "SELECT decision_id, mode, action, applied, risk_score, risk_level, created_at_ms "
                    "FROM decisions WHERE batch_id=? ORDER BY created_at_ms DESC LIMIT 1",
                    (row["batch_id"],),
                ).fetchone()
                item["latest_decision"] = (
                    dict(latest_decision) if latest_decision is not None else None
                )
                cooldown = connection.execute(
                    "SELECT cooldown_until_ms FROM cooldowns WHERE service=?",
                    (row["service"],),
                ).fetchone()
                item["cooldown_until_ms"] = (
                    cooldown["cooldown_until_ms"] if cooldown is not None else None
                )
                result.append(item)
        return result

    def get_batch(self, batch_id: str) -> dict[str, Any]:
        with self.db.read_snapshot() as connection:
            batch_row = self._get_batch_row(connection, batch_id)
            batch = self._batch_dict(batch_row)
            windows = connection.execute(
                "SELECT * FROM windows WHERE batch_id=? ORDER BY window_index",
                (batch_id,),
            ).fetchall()
            batch["windows"] = [
                {
                    **self._window_dict(row, batch["late_tolerance_ms"]),
                    "close_boundary_ms": row["end_ms"] + batch["late_tolerance_ms"],
                }
                for row in windows
            ]
            transitions = connection.execute(
                "SELECT * FROM batch_transitions WHERE batch_id=? ORDER BY id", (batch_id,)
            ).fetchall()
            batch["transitions"] = rows_to_dicts(transitions)
            decisions = connection.execute(
                "SELECT decision_id, mode, policy_version, window_index, window_version, "
                "window_epoch, decision_time_ms, risk_score, risk_level, action, applied, created_at_ms "
                "FROM decisions WHERE batch_id=? ORDER BY created_at_ms DESC LIMIT 20",
                (batch_id,),
            ).fetchall()
            batch["recent_decisions"] = rows_to_dicts(decisions)
            cooldown = connection.execute(
                "SELECT * FROM cooldowns WHERE service=?", (batch["service"],)
            ).fetchone()
            batch["cooldown"] = dict(cooldown) if cooldown is not None else None
            return batch

    # ------------------------------------------------------------------
    # Observation ingestion and window lifecycle
    # ------------------------------------------------------------------
    def _normalize_observation(
        self, raw: Any, batch: Any, *, position: int
    ) -> dict[str, Any]:
        code = "E_OBSERVATION_INVALID"
        if not isinstance(raw, dict):
            raise BusinessError(
                code,
                "单条观测必须是对象",
                {"field": f"observations[{position}]"},
            )
        sequence = _require_int(
            raw.get("sequence"), f"observations[{position}].sequence", code, 1
        )
        timestamp_ms = _require_int(
            raw.get("timestamp_ms"), f"observations[{position}].timestamp_ms", code, 0
        )
        if timestamp_ms < batch["started_at_ms"]:
            raise BusinessError(
                code,
                "观测时间早于批次开始时间",
                {
                    "field": f"observations[{position}].timestamp_ms",
                    "timestamp_ms": timestamp_ms,
                    "started_at_ms": batch["started_at_ms"],
                },
            )
        metric_type_raw = raw.get("metric_type", "CANDIDATE")
        if not isinstance(metric_type_raw, str):
            raise BusinessError(
                code,
                "metric_type 必须是字符串",
                {"field": f"observations[{position}].metric_type"},
            )
        metric_type = metric_type_raw.upper()
        if metric_type != "CANDIDATE":
            raise BusinessError(
                code,
                "当前仅接受 CANDIDATE 指标类型；基线在批次创建时固化",
                {
                    "field": f"observations[{position}].metric_type",
                    "value": metric_type_raw,
                    "allowed": ["CANDIDATE"],
                },
            )
        success = raw.get("success")
        if not isinstance(success, bool):
            raise BusinessError(
                code,
                "success 必须是布尔值",
                {"field": f"observations[{position}].success", "value": success},
            )
        latency = to_decimal(
            raw.get("latency_ms"),
            f"observations[{position}].latency_ms",
            "E_OBSERVATION_INVALID",
        )
        if latency < 0 or latency > Decimal("3600000"):
            raise BusinessError(
                code,
                "延迟必须位于 [0, 3600000] 毫秒",
                {
                    "field": f"observations[{position}].latency_ms",
                    "value": decimal_text(latency),
                },
            )
        latency_micros = int(
            (latency * Decimal(1000)).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
        )
        window_index = math.floor(
            (timestamp_ms - batch["started_at_ms"]) / batch["window_ms"]
        )
        window_start = batch["started_at_ms"] + window_index * batch["window_ms"]
        window_end = window_start + batch["window_ms"]
        signature = _content_signature(
            timestamp_ms=timestamp_ms,
            metric_type=metric_type,
            latency_micros=latency_micros,
            success=success,
        )
        return {
            "sequence": sequence,
            "timestamp_ms": timestamp_ms,
            "metric_type": metric_type,
            "latency_micros": latency_micros,
            "success": success,
            "window_index": window_index,
            "window_start_ms": window_start,
            "window_end_ms": window_end,
            "signature": signature,
        }

    def ingest_observations(self, batch_id: str, raw_request: Any) -> dict[str, Any]:
        if not isinstance(raw_request, dict):
            raise BusinessError(
                "E_OBSERVATION_INVALID", "观测请求必须是对象", {"field": "$"}
            )
        raw_observations = raw_request.get("observations", [])
        if not isinstance(raw_observations, list):
            raise BusinessError(
                "E_OBSERVATION_INVALID",
                "observations 必须是数组",
                {"field": "observations"},
            )
        if len(raw_observations) > 5000:
            raise BusinessError(
                "E_OBSERVATION_INVALID",
                "单次观测提交不得超过 5000 条",
                {"field": "observations", "count": len(raw_observations)},
            )
        supplied_watermark = raw_request.get("watermark_ms")
        if supplied_watermark is not None:
            supplied_watermark = _require_int(
                supplied_watermark, "watermark_ms", "E_OBSERVATION_INVALID", 0
            )
        if not raw_observations and supplied_watermark is None:
            raise BusinessError(
                "E_OBSERVATION_INVALID",
                "空观测请求必须提供 watermark_ms 以推进或关闭窗口",
                {"field": "watermark_ms"},
            )

        with self.db.read_snapshot() as connection:
            batch_row = self._get_batch_row(connection, batch_id)
            batch = self._batch_dict(batch_row)

        normalized = [
            self._normalize_observation(item, batch, position=index)
            for index, item in enumerate(raw_observations)
        ]
        proposed_watermark = max(
            [batch["watermark_ms"] or 0, supplied_watermark or 0]
            + [item["timestamp_ms"] for item in normalized]
        )
        if proposed_watermark == 0 and supplied_watermark is None:
            proposed_watermark = batch["watermark_ms"] or 0

        pre_state = {
            "batch_id": batch_id,
            "batch_status": batch["status"],
            "window_epoch": batch["window_epoch"],
            "watermark_ms": batch["watermark_ms"],
        }
        intent_id = self._create_intent(
            "OBSERVATION_INGEST",
            {
                "batch_id": batch_id,
                "observation_count": len(normalized),
                "proposed_watermark_ms": proposed_watermark,
                "pre_state": pre_state,
            },
        )
        try:
            with self.db.transaction() as connection:
                batch_row = self._get_batch_row(connection, batch_id)
                if batch_row["window_epoch"] != batch["window_epoch"]:
                    raise BusinessError(
                        "E_CONFLICTING_REPORT",
                        "批次窗口版本在摄入期间发生变化，请重试",
                        {
                            "batch_id": batch_id,
                            "expected_window_epoch": batch["window_epoch"],
                            "observed_window_epoch": batch_row["window_epoch"],
                        },
                    )
                existing_rows = connection.execute(
                    "SELECT sequence, timestamp_ms, metric_type, latency_micros, success "
                    "FROM observations WHERE batch_id=? AND sequence IN ({})".format(
                        ",".join("?" for _ in normalized) or "NULL"
                    ),
                    (batch_id, *[item["sequence"] for item in normalized]),
                ).fetchall() if normalized else []
                existing_by_sequence = {row["sequence"]: row for row in existing_rows}

                accepted: dict[int, dict[str, Any]] = {}
                duplicates: list[dict[str, Any]] = []
                seen_in_request: dict[int, dict[str, Any]] = {}
                for item in normalized:
                    sequence = item["sequence"]
                    existing = existing_by_sequence.get(sequence)
                    if existing is not None:
                        existing_signature = _content_signature(
                            timestamp_ms=existing["timestamp_ms"],
                            metric_type=existing["metric_type"],
                            latency_micros=existing["latency_micros"],
                            success=bool(existing["success"]),
                        )
                        if existing_signature != item["signature"]:
                            raise BusinessError(
                                "E_CONFLICTING_REPORT",
                                "同一批次同一序号的内容不一致，保留首次上报",
                                {
                                    "batch_id": batch_id,
                                    "sequence": sequence,
                                    "existing": existing_signature,
                                    "incoming": item["signature"],
                                },
                            )
                        duplicates.append(
                            {"sequence": sequence, "reason": "identical_duplicate"}
                        )
                        continue
                    prior = seen_in_request.get(sequence)
                    if prior is not None:
                        if prior["signature"] != item["signature"]:
                            raise BusinessError(
                                "E_CONFLICTING_REPORT",
                                "同一请求内同序号内容不一致",
                                {
                                    "batch_id": batch_id,
                                    "sequence": sequence,
                                    "first": prior["signature"],
                                    "second": item["signature"],
                                },
                            )
                        duplicates.append(
                            {"sequence": sequence, "reason": "duplicate_within_request"}
                        )
                        continue
                    existing_window = connection.execute(
                        "SELECT * FROM windows WHERE batch_id=? AND window_index=?",
                        (batch_id, item["window_index"]),
                    ).fetchone()
                    close_boundary = item["window_end_ms"] + batch["late_tolerance_ms"]
                    if existing_window is not None and existing_window["state"] == "CLOSED":
                        raise BusinessError(
                            "E_LATE_SAMPLE",
                            "目标观察窗口已关闭，迟到样本被拒绝且未改写统计",
                            {
                                "batch_id": batch_id,
                                "sequence": sequence,
                                "window_index": item["window_index"],
                                "window_version": existing_window["version"],
                                "window_state": existing_window["state"],
                                "close_boundary_ms": close_boundary,
                                "proposed_watermark_ms": proposed_watermark,
                            },
                        )
                    if close_boundary <= proposed_watermark:
                        raise BusinessError(
                            "E_LATE_SAMPLE",
                            "样本本应在 watermark 关闭边界前到达，迟到样本被拒绝",
                            {
                                "batch_id": batch_id,
                                "sequence": sequence,
                                "window_index": item["window_index"],
                                "close_boundary_ms": close_boundary,
                                "proposed_watermark_ms": proposed_watermark,
                            },
                        )
                    accepted[sequence] = item
                    seen_in_request[sequence] = item

                now = now_ms()
                for item in accepted.values():
                    connection.execute(
                        "INSERT INTO observations(batch_id, sequence, metric_type, timestamp_ms, "
                        "window_index, latency_micros, success, received_at_ms) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            batch_id,
                            item["sequence"],
                            item["metric_type"],
                            item["timestamp_ms"],
                            item["window_index"],
                            item["latency_micros"],
                            int(item["success"]),
                            now,
                        ),
                    )
                    window = connection.execute(
                        "SELECT * FROM windows WHERE batch_id=? AND window_index=?",
                        (batch_id, item["window_index"]),
                    ).fetchone()
                    if window is None:
                        connection.execute(
                            "INSERT INTO windows(batch_id, window_index, policy_version, start_ms, end_ms, "
                            "version, state, sample_count, success_count, latency_sum_micros, watermark_ms, closed_at_ms) "
                            "VALUES (?, ?, ?, ?, ?, 1, 'OPEN', 1, ?, ?, ?, NULL)",
                            (
                                batch_id,
                                item["window_index"],
                                batch["policy_version"],
                                item["window_start_ms"],
                                item["window_end_ms"],
                                int(item["success"]),
                                item["latency_micros"],
                                proposed_watermark,
                            ),
                        )
                    else:
                        connection.execute(
                            "UPDATE windows SET version=version+1, sample_count=sample_count+1, "
                            "success_count=success_count+?, latency_sum_micros=latency_sum_micros+?, "
                            "watermark_ms=? WHERE batch_id=? AND window_index=?",
                            (
                                int(item["success"]),
                                item["latency_micros"],
                                proposed_watermark,
                                batch_id,
                                item["window_index"],
                            ),
                        )

                windows_to_close = connection.execute(
                    "SELECT * FROM windows WHERE batch_id=? AND state='OPEN' "
                    "AND end_ms + ? <= ? ORDER BY window_index",
                    (batch_id, batch["late_tolerance_ms"], proposed_watermark),
                ).fetchall()
                for window in windows_to_close:
                    connection.execute(
                        "UPDATE windows SET state='CLOSED', version=version+1, watermark_ms=?, "
                        "closed_at_ms=? WHERE batch_id=? AND window_index=?",
                        (
                            proposed_watermark,
                            proposed_watermark,
                            batch_id,
                            window["window_index"],
                        ),
                    )

                changed = bool(accepted) or bool(windows_to_close)
                watermark_changed = proposed_watermark != (batch["watermark_ms"] or 0)
                if changed or watermark_changed:
                    connection.execute(
                        "UPDATE batches SET watermark_ms=?, window_epoch=window_epoch+1, "
                        "row_version=row_version+1, updated_at_ms=? WHERE batch_id=?",
                        (proposed_watermark, now, batch_id),
                    )
                else:
                    connection.execute(
                        "UPDATE windows SET watermark_ms=? WHERE batch_id=? AND watermark_ms IS NULL",
                        (proposed_watermark, batch_id),
                    )
                refreshed_windows = connection.execute(
                    "SELECT * FROM windows WHERE batch_id=? ORDER BY window_index",
                    (batch_id,),
                ).fetchall()
                refreshed_batch = self._get_batch_row(connection, batch_id)
                self._mark_intent_committed(connection, intent_id)
                result = {
                    "batch_id": batch_id,
                    "accepted_count": len(accepted),
                    "duplicate_count": len(duplicates),
                    "duplicates": duplicates,
                    "rejected_count": 0,
                    "watermark_ms": proposed_watermark,
                    "window_epoch": refreshed_batch["window_epoch"],
                    "window_epoch_changed": changed or watermark_changed,
                    "windows": [
                        {
                            **self._window_dict(window, batch["late_tolerance_ms"]),
                            "close_boundary_ms": window["end_ms"] + batch["late_tolerance_ms"],
                        }
                        for window in refreshed_windows
                    ],
                    "intent_id": intent_id,
                }
            return result
        except BaseException as error:
            self._mark_intent_aborted(intent_id, error)
            raise

    # ------------------------------------------------------------------
    # Risk computation
    # ------------------------------------------------------------------
    def _baseline_for_batch(self, batch: Any) -> BaselineStats:
        if "baseline" in batch:
            baseline = batch["baseline"]
        else:
            baseline = json.loads(batch["baseline_json"])
        return BaselineStats.from_mapping(baseline)

    def _build_risk_payload(
        self,
        *,
        connection: Any,
        batch: Any,
        policy: PolicySnapshot,
        window: Any,
        mode: str,
        at_ms: int,
    ) -> dict[str, Any]:
        observations = self._load_observations(
            connection, batch["batch_id"], window["window_index"]
        )
        if len(observations) < policy.min_samples:
            raise BusinessError(
                "E_INSUFFICIENT_SAMPLES",
                "样本不足，拒绝静默返回零风险",
                {
                    "batch_id": batch["batch_id"],
                    "window_index": window["window_index"],
                    "window_version": window["version"],
                    "window_epoch": batch["window_epoch"],
                    "policy_version": policy.version,
                    "sample_count": len(observations),
                    "min_samples": policy.min_samples,
                    "mode": mode,
                },
            )
        baseline = self._baseline_for_batch(batch)
        population_stats = compute_stats(observations)
        sampling: dict[str, Any] | None = None
        selected = observations
        if mode == "FAST":
            selected, sampling = systematic_sample(observations, policy.fast.sample_cap)
        ledger = None
        risk = calculate_risk(selected, baseline, policy, ledger)
        sampling_key = (
            ",".join(str(item.sequence) for item in selected) if mode == "FAST" else "all"
        )
        if mode == "FAST":
            confidence = bootstrap_interval(
                selected,
                baseline,
                policy,
                salt=f"{batch['batch_id']}:{window['window_index']}:{window['version']}:{batch['window_epoch']}:{sampling_key}",
            )
            decision = decision_for_fast(risk, confidence, policy, window["state"])
        else:
            confidence = {
                "method": "exhaustive_window",
                "confidence_level": "1",
                "lower": decimal_text(risk.risk_score),
                "upper": decimal_text(risk.risk_score),
                "lower_fixed_point": fixed_micros(risk.risk_score),
                "upper_fixed_point": fixed_micros(risk.risk_score),
                "resamples": 0,
                "note": "完整模式使用窗口内全部观测，不引入抽样区间。",
            }
            action, target_status, rationale = decision_for_full(
                risk, policy, window["state"]
            )
            decision = {
                "action": action,
                "target_status": target_status,
                "requires_full_review": False,
                "can_auto_rollback": True,
                "rationale": rationale,
            }
        reproducibility_material = json_dumps(
            {
                "batch_id": batch["batch_id"],
                "policy_version": policy.version,
                "policy_digest": policy.digest,
                "window_index": window["window_index"],
                "window_version": window["version"],
                "window_epoch": batch["window_epoch"],
                "mode": mode,
                "sample_sequences": [item.sequence for item in selected],
            }
        )
        reproducibility_key = hashlib.sha256(
            reproducibility_material.encode("utf-8")
        ).hexdigest()
        risk_payload = risk.as_dict()
        return {
            "batch_id": batch["batch_id"],
            "service": batch["service"],
            "mode": mode,
            "policy_version": policy.version,
            "policy_digest": policy.digest,
            "risk_threshold": decimal_text(policy.risk_threshold),
            "risk_score": decimal_text(risk.risk_score),
            "risk_score_fixed_point": fixed_micros(risk.risk_score),
            "risk_level": risk.risk_level,
            "window": {
                **self._window_dict(window, batch["late_tolerance_ms"]),
                "close_boundary_ms": window["end_ms"] + batch["late_tolerance_ms"],
            },
            "window_versions": {
                "batch_window_epoch": batch["window_epoch"],
                "window_index": window["window_index"],
                "window_version": window["version"],
                "policy_version": policy.version,
                "policy_digest": policy.digest,
                "all_aligned": True,
            },
            "candidate_stats": risk_payload["candidate_stats"],
            "population_stats": population_stats.as_dict(),
            "baseline_stats": risk_payload["baseline_stats"],
            "components": risk_payload["components"],
            "rounding": risk_payload["rounding"],
            "formula": risk_payload["formula"],
            "p90_contract": risk_payload["p90_contract"],
            "sampling": sampling,
            "confidence": confidence,
            "decision": decision,
            "reproducibility_key": reproducibility_key,
            "computed_at_ms": at_ms,
        }

    def get_risk(
        self,
        batch_id: str,
        *,
        mode: str = "FULL",
        policy_version: int | None = None,
        window_index: int | None = None,
        at_ms: int | None = None,
    ) -> dict[str, Any]:
        mode = mode.upper()
        if mode not in {"FAST", "FULL"}:
            raise BusinessError(
                "E_DECISION_INVALID",
                "mode 必须是 FAST 或 FULL",
                {"field": "mode", "value": mode},
            )
        with self.db.read_snapshot() as connection:
            batch_row = self._get_batch_row(connection, batch_id)
            batch = self._batch_dict(batch_row)
            policy = self._resolve_policy(connection, batch_row, policy_version)
            window = self._resolve_window(connection, batch_id, window_index)
            return self._build_risk_payload(
                connection=connection,
                batch=batch,
                policy=policy,
                window=window,
                mode=mode,
                at_ms=at_ms if at_ms is not None else now_ms(),
            )

    def compare_modes(
        self,
        batch_id: str,
        *,
        policy_version: int | None = None,
        window_index: int | None = None,
        at_ms: int | None = None,
    ) -> dict[str, Any]:
        with self.db.read_snapshot() as connection:
            batch_row = self._get_batch_row(connection, batch_id)
            batch = self._batch_dict(batch_row)
            policy = self._resolve_policy(connection, batch_row, policy_version)
            window = self._resolve_window(connection, batch_id, window_index)
            at = at_ms if at_ms is not None else now_ms()
            full = self._build_risk_payload(
                connection=connection,
                batch=batch,
                policy=policy,
                window=window,
                mode="FULL",
                at_ms=at,
            )
            fast = self._build_risk_payload(
                connection=connection,
                batch=batch,
                policy=policy,
                window=window,
                mode="FAST",
                at_ms=at,
            )
            full_score = Decimal(full["risk_score"])
            fast_score = Decimal(fast["risk_score"])
            fields = [
                ("risk_score", full_score, fast_score),
                ("risk_level", full["risk_level"], fast["risk_level"]),
                ("decision_action", full["decision"]["action"], fast["decision"]["action"]),
                ("sample_count", full["candidate_stats"]["sample_count"], fast["candidate_stats"]["sample_count"]),
                ("error_rate", Decimal(full["candidate_stats"]["error_rate"]), Decimal(fast["candidate_stats"]["error_rate"])),
                ("success_rate", Decimal(full["candidate_stats"]["success_rate"]), Decimal(fast["candidate_stats"]["success_rate"])),
                ("p90_latency_ms", Decimal(full["candidate_stats"]["p90_latency_ms"]), Decimal(fast["candidate_stats"]["p90_latency_ms"])),
            ]
            differences = []
            for name, full_value, fast_value in fields:
                if isinstance(full_value, Decimal):
                    delta = decimal_text(q6(fast_value - full_value))
                    same = q6(full_value) == q6(fast_value)
                    differences.append(
                        {
                            "field": name,
                            "full": decimal_text(full_value),
                            "fast": decimal_text(fast_value),
                            "delta_fast_minus_full": delta,
                            "same": same,
                        }
                    )
                else:
                    differences.append(
                        {
                            "field": name,
                            "full": full_value,
                            "fast": fast_value,
                            "delta_fast_minus_full": None,
                            "same": full_value == fast_value,
                        }
                    )
            return {
                "batch_id": batch_id,
                "policy_version": policy.version,
                "policy_digest": policy.digest,
                "window_versions": full["window_versions"],
                "assertions": {
                    "same_policy_version": full["policy_version"] == fast["policy_version"],
                    "same_policy_digest": full["policy_digest"] == fast["policy_digest"],
                    "same_window_version": full["window_versions"]["window_version"]
                    == fast["window_versions"]["window_version"],
                    "same_window_epoch": full["window_versions"]["batch_window_epoch"]
                    == fast["window_versions"]["batch_window_epoch"],
                    "full_used_all_samples": full["candidate_stats"]["sample_count"]
                    == full["population_stats"]["sample_count"],
                },
                "full": full,
                "fast": fast,
                "differences": differences,
                "integrity_note": (
                    "两种模式在同一次 SQLite 读快照内、使用同一策略摘要与同一窗口版本计算；"
                    "快速模式不写状态、不触发自动回滚。"
                ),
            }

    # ------------------------------------------------------------------
    # Cooldowns and decisions
    # ------------------------------------------------------------------
    def _cooldown_row(self, connection: Any, service: str) -> Any:
        return connection.execute(
            "SELECT * FROM cooldowns WHERE service=?", (service,)
        ).fetchone()

    def cooldown_status(
        self, service: str, *, at_ms: int | None = None
    ) -> dict[str, Any]:
        at = at_ms if at_ms is not None else now_ms()
        with self.db.read_snapshot() as connection:
            row = self._cooldown_row(connection, service)
        if row is None:
            return {
                "service": service,
                "active": False,
                "cooldown_until_ms": None,
                "remaining_ms": 0,
                "at_ms": at,
            }
        remaining = max(0, row["cooldown_until_ms"] - at)
        return {
            "service": service,
            "active": remaining > 0,
            "cooldown_until_ms": row["cooldown_until_ms"],
            "remaining_ms": remaining,
            "source_batch_id": row["source_batch_id"],
            "policy_version": row["policy_version"],
            "at_ms": at,
        }

    def apply_decision(self, batch_id: str, raw_request: Any) -> dict[str, Any]:
        if not isinstance(raw_request, dict):
            raise BusinessError(
                "E_DECISION_INVALID", "裁决请求必须是对象", {"field": "$"}
            )
        mode = str(raw_request.get("mode", "FULL")).upper()
        if mode not in {"FAST", "FULL"}:
            raise BusinessError(
                "E_DECISION_INVALID",
                "mode 必须是 FAST 或 FULL",
                {"field": "mode", "value": mode},
            )
        policy_version = raw_request.get("policy_version")
        if policy_version is not None:
            policy_version = _require_int(
                policy_version, "policy_version", "E_DECISION_INVALID", 1
            )
        window_index = raw_request.get("window_index")
        if window_index is not None:
            window_index = _require_int(
                window_index, "window_index", "E_DECISION_INVALID", 0
            )
        decision_time_ms = raw_request.get("decision_time_ms")
        if decision_time_ms is None:
            decision_time_ms = now_ms()
        else:
            decision_time_ms = _require_int(
                decision_time_ms, "decision_time_ms", "E_DECISION_INVALID", 0
            )

        with self.db.read_snapshot() as connection:
            batch_row = self._get_batch_row(connection, batch_id)
            batch = self._batch_dict(batch_row)
            policy = self._resolve_policy(connection, batch_row, policy_version)
            window = self._resolve_window(connection, batch_id, window_index)
            cooldown = self._cooldown_row(connection, batch["service"])
            pre_state = {
                "batch_id": batch_id,
                "batch_status": batch["status"],
                "window_epoch": batch["window_epoch"],
                "window_index": window["window_index"],
                "window_version": window["version"],
                "window_state": window["state"],
                "window_sample_count": window["sample_count"],
                "window_success_count": window["success_count"],
                "policy_version": batch["policy_version"],
                "cooldown_until_ms": cooldown["cooldown_until_ms"] if cooldown else None,
            }
        intent_payload = {
            "batch_id": batch_id,
            "mode": mode,
            "policy_version": policy.version,
            "window_index": window["window_index"],
            "window_version": window["version"],
            "decision_time_ms": decision_time_ms,
            "pre_state": pre_state,
        }
        intent_id = self._create_intent("RISK_DECISION", intent_payload)
        decision_id = f"dec_{uuid.uuid4().hex}"
        try:
            with self.db.transaction() as connection:
                batch_row = self._get_batch_row(connection, batch_id)
                if batch_row["window_epoch"] != batch["window_epoch"]:
                    raise BusinessError(
                        "E_CONFLICTING_REPORT",
                        "裁决期间窗口版本变化，拒绝混合版本迁移",
                        {
                            "batch_id": batch_id,
                            "expected_window_epoch": batch["window_epoch"],
                            "observed_window_epoch": batch_row["window_epoch"],
                        },
                    )
                policy = self._resolve_policy(connection, batch_row, policy.version)
                window = self._resolve_window(
                    connection, batch_id, window["window_index"]
                )
                if window["version"] != pre_state["window_version"]:
                    raise BusinessError(
                        "E_CONFLICTING_REPORT",
                        "裁决期间目标窗口版本变化，拒绝混合版本迁移",
                        {
                            "batch_id": batch_id,
                            "window_index": window["window_index"],
                            "expected_window_version": pre_state["window_version"],
                            "observed_window_version": window["version"],
                        },
                    )
                batch = self._batch_dict(batch_row)
                risk_payload = self._build_risk_payload(
                    connection=connection,
                    batch=batch,
                    policy=policy,
                    window=window,
                    mode=mode,
                    at_ms=decision_time_ms,
                )
                action = risk_payload["decision"]["action"]
                applied = False
                transition_events: list[dict[str, Any]] = []
                target_status: str | None = None
                cooldown_result: dict[str, Any] | None = None

                if mode == "FAST":
                    applied = False
                    target_status = None
                elif action == "AUTO_ROLLBACK":
                    cooldown = self._cooldown_row(connection, batch["service"])
                    if cooldown is not None and cooldown["cooldown_until_ms"] > decision_time_ms:
                        remaining = cooldown["cooldown_until_ms"] - decision_time_ms
                        raise BusinessError(
                            "E_ROLLBACK_COOLDOWN",
                            "服务处于自动回滚冷却期，拒绝重复触发",
                            {
                                "service": batch["service"],
                                "batch_id": batch_id,
                                "cooldown_until_ms": cooldown["cooldown_until_ms"],
                                "remaining_ms": remaining,
                                "decision_time_ms": decision_time_ms,
                            },
                        )
                    if batch["status"] != "OBSERVING":
                        raise BusinessError(
                            "E_INVALID_TRANSITION",
                            "当前批次状态不允许开始自动回滚",
                            {
                                "batch_id": batch_id,
                                "from_status": batch["status"],
                                "to_status": "ROLLING_BACK",
                                "allowed_from": ["OBSERVING"],
                            },
                        )
                    transition_events = [
                        {
                            "from_status": "OBSERVING",
                            "to_status": "ROLLING_BACK",
                            "action": "AUTO_ROLLBACK_START",
                        },
                        {
                            "from_status": "ROLLING_BACK",
                            "to_status": "ROLLED_BACK",
                            "action": "AUTO_ROLLBACK_COMPLETE",
                        },
                    ]
                    target_status = "ROLLED_BACK"
                    cooldown_until = decision_time_ms + policy.cooldown_ms
                    connection.execute(
                        "INSERT INTO cooldowns(service, cooldown_until_ms, source_batch_id, "
                        "policy_version, created_at_ms, updated_at_ms) VALUES (?, ?, ?, ?, ?, ?) "
                        "ON CONFLICT(service) DO UPDATE SET cooldown_until_ms=excluded.cooldown_until_ms, "
                        "source_batch_id=excluded.source_batch_id, policy_version=excluded.policy_version, "
                        "updated_at_ms=excluded.updated_at_ms",
                        (
                            batch["service"],
                            cooldown_until,
                            batch_id,
                            policy.version,
                            decision_time_ms,
                            decision_time_ms,
                        ),
                    )
                    cooldown_result = {
                        "service": batch["service"],
                        "active": cooldown_until > decision_time_ms,
                        "cooldown_until_ms": cooldown_until,
                        "remaining_ms": max(0, cooldown_until - decision_time_ms),
                        "source_batch_id": batch_id,
                        "policy_version": policy.version,
                    }
                    applied = True
                else:
                    if action == "RELEASE":
                        desired = "PASSED"
                    elif action == "ESCALATE":
                        desired = "ESCALATED"
                    else:
                        desired = None
                    if desired is not None:
                        if batch["status"] != "OBSERVING":
                            raise BusinessError(
                                "E_INVALID_TRANSITION",
                                "当前批次状态不允许该迁移",
                                {
                                    "batch_id": batch_id,
                                    "from_status": batch["status"],
                                    "to_status": desired,
                                    "allowed_from": ["OBSERVING"],
                                },
                            )
                        transition_events = [
                            {
                                "from_status": "OBSERVING",
                                "to_status": desired,
                                "action": action,
                            }
                        ]
                        target_status = desired
                        applied = True
                    else:
                        applied = False
                        target_status = None

                details = {
                    **risk_payload,
                    "intent_id": intent_id,
                    "decision_id": decision_id,
                    "decision_time_ms": decision_time_ms,
                    "applied": applied,
                    "target_status": target_status,
                    "transition_events": transition_events,
                    "cooldown": cooldown_result,
                    "version_guard": {
                        "policy_version": policy.version,
                        "policy_digest": policy.digest,
                        "window_epoch": batch["window_epoch"],
                        "window_index": window["window_index"],
                        "window_version": window["version"],
                        "mixed_version_transition_forbidden": True,
                    },
                }
                connection.execute(
                    "INSERT INTO decisions(decision_id, batch_id, mode, policy_version, window_index, "
                    "window_version, window_epoch, decision_time_ms, risk_score_micros, risk_score, "
                    "risk_level, action, applied, details_json, created_at_ms) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        decision_id,
                        batch_id,
                        mode,
                        policy.version,
                        window["window_index"],
                        window["version"],
                        batch["window_epoch"],
                        decision_time_ms,
                        fixed_micros(Decimal(risk_payload["risk_score"])),
                        risk_payload["risk_score"],
                        risk_payload["risk_level"],
                        action,
                        int(applied),
                        json_dumps(details),
                        decision_time_ms,
                    ),
                )
                if applied and target_status is not None:
                    for event in transition_events:
                        connection.execute(
                            "INSERT INTO batch_transitions(batch_id, from_status, to_status, action, "
                            "policy_version, window_index, window_version, window_epoch, risk_score, decision_id, at_ms) "
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            (
                                batch_id,
                                event["from_status"],
                                event["to_status"],
                                event["action"],
                                policy.version,
                                window["window_index"],
                                window["version"],
                                batch["window_epoch"],
                                risk_payload["risk_score"],
                                decision_id,
                                decision_time_ms,
                            ),
                        )
                    updated = connection.execute(
                        "UPDATE batches SET status=?, policy_version=?, row_version=row_version+1, "
                        "updated_at_ms=? WHERE batch_id=? AND window_epoch=? AND status=?",
                        (
                            target_status,
                            policy.version,
                            decision_time_ms,
                            batch_id,
                            batch["window_epoch"],
                            "OBSERVING",
                        ),
                    )
                    if updated.rowcount != 1:
                        raise BusinessError(
                            "E_INVALID_TRANSITION",
                            "状态迁移版本守卫失败，事务已回滚",
                            {
                                "batch_id": batch_id,
                                "expected_window_epoch": batch["window_epoch"],
                                "target_status": target_status,
                            },
                        )
                elif mode == "FULL" and policy.version != batch["policy_version"]:
                    connection.execute(
                        "UPDATE batches SET policy_version=?, row_version=row_version+1, updated_at_ms=? "
                        "WHERE batch_id=? AND window_epoch=?",
                        (policy.version, decision_time_ms, batch_id, batch["window_epoch"]),
                    )

                # Deterministic hard-crash injection: all business writes for a
                # rollback are present in the open transaction, but COMMIT has
                # not happened. The separately committed intent remains PENDING.
                if mode == "FULL" and action == "AUTO_ROLLBACK":
                    self.faults.maybe_crash("RISK_DECISION_ROLLBACK", batch_id)
                elif mode == "FULL":
                    self.faults.maybe_crash("RISK_DECISION", batch_id)
                self._mark_intent_committed(connection, intent_id)
                batch_after = self._batch_dict(self._get_batch_row(connection, batch_id))
            return {
                "decision_id": decision_id,
                "intent_id": intent_id,
                "batch_id": batch_id,
                "mode": mode,
                "applied": applied,
                "action": action,
                "target_status": target_status,
                "policy_version": policy.version,
                "policy_digest": policy.digest,
                "window_versions": details["version_guard"],
                "risk": risk_payload,
                "cooldown": cooldown_result
                if cooldown_result is not None
                else self.cooldown_status(batch["service"], at_ms=decision_time_ms),
                "batch": batch_after,
                "state_trajectory": self._trajectory(batch_id),
            }
        except BaseException as error:
            self._mark_intent_aborted(intent_id, error)
            raise

    def _trajectory(self, batch_id: str) -> list[dict[str, Any]]:
        with self.db.read_snapshot() as connection:
            rows = connection.execute(
                "SELECT from_status, to_status, action, policy_version, window_index, "
                "window_version, window_epoch, risk_score, at_ms FROM batch_transitions "
                "WHERE batch_id=? ORDER BY id",
                (batch_id,),
            ).fetchall()
        return rows_to_dicts(rows)

    def list_decisions(self, batch_id: str, limit: int = 20) -> list[dict[str, Any]]:
        limit = max(1, min(100, limit))
        with self.db.read_snapshot() as connection:
            self._get_batch_row(connection, batch_id)
            rows = connection.execute(
                "SELECT * FROM decisions WHERE batch_id=? ORDER BY created_at_ms DESC LIMIT ?",
                (batch_id, limit),
            ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["details"] = json.loads(item.pop("details_json"))
            result.append(item)
        return result

    # ------------------------------------------------------------------
    # Recovery
    # ------------------------------------------------------------------
    def recover_pending_intents(self) -> dict[str, Any]:
        run_id = f"rec_{uuid.uuid4().hex}"
        started_at = now_ms()
        with self.db.read_snapshot() as connection:
            pending_rows = connection.execute(
                "SELECT * FROM intents WHERE status='PENDING' ORDER BY created_at_ms"
            ).fetchall()
            policies = connection.execute("SELECT COUNT(*) FROM policies").fetchone()[0]
            batches = connection.execute("SELECT COUNT(*) FROM batches").fetchone()[0]
            decisions = connection.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
            transitions = connection.execute("SELECT COUNT(*) FROM batch_transitions").fetchone()[0]
        entries: list[dict[str, Any]] = []
        aborted = 0
        mismatches = 0
        for row in pending_rows:
            try:
                payload = json.loads(row["payload_json"])
            except Exception:
                payload = {}
            pre_state = payload.get("pre_state") or {}
            batch_id = payload.get("batch_id")
            observed: dict[str, Any] = {}
            boundary_match = True
            if batch_id:
                with self.db.read_snapshot() as connection:
                    batch_row = connection.execute(
                        "SELECT * FROM batches WHERE batch_id=?", (batch_id,)
                    ).fetchone()
                    if batch_row is None:
                        observed = {"batch_exists": False}
                        boundary_match = pre_state.get("exists") is False or not pre_state
                    else:
                        cooldown = self._cooldown_row(connection, batch_row["service"])
                        window_row = None
                        if pre_state.get("window_index") is not None:
                            window_row = connection.execute(
                                "SELECT * FROM windows WHERE batch_id=? AND window_index=?",
                                (batch_id, pre_state["window_index"]),
                            ).fetchone()
                        observed = {
                            "batch_exists": True,
                            "batch_status": batch_row["status"],
                            "window_epoch": batch_row["window_epoch"],
                            "policy_version": batch_row["policy_version"],
                            "cooldown_until_ms": cooldown["cooldown_until_ms"]
                            if cooldown is not None
                            else None,
                            "window_version": window_row["version"]
                            if window_row is not None
                            else None,
                            "window_state": window_row["state"]
                            if window_row is not None
                            else None,
                            "window_sample_count": window_row["sample_count"]
                            if window_row is not None
                            else None,
                            "window_success_count": window_row["success_count"]
                            if window_row is not None
                            else None,
                        }
                        for key in (
                            "batch_status",
                            "window_epoch",
                            "policy_version",
                            "cooldown_until_ms",
                            "window_version",
                            "window_state",
                            "window_sample_count",
                            "window_success_count",
                        ):
                            if key in pre_state and pre_state[key] != observed.get(key):
                                boundary_match = False
            if not boundary_match:
                mismatches += 1
            at = now_ms()
            error_json = json_dumps(
                {
                    "code": "E_RECOVERED_ABORTED_INTENT",
                    "message": "启动恢复扫描发现未提交意图，标记为已中止",
                    "context": {
                        "run_id": run_id,
                        "boundary_match": boundary_match,
                    },
                }
            )
            with self.db.transaction() as connection:
                connection.execute(
                    "UPDATE intents SET status='ABORTED', updated_at_ms=?, completed_at_ms=?, "
                    "error_json=?, recovery_run_id=? WHERE intent_id=? AND status='PENDING'",
                    (at, at, error_json, run_id, row["intent_id"]),
                )
            aborted += 1
            entries.append(
                {
                    "intent_id": row["intent_id"],
                    "operation": row["operation"],
                    "created_at_ms": row["created_at_ms"],
                    "batch_id": batch_id,
                    "status_after_scan": "ABORTED",
                    "uncommitted_work_observed": False,
                    "boundary_match": boundary_match,
                    "last_committed_boundary": pre_state,
                    "observed_persisted_state": observed,
                    "note": (
                        "未完成意图的业务写入未提交；SQLite 已回到最后完整提交边界。"
                        if boundary_match
                        else "观测状态与此前记录边界不一致，已保守标记中止，请基于审计日志继续核查。"
                    ),
                }
            )
        completed_at = now_ms()
        report = {
            "run_id": run_id,
            "started_at_ms": started_at,
            "completed_at_ms": completed_at,
            "pending_found": len(pending_rows),
            "aborted_count": aborted,
            "reconciled_count": 0,
            "boundary_mismatch_count": mismatches,
            "persisted_fact_counts": {
                "policies": policies,
                "batches": batches,
                "decisions": decisions,
                "transitions": transitions,
            },
            "intents": entries,
            "recovery_basis": "SQLite WAL committed snapshot + PENDING intent scan",
            "forced_exit_report_available": False,
            "caveat": (
                "恢复报告只描述数据库扫描时已持久化的事实。强制终止前未提交的操作"
                "不保证存在进程内退出报告；本报告不声称能重建未落盘的内存状态。"
            ),
            "generated_at_ms": completed_at,
        }
        with self.db.transaction() as connection:
            connection.execute(
                "INSERT INTO recovery_reports(run_id, started_at_ms, completed_at_ms, pending_count, "
                "aborted_count, reconciled_count, report_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    run_id,
                    started_at,
                    completed_at,
                    len(pending_rows),
                    aborted,
                    0,
                    json_dumps(report),
                ),
            )
        return report

    def latest_recovery_report(self) -> dict[str, Any] | None:
        with self.db.read_snapshot() as connection:
            row = connection.execute(
                "SELECT report_json FROM recovery_reports ORDER BY id DESC LIMIT 1"
            ).fetchone()
        return json.loads(row["report_json"]) if row is not None else None

    def recovery_reports(self, limit: int = 20) -> list[dict[str, Any]]:
        limit = max(1, min(100, limit))
        with self.db.read_snapshot() as connection:
            rows = connection.execute(
                "SELECT report_json FROM recovery_reports ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [json.loads(row["report_json"]) for row in rows]

    def health(self) -> dict[str, Any]:
        with self.db.read_snapshot() as connection:
            connection.execute("SELECT 1").fetchone()
            policy_count = connection.execute("SELECT COUNT(*) FROM policies").fetchone()[0]
            batch_count = connection.execute("SELECT COUNT(*) FROM batches").fetchone()[0]
            pending_intents = connection.execute(
                "SELECT COUNT(*) FROM intents WHERE status='PENDING'"
            ).fetchone()[0]
        return {
            "status": "ok",
            "database_path": str(self.db.path),
            "policy_count": policy_count,
            "batch_count": batch_count,
            "pending_intents": pending_intents,
            "fault_injection_enabled": self.faults.enabled,
            "now_ms": now_ms(),
        }
