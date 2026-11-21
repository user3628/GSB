from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from .errors import (
    BusinessError,
    normalization_required,
    normalization_unknown,
    report_invalid,
)
from .numerics import (
    PROGRESS_SCALE,
    digest_payload,
    parse_nonnegative_int,
    quantize_decimal,
    decimal_value,
)


MAX_SIGNED_64 = 2**63 - 1
MAX_REPLICAS = 1024
NORMALIZATION_MODES = ("ALL_EQUAL", "REPORTED_RATIO", "MISSING_ZERO")
ROUND_STATUSES = ("OPEN", "AGGREGATING", "COMMITTED", "ABORTED")


def parse_timestamp(value: Any, *, field: str, code: str = "E_REPORT_INVALID") -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise BusinessError(
            code,
            f"{field} 必须是 ISO-8601 时间字符串",
            http_status=422,
            details={"field": field, "value": value},
        )
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise BusinessError(
            code,
            f"{field} 不是合法 ISO-8601 时间",
            http_status=422,
            details={"field": field, "value": value},
        ) from exc
    if parsed.tzinfo is None:
        raise BusinessError(
            code,
            f"{field} 必须包含时区",
            http_status=422,
            details={"field": field, "value": value},
        )
    return parsed.astimezone(timezone.utc)


def format_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def require_text(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise BusinessError(
            "E_REQUEST_INVALID",
            f"{field} 必须是非空字符串",
            http_status=400,
            details={"field": field, "value": value},
        )
    text = value.strip()
    if len(text) > 128:
        raise BusinessError(
            "E_REQUEST_INVALID",
            f"{field} 长度不能超过 128",
            http_status=400,
            details={"field": field},
        )
    return text


def parse_positive_int(value: Any, *, field: str, maximum: int = MAX_SIGNED_64) -> int:
    parsed = parse_nonnegative_int(value, field=field, upper=maximum)
    if parsed <= 0:
        raise BusinessError(
            "E_REQUEST_INVALID",
            f"{field} 必须为正整数",
            http_status=400,
            details={"field": field, "value": value},
        )
    return parsed


@dataclass(frozen=True)
class ReplicaGroup:
    group_id: str
    version: int
    replica_count: int
    block_size_bytes: int
    weights: dict[str, Decimal]
    created_at: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "group_id": self.group_id,
            "version": self.version,
            "replica_count": self.replica_count,
            "block_size_bytes": self.block_size_bytes,
            "weights": {
                replica_id: str(weight)
                for replica_id, weight in sorted(self.weights.items())
            },
        }


def parse_group(payload: Any) -> ReplicaGroup:
    if not isinstance(payload, dict):
        raise BusinessError(
            "E_REQUEST_INVALID",
            "副本组请求必须是 JSON 对象",
            http_status=400,
        )
    group_id = require_text(payload.get("group_id"), field="group_id")
    version = parse_positive_int(payload.get("version"), field="version")
    replica_count = parse_positive_int(
        payload.get("replica_count"),
        field="replica_count",
        maximum=MAX_REPLICAS,
    )
    block_size_bytes = parse_positive_int(
        payload.get("block_size_bytes"),
        field="block_size_bytes",
    )
    weights_payload = payload.get("weights")
    if not isinstance(weights_payload, dict):
        raise BusinessError(
            "E_REQUEST_INVALID",
            "weights 必须是 replica_id 到数值的 JSON 对象",
            http_status=400,
        )
    if len(weights_payload) != replica_count:
        raise BusinessError(
            "E_GROUP_INVALID",
            "weights 条目数必须与 replica_count 一致",
            http_status=422,
            details={
                "replica_count": replica_count,
                "weight_count": len(weights_payload),
            },
        )
    weights: dict[str, Decimal] = {}
    for raw_replica_id, raw_weight in weights_payload.items():
        replica_id = require_text(raw_replica_id, field="weights key")
        if raw_replica_id != replica_id:
            raise BusinessError(
                "E_GROUP_INVALID",
                "副本标识不允许首尾空白",
                http_status=422,
                details={"replica_id": raw_replica_id},
            )
        try:
            weight = decimal_value(raw_weight, field=f"weights.{replica_id}")
        except BusinessError as exc:
            raise BusinessError(
                "E_GROUP_INVALID",
                exc.message,
                http_status=422,
                details=exc.details,
            ) from exc
        if weight < 0:
            raise BusinessError(
                "E_GROUP_INVALID",
                "副本权重不能为负数",
                http_status=422,
                details={"replica_id": replica_id, "weight": str(weight)},
            )
        weights[replica_id] = weight
    if len(weights) != replica_count:
        raise BusinessError(
            "E_GROUP_INVALID",
            "副本标识必须唯一",
            http_status=422,
        )
    if sum(weights.values(), Decimal(0)) <= 0:
        raise BusinessError(
            "E_GROUP_INVALID",
            "副本权重合计必须为正",
            http_status=422,
        )
    return ReplicaGroup(
        group_id=group_id,
        version=version,
        replica_count=replica_count,
        block_size_bytes=block_size_bytes,
        weights={key: weights[key] for key in sorted(weights)},
        created_at="",
    )


@dataclass(frozen=True)
class RoundSpec:
    round_id: str
    group_id: str
    group_version: int
    deadline: datetime
    target_progress_raw: Decimal
    target_progress: Decimal
    target_rounding_error: Decimal
    relative_tolerance: Decimal

    def as_dict(self) -> dict[str, Any]:
        return {
            "round_id": self.round_id,
            "group_id": self.group_id,
            "group_version": self.group_version,
            "deadline": format_timestamp(self.deadline),
            "target_progress": str(self.target_progress),
            "target_progress_raw": str(self.target_progress_raw),
            "target_progress_scale": PROGRESS_SCALE,
            "relative_tolerance": str(self.relative_tolerance),
        }


def parse_round(payload: Any) -> RoundSpec:
    if not isinstance(payload, dict):
        raise BusinessError(
            "E_REQUEST_INVALID",
            "轮次请求必须是 JSON 对象",
            http_status=400,
        )
    round_id = require_text(payload.get("round_id"), field="round_id")
    group_id = require_text(payload.get("group_id"), field="group_id")
    group_version = parse_positive_int(payload.get("group_version"), field="group_version")
    deadline = parse_timestamp(
        payload.get("deadline"),
        field="deadline",
        code="E_ROUND_INVALID",
    )
    try:
        target_raw = decimal_value(payload.get("target_progress"), field="target_progress")
    except BusinessError as exc:
        raise BusinessError(
            "E_ROUND_INVALID",
            exc.message,
            http_status=422,
            details=exc.details,
        ) from exc
    if target_raw < 0 or target_raw > 1:
        raise BusinessError(
            "E_ROUND_INVALID",
            "target_progress 必须位于 [0,1]",
            http_status=422,
            details={"target_progress": str(target_raw)},
        )
    try:
        tolerance = decimal_value(
            payload.get("relative_tolerance"), field="relative_tolerance"
        )
    except BusinessError as exc:
        raise BusinessError(
            "E_ROUND_INVALID",
            exc.message,
            http_status=422,
            details=exc.details,
        ) from exc
    if tolerance < 0 or tolerance > 1:
        raise BusinessError(
            "E_ROUND_INVALID",
            "relative_tolerance 必须位于 [0,1]",
            http_status=422,
            details={"relative_tolerance": str(tolerance)},
        )
    target, target_error = quantize_decimal(target_raw, PROGRESS_SCALE)
    return RoundSpec(
        round_id=round_id,
        group_id=group_id,
        group_version=group_version,
        deadline=deadline,
        target_progress_raw=target_raw,
        target_progress=target,
        target_rounding_error=target_error,
        relative_tolerance=tolerance,
    )


@dataclass(frozen=True)
class ReportInput:
    report_id: str
    replica_id: str
    reported_at: datetime
    block_count: int
    synced_bytes: int
    digest: str

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "report_id": self.report_id,
            "replica_id": self.replica_id,
            "reported_at": format_timestamp(self.reported_at),
            "block_count": self.block_count,
            "synced_bytes": self.synced_bytes,
        }


def parse_report(
    payload: Any,
    *,
    group: ReplicaGroup,
    index: int,
) -> ReportInput:
    prefix = f"reports[{index}]"
    if not isinstance(payload, dict):
        raise report_invalid(f"{prefix} 必须是 JSON 对象")
    try:
        report_id = require_text(payload.get("report_id"), field=f"{prefix}.report_id")
        replica_id = require_text(payload.get("replica_id"), field=f"{prefix}.replica_id")
        if replica_id not in group.weights:
            raise report_invalid(
                f"{prefix}.replica_id 不属于副本组 {group.group_id}",
                {"replica_id": replica_id, "group_id": group.group_id},
            )
        reported_at = parse_timestamp(
            payload.get("reported_at"),
            field=f"{prefix}.reported_at",
        )
        block_count = parse_nonnegative_int(
            payload.get("block_count"),
            field=f"{prefix}.block_count",
            upper=MAX_SIGNED_64,
        )
        synced_bytes = parse_nonnegative_int(
            payload.get("synced_bytes"),
            field=f"{prefix}.synced_bytes",
            upper=MAX_SIGNED_64,
        )
    except BusinessError as exc:
        details = dict(exc.details)
        details.setdefault("index", index)
        raise BusinessError(
            "E_REPORT_INVALID",
            exc.message,
            http_status=422,
            details=details,
        ) from exc
    report = ReportInput(
        report_id=report_id,
        replica_id=replica_id,
        reported_at=reported_at,
        block_count=block_count,
        synced_bytes=synced_bytes,
        digest="",
    )
    digest = digest_payload(
        {
            "group_id": group.group_id,
            **report.canonical_payload(),
        }
    )
    return ReportInput(
        report_id=report.report_id,
        replica_id=report.replica_id,
        reported_at=report.reported_at,
        block_count=report.block_count,
        synced_bytes=report.synced_bytes,
        digest=digest,
    )


def parse_report_batch(
    payload: Any,
    *,
    group: ReplicaGroup,
) -> list[ReportInput]:
    if not isinstance(payload, dict):
        raise report_invalid("上报请求必须是 JSON 对象")
    reports_payload = payload.get("reports")
    if not isinstance(reports_payload, list) or not reports_payload:
        raise report_invalid("reports 必须是非空数组")
    reports: list[ReportInput] = []
    seen_ids: set[str] = set()
    seen_replicas: set[str] = set()
    for index, item in enumerate(reports_payload):
        report = parse_report(item, group=group, index=index)
        if report.report_id in seen_ids:
            raise report_invalid(
                "同一批次中 report_id 不得重复",
                {"index": index, "report_id": report.report_id},
            )
        seen_ids.add(report.report_id)
        if report.replica_id in seen_replicas:
            raise report_invalid(
                "同一批次中同一副本只能出现一次",
                {"index": index, "replica_id": report.replica_id},
            )
        seen_replicas.add(report.replica_id)
        reports.append(report)
    return reports


def parse_aggregate_request(payload: Any) -> dict[str, Any]:
    if payload is None:
        payload = {}
    if not isinstance(payload, dict):
        raise BusinessError(
            "E_REQUEST_INVALID",
            "聚合请求必须是 JSON 对象",
            http_status=400,
        )
    if "normalization" not in payload:
        raise normalization_required()
    mode = payload.get("normalization")
    if mode not in NORMALIZATION_MODES:
        raise normalization_unknown(mode)
    strict_wait = payload.get("strict_wait", False)
    if not isinstance(strict_wait, bool):
        raise BusinessError(
            "E_REQUEST_INVALID",
            "strict_wait 必须是布尔值",
            http_status=400,
        )
    grace_ms = payload.get("grace_ms", 0)
    if isinstance(grace_ms, bool) or not isinstance(grace_ms, int):
        raise BusinessError(
            "E_REQUEST_INVALID",
            "grace_ms 必须是整数",
            http_status=400,
        )
    if grace_ms < 0 or grace_ms > 30_000:
        raise BusinessError(
            "E_REQUEST_INVALID",
            "grace_ms 必须位于 [0,30000]",
            http_status=400,
        )
    max_wait_ms = payload.get("max_wait_ms", 30_000)
    if isinstance(max_wait_ms, bool) or not isinstance(max_wait_ms, int):
        raise BusinessError(
            "E_REQUEST_INVALID",
            "max_wait_ms 必须是整数",
            http_status=400,
        )
    if max_wait_ms < 0 or max_wait_ms > 120_000:
        raise BusinessError(
            "E_REQUEST_INVALID",
            "max_wait_ms 必须位于 [0,120000]",
            http_status=400,
        )
    return {
        "normalization": mode,
        "strict_wait": strict_wait,
        "grace_ms": grace_ms,
        "max_wait_ms": max_wait_ms,
    }

