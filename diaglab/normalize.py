"""Time, unit and invariant normalization for decoded diagnostic records."""

from __future__ import annotations

from collections import Counter
from decimal import Decimal

from .errors import frame_corrupt, unit_invalid
from .models import (
    FAILURE_TYPES,
    PROTOCOLS,
    TIMESTAMP_POLICIES,
    UINT32_MAX,
    UINT64_MAX,
    DecodedBundle,
    DiagnosticRecord,
    NormalizationResult,
    RawRecord,
)


RTT_FACTORS_TO_US = {
    "us": 1,
    "µs": 1,
    "microsecond": 1,
    "microseconds": 1,
    "ms": 1_000,
    "millisecond": 1_000,
    "milliseconds": 1_000,
    "s": 1_000_000,
    "sec": 1_000_000,
    "second": 1_000_000,
    "seconds": 1_000_000,
}

PAYLOAD_FACTORS_TO_BYTES = {
    "b": 1,
    "byte": 1,
    "bytes": 1,
    "kb": 1_000,
    "mb": 1_000_000,
    "kib": 1_024,
    "mib": 1_048_576,
}


def _as_integer(value: object, *, field: str, ordinal: int) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise unit_invalid(
            "量纲字段必须为整数",
            field=field,
            ordinal=ordinal,
            value=value,
        )
    return value


def _convert_nonnegative(
    value: int,
    unit: str,
    factors: dict[str, int],
    *,
    field: str,
    ordinal: int,
) -> int:
    if value < 0:
        raise unit_invalid(
            "量纲字段不得为负数",
            field=field,
            ordinal=ordinal,
            value=value,
            unit=unit,
        )
    key = unit.strip().lower()
    factor = factors.get(key)
    if factor is None:
        raise unit_invalid(
            "不支持的量纲单位",
            field=field,
            ordinal=ordinal,
            unit=unit,
            supported_units=sorted(set(factors)),
        )
    converted = value * factor
    if converted > UINT32_MAX:
        raise unit_invalid(
            "量纲换算后超过内部 u32 上限，整批拒绝",
            field=field,
            ordinal=ordinal,
            source_value=value,
            source_unit=unit,
            converted_value=converted,
            max_value=UINT32_MAX,
        )
    return converted


def _normalize_timestamp(
    value: int,
    unit: str,
    policy: str,
    *,
    ordinal: int,
) -> tuple[int, int, int, int]:
    """Return (timestamp_ms, rounding_bit, sub_ms_remainder_us, error_us)."""

    if value < 0:
        raise unit_invalid(
            "时间戳不得为负数",
            field="timestamp",
            ordinal=ordinal,
            value=value,
            unit=unit,
        )
    if unit == "ms":
        if value > UINT64_MAX:
            raise unit_invalid(
                "毫秒时间戳超过内部 u64 上限",
                field="timestamp",
                ordinal=ordinal,
                value=value,
                max_value=UINT64_MAX,
            )
        return value, 0, 0, 0
    if unit != "us":
        raise unit_invalid("未知时间戳单位", ordinal=ordinal, unit=unit)
    if value > UINT64_MAX * 1_000:
        raise unit_invalid(
            "微秒时间戳换算后超过内部 u64 上限",
            field="timestamp",
            ordinal=ordinal,
            value=value,
        )
    remainder = value % 1_000
    if policy in {"truncate", "trunc", "floor"}:
        timestamp_ms = value // 1_000
    elif policy == "ceil":
        timestamp_ms = (value + 999) // 1_000
    elif policy in {"half_up", "round"}:
        timestamp_ms = (value + 500) // 1_000
    else:
        raise unit_invalid("未知时间戳舍入规则", policy=policy)
    if timestamp_ms > UINT64_MAX:
        raise unit_invalid(
            "归一化后的毫秒时间戳超过内部 u64 上限",
            field="timestamp",
            ordinal=ordinal,
            value=timestamp_ms,
        )
    error_us = timestamp_ms * 1_000 - value
    return timestamp_ms, int(remainder != 0), remainder, error_us


def _validate_record_text(record: RawRecord, ordinal: int) -> None:
    for field, value in (("probe_id", record.probe_id), ("path_node", record.path_node)):
        if not value or not value.strip():
            raise frame_corrupt("文本字段不得为空", ordinal=ordinal, field=field)
        if len(value.encode("utf-8")) > 0xFFFF:
            raise frame_corrupt("文本字段超过 u16 长度上限", ordinal=ordinal, field=field)
    if record.protocol not in PROTOCOLS:
        raise frame_corrupt(
            "未知探测协议",
            ordinal=ordinal,
            protocol=record.protocol,
            supported=sorted(PROTOCOLS),
        )
    if record.failure_type not in FAILURE_TYPES:
        raise frame_corrupt(
            "未知失败类型",
            ordinal=ordinal,
            failure_type=record.failure_type,
            supported=sorted(FAILURE_TYPES),
        )
    if record.success and record.failure_type != "none":
        raise frame_corrupt(
            "成功探测不能携带失败类型",
            ordinal=ordinal,
            failure_type=record.failure_type,
        )


def normalize_bundle(bundle: DecodedBundle, timestamp_policy: str = "truncate") -> NormalizationResult:
    if timestamp_policy not in TIMESTAMP_POLICIES:
        raise unit_invalid(
            "未知时间戳截断规则",
            policy=timestamp_policy,
            supported=sorted(TIMESTAMP_POLICIES),
        )

    normalized: list[DiagnosticRecord] = []
    audit_rows: list[dict[str, object]] = []
    missing_rtt: list[int] = []
    missing_payload: list[int] = []
    adjusted_failures: list[int] = []
    rtt_units: Counter[str] = Counter()
    payload_units: Counter[str] = Counter()
    rtt_factors: set[int] = set()
    payload_factors: set[int] = set()
    cumulative_error_us = 0

    for ordinal, raw in enumerate(bundle.records):
        _validate_record_text(raw, ordinal)
        timestamp_ms, rounding_bit, remainder_us, error_us = _normalize_timestamp(
            raw.timestamp_value,
            raw.timestamp_unit,
            timestamp_policy,
            ordinal=ordinal,
        )

        rtt_missing = raw.rtt_value is None
        payload_missing = raw.payload_value is None
        if rtt_missing:
            missing_rtt.append(ordinal)
            rtt_us = 0
            rtt_factor = 1
            rtt_unit = "default"
        else:
            rtt_value = _as_integer(raw.rtt_value, field="rtt", ordinal=ordinal)
            rtt_us = _convert_nonnegative(
                rtt_value,
                raw.rtt_unit,
                RTT_FACTORS_TO_US,
                field="rtt",
                ordinal=ordinal,
            )
            rtt_unit = raw.rtt_unit
            rtt_factor = RTT_FACTORS_TO_US.get(raw.rtt_unit.strip().lower(), 0)
            rtt_units[rtt_unit] += 1
            rtt_factors.add(rtt_factor)

        if payload_missing:
            missing_payload.append(ordinal)
            payload_bytes = 0
            payload_factor = 1
            payload_unit = "default"
        else:
            payload_value = _as_integer(raw.payload_value, field="payload", ordinal=ordinal)
            payload_bytes = _convert_nonnegative(
                payload_value,
                raw.payload_unit,
                PAYLOAD_FACTORS_TO_BYTES,
                field="payload",
                ordinal=ordinal,
            )
            payload_unit = raw.payload_unit
            payload_factor = PAYLOAD_FACTORS_TO_BYTES.get(raw.payload_unit.strip().lower(), 0)
            payload_units[payload_unit] += 1
            payload_factors.add(payload_factor)

        failure_type = raw.failure_type
        if not raw.success and failure_type == "none":
            failure_type = "error"
            adjusted_failures.append(ordinal)

        normalized.append(
            DiagnosticRecord(
                timestamp_ms=timestamp_ms,
                probe_id=raw.probe_id,
                path_node=raw.path_node,
                protocol=raw.protocol,
                success=raw.success,
                rtt_us=rtt_us,
                payload_bytes=payload_bytes,
                failure_type=failure_type,
                ordinal=ordinal,
                source_format=bundle.report.format,
                source_version=bundle.report.version,
                rounding_bit=rounding_bit,
                rounding_remainder_us=remainder_us,
                rounding_error_us=error_us,
                rtt_missing=rtt_missing,
                payload_missing=payload_missing,
            )
        )
        cumulative_error_us += error_us
        audit_rows.append(
            {
                "ordinal": ordinal,
                "source_timestamp": raw.timestamp_value,
                "source_timestamp_unit": raw.timestamp_unit,
                "timestamp_ms": timestamp_ms,
                "sub_ms_remainder_us": remainder_us,
                "rounding_bit": rounding_bit,
                "rounding_error_us": error_us,
                "rtt_source_value": raw.rtt_value,
                "rtt_source_unit": rtt_unit,
                "rtt_us": rtt_us,
                "rtt_missing": rtt_missing,
                "payload_source_value": raw.payload_value,
                "payload_source_unit": payload_unit,
                "payload_bytes": payload_bytes,
                "payload_missing": payload_missing,
            }
        )

    policy_note = {
        "truncate": "向零截断到毫秒；非负时间戳等同 floor",
        "trunc": "truncate 的别名；向零截断到毫秒",
        "floor": "向下取整到毫秒",
        "ceil": "向上取整到毫秒",
        "half_up": "亚毫秒部分 >= 500µs 时进位，否则舍去",
        "round": "half_up 的别名；亚毫秒部分 >= 500µs 时进位",
    }[timestamp_policy]
    precision = Decimal(cumulative_error_us) / Decimal(1_000)
    report = {
        "record_count": len(normalized),
        "timestamp_policy": timestamp_policy,
        "timestamp_policy_note": policy_note,
        "rounding": {
            "unit": "us",
            "sub_ms_remainder_field": "sub_ms_remainder_us",
            "rounding_bit_definition": "源时间戳存在非零亚毫秒部分时为 1，否则为 0",
            "records_with_sub_ms": sum(1 for row in audit_rows if row["rounding_bit"]),
            "cumulative_error_us": cumulative_error_us,
            "cumulative_error_ms": f"{precision:.3f}",
            "note": "累计误差按每条记录 (归一化毫秒×1000 - 源微秒) 的整数和计算，无浮点累计误差。",
        },
        "unit_conversion": {
            "internal_rtt_unit": "us",
            "internal_payload_unit": "B",
            "rtt_source_units": dict(sorted(rtt_units.items())),
            "payload_source_units": dict(sorted(payload_units.items())),
            "rtt_factors_to_us": sorted(rtt_factors),
            "payload_factors_to_bytes": sorted(payload_factors),
            "overflow_limit": UINT32_MAX,
            "negative_values": "rejected",
        },
        "degradation": {
            "missing_rtt_records": missing_rtt,
            "missing_payload_records": missing_payload,
            "defaults": {"rtt_us": 0, "payload_bytes": 0},
            "missing_rtt_count": len(missing_rtt),
            "missing_payload_count": len(missing_payload),
            "failure_type_defaulted_records": adjusted_failures,
            "note": "缺省 RTT=0 不计入有效分位/抖动样本。",
        },
        "records": audit_rows,
    }
    return NormalizationResult(records=normalized, report=report)
