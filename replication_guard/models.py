from __future__ import annotations

import math
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

from .errors import policy_invalid
from .versions import parse_version


STANDARD_ACTIONS = {
    "NONE",
    "MONITOR",
    "THROTTLE_WRITES",
    "READ_ONLY",
    "FAILOVER",
    "PAUSE_REPLICATION",
}

ACTION_ALIASES = {
    "ALERT": "MONITOR",
    "ALERT_ONLY": "MONITOR",
    "THROTTLE": "THROTTLE_WRITES",
    "DEGRADE_READ_ONLY": "READ_ONLY",
    "DEGRADE_TO_READ_ONLY": "READ_ONLY",
    "SWITCHOVER": "FAILOVER",
}


def now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _first(raw: Mapping[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in raw and raw[key] is not None:
            return raw[key]
    return default


def as_non_negative_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or value is None:
        raise policy_invalid(field, "必须为非负整数")
    if isinstance(value, int):
        number = value
    elif isinstance(value, float):
        if not math.isfinite(value) or not value.is_integer():
            raise policy_invalid(field, "必须为非负整数")
        number = int(value)
    elif isinstance(value, str):
        text = value.strip()
        if not text or not text.isdigit():
            raise policy_invalid(field, "必须为非负整数")
        number = int(text)
    else:
        raise policy_invalid(field, "必须为非负整数")
    if number < 0:
        raise policy_invalid(field, "必须为非负整数")
    return number


def parse_time_ms(value: Any, field: str, *, default: int | None = None) -> int:
    if value is None:
        if default is None:
            raise policy_invalid(field, "缺少毫秒时间")
        return default
    if isinstance(value, bool):
        raise policy_invalid(field, "必须为毫秒整数或 ISO-8601 时间")
    if isinstance(value, (int, float)):
        if isinstance(value, float) and not value.is_integer():
            raise policy_invalid(field, "毫秒时间必须是整数")
        number = int(value)
        if number < 0:
            raise policy_invalid(field, "毫秒时间不能为负")
        return number
    if isinstance(value, str):
        text = value.strip()
        if text.isdigit():
            return int(text)
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError as exc:
            raise policy_invalid(field, "必须为毫秒整数或 ISO-8601 时间") from exc
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp() * 1000)
    raise policy_invalid(field, "必须为毫秒整数或 ISO-8601 时间")


def _bool(value: Any, field: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes"}:
            return True
        if lowered in {"false", "0", "no"}:
            return False
    raise policy_invalid(field, "必须为布尔值")


@dataclass(frozen=True)
class Policy:
    slave_id: str
    replication_group: str
    delay_threshold_ms: int
    tolerable_delay_ms: int
    expected_version: str
    degradation_action: str
    priority: int
    forward_compatible: bool
    policy_version: int
    updated_at_ms: int

    def to_internal_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_output_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["expected_version_segments"] = list(parse_version(self.expected_version).segments)
        return data

    def to_jsonl_dict(self) -> dict[str, Any]:
        return {
            "policyId": self.slave_id,
            "replicationGroup": self.replication_group,
            "delayThresholdMs": self.delay_threshold_ms,
            "tolerableDelayMs": self.tolerable_delay_ms,
            "expectedVersion": self.expected_version,
            "degradationAction": self.degradation_action,
            "priority": self.priority,
            "forwardCompatible": self.forward_compatible,
            "policyVersion": self.policy_version,
            "updatedAtMs": self.updated_at_ms,
        }

    def canonical_key(self) -> tuple[Any, ...]:
        return (
            self.slave_id,
            self.replication_group,
            self.delay_threshold_ms,
            self.tolerable_delay_ms,
            self.expected_version,
            self.degradation_action,
            self.priority,
            self.forward_compatible,
            self.policy_version,
            self.updated_at_ms,
        )

    def precedence(self) -> tuple[int, int, int]:
        # Lower priority number means operationally more important.
        return (self.policy_version, -self.priority, self.updated_at_ms)


def normalize_policy(raw: Mapping[str, Any], *, default_updated_at_ms: int | None = None) -> Policy:
    if not isinstance(raw, Mapping):
        raise policy_invalid("policy", "必须是对象")

    slave_id = _first(
        raw,
        "slave_id",
        "slaveId",
        "policy_id",
        "policyId",
        "replica_id",
        "replicaId",
        "target_id",
        "targetId",
        "id",
    )
    if not isinstance(slave_id, str) or not slave_id.strip():
        raise policy_invalid("slave_id", "必须为非空字符串")
    replication_group = _first(raw, "replication_group", "replicationGroup", "group_id", "groupId", "group")
    if not isinstance(replication_group, str) or not replication_group.strip():
        raise policy_invalid("replication_group", "必须为非空字符串")

    threshold = as_non_negative_int(
        _first(
            raw,
            "delay_threshold_ms",
            "delayThresholdMs",
            "threshold_ms",
            "thresholdMs",
            "delayThreshold",
            "lagThresholdMs",
        ),
        "delay_threshold_ms",
    )
    tolerable = as_non_negative_int(
        _first(
            raw,
            "tolerable_delay_ms",
            "tolerableDelayMs",
            "tolerable_delay",
            "tolerableDelay",
            "maxTolerableDelayMs",
        ),
        "tolerable_delay_ms",
    )
    if tolerable < threshold:
        raise policy_invalid(
            "tolerable_delay_ms",
            "可容忍延迟不能小于判定阈值",
            delay_threshold_ms=threshold,
            tolerable_delay_ms=tolerable,
        )

    expected_raw = _first(
        raw, "expected_version", "expectedVersion", "expected_slave_version", "expectedSlaveVersion"
    )
    expected = parse_version(expected_raw, "expected_version")

    action_raw = _first(raw, "degradation_action", "degradationAction", "action", default="NONE")
    if not isinstance(action_raw, str):
        raise policy_invalid("degradation_action", "必须为字符串")
    action = action_raw.strip().upper()
    action = ACTION_ALIASES.get(action, action)
    if not action or len(action) > 64 or re.fullmatch(r"[A-Z][A-Z0-9_]*", action) is None:
        raise policy_invalid(
            "degradation_action",
            "必须为非空大写动作标识（A-Z、0-9、下划线，最长 64）",
            standardActions=sorted(STANDARD_ACTIONS),
        )

    priority = as_non_negative_int(_first(raw, "priority", default=100), "priority")

    forward_value = _first(
        raw,
        "forward_compatible",
        "forwardCompatible",
        "allow_forward_compatible",
        "allowForwardCompatible",
        default=False,
    )
    forward_compatible = _bool(forward_value, "forward_compatible")

    policy_version = as_non_negative_int(
        _first(
            raw,
            "policy_version",
            "policyVersion",
            "revision",
            "version_no",
            "versionNo",
            "record_version",
            "recordVersion",
            default=1,
        ),
        "policy_version",
    )
    if policy_version < 1:
        raise policy_invalid("policy_version", "必须为正整数")

    updated_at_ms = parse_time_ms(
        _first(
            raw,
            "updated_at_ms",
            "updatedAtMs",
            "updated_at",
            "updatedAt",
            "last_write_time_ms",
            "lastWriteTimeMs",
        ),
        "updated_at_ms",
        default=default_updated_at_ms if default_updated_at_ms is not None else now_ms(),
    )

    return Policy(
        slave_id=slave_id.strip(),
        replication_group=replication_group.strip(),
        delay_threshold_ms=threshold,
        tolerable_delay_ms=tolerable,
        expected_version=expected.raw,
        degradation_action=action,
        priority=priority,
        forward_compatible=forward_compatible,
        policy_version=policy_version,
        updated_at_ms=updated_at_ms,
    )


@dataclass(frozen=True)
class Observation:
    slave_id: str
    sampled_at_ms: int
    binlog_gap: int
    delay_ms: int
    reported_version: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def deterministic_key(self) -> tuple[Any, ...]:
        # Do not include database insertion order: equal-gap ties remain reproducible.
        return (self.binlog_gap, self.delay_ms, parse_version(self.reported_version).segments, self.reported_version)


def normalize_observation(raw: Mapping[str, Any]) -> Observation:
    if not isinstance(raw, Mapping):
        raise policy_invalid("observation", "必须是对象")
    slave_id = _first(raw, "slave_id", "slaveId", "replica_id", "replicaId")
    if not isinstance(slave_id, str) or not slave_id.strip():
        raise policy_invalid("slave_id", "必须为非空字符串")
    sampled_at_ms = parse_time_ms(
        _first(raw, "sampled_at_ms", "sampledAtMs", "sampled_at", "sampledAt", "sample_time_ms", "sampleTimeMs"),
        "sampled_at_ms",
    )
    binlog_gap = as_non_negative_int(
        _first(raw, "binlog_gap", "binlogGap", "binlog_gap_bytes", "binlogGapBytes", "position_gap"),
        "binlog_gap",
    )
    delay_ms = as_non_negative_int(_first(raw, "delay_ms", "delayMs", "replication_delay_ms"), "delay_ms")
    reported_raw = _first(raw, "reported_version", "reportedVersion", "version", "slave_version", "slaveVersion")
    reported = parse_version(reported_raw, "reported_version")
    return Observation(
        slave_id=slave_id.strip(),
        sampled_at_ms=sampled_at_ms,
        binlog_gap=binlog_gap,
        delay_ms=delay_ms,
        reported_version=reported.raw,
    )

