from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping
from urllib.parse import urlencode

from .errors import WorkbenchError, wb_error
from .store import Store

RESERVED_PREFIX = "__sim_"

RESERVED_QUERY_KEYS = {
    "__sim_request_id",
    "__sim_revalidate",
    "__sim_inject_delay_ms",
    "__sim_inject_timeout_ms",
    "__sim_inject_origin_status",
    "__sim_inject_body_pad",
    "__sim_inject_connection_refused",
    "__sim_inject_purge_fail",
    "__sim_inject_target_node",
}


def _as_positive_number(value: Any, name: str, *, integer: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise wb_error("E_INJECT_INVALID", f"Injection value '{name}' must be numeric", field=name)
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise wb_error("E_INJECT_INVALID", f"Injection value '{name}' must be numeric", field=name) from exc
    if integer and not number.is_integer():
        raise wb_error("E_INJECT_INVALID", f"Injection value '{name}' must be an integer", field=name)
    if number <= 0:
        raise wb_error("E_INJECT_INVALID", f"Injection value '{name}' must be positive", field=name)
    return number


def _as_bool(value: Any, name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off", ""}:
            return False
    if isinstance(value, int) and value in (0, 1):
        return bool(value)
    raise wb_error("E_INJECT_INVALID", f"Injection value '{name}' must be boolean", field=name)


def split_reserved_query(pairs: Iterable[tuple[str, str]]) -> tuple[str, dict[str, list[str]]]:
    public_pairs: list[tuple[str, str]] = []
    controls: dict[str, list[str]] = {}
    for key, value in pairs:
        if key.startswith(RESERVED_PREFIX):
            if key not in RESERVED_QUERY_KEYS:
                raise wb_error("E_INJECT_INVALID", f"Unknown reserved request parameter: {key}", field=key)
            controls.setdefault(key, []).append(value)
        else:
            public_pairs.append((key, value))
    public_query = urlencode(public_pairs, doseq=True)
    return public_query, controls


def _last(controls: Mapping[str, list[str]], key: str, default: str | None = None) -> str | None:
    values = controls.get(key)
    return values[-1] if values else default


def query_control_values(controls: Mapping[str, list[str]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    if _last(controls, "__sim_request_id") is not None:
        result["request_id"] = _last(controls, "__sim_request_id")
    if _last(controls, "__sim_revalidate") is not None:
        result["force_revalidate"] = _as_bool(_last(controls, "__sim_revalidate"), "revalidate")
    if _last(controls, "__sim_inject_delay_ms") is not None:
        result["inject_delay_ms"] = _as_positive_number(
            _last(controls, "__sim_inject_delay_ms"), "inject_delay_ms"
        )
    if _last(controls, "__sim_inject_timeout_ms") is not None:
        result["inject_timeout_ms"] = _as_positive_number(
            _last(controls, "__sim_inject_timeout_ms"), "inject_timeout_ms"
        )
    if _last(controls, "__sim_inject_origin_status") is not None:
        result["inject_origin_status"] = int(
            _as_positive_number(
                _last(controls, "__sim_inject_origin_status"), "inject_origin_status", integer=True
            )
        )
    if _last(controls, "__sim_inject_body_pad") is not None:
        result["inject_body_pad"] = int(
            _as_positive_number(_last(controls, "__sim_inject_body_pad"), "inject_body_pad", integer=True)
        )
    if _last(controls, "__sim_inject_connection_refused") is not None:
        result["inject_connection_refused"] = _as_bool(
            _last(controls, "__sim_inject_connection_refused"), "inject_connection_refused"
        )
    if _last(controls, "__sim_inject_purge_fail") is not None:
        result["inject_purge_fail"] = _as_bool(
            _last(controls, "__sim_inject_purge_fail"), "inject_purge_fail"
        )
    if _last(controls, "__sim_inject_target_node") is not None:
        result["inject_target_node"] = _last(controls, "__sim_inject_target_node")
    return result


@dataclass(slots=True)
class InjectionPlan:
    origin_delay_ms: int = 0
    timeout_ms: int | None = None
    origin_status: int | None = None
    body_pad_bytes: int = 0
    connection_refused: bool = False
    purge_interrupt: bool = False
    target_node: str | None = None
    records: list[dict[str, Any]] = field(default_factory=list)

    @property
    def any(self) -> bool:
        return bool(
            self.origin_delay_ms
            or self.origin_status
            or self.body_pad_bytes
            or self.connection_refused
            or self.purge_interrupt
        )

    def origin_params(self) -> dict[str, Any]:
        params: dict[str, Any] = {}
        if self.origin_delay_ms:
            params["__delay_ms"] = self.origin_delay_ms
        if self.origin_status:
            params["__status"] = self.origin_status
        if self.body_pad_bytes:
            params["__pad_bytes"] = self.body_pad_bytes
        return params

    def as_dict(self) -> dict[str, Any]:
        return {
            "origin_delay_ms": self.origin_delay_ms,
            "timeout_ms": self.timeout_ms,
            "origin_status": self.origin_status,
            "body_pad_bytes": self.body_pad_bytes,
            "connection_refused": self.connection_refused,
            "purge_interrupt": self.purge_interrupt,
            "target_node": self.target_node,
        }


def _legacy_specs(controls: Mapping[str, Any]) -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = []
    if controls.get("inject_delay_ms") is not None:
        specs.append(
            {
                "type": "origin_delay",
                "delay_ms": controls["inject_delay_ms"],
                "timeout_ms": controls.get("inject_timeout_ms", 1500),
            }
        )
    elif controls.get("inject_timeout_ms") is not None:
        specs.append({"type": "origin_delay", "delay_ms": 1, "timeout_ms": controls["inject_timeout_ms"]})
    if controls.get("inject_origin_status") is not None:
        specs.append({"type": "origin_status", "status": controls["inject_origin_status"]})
    if controls.get("inject_body_pad") is not None:
        specs.append({"type": "body_length", "pad_bytes": controls["inject_body_pad"]})
    if controls.get("inject_connection_refused"):
        specs.append({"type": "connection_refused"})
    if controls.get("inject_purge_fail"):
        specs.append({"type": "purge_broadcast_interrupt"})
    target = controls.get("inject_target_node")
    if target:
        for spec in specs:
            spec["target_node"] = target
    return specs


def normalize_injection_specs(
    *,
    controls: Mapping[str, Any] | None = None,
    injections: Any = None,
) -> list[dict[str, Any]]:
    specs = _legacy_specs(controls or {})
    if injections is None:
        return specs
    if isinstance(injections, Mapping):
        injections = [injections]
    if not isinstance(injections, list):
        raise wb_error("E_INJECT_INVALID", "injections must be an array of objects")
    for item in injections:
        if not isinstance(item, Mapping):
            raise wb_error("E_INJECT_INVALID", "Each injection must be an object")
        if "type" not in item:
            raise wb_error("E_INJECT_INVALID", "Each injection requires a type")
        specs.append(dict(item))
    return specs


def build_injection_plan(
    specs: Iterable[Mapping[str, Any]],
    *,
    node_id: str,
    route: str | None,
    context: str,
    max_body_pad_bytes: int,
    default_timeout_ms: float,
) -> InjectionPlan:
    plan = InjectionPlan()
    seen: set[str] = set()
    for raw in specs:
        injection_type = str(raw.get("type", "")).strip()
        if not injection_type:
            raise wb_error("E_INJECT_INVALID", "Injection type must not be empty")
        target_node = raw.get("target_node", raw.get("node_id"))
        if target_node is not None and str(target_node) != node_id:
            raise wb_error(
                "E_INJECT_INVALID",
                "Injection target node does not match the request node",
                target_node=str(target_node),
                node_id=node_id,
            )
        if plan.target_node is not None and plan.target_node != node_id:
            raise wb_error("E_INJECT_INVALID", "Injection targets disagree", node_id=node_id)
        plan.target_node = str(target_node) if target_node is not None else node_id
        record = {"type": injection_type, **dict(raw)}
        if injection_type in seen:
            raise wb_error("E_INJECT_INVALID", "Duplicate injection type", type=injection_type)
        seen.add(injection_type)

        if injection_type == "origin_delay":
            if context != "request":
                raise wb_error("E_INJECT_INVALID", "origin_delay is only valid for edge requests", type=injection_type)
            delay = _as_positive_number(raw.get("delay_ms"), "delay_ms", integer=True)
            timeout = raw.get("timeout_ms")
            if timeout is None:
                timeout_value = float(default_timeout_ms)
            else:
                timeout_value = _as_positive_number(timeout, "timeout_ms", integer=True)
            if delay > 120_000 or timeout_value > 120_000:
                raise wb_error("E_INJECT_INVALID", "Injected delay/timeout exceeds 120000 ms")
            plan.origin_delay_ms = int(delay)
            plan.timeout_ms = int(timeout_value)
        elif injection_type == "origin_status":
            if context != "request":
                raise wb_error("E_INJECT_INVALID", "origin_status is only valid for edge requests", type=injection_type)
            status = int(_as_positive_number(raw.get("status", raw.get("status_code")), "status", integer=True))
            if not 500 <= status <= 599:
                raise wb_error("E_INJECT_INVALID", "origin_status must be a 5xx status", status=status)
            plan.origin_status = status
            record["status"] = status
        elif injection_type == "body_length":
            if context != "request":
                raise wb_error("E_INJECT_INVALID", "body_length is only valid for edge requests", type=injection_type)
            pad = int(_as_positive_number(raw.get("pad_bytes", raw.get("bytes")), "pad_bytes", integer=True))
            if pad > max_body_pad_bytes:
                raise wb_error(
                    "E_INJECT_INVALID",
                    "body_length pad exceeds configured maximum",
                    max_body_pad_bytes=max_body_pad_bytes,
                )
            plan.body_pad_bytes = pad
            record["pad_bytes"] = pad
        elif injection_type == "connection_refused":
            if context != "request":
                raise wb_error("E_INJECT_INVALID", "connection_refused is only valid for edge requests")
            plan.connection_refused = True
        elif injection_type == "purge_broadcast_interrupt":
            if context != "purge":
                raise wb_error("E_INJECT_INVALID", "purge_broadcast_interrupt is only valid for purge attempts")
            plan.purge_interrupt = True
        else:
            raise wb_error("E_INJECT_INVALID", "Unknown injection type", type=injection_type)
        plan.records.append(record)
    return plan


def record_plan(
    store: Store,
    plan: InjectionPlan,
    *,
    request_id: str,
    node_id: str,
    route: str | None,
) -> list[str]:
    ids: list[str] = []
    for record in plan.records:
        injection_id = str(uuid.uuid4())
        store.record_injection(
            injection_id=injection_id,
            request_id=request_id,
            node_id=node_id,
            route=route,
            injection_type=str(record.get("type", "unknown")),
            params=record,
            status="APPLIED",
            details={"mode": "request_parameter"},
        )
        ids.append(injection_id)
    return ids


def record_rejected_injection(
    store: Store,
    error: WorkbenchError,
    *,
    request_id: str,
    node_id: str,
    route: str | None,
    raw_specs: Iterable[Mapping[str, Any]],
) -> None:
    for record in raw_specs:
        injection_type = str(record.get("type", "unknown")) if isinstance(record, Mapping) else "unknown"
        store.record_injection(
            injection_id=str(uuid.uuid4()),
            request_id=request_id,
            node_id=node_id,
            route=route,
            injection_type=injection_type,
            params=dict(record) if isinstance(record, Mapping) else {"raw": str(record)},
            status="REJECTED",
            business_code=error.code,
            details=error.details,
        )


def injections_as_json(plan: InjectionPlan) -> str:
    return json.dumps(plan.as_dict(), ensure_ascii=False, sort_keys=True)

