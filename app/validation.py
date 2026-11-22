from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any

from .errors import BusinessError, E_SPEC_INVALID, E_TRACE_OUT_OF_ORDER

MAX_SQLITE_INT = 2**63 - 1
FUNCTION_ID_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
QUOTA_CLASSES = {"AUTO", "ON_DEMAND", "RESERVED"}


@dataclass(frozen=True)
class FunctionSpec:
    function_id: str
    runtime: str
    init_us: int
    memory_bytes: int
    per_instance_concurrency: int
    function_concurrency_limit: int
    reserved_quota: int
    init_concurrency: int
    prewarm_instances: int
    queue_capacity: int

    @property
    def on_demand_capacity(self) -> int:
        return self.function_concurrency_limit - self.reserved_quota

    @property
    def max_instances(self) -> int:
        return (
            self.function_concurrency_limit + self.per_instance_concurrency - 1
        ) // self.per_instance_concurrency

    def as_dict(self) -> dict[str, Any]:
        return {
            "function_id": self.function_id,
            "runtime": self.runtime,
            "init_us": self.init_us,
            "memory_bytes": self.memory_bytes,
            "per_instance_concurrency": self.per_instance_concurrency,
            "function_concurrency_limit": self.function_concurrency_limit,
            "reserved_quota": self.reserved_quota,
            "init_concurrency": self.init_concurrency,
            "prewarm_instances": self.prewarm_instances,
            "queue_capacity": self.queue_capacity,
        }


@dataclass(frozen=True)
class TraceCall:
    sequence: int
    call_id: str
    function_id: str
    arrival_ms: int
    execution_us: int
    priority: int
    retry: bool
    request_fingerprint: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "sequence": self.sequence,
            "call_id": self.call_id,
            "function_id": self.function_id,
            "arrival_ms": self.arrival_ms,
            "execution_us": self.execution_us,
            "priority": self.priority,
            "retry": self.retry,
        }


def _fail(errors: list[dict[str, Any]]) -> None:
    raise BusinessError(
        E_SPEC_INVALID,
        "specification batch validation failed; no rows were written",
        detail={"errors": errors},
    )


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _require_int(
    obj: dict[str, Any],
    key: str,
    *,
    path: str,
    minimum: int,
    errors: list[dict[str, Any]],
) -> int | None:
    if key not in obj:
        errors.append({"path": f"{path}.{key}", "reason": "required integer is missing"})
        return None
    value = obj[key]
    if not _is_int(value):
        errors.append({"path": f"{path}.{key}", "reason": "must be an integer, not bool/string/float"})
        return None
    if value < minimum:
        errors.append({"path": f"{path}.{key}", "reason": f"must be >= {minimum}", "value": value})
        return None
    if value > MAX_SQLITE_INT:
        errors.append(
            {
                "path": f"{path}.{key}",
                "reason": f"integer overflow: must be <= {MAX_SQLITE_INT}",
                "value": value,
            }
        )
        return None
    return value


def _optional_int(
    obj: dict[str, Any],
    key: str,
    *,
    path: str,
    default: int,
    minimum: int,
    errors: list[dict[str, Any]],
) -> int:
    if key not in obj:
        return default
    value = _require_int(obj, key, path=path, minimum=minimum, errors=errors)
    return default if value is None else value


def normalize_function_batch(payload: Any) -> tuple[list[FunctionSpec], int | None]:
    if isinstance(payload, list):
        rows, seed = payload, None
    elif isinstance(payload, dict):
        rows = payload.get("functions", payload.get("specs"))
        seed = payload.get("seed")
    else:
        raise BusinessError(
            E_SPEC_INVALID,
            "function specification payload must be an object or array",
            detail={"errors": [{"path": "$", "reason": "expected object or array"}]},
        )
    if not isinstance(rows, list) or not rows:
        raise BusinessError(
            E_SPEC_INVALID,
            "function specification batch must contain a non-empty functions array",
            detail={"errors": [{"path": "$.functions", "reason": "expected non-empty array"}]},
        )
    errors: list[dict[str, Any]] = []
    normalized: list[FunctionSpec] = []
    seen: set[str] = set()
    for index, raw in enumerate(rows):
        path = f"$.functions[{index}]"
        if not isinstance(raw, dict):
            errors.append({"path": path, "reason": "expected object"})
            continue
        function_id = raw.get("function_id", raw.get("id"))
        if not isinstance(function_id, str) or not FUNCTION_ID_RE.match(function_id):
            errors.append(
                {
                    "path": f"{path}.function_id",
                    "reason": "must match [A-Za-z0-9_.:-]{1,128}",
                }
            )
            continue
        if function_id in seen:
            errors.append({"path": f"{path}.function_id", "reason": "duplicate function_id"})
            continue
        seen.add(function_id)
        runtime = raw.get("runtime")
        if not isinstance(runtime, str) or not runtime.strip() or len(runtime) > 64:
            errors.append(
                {"path": f"{path}.runtime", "reason": "must be a non-empty string up to 64 chars"}
            )
        init_us = _require_int(raw, "init_us", path=path, minimum=0, errors=errors)
        memory_bytes = _require_int(raw, "memory_bytes", path=path, minimum=0, errors=errors)
        per_instance = _require_int(
            raw, "per_instance_concurrency", path=path, minimum=1, errors=errors
        )
        function_limit = _require_int(
            raw, "function_concurrency_limit", path=path, minimum=1, errors=errors
        )
        reserved = _require_int(raw, "reserved_quota", path=path, minimum=0, errors=errors)
        init_concurrency = _optional_int(
            raw,
            "init_concurrency",
            path=path,
            default=1,
            minimum=1,
            errors=errors,
        )
        prewarm = _optional_int(
            raw,
            "prewarm_instances",
            path=path,
            default=0,
            minimum=0,
            errors=errors,
        )
        queue_capacity = _optional_int(
            raw,
            "queue_capacity",
            path=path,
            default=8,
            minimum=0,
            errors=errors,
        )
        if (
            function_limit is not None
            and reserved is not None
            and reserved > function_limit
        ):
            errors.append(
                {
                    "path": f"{path}.reserved_quota",
                    "reason": "must not exceed function_concurrency_limit",
                    "value": reserved,
                    "limit": function_limit,
                }
            )
        if init_us is not None and memory_bytes is not None and init_us > MAX_SQLITE_INT - memory_bytes:
            errors.append(
                {
                    "path": path,
                    "reason": "derived numeric overflow while normalizing resource fields",
                }
            )
        if (
            init_us is not None
            and memory_bytes is not None
            and per_instance is not None
            and function_limit is not None
            and reserved is not None
            and init_concurrency is not None
            and prewarm is not None
            and queue_capacity is not None
            and isinstance(runtime, str)
            and runtime.strip()
        ):
            normalized.append(
                FunctionSpec(
                    function_id=function_id,
                    runtime=runtime.strip(),
                    init_us=init_us,
                    memory_bytes=memory_bytes,
                    per_instance_concurrency=per_instance,
                    function_concurrency_limit=function_limit,
                    reserved_quota=reserved,
                    init_concurrency=init_concurrency,
                    prewarm_instances=prewarm,
                    queue_capacity=queue_capacity,
                )
            )
    if errors:
        _fail(errors)
    return normalized, seed


def normalize_trace_batch(
    payload: Any,
    *,
    known_functions: set[str],
    strict_order: bool = True,
    batch_id: str | None = None,
    source: str = "inline",
) -> tuple[str, list[TraceCall], int | None]:
    if isinstance(payload, list):
        rows = payload
        seed = None
        supplied_batch_id = None
    elif isinstance(payload, dict):
        rows = payload.get("calls", payload.get("trace"))
        seed = payload.get("seed")
        supplied_batch_id = payload.get("batch_id")
    else:
        raise BusinessError(
            E_SPEC_INVALID,
            "trace payload must be an object or array",
            detail={"errors": [{"path": "$", "reason": "expected object or array"}]},
        )
    if not isinstance(rows, list) or not rows:
        raise BusinessError(
            E_SPEC_INVALID,
            "trace batch must contain a non-empty calls array",
            detail={"errors": [{"path": "$.calls", "reason": "expected non-empty array"}]},
        )
    batch_id = batch_id or supplied_batch_id or f"trace-{_digest_json(rows)[:16]}"
    errors: list[dict[str, Any]] = []
    normalized: list[TraceCall] = []
    seen_call_ids: set[str] = set()
    previous_arrival: int | None = None
    for index, raw in enumerate(rows):
        path = f"$.calls[{index}]"
        if not isinstance(raw, dict):
            errors.append({"path": path, "reason": "expected object"})
            continue
        function_id = raw.get("function_id")
        if not isinstance(function_id, str) or function_id not in known_functions:
            errors.append(
                {
                    "path": f"{path}.function_id",
                    "reason": "references an undeclared function",
                    "value": function_id,
                }
            )
        call_id = raw.get("call_id")
        if call_id is None:
            call_id = f"{batch_id}-{index:05d}"
        if not isinstance(call_id, str) or not call_id or len(call_id) > 200:
            errors.append(
                {"path": f"{path}.call_id", "reason": "must be a non-empty string up to 200 chars"}
            )
            continue
        if call_id in seen_call_ids:
            errors.append({"path": f"{path}.call_id", "reason": "duplicate call_id in batch"})
            continue
        seen_call_ids.add(call_id)
        arrival = _require_int(raw, "arrival_ms", path=path, minimum=0, errors=errors)
        execution = _require_int(raw, "execution_us", path=path, minimum=0, errors=errors)
        priority = _require_int(raw, "priority", path=path, minimum=0, errors=errors)
        retry = raw.get("retry", False)
        if not isinstance(retry, bool):
            errors.append({"path": f"{path}.retry", "reason": "must be a boolean"})
        if (
            arrival is not None
            and execution is not None
            and arrival > MAX_SQLITE_INT - execution
        ):
            errors.append({"path": path, "reason": "arrival_ms + execution_us overflows"})
        if arrival is not None and previous_arrival is not None and arrival < previous_arrival:
            if strict_order:
                raise BusinessError(
                    E_TRACE_OUT_OF_ORDER,
                    "trace arrival_ms is not monotonic; the entire batch was rejected",
                    detail={
                        "errors": [
                            {
                                "path": f"{path}.arrival_ms",
                                "reason": "arrival_ms is earlier than the preceding record",
                                "previous": previous_arrival,
                                "value": arrival,
                            }
                        ],
                        "batch_id": batch_id,
                    },
                )
        if arrival is not None:
            previous_arrival = arrival
        if (
            isinstance(function_id, str)
            and function_id in known_functions
            and isinstance(call_id, str)
            and call_id
            and arrival is not None
            and execution is not None
            and priority is not None
            and isinstance(retry, bool)
        ):
            semantic = {
                "function_id": function_id,
                "execution_us": execution,
                "priority": priority,
                "retry": retry,
                "quota_class": raw.get("quota_class", "AUTO"),
            }
            normalized.append(
                TraceCall(
                    sequence=index,
                    call_id=call_id,
                    function_id=function_id,
                    arrival_ms=arrival,
                    execution_us=execution,
                    priority=priority,
                    retry=retry,
                    request_fingerprint=request_fingerprint(**semantic),
                )
            )
    if errors:
        _fail(errors)
    return batch_id, normalized, seed


def validate_invocation_raw(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        _fail([{"path": "$", "reason": "invocation body must be an object"}])
    errors: list[dict[str, Any]] = []
    function_id = raw.get("function_id")
    if not isinstance(function_id, str) or not function_id:
        errors.append({"path": "$.function_id", "reason": "must be a non-empty string"})
    call_id = raw.get("call_id")
    if not isinstance(call_id, str) or not call_id or len(call_id) > 200:
        errors.append({"path": "$.call_id", "reason": "must be a non-empty string up to 200 chars"})
    execution_us = _require_int(
        raw, "execution_us", path="$", minimum=0, errors=errors
    )
    priority = _optional_int(raw, "priority", path="$", default=0, minimum=0, errors=errors)
    arrival = _optional_int(raw, "arrival_ms", path="$", default=0, minimum=0, errors=errors)
    retry = raw.get("retry", False)
    if not isinstance(retry, bool):
        errors.append({"path": "$.retry", "reason": "must be a boolean"})
    quota_class = raw.get("quota_class", "AUTO")
    if quota_class not in QUOTA_CLASSES:
        errors.append(
            {
                "path": "$.quota_class",
                "reason": f"must be one of {sorted(QUOTA_CLASSES)}",
                "value": quota_class,
            }
        )
    injection = raw.get("injection", {})
    if injection is None:
        injection = {}
    if not isinstance(injection, dict):
        errors.append({"path": "$.injection", "reason": "must be an object"})
    else:
        for key in ("create_timeout", "execution_interrupt"):
            if key in injection and not isinstance(injection[key], bool):
                errors.append({"path": f"$.injection.{key}", "reason": "must be a boolean"})
    if errors:
        _fail(errors)
    if (
        execution_us is not None
        and arrival is not None
        and arrival > MAX_SQLITE_INT - execution_us
    ):
        _fail([{"path": "$", "reason": "arrival_ms + execution_us overflows"}])
    semantic = {
        "function_id": function_id,
        "execution_us": execution_us,
        "priority": priority,
        "retry": retry,
        "quota_class": quota_class,
        "injection": injection,
    }
    return {
        "call_id": call_id,
        "function_id": function_id,
        "execution_us": execution_us,
        "priority": priority,
        "arrival_ms": arrival,
        "retry": retry,
        "quota_class": quota_class,
        "injection": injection,
        "fingerprint": request_fingerprint(**semantic),
        "request": semantic,
    }


def request_fingerprint(
    *,
    function_id: str,
    execution_us: int,
    priority: int,
    retry: bool,
    quota_class: str,
    injection: dict[str, Any] | None = None,
) -> str:
    canonical = {
        "function_id": function_id,
        "execution_us": execution_us,
        "priority": priority,
        "retry": retry,
        "quota_class": quota_class,
        "injection": injection or {},
    }
    return _digest_json(canonical)


def _digest_json(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

