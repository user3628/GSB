from __future__ import annotations

import hashlib
import ipaddress
import json
import re
from datetime import datetime
from enum import Enum
from typing import Any, Literal
from urllib.parse import urlparse

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .errors import GatewayError


class RequestStatus(str, Enum):
    PENDING = "PENDING"
    QUEUED = "QUEUED"
    DISPATCHED = "DISPATCHED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    SHED = "SHED"
    ABORTED = "ABORTED"


BUSINESS_STATES = {
    RequestStatus.PENDING,
    RequestStatus.QUEUED,
    RequestStatus.DISPATCHED,
    RequestStatus.COMPLETED,
    RequestStatus.FAILED,
    RequestStatus.SHED,
}

# DISPATCHED -> QUEUED and DISPATCHED -> ABORTED are reserved for durable
# recovery after the owning process disappeared.
LEGAL_TRANSITIONS: dict[RequestStatus, frozenset[RequestStatus]] = {
    RequestStatus.PENDING: frozenset({RequestStatus.QUEUED, RequestStatus.FAILED, RequestStatus.SHED}),
    RequestStatus.QUEUED: frozenset(
        {
            RequestStatus.DISPATCHED,
            RequestStatus.FAILED,
            RequestStatus.SHED,
            RequestStatus.ABORTED,
        }
    ),
    RequestStatus.DISPATCHED: frozenset(
        {
            RequestStatus.COMPLETED,
            RequestStatus.FAILED,
            RequestStatus.QUEUED,
            RequestStatus.ABORTED,
        }
    ),
    RequestStatus.COMPLETED: frozenset(),
    RequestStatus.FAILED: frozenset(),
    RequestStatus.SHED: frozenset(),
    RequestStatus.ABORTED: frozenset(),
}

TERMINAL_STATES = frozenset(
    {
        RequestStatus.COMPLETED,
        RequestStatus.FAILED,
        RequestStatus.SHED,
        RequestStatus.ABORTED,
    }
)


def assert_legal_transition(
    from_status: RequestStatus | str,
    to_status: RequestStatus | str,
    *,
    recovery: bool = False,
) -> None:
    source = RequestStatus(from_status)
    target = RequestStatus(to_status)
    if target not in LEGAL_TRANSITIONS[source]:
        raise GatewayError(
            "E_INVALID_TRANSITION",
            f"Illegal request transition {source.value} -> {target.value}",
            http_status=409,
            details={"from": source.value, "to": target.value},
        )
    if not recovery and target in {RequestStatus.QUEUED, RequestStatus.ABORTED} and source is RequestStatus.DISPATCHED:
        raise GatewayError(
            "E_INVALID_TRANSITION",
            "DISPATCHED -> QUEUED/ABORTED is reserved for crash recovery",
            http_status=409,
            details={"from": source.value, "to": target.value},
        )


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def payload_digest(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


_CHECKSUM_KEYS = {"checksum", "digest", "sha256", "route_checksum"}


def compute_route_checksum(document: dict[str, Any]) -> str:
    material = {k: v for k, v in document.items() if k.lower() not in _CHECKSUM_KEYS}
    return "sha256:" + hashlib.sha256(canonical_json(material).encode("utf-8")).hexdigest()


def normalize_checksum(value: str) -> str:
    text = value.strip().lower()
    return text[7:] if text.startswith("sha256:") else text


class BackendSpec(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    backend_id: str = Field(min_length=1, max_length=128)
    endpoint: str = Field(min_length=1, max_length=512)
    vram_limit_mb: float = Field(gt=0)
    concurrency_limit: int = Field(gt=0)
    vram_per_sample_mb: float = Field(gt=0)
    estimated_latency_ms: int = Field(ge=1)
    failure_threshold: int = Field(default=2, ge=1, le=100)

    @field_validator("endpoint")
    @classmethod
    def validate_endpoint(cls, value: str) -> str:
        value = value.rstrip("/")
        parsed = urlparse(value)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError("endpoint must be an absolute http(s) URL")
        if parsed.username or parsed.password:
            raise ValueError("endpoint credentials are not allowed")
        return value


class ModelSpec(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    model_id: str = Field(min_length=1, max_length=128)
    backend_id: str = Field(min_length=1)
    estimated_latency_ms: int | None = Field(default=None, ge=1)


class RouteTable(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: int = Field(gt=0)
    generated_at: str
    models: dict[str, ModelSpec]
    backends: dict[str, BackendSpec]
    checksum: str
    global_concurrency_limit: int | None = Field(default=None, ge=1)

    @field_validator("generated_at")
    @classmethod
    def validate_generated_at(cls, value: str) -> str:
        candidate = value.replace("Z", "+00:00")
        try:
            datetime.fromisoformat(candidate)
        except ValueError as exc:
            raise ValueError("generated_at must be ISO-8601") from exc
        return value


def _alias_get(source: dict[str, Any], *names: str, default: Any = None) -> Any:
    for name in names:
        if name in source:
            return source[name]
    return default


def _normalise_endpoint(endpoint: str, validate_loopback: bool) -> str:
    endpoint = endpoint.rstrip("/")
    parsed = urlparse(endpoint)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise GatewayError("E_ROUTE_INVALID", f"Invalid backend endpoint: {endpoint!r}", http_status=422)
    if parsed.username or parsed.password:
        raise GatewayError("E_ROUTE_INVALID", "Endpoint credentials are not allowed", http_status=422)
    if validate_loopback:
        host = parsed.hostname
        is_loopback = host.lower() == "localhost"
        if not is_loopback:
            try:
                is_loopback = ipaddress.ip_address(host).is_loopback
            except ValueError:
                is_loopback = False
        if not is_loopback:
            raise GatewayError(
                "E_ROUTE_INVALID",
                "Only loopback backend endpoints are allowed in local mode",
                http_status=422,
                details={"endpoint": endpoint, "host": host},
            )
    return endpoint


def normalize_route_document(
    document: dict[str, Any], *, validate_loopback: bool = True
) -> tuple[RouteTable, dict[str, Any], str]:
    if not isinstance(document, dict):
        raise GatewayError("E_ROUTE_INVALID", "Route table must be a JSON object", http_status=422)

    submitted_checksum = _alias_get(
        document, "checksum", "digest", "sha256", "route_checksum"
    )
    if not isinstance(submitted_checksum, str) or not submitted_checksum.strip():
        raise GatewayError("E_ROUTE_INVALID", "Route checksum is required", http_status=422)

    raw_material = {
        k: v for k, v in document.items() if k.lower() not in _CHECKSUM_KEYS
    }
    raw_expected = compute_route_checksum(raw_material)
    if normalize_checksum(submitted_checksum) != normalize_checksum(raw_expected):
        raise GatewayError(
            "E_ROUTE_INVALID",
            "Route checksum mismatch",
            http_status=422,
            details={"submitted": submitted_checksum, "expected": raw_expected},
        )

    raw_backends = document.get("backends")
    if not isinstance(raw_backends, (dict, list)) or not raw_backends:
        raise GatewayError("E_ROUTE_INVALID", "backends must be a non-empty object or array", http_status=422)

    backends: dict[str, dict[str, Any]] = {}
    if isinstance(raw_backends, dict):
        items = raw_backends.items()
    else:
        items = ((str(item.get("backend_id", "")), item) for item in raw_backends if isinstance(item, dict))
    for backend_id, raw in items:
        if not isinstance(raw, dict):
            raise GatewayError("E_ROUTE_INVALID", "Each backend must be an object", http_status=422)
        item = dict(raw)
        item["backend_id"] = str(item.get("backend_id") or backend_id)
        item["endpoint"] = _normalise_endpoint(
            str(_alias_get(item, "endpoint", "url", default="")), validate_loopback
        )
        item["vram_limit_mb"] = _alias_get(
            item, "vram_limit_mb", "vram_budget_mb", "vram_budget"
        )
        item["concurrency_limit"] = _alias_get(
            item, "concurrency_limit", "max_concurrency"
        )
        item["vram_per_sample_mb"] = _alias_get(
            item, "vram_per_sample_mb", "vram_mb_per_sample"
        )
        item["estimated_latency_ms"] = _alias_get(
            item, "estimated_latency_ms", "latency_ms"
        )
        item.setdefault("failure_threshold", 2)
        for stale_key in ("url", "vram_budget", "max_concurrency", "vram_mb_per_sample", "latency_ms"):
            item.pop(stale_key, None)
        backends[item["backend_id"]] = item

    raw_models = document.get("models")
    if not isinstance(raw_models, (dict, list)) or not raw_models:
        raise GatewayError("E_ROUTE_INVALID", "models must be a non-empty object or array", http_status=422)
    models: dict[str, dict[str, Any]] = {}
    if isinstance(raw_models, dict):
        model_items = raw_models.items()
    else:
        model_items = ((str(item.get("model_id", "")), item) for item in raw_models if isinstance(item, dict))
    for model_id, raw in model_items:
        if isinstance(raw, str):
            item = {"backend_id": raw}
        elif isinstance(raw, dict):
            item = dict(raw)
        else:
            raise GatewayError("E_ROUTE_INVALID", "Each model route must be an object or backend id", http_status=422)
        item["model_id"] = str(item.get("model_id") or model_id)
        item["backend_id"] = str(_alias_get(item, "backend_id", "backend", default=""))
        for stale_key in ("backend",):
            item.pop(stale_key, None)
        item.setdefault("estimated_latency_ms", None)
        models[item["model_id"]] = item

    normalized: dict[str, Any] = {
        "version": document.get("version", document.get("route_version")),
        "generated_at": document.get("generated_at"),
        "models": models,
        "backends": backends,
    }
    if document.get("global_concurrency_limit") is not None:
        normalized["global_concurrency_limit"] = document["global_concurrency_limit"]

    # The published checksum is over the original submitted JSON (sans checksum
    # fields).  The normalized payload is persisted separately for scheduling.
    normalized_for_model = dict(normalized)
    normalized_for_model["checksum"] = "pending"
    try:
        route = RouteTable.model_validate(normalized_for_model)
    except Exception as exc:
        raise GatewayError(
            "E_ROUTE_INVALID",
            f"Route table validation failed: {exc}",
            http_status=422,
        ) from exc

    if route.global_concurrency_limit is not None and route.global_concurrency_limit > 0:
        for spec in route.backends.values():
            if spec.concurrency_limit > route.global_concurrency_limit * 64:
                raise GatewayError(
                    "E_ROUTE_INVALID",
                    "Backend concurrency limit is unreasonably large",
                    http_status=422,
                )
    for model_id, model in route.models.items():
        if model.backend_id not in route.backends:
            raise GatewayError(
                "E_ROUTE_INVALID",
                f"Model {model_id!r} references unknown backend {model.backend_id!r}",
                http_status=422,
            )
        if model.estimated_latency_ms is None:
            model.estimated_latency_ms = route.backends[model.backend_id].estimated_latency_ms

    persisted = route.model_dump(mode="json", exclude_none=False)
    checksum = raw_expected
    persisted["checksum"] = checksum
    route.checksum = checksum
    return route, persisted, checksum


class InferenceRequestSpec(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    request_id: str = Field(min_length=1, max_length=200)
    model_id: str = Field(min_length=1, max_length=128)
    sample_count: int = Field(ge=1, le=10_000_000)
    latency_cap_ms: int = Field(ge=1, le=3_600_000)
    priority: int = Field(default=0, ge=0, le=1_000_000)
    tenant_id: str = Field(min_length=1, max_length=128)
    fault_mode: Literal["success", "fail", "hang"] | None = None

    @model_validator(mode="before")
    @classmethod
    def accept_latency_aliases(cls, value: Any) -> Any:
        if isinstance(value, dict):
            item = dict(value)
            for alias in ("expected_latency_ms", "latency_ms", "timeout_ms"):
                if alias in item:
                    if "latency_cap_ms" not in item:
                        item["latency_cap_ms"] = item[alias]
                    item.pop(alias, None)
            return item
        return value

    def canonical_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude_none=True)


class TransitionRequest(BaseModel):
    to_status: RequestStatus
    reason_code: str = Field(default="MANUAL", max_length=128)
    details: dict[str, Any] = Field(default_factory=dict)


class RoundRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    round_id: int | None = Field(default=None, ge=1)
    time_budget_ms: int | None = Field(default=None, ge=1, le=3_600_000)
    degrade_excess: bool = False
    max_dispatches: int | None = Field(default=None, ge=0)


class BatchSubmission(BaseModel):
    requests: list[InferenceRequestSpec]
    wait: bool = False
    wait_timeout_ms: int = Field(default=2_000, ge=0, le=60_000)


class CancelRequest(BaseModel):
    reason: str = Field(default="client_cancelled", max_length=512)


class ResetBackendRequest(BaseModel):
    route_version: int
    backend_id: str


_SAFE_TENANT = re.compile(r"^[^,;\x00-\x1f]{1,128}$")


def validate_tenant(value: str) -> None:
    if not _SAFE_TENANT.fullmatch(value):
        raise GatewayError("E_REQUEST_INVALID", "tenant_id contains unsupported characters", http_status=422)

