from __future__ import annotations

import re
from enum import Enum
from ipaddress import ip_address
from typing import Any
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from app.http_headers import HOP_BY_HOP_HEADERS, REQUEST_BODY_HEADERS


LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})
ROUTE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,63}$")
METHOD_RE = re.compile(r"^[A-Z][A-Z0-9_-]{0,31}$")


def is_loopback_host(host: str) -> bool:
    lowered = host.strip("[]").lower()
    if lowered in LOOPBACK_HOSTS:
        return True
    try:
        return ip_address(lowered).is_loopback
    except ValueError:
        return False


class RouteKind(str, Enum):
    reverse = "reverse"
    forward = "forward"


class HealthStatus(str, Enum):
    healthy = "healthy"
    degraded = "degraded"
    unhealthy = "unhealthy"


class Route(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    route_id: str
    kind: RouteKind
    methods: list[str] = Field(default_factory=lambda: ["GET"])
    path_prefix: str | None = None
    target_host: str | None = None
    target_port: int | None = Field(default=None, ge=1, le=65535)
    upstream: str
    timeout_ms: int = Field(ge=1, le=600_000)
    retries: int = Field(default=0, ge=0, le=10)
    headers: dict[str, str] = Field(default_factory=dict)
    health_status: HealthStatus = HealthStatus.healthy
    priority: int = Field(default=100, ge=-10_000, le=10_000)

    @field_validator("route_id")
    @classmethod
    def validate_route_id(cls, value: str) -> str:
        if not ROUTE_ID_RE.match(value):
            raise ValueError("route_id must match [A-Za-z0-9][A-Za-z0-9_.:-]{0,63}")
        return value

    @field_validator("methods")
    @classmethod
    def validate_methods(cls, value: list[str]) -> list[str]:
        if not value:
            raise ValueError("methods must not be empty")
        normalized: list[str] = []
        for method in value:
            upper = method.upper()
            if upper != "*" and not METHOD_RE.match(upper):
                raise ValueError(f"invalid HTTP method: {method!r}")
            if upper not in normalized:
                normalized.append(upper)
        return normalized

    @field_validator("path_prefix")
    @classmethod
    def validate_path_prefix(cls, value: str | None) -> str | None:
        if value is None:
            return value
        if not value.startswith("/") or "?" in value or "#" in value:
            raise ValueError("path_prefix must be an absolute path without query or fragment")
        return value

    @field_validator("target_host")
    @classmethod
    def validate_target_host(cls, value: str | None) -> str | None:
        if value is None:
            return value
        if not is_loopback_host(value):
            raise ValueError("target_host must be a loopback host")
        return value.strip("[]").lower()

    @field_validator("upstream")
    @classmethod
    def validate_upstream(cls, value: str) -> str:
        parsed = urlsplit(value)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError("upstream must be an absolute http(s) URL")
        if not is_loopback_host(parsed.hostname):
            raise ValueError("upstream must resolve to a loopback host")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("upstream must not contain credentials, query or fragment")
        if parsed.path not in {"", "/"}:
            raise ValueError("upstream must be an origin without a path")
        return value.rstrip("/")

    @field_validator("headers")
    @classmethod
    def validate_headers(cls, value: dict[str, str]) -> dict[str, str]:
        normalized: dict[str, str] = {}
        for name, header_value in value.items():
            if not name or any(ch in name for ch in "\r\n:") or any(
                ch in header_value for ch in "\r\n"
            ):
                raise ValueError("invalid header name or value")
            lowered = name.lower()
            if lowered in HOP_BY_HOP_HEADERS | REQUEST_BODY_HEADERS | {"accept-encoding"}:
                raise ValueError(f"forbidden forwarded header: {name}")
            normalized[name] = header_value
        return normalized

    @model_validator(mode="after")
    def validate_route_shape(self) -> "Route":
        if self.kind is RouteKind.reverse:
            if not self.path_prefix:
                raise ValueError("reverse routes require path_prefix")
            if self.target_host is not None or self.target_port is not None:
                raise ValueError("reverse routes must not define target_host/target_port")
        else:
            if not self.target_host or self.target_port is None:
                raise ValueError("forward routes require target_host and target_port")
            if self.path_prefix is not None:
                raise ValueError("forward routes match target host/port, not path_prefix")
        return self

    def normalized_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude_none=True)


def _validate_unique_routes(routes: list[Route]) -> list[Route]:
    ids = [route.route_id for route in routes]
    if len(ids) != len(set(ids)):
        raise ValueError("route_id values must be unique")
    return sorted(routes, key=lambda route: route.route_id)


class SnapshotRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: int = Field(ge=1)
    routes: list[Route]
    digest: str | None = None

    @field_validator("routes")
    @classmethod
    def validate_routes(cls, value: list[Route]) -> list[Route]:
        return _validate_unique_routes(value)


class IncrementalRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    base_version: int = Field(ge=1)
    upserts: list[Route] = Field(default_factory=list)
    delete_route_ids: list[str] = Field(default_factory=list)
    digest: str | None = None

    @field_validator("upserts")
    @classmethod
    def validate_upserts(cls, value: list[Route]) -> list[Route]:
        return _validate_unique_routes(value)

    @field_validator("delete_route_ids")
    @classmethod
    def validate_deletes(cls, value: list[str]) -> list[str]:
        if len(value) != len(set(value)):
            raise ValueError("delete_route_ids must be unique")
        for route_id in value:
            if not ROUTE_ID_RE.match(route_id):
                raise ValueError(f"invalid route_id in delete_route_ids: {route_id}")
        return value

    @model_validator(mode="after")
    def validate_changes(self) -> "IncrementalRequest":
        upsert_ids = {route.route_id for route in self.upserts}
        if not upsert_ids and not self.delete_route_ids:
            raise ValueError("incremental request must contain upserts or delete_route_ids")
        if upsert_ids.intersection(self.delete_route_ids):
            raise ValueError("a route cannot be both upserted and deleted")
        return self
