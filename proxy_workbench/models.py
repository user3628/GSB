"""Validated, immutable route configuration and snapshot envelopes."""

from __future__ import annotations

import hashlib
import ipaddress
import json
import math
import re
from dataclasses import dataclass
from typing import Any, Iterable, Mapping
from urllib.parse import urlsplit

from .errors import E_CONFIG_INVALID, ProxyError, config_error


_ROUTE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
_HEADER_NAME = re.compile(r"^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$")
_METHOD = re.compile(r"^[A-Z*][A-Z0-9*_-]{0,31}$")
HOP_BY_HOP_HEADERS = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "proxy-connection",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    }
)


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def content_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def is_loopback_host(host: str) -> bool:
    normalized = host.strip().strip("[]").lower()
    if normalized == "localhost":
        return True
    try:
        return ipaddress.ip_address(normalized).is_loopback
    except ValueError:
        return False


def _require_mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise config_error(f"{name} must be a JSON object", field=name)
    return value


def _require_number(value: Any, name: str, *, minimum: float = 0.0) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise config_error(f"{name} must be a number", field=name)
    number = float(value)
    if not math.isfinite(number) or number < minimum:
        raise config_error(f"{name} must be >= {minimum}", field=name)
    return number


def _require_int(value: Any, name: str, *, minimum: int = 0, maximum: int = 2**31) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise config_error(f"{name} must be an integer", field=name)
    if value < minimum or value > maximum:
        raise config_error(f"{name} must be between {minimum} and {maximum}", field=name)
    return value


def _normalize_methods(values: Any) -> tuple[str, ...]:
    if not isinstance(values, list) or not values:
        raise config_error("match.methods must be a non-empty list", field="match.methods")
    methods: list[str] = []
    for value in values:
        if not isinstance(value, str):
            raise config_error("match.methods entries must be strings", field="match.methods")
        method = value.upper()
        if not _METHOD.match(method):
            raise config_error(f"invalid HTTP method: {value!r}", field="match.methods")
        if method not in methods:
            methods.append(method)
    return tuple(methods)


def _normalize_headers(value: Any, *, reject_hop_by_hop: bool = True) -> tuple[tuple[str, str], ...]:
    if value is None:
        return ()
    if not isinstance(value, dict):
        raise config_error("headers must be a JSON object", field="headers")
    result: list[tuple[str, str]] = []
    seen: set[str] = set()
    for name, header_value in value.items():
        if not isinstance(name, str) or not _HEADER_NAME.match(name):
            raise config_error(f"invalid header name: {name!r}", field="headers")
        lower = name.lower()
        if lower in seen:
            raise config_error(f"duplicate header name (case-insensitive): {name}", field="headers")
        if reject_hop_by_hop and lower in HOP_BY_HOP_HEADERS:
            raise config_error(
                f"hop-by-hop header cannot be configured: {name}",
                field="headers",
                header=name,
            )
        if lower in ("host", "content-length"):
            raise config_error(
                f"transport-managed header cannot be configured: {name}",
                field="headers",
                header=name,
            )
        if not isinstance(header_value, str) or "\r" in header_value or "\n" in header_value:
            raise config_error(f"invalid value for header {name}", field="headers")
        try:
            header_value.encode("latin-1")
        except UnicodeEncodeError as exc:
            raise config_error(
                f"header {name} must be latin-1 encodable on the wire", field="headers"
            ) from exc
        seen.add(lower)
        result.append((name, header_value))
    return tuple(result)


def normalize_upstream(value: Any, name: str = "upstream") -> str:
    if not isinstance(value, str):
        raise config_error(f"{name} must be a URL string", field=name)
    try:
        parts = urlsplit(value)
    except ValueError as exc:
        raise config_error(f"invalid {name}: {value!r}", field=name) from exc
    if parts.scheme.lower() != "http":
        raise config_error("only http upstreams are supported", field=name)
    if parts.username is not None or parts.password is not None:
        raise config_error("credentials in upstream URLs are not allowed", field=name)
    if parts.query or parts.fragment:
        raise config_error("upstream URL cannot contain query or fragment", field=name)
    host = parts.hostname
    if not host or not is_loopback_host(host):
        raise config_error("upstream host must be a loopback address", field=name)
    try:
        port = parts.port if parts.port is not None else 80
    except ValueError as exc:
        raise config_error(f"invalid port in {name}", field=name) from exc
    if not 1 <= port <= 65535:
        raise config_error(f"invalid port in {name}", field=name)
    display_host = f"[{host}]" if ":" in host else host
    path = parts.path or ""
    if path in ("", "/"):
        path = ""
    elif path.endswith("/"):
        path = path.rstrip("/")
    return f"http://{display_host}:{port}{path}"


def upstream_origin(upstream: str) -> str:
    parts = urlsplit(upstream)
    return f"{parts.scheme}://{parts.netloc}"


@dataclass(frozen=True)
class MatchRule:
    methods: tuple[str, ...]
    path_prefix: str
    target_host: str | None = None
    target_port: int | None = None

    def allows(self, method: str) -> bool:
        return "*" in self.methods or method.upper() in self.methods

    def public(self) -> dict[str, Any]:
        value: dict[str, Any] = {
            "methods": list(self.methods),
            "path_prefix": self.path_prefix,
        }
        if self.target_host is not None:
            value["target_host"] = self.target_host
            value["target_port"] = self.target_port
        return value


@dataclass(frozen=True)
class TimeoutConfig:
    connect: float
    read: float

    def public(self) -> dict[str, float]:
        return {"connect": self.connect, "read": self.read}


@dataclass(frozen=True)
class RetryConfig:
    attempts: int
    backoff_ms: int
    retry_on: tuple[str, ...]

    def public(self) -> dict[str, Any]:
        return {
            "attempts": self.attempts,
            "backoff_ms": self.backoff_ms,
            "retry_on": list(self.retry_on),
        }


@dataclass(frozen=True)
class HealthConfig:
    enabled: bool
    path: str
    interval: float
    timeout: float
    fail_threshold: int
    success_threshold: int

    def public(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "path": self.path,
            "interval": self.interval,
            "timeout": self.timeout,
            "fail_threshold": self.fail_threshold,
            "success_threshold": self.success_threshold,
        }


@dataclass(frozen=True)
class FailurePolicy:
    mode: str
    backup_upstreams: tuple[str, ...]
    group: str

    def public(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "backup_upstreams": list(self.backup_upstreams),
            "group": self.group,
        }


@dataclass(frozen=True)
class RouteConfig:
    id: str
    kind: str
    match: MatchRule
    upstream: str
    timeout: TimeoutConfig
    retries: RetryConfig
    headers: tuple[tuple[str, str], ...]
    health: HealthConfig
    failure_policy: FailurePolicy
    in_flight_policy: str

    @property
    def upstream_candidates(self) -> tuple[str, ...]:
        return (self.upstream,) + self.failure_policy.backup_upstreams

    def canonical(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "kind": self.kind,
            "match": self.match.public(),
            "upstream": self.upstream,
            "timeout": self.timeout.public(),
            "retries": self.retries.public(),
            "headers": {name: value for name, value in self.headers},
            "health": self.health.public(),
            "failure_policy": self.failure_policy.public(),
            "in_flight_policy": self.in_flight_policy,
        }

    @property
    def fingerprint(self) -> str:
        return content_hash(self.canonical())

    def public(self) -> dict[str, Any]:
        return self.canonical()


def parse_route(value: Any, *, index: int | None = None) -> RouteConfig:
    location = f"routes[{index}]" if index is not None else "route"
    data = _require_mapping(value, location)
    route_id = data.get("id")
    if not isinstance(route_id, str) or not _ROUTE_ID.match(route_id):
        raise config_error(f"{location}.id is invalid", field="id")
    kind = data.get("kind")
    if kind not in ("reverse", "forward"):
        raise config_error(f"{location}.kind must be reverse or forward", field="kind")

    match_data = _require_mapping(data.get("match"), f"{location}.match")
    methods = _normalize_methods(match_data.get("methods"))
    prefix = match_data.get("path_prefix", "/")
    if not isinstance(prefix, str) or not prefix.startswith("/") or any(c in prefix for c in "?#"):
        raise config_error("match.path_prefix must start with / and contain no ? or #", field="path_prefix")
    target_host: str | None = None
    target_port: int | None = None
    if kind == "reverse":
        if "target_host" in match_data or "target_port" in match_data:
            raise config_error("reverse routes cannot specify target_host/target_port", field="match")
    else:
        host = match_data.get("target_host")
        if not isinstance(host, str) or not is_loopback_host(host):
            raise config_error("forward match.target_host must be loopback", field="target_host")
        target_host = host.strip().strip("[]").lower()
        target_port = _require_int(match_data.get("target_port"), "match.target_port", minimum=1, maximum=65535)

    upstream = normalize_upstream(data.get("upstream"))
    timeout_data = data.get("timeout", {"connect": 1.0, "read": 3.0})
    if isinstance(timeout_data, (int, float)) and not isinstance(timeout_data, bool):
        connect_timeout = read_timeout = _require_number(timeout_data, "timeout", minimum=0.001)
    else:
        timeout_data = _require_mapping(timeout_data, f"{location}.timeout")
        connect_timeout = _require_number(timeout_data.get("connect", 1.0), "timeout.connect", minimum=0.001)
        read_timeout = _require_number(timeout_data.get("read", 3.0), "timeout.read", minimum=0.001)

    retry_data = data.get("retries", {"attempts": 1})
    if isinstance(retry_data, int) and not isinstance(retry_data, bool):
        retry_data = {"attempts": retry_data}
    retry_data = _require_mapping(retry_data, f"{location}.retries")
    attempts = _require_int(retry_data.get("attempts", 1), "retries.attempts", minimum=1, maximum=8)
    backoff_ms = _require_int(retry_data.get("backoff_ms", 0), "retries.backoff_ms", maximum=10_000)
    raw_retry_on = retry_data.get(
        "retry_on", ["connect_timeout", "connection_refused", "read_timeout", "5xx"]
    )
    allowed_retry = {"connect_timeout", "connection_refused", "read_timeout", "write_timeout", "5xx"}
    if not isinstance(raw_retry_on, list) or any(item not in allowed_retry for item in raw_retry_on):
        raise config_error("retries.retry_on contains an unsupported code", field="retries.retry_on")
    retry_on = tuple(dict.fromkeys(raw_retry_on))

    headers = _normalize_headers(data.get("headers", {}))

    health_data = data.get("health", {})
    health_data = _require_mapping(health_data, f"{location}.health")
    health_enabled = health_data.get("enabled", True)
    if not isinstance(health_enabled, bool):
        raise config_error("health.enabled must be boolean", field="health.enabled")
    health_path = health_data.get("path", "/health")
    if not isinstance(health_path, str) or not health_path.startswith("/") or "?" in health_path or "#" in health_path:
        raise config_error("health.path must be an absolute path", field="health.path")
    health = HealthConfig(
        enabled=health_enabled,
        path=health_path,
        interval=_require_number(health_data.get("interval", 5.0), "health.interval", minimum=0.01),
        timeout=_require_number(health_data.get("timeout", 0.5), "health.timeout", minimum=0.001),
        fail_threshold=_require_int(
            health_data.get("fail_threshold", 1), "health.fail_threshold", minimum=1, maximum=100
        ),
        success_threshold=_require_int(
            health_data.get("success_threshold", 1), "health.success_threshold", minimum=1, maximum=100
        ),
    )

    policy_data = _require_mapping(data.get("failure_policy", {"mode": "fail"}), f"{location}.failure_policy")
    mode = policy_data.get("mode", "fail")
    if mode not in ("fail", "failover"):
        raise config_error("failure_policy.mode must be fail or failover", field="failure_policy.mode")
    raw_backups = policy_data.get("backup_upstreams", [])
    if not isinstance(raw_backups, list):
        raise config_error("failure_policy.backup_upstreams must be a list", field="failure_policy")
    backups: list[str] = []
    for backup_index, backup in enumerate(raw_backups):
        normalized = normalize_upstream(backup, f"failure_policy.backup_upstreams[{backup_index}]")
        if normalized != upstream and normalized not in backups:
            backups.append(normalized)
    group = policy_data.get("group", route_id)
    if not isinstance(group, str) or not group or len(group) > 64:
        raise config_error("failure_policy.group must be a non-empty string", field="failure_policy.group")
    if mode == "failover" and not backups:
        raise config_error("failover mode requires at least one backup upstream", field="failure_policy")

    policy = FailurePolicy(mode=mode, backup_upstreams=tuple(backups), group=group)
    in_flight_policy = data.get("in_flight_policy", "continue")
    if in_flight_policy not in ("continue", "abort"):
        raise config_error("in_flight_policy must be continue or abort", field="in_flight_policy")

    return RouteConfig(
        id=route_id,
        kind=kind,
        match=MatchRule(methods=methods, path_prefix=prefix, target_host=target_host, target_port=target_port),
        upstream=upstream,
        timeout=TimeoutConfig(connect=connect_timeout, read=read_timeout),
        retries=RetryConfig(attempts=attempts, backoff_ms=backoff_ms, retry_on=retry_on),
        headers=headers,
        health=health,
        failure_policy=policy,
        in_flight_policy=in_flight_policy,
    )


def _unwrap_config(payload: Any) -> Mapping[str, Any]:
    if not isinstance(payload, dict):
        raise config_error("configuration payload must be a JSON object")
    if "config" in payload and not any(key in payload for key in ("kind", "version", "base_version")):
        nested = payload["config"]
        if not isinstance(nested, dict):
            raise config_error("config must be a JSON object")
        return nested
    return payload


@dataclass(frozen=True)
class SnapshotConfig:
    version: int
    routes: tuple[RouteConfig, ...]

    def public(self) -> dict[str, Any]:
        return {
            "kind": "snapshot",
            "version": self.version,
            "routes": [route.public() for route in self.routes],
        }


@dataclass(frozen=True)
class IncrementConfig:
    version: int
    base_version: int
    routes: tuple[RouteConfig, ...]
    remove_route_ids: tuple[str, ...]


def _parse_routes(value: Any, *, allow_empty: bool = False) -> tuple[RouteConfig, ...]:
    if value is None and allow_empty:
        return ()
    if not isinstance(value, list) or (not value and not allow_empty):
        raise config_error(
            "routes must be a non-empty list" if not allow_empty else "routes must be a list",
            field="routes",
        )
    routes = tuple(parse_route(item, index=index) for index, item in enumerate(value))
    ids = [route.id for route in routes]
    if len(ids) != len(set(ids)):
        raise config_error("route ids must be unique", field="routes")
    return routes


def parse_snapshot(payload: Any) -> SnapshotConfig:
    data = _unwrap_config(payload)
    kind = data.get("kind", "snapshot")
    if kind not in ("snapshot", "full"):
        raise config_error("expected a snapshot payload", field="kind")
    version = _require_int(data.get("version"), "version", minimum=1)
    routes = _parse_routes(data.get("routes"), allow_empty=True)
    normalized_routes = tuple(sorted(routes, key=lambda route: route.id))
    return SnapshotConfig(version=version, routes=normalized_routes)


def parse_increment(payload: Any) -> IncrementConfig:
    data = _unwrap_config(payload)
    if data.get("kind") not in ("incremental", "increment", "patch"):
        raise config_error("expected an incremental payload", field="kind")
    version = _require_int(data.get("version"), "version", minimum=1)
    base_version = _require_int(data.get("base_version"), "base_version", minimum=1)
    routes = _parse_routes(data.get("routes"))
    removals = data.get("remove_route_ids", [])
    if not isinstance(removals, list) or any(not isinstance(item, str) for item in removals):
        raise config_error("remove_route_ids must be a list of strings", field="remove_route_ids")
    if len(removals) != len(set(removals)):
        raise config_error("remove_route_ids must be unique", field="remove_route_ids")
    if not routes and not removals:
        raise config_error("incremental payload must upsert or remove at least one route", field="routes")
    upsert_ids = {route.id for route in routes}
    overlap = upsert_ids.intersection(removals)
    if overlap:
        raise config_error("a route cannot be both upserted and removed", routes=sorted(overlap))
    return IncrementConfig(
        version=version,
        base_version=base_version,
        routes=routes,
        remove_route_ids=tuple(removals),
    )


def apply_increment(existing: Iterable[RouteConfig], increment: IncrementConfig) -> tuple[RouteConfig, ...]:
    by_id = {route.id: route for route in existing}
    for route in increment.routes:
        by_id[route.id] = route
    for route_id in increment.remove_route_ids:
        by_id.pop(route_id, None)
    if not by_id:
        raise config_error("incremental update would remove all routes", field="routes")
    return tuple(sorted(by_id.values(), key=lambda route: route.id))


def parse_config_payload(payload: Any) -> SnapshotConfig | IncrementConfig:
    data = _unwrap_config(payload)
    kind = data.get("kind")
    if kind is None:
        return parse_increment(data) if "base_version" in data else parse_snapshot(data)
    if kind in ("incremental", "increment", "patch"):
        return parse_increment(data)
    if kind in ("snapshot", "full"):
        return parse_snapshot(data)
    raise config_error("unknown configuration kind", field="kind")


def snapshot_payload(version: int, routes: Iterable[RouteConfig]) -> dict[str, Any]:
    return {
        "kind": "snapshot",
        "version": version,
        "routes": [route.public() for route in sorted(routes, key=lambda route: route.id)],
    }


def restore_snapshot(payload: Any) -> SnapshotConfig:
    return parse_snapshot(payload)


def changed_fields(old: RouteConfig | None, new: RouteConfig | None) -> list[str]:
    if old is None or new is None:
        return ["<added>" if old is None else "<removed>"]
    before = old.canonical()
    after = new.canonical()
    result: list[str] = []
    for key in sorted(set(before) | set(after)):
        if before.get(key) != after.get(key):
            result.append(key)
    return result

