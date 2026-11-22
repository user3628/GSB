from __future__ import annotations

import ipaddress
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


@dataclass(slots=True)
class AuthorityConfig:
    id: str
    host: str
    port: int
    zones: list[str] = field(default_factory=list)
    transport: str = "udp"
    zone_file: str | None = None

    def validate(self) -> None:
        if not self.id:
            raise ValueError("authority id cannot be empty")
        address = ipaddress.ip_address(self.host)
        if not address.is_loopback:
            raise ValueError(f"authority host must be loopback: {self.host}")
        if not (1 <= self.port <= 65535):
            raise ValueError(f"invalid authority port for {self.id}: {self.port}")
        if self.transport not in {"udp", "tcp"}:
            raise ValueError(f"invalid authority transport: {self.transport}")
        if not self.zones:
            raise ValueError(f"authority {self.id} has no zones")
        self.zones = [zone.lower().rstrip(".") + "." for zone in self.zones]


@dataclass(slots=True)
class SchedulerConfig:
    global_upstream_limit: int = 8
    per_authority_limit: int = 2
    queue_limit: int = 64
    round_budget_ms: int = 800
    upstream_timeout_ms: int = 300
    merge_split_enabled: bool = True
    merge_split_after_ms: int = 20
    merge_split_reserve_ms: int = 20
    min_split_budget_ms: int = 40
    max_splits_per_flight: int = 1

    def validate(self) -> None:
        if self.global_upstream_limit < 1:
            raise ValueError("global_upstream_limit must be >= 1")
        if self.per_authority_limit < 1:
            raise ValueError("per_authority_limit must be >= 1")
        if self.queue_limit < 0:
            raise ValueError("queue_limit must be >= 0")
        if self.round_budget_ms < 1:
            raise ValueError("round_budget_ms must be >= 1")
        if self.upstream_timeout_ms < 1:
            raise ValueError("upstream_timeout_ms must be >= 1")


@dataclass(slots=True)
class AppConfig:
    db_path: str = "artifacts/dns-workbench.sqlite3"
    log_dir: str = "artifacts/logs"
    http_host: str = "127.0.0.1"
    http_port: int = 18080
    dns_host: str = "127.0.0.1"
    dns_port: int = 15353
    enable_dns_listener: bool = True
    enable_authorities: bool = False
    hot_names: list[str] = field(default_factory=list)
    max_cname_hops: int = 8
    max_udp_response_bytes: int = 1232
    authorities: list[AuthorityConfig] = field(default_factory=list)
    scheduler: SchedulerConfig = field(default_factory=SchedulerConfig)
    raw: dict[str, Any] = field(default_factory=dict)

    def validate(self) -> None:
        self.http_port = int(self.http_port)
        self.dns_port = int(self.dns_port)
        http_address = ipaddress.ip_address(self.http_host)
        dns_address = ipaddress.ip_address(self.dns_host)
        if not http_address.is_loopback:
            raise ValueError("http_host must be a loopback address")
        if not dns_address.is_loopback:
            raise ValueError("dns_host must be a loopback address")
        if not (1 <= self.http_port <= 65535):
            raise ValueError("http_port must be in 1..65535")
        if self.enable_dns_listener and not (1 <= self.dns_port <= 65535):
            raise ValueError("dns_port must be in 1..65535")
        if self.max_cname_hops < 0:
            raise ValueError("max_cname_hops must be >= 0")
        if self.max_udp_response_bytes < 512:
            raise ValueError("max_udp_response_bytes must be >= 512")
        self.scheduler.validate()
        seen: set[str] = set()
        for authority in self.authorities:
            authority.validate()
            if authority.id in seen:
                raise ValueError(f"duplicate authority id: {authority.id}")
            seen.add(authority.id)

    @property
    def db_file(self) -> Path:
        return Path(self.db_path).expanduser().resolve()

    @property
    def log_path(self) -> Path:
        return Path(self.log_dir).expanduser().resolve()

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "AppConfig":
        scheduler_data = data.get("scheduler", {})
        scheduler = SchedulerConfig(
            **{key: scheduler_data[key] for key in SchedulerConfig.__dataclass_fields__ if key in scheduler_data}
        )
        authorities = []
        for item in data.get("authorities", []):
            authorities.append(
                AuthorityConfig(
                    **{key: item[key] for key in AuthorityConfig.__dataclass_fields__ if key in item}
                )
            )
        fields = {
            key: data[key]
            for key in AppConfig.__dataclass_fields__
            if key in data and key not in {"scheduler", "authorities", "raw"}
        }
        config = cls(**fields, scheduler=scheduler, authorities=authorities, raw=data)
        if isinstance(config.enable_dns_listener, str):
            config.enable_dns_listener = _bool(config.enable_dns_listener)
        if isinstance(config.enable_authorities, str):
            config.enable_authorities = _bool(config.enable_authorities)
        config.validate()
        return config

    @classmethod
    def load(cls, path: str | os.PathLike[str]) -> "AppConfig":
        config_path = Path(path)
        with config_path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        base = config_path.parent
        for key in ("db_path", "log_dir"):
            value = data.get(key)
            if value and not Path(value).is_absolute():
                data[key] = str((base / value).resolve())
        for item in data.get("authorities", []):
            value = item.get("zone_file")
            if value and not Path(value).is_absolute():
                item["zone_file"] = str((base / value).resolve())
        return cls.from_dict(data)

    def redacted(self) -> dict[str, Any]:
        return {
            "http": f"{self.http_host}:{self.http_port}",
            "dns": f"{self.dns_host}:{self.dns_port}" if self.enable_dns_listener else None,
            "db_path": str(self.db_file),
            "log_dir": str(self.log_path),
            "max_cname_hops": self.max_cname_hops,
            "max_udp_response_bytes": self.max_udp_response_bytes,
            "scheduler": {
                "global_upstream_limit": self.scheduler.global_upstream_limit,
                "per_authority_limit": self.scheduler.per_authority_limit,
                "queue_limit": self.scheduler.queue_limit,
                "round_budget_ms": self.scheduler.round_budget_ms,
                "upstream_timeout_ms": self.scheduler.upstream_timeout_ms,
            },
            "authorities": [
                {"id": item.id, "host": item.host, "port": item.port, "zones": item.zones}
                for item in self.authorities
            ],
        }

