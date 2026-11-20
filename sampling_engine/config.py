"""Engine configuration loaded from JSON with conservative defaults."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .errors import invalid_manifest


@dataclass(frozen=True, slots=True)
class EngineConfig:
    host: str = "127.0.0.1"
    port: int = 8765
    max_temperature: float = 5.0
    v1_default_top_p: float = 0.9
    v1_temperature_scale: float = 100.0
    lock_timeout_seconds: float = 30.0
    workspace: str = "work/demo"
    initial_manifests: tuple[str, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if self.host not in {"127.0.0.1", "localhost"}:
            raise invalid_manifest(
                "the local HTTP service must bind to a loopback address",
                field="host",
                value=self.host,
            )
        if not 0 <= int(self.port) <= 65535:
            raise invalid_manifest(
                "port must be in [0, 65535]", field="port", value=self.port
            )
        if not (self.max_temperature > 0.0 and self.max_temperature < float("inf")):
            raise invalid_manifest(
                "max_temperature must be positive and finite",
                field="max_temperature",
                value=self.max_temperature,
            )
        if not (0.0 < self.v1_default_top_p <= 1.0):
            raise invalid_manifest(
                "v1_default_top_p must be in (0, 1]",
                field="v1_default_top_p",
                value=self.v1_default_top_p,
            )
        if not (self.v1_temperature_scale > 0.0 and self.v1_temperature_scale <= 10000.0):
            raise invalid_manifest(
                "v1_temperature_scale must be in (0, 10000]",
                field="v1_temperature_scale",
                value=self.v1_temperature_scale,
            )
        if self.lock_timeout_seconds <= 0.0:
            raise invalid_manifest(
                "lock_timeout_seconds must be positive",
                field="lock_timeout_seconds",
                value=self.lock_timeout_seconds,
            )

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "EngineConfig":
        allowed = {
            "host",
            "port",
            "max_temperature",
            "v1_default_top_p",
            "v1_temperature_scale",
            "lock_timeout_seconds",
            "workspace",
            "initial_manifests",
        }
        unknown = sorted(set(payload) - allowed)
        if unknown:
            raise invalid_manifest(
                "unknown configuration fields", fields=unknown
            )
        manifests = payload.get("initial_manifests", [])
        if not isinstance(manifests, list) or not all(
            isinstance(item, str) for item in manifests
        ):
            raise invalid_manifest(
                "initial_manifests must be a list of paths",
                field="initial_manifests",
            )
        return cls(
            host=payload.get("host", "127.0.0.1"),
            port=int(payload.get("port", 8765)),
            max_temperature=float(payload.get("max_temperature", 5.0)),
            v1_default_top_p=float(payload.get("v1_default_top_p", 0.9)),
            v1_temperature_scale=float(payload.get("v1_temperature_scale", 100.0)),
            lock_timeout_seconds=float(payload.get("lock_timeout_seconds", 30.0)),
            workspace=str(payload.get("workspace", "work/demo")),
            initial_manifests=tuple(manifests),
        )

    @classmethod
    def load(cls, path: str | Path) -> "EngineConfig":
        source = Path(path)
        try:
            payload = json.loads(source.read_text(encoding="utf-8"))
        except FileNotFoundError:
            raise invalid_manifest("configuration file not found", path=str(source))
        except json.JSONDecodeError as exc:
            raise invalid_manifest(
                "configuration is not valid JSON", path=str(source), detail=str(exc)
            )
        if not isinstance(payload, dict):
            raise invalid_manifest(
                "configuration root must be a JSON object", path=str(source)
            )
        return cls.from_dict(payload)

    def to_dict(self) -> dict[str, Any]:
        return {
            "host": self.host,
            "port": self.port,
            "max_temperature": self.max_temperature,
            "v1_default_top_p": self.v1_default_top_p,
            "v1_temperature_scale": self.v1_temperature_scale,
            "lock_timeout_seconds": self.lock_timeout_seconds,
            "workspace": self.workspace,
            "initial_manifests": list(self.initial_manifests),
        }
