from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class WorkbenchConfig:
    host: str = "127.0.0.1"
    port: int = 8765
    sqlite_path: Path = Path("data/workbench.db")
    samples_dir: Path = Path("samples")
    edge_nodes: tuple[str, ...] = ("edge-a", "edge-b", "edge-c")
    origin_base_url: str | None = None
    refused_origin_url: str = "http://127.0.0.1:1"
    origin_timeout_ms: float = 1500.0
    max_body_pad_bytes: int = 65536
    request_max_age_seconds: float = 86400.0
    seed_content_manifest: Path | None = Path("samples/origin_content_v1.json")
    seed_policy_file: Path | None = None
    auto_recover: bool = True
    debug_errors: bool = False
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def effective_origin_base_url(self) -> str:
        return (self.origin_base_url or f"http://{self.host}:{self.port}").rstrip("/")

    @classmethod
    def from_dict(cls, data: dict[str, Any], *, base_dir: Path | None = None) -> "WorkbenchConfig":
        base = (base_dir or Path.cwd()).resolve()

        def path_value(value: Any, default: Path | str | None) -> Path | None:
            if value is None:
                return None
            candidate = Path(str(value)).expanduser()
            if not candidate.is_absolute():
                candidate = base / candidate
            return candidate.resolve()

        raw = dict(data)
        nodes = raw.get("edge_nodes", ["edge-a", "edge-b", "edge-c"])
        if not isinstance(nodes, list) or not all(isinstance(node, str) and node for node in nodes):
            raise ValueError("edge_nodes must be a non-empty list of strings")
        if not nodes:
            raise ValueError("edge_nodes must not be empty")
        known = set(raw) & {
            "host",
            "port",
            "sqlite_path",
            "samples_dir",
            "edge_nodes",
            "origin_base_url",
            "refused_origin_url",
            "origin_timeout_ms",
            "max_body_pad_bytes",
            "request_max_age_seconds",
            "seed_content_manifest",
            "seed_policy_file",
            "auto_recover",
            "debug_errors",
        }
        extra = {key: value for key, value in raw.items() if key not in known}
        config = cls(
            host=str(raw.get("host", "127.0.0.1")),
            port=int(raw.get("port", 8765)),
            sqlite_path=path_value(raw.get("sqlite_path", "data/workbench.db"), "data/workbench.db"),  # type: ignore[arg-type]
            samples_dir=path_value(raw.get("samples_dir", "samples"), "samples"),  # type: ignore[arg-type]
            edge_nodes=tuple(nodes),
            origin_base_url=raw.get("origin_base_url"),
            refused_origin_url=str(raw.get("refused_origin_url", "http://127.0.0.1:1")),
            origin_timeout_ms=float(raw.get("origin_timeout_ms", 1500)),
            max_body_pad_bytes=int(raw.get("max_body_pad_bytes", 65536)),
            request_max_age_seconds=float(raw.get("request_max_age_seconds", 86400)),
            seed_content_manifest=path_value(raw.get("seed_content_manifest", "samples/origin_content_v1.json"), None),
            seed_policy_file=path_value(raw.get("seed_policy_file"), None),
            auto_recover=bool(raw.get("auto_recover", True)),
            debug_errors=bool(raw.get("debug_errors", False)),
            extra=extra,
        )
        if not (0 <= config.port <= 65535):
            raise ValueError("port must be between 0 and 65535")
        if not config.edge_nodes:
            raise ValueError("at least one edge node is required")
        if config.origin_timeout_ms <= 0:
            raise ValueError("origin_timeout_ms must be positive")
        if config.max_body_pad_bytes < 0:
            raise ValueError("max_body_pad_bytes must be non-negative")
        return config

    @classmethod
    def load(cls, path: str | Path | None = None) -> "WorkbenchConfig":
        selected = path or os.environ.get("CDN_SIM_CONFIG")
        if selected is None:
            candidates = [
                Path.cwd() / "config" / "default.json",
                Path(__file__).resolve().parents[1] / "config" / "default.json",
            ]
            config_path = next((candidate for candidate in candidates if candidate.exists()), candidates[0])
        else:
            config_path = Path(selected)
        config_path = config_path.expanduser().resolve()
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        data = json.loads(config_path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError("Configuration root must be a JSON object")
        return cls.from_dict(data, base_dir=config_path.parent)

    def ensure_paths(self) -> None:
        assert isinstance(self.sqlite_path, Path)
        assert isinstance(self.samples_dir, Path)
        self.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        self.samples_dir.mkdir(parents=True, exist_ok=True)

    def as_public_dict(self) -> dict[str, Any]:
        return {
            "host": self.host,
            "port": self.port,
            "sqlite_path": str(self.sqlite_path),
            "samples_dir": str(self.samples_dir),
            "edge_nodes": list(self.edge_nodes),
            "origin_base_url": self.effective_origin_base_url,
            "refused_origin_url": self.refused_origin_url,
            "origin_timeout_ms": self.origin_timeout_ms,
            "max_body_pad_bytes": self.max_body_pad_bytes,
            "auto_recover": self.auto_recover,
        }

