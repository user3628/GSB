from __future__ import annotations

import json
import os
from dataclasses import dataclass, fields, replace
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True, slots=True)
class WorkbenchConfig:
    database_path: Path = PROJECT_ROOT / "data" / "workbench.sqlite3"
    sample_database_path: Path = PROJECT_ROOT / "data" / "slowquery_sample.sqlite3"
    slow_duration_ms: int = 500
    slow_scan_rows: int = 500
    lock_wait_threshold_ms: int = 100
    returned_rows_threshold: int = 100
    selectivity_bias_ratio: float = 0.20
    limit_ceiling: int = 500
    max_result_rows: int = 10_000
    sqlite_busy_timeout_ms: int = 5_000
    verification_temp_dir: Path | None = None
    host: str = "127.0.0.1"
    port: int = 8765

    def __post_init__(self) -> None:
        object.__setattr__(self, "database_path", Path(self.database_path).expanduser().resolve())
        object.__setattr__(
            self, "sample_database_path", Path(self.sample_database_path).expanduser().resolve()
        )
        if self.verification_temp_dir is not None:
            object.__setattr__(
                self,
                "verification_temp_dir",
                Path(self.verification_temp_dir).expanduser().resolve(),
            )
        for name in (
            "slow_duration_ms",
            "slow_scan_rows",
            "lock_wait_threshold_ms",
            "returned_rows_threshold",
            "limit_ceiling",
            "max_result_rows",
            "sqlite_busy_timeout_ms",
            "port",
        ):
            value = getattr(self, name)
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        if self.limit_ceiling < 1:
            raise ValueError("limit_ceiling must be at least 1")
        if not (0 < self.selectivity_bias_ratio <= 1):
            raise ValueError("selectivity_bias_ratio must be in (0, 1]")

    @classmethod
    def from_mapping(cls, mapping: dict[str, Any], base: "WorkbenchConfig | None" = None):
        base = base or cls()
        known = {item.name for item in fields(cls)}
        unknown = sorted(set(mapping) - known)
        if unknown:
            raise ValueError(f"unknown configuration keys: {', '.join(unknown)}")
        converted: dict[str, Any] = {}
        for key, value in mapping.items():
            if key in {"database_path", "sample_database_path", "verification_temp_dir"}:
                converted[key] = None if value is None else Path(value)
            else:
                converted[key] = value
        return replace(base, **converted)

    @classmethod
    def from_sources(
        cls,
        config_file: str | Path | None = None,
        overrides: dict[str, Any] | None = None,
    ) -> "WorkbenchConfig":
        config = cls()
        if config_file:
            with open(config_file, "r", encoding="utf-8") as handle:
                config = cls.from_mapping(json.load(handle), config)
        env_mapping: dict[str, Any] = {}
        env_names = {
            "SLOWQUERY_DB": "database_path",
            "SLOWQUERY_SAMPLE_DB": "sample_database_path",
            "SLOWQUERY_HOST": "host",
            "SLOWQUERY_PORT": "port",
        }
        for env_name, key in env_names.items():
            if env_name in os.environ:
                env_mapping[key] = os.environ[env_name]
        if env_mapping:
            if "port" in env_mapping:
                env_mapping["port"] = int(env_mapping["port"])
            config = cls.from_mapping(env_mapping, config)
        if overrides:
            config = cls.from_mapping(
                {key: value for key, value in overrides.items() if value is not None}, config
            )
        return config
