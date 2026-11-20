"""Runtime configuration and time/window parsing helpers."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

from .errors import invalid_request

_WINDOW_RE = re.compile(r"^(?P<value>\d+)(?P<unit>s|m|h)?$", re.IGNORECASE)


def parse_window(value: str | int, supported: set[int] | None = None) -> int:
    """Parse ``300``, ``300s``, ``5m`` or ``1h`` into seconds."""

    if isinstance(value, int):
        seconds = value
    else:
        match = _WINDOW_RE.fullmatch(str(value).strip())
        if not match:
            raise invalid_request(f"Unsupported window format: {value!r}")
        seconds = int(match.group("value"))
        unit = (match.group("unit") or "s").lower()
        seconds *= {"s": 1, "m": 60, "h": 3600}[unit]
    if seconds <= 0:
        raise invalid_request("Window duration must be positive.", window=value)
    if supported is not None and seconds not in supported:
        raise invalid_request(
            "Window is not configured for this instance.",
            window=value,
            supported_windows=[window_label(item) for item in sorted(supported)],
        )
    return seconds


def window_label(seconds: int) -> str:
    if seconds % 3600 == 0:
        return f"{seconds // 3600}h"
    if seconds % 60 == 0:
        return f"{seconds // 60}m"
    return f"{seconds}s"


def _env_int(env: Mapping[str, str], name: str, default: int) -> int:
    raw = env.get(name)
    return default if raw in (None, "") else int(raw)


def _env_float(env: Mapping[str, str], name: str, default: float) -> float:
    raw = env.get(name)
    return default if raw in (None, "") else float(raw)


@dataclass(frozen=True)
class Settings:
    """All tunables. Values are intentionally process-level for reproducibility."""

    base_dir: Path = field(default_factory=lambda: Path.cwd())
    data_dir: Path = field(default_factory=lambda: Path.cwd() / "data")
    db_path: Path = field(default_factory=lambda: Path.cwd() / "data" / "slo.db")
    bucket_seconds: int = 60
    windows: tuple[int, ...] = (300, 1800, 3600)
    slo_target: float = 0.999
    minimum_samples: int = 100
    exact_event_limit: int = 10_000
    late_tolerance_seconds: int = 120
    warning_burn_rate: float = 2.0
    critical_burn_rate: float = 6.0
    alert_short_window: int = 300
    alert_long_window: int = 3600
    confidence_level: float = 0.95
    max_batch_events: int = 100_000
    query_cache_size: int = 512
    stall_after_intent_ms: int = 0
    sqlite_busy_timeout_ms: int = 30_000

    @classmethod
    def from_env(
        cls,
        env: Mapping[str, str] | None = None,
        *,
        base_dir: Path | None = None,
    ) -> "Settings":
        source = os.environ if env is None else env
        root = Path(base_dir) if base_dir is not None else Path.cwd()
        data_dir_raw = source.get("SLO_DATA_DIR", "data")
        data_dir = Path(data_dir_raw)
        if not data_dir.is_absolute():
            data_dir = root / data_dir
        db_raw = source.get("SLO_DB_PATH", str(data_dir / "slo.db"))
        db_path = Path(db_raw)
        if not db_path.is_absolute():
            db_path = root / db_path

        windows_raw = source.get("SLO_WINDOWS", "5m,30m,1h")
        windows = tuple(parse_window(item) for item in windows_raw.split(",") if item)
        if not windows:
            raise ValueError("SLO_WINDOWS must contain at least one duration")

        settings = cls(
            base_dir=root,
            data_dir=data_dir,
            db_path=db_path,
            bucket_seconds=_env_int(source, "SLO_BUCKET_SECONDS", 60),
            windows=windows,
            slo_target=_env_float(source, "SLO_TARGET", 0.999),
            minimum_samples=_env_int(source, "SLO_MINIMUM_SAMPLES", 100),
            exact_event_limit=_env_int(source, "SLO_EXACT_EVENT_LIMIT", 10_000),
            late_tolerance_seconds=_env_int(
                source, "SLO_LATE_TOLERANCE_SECONDS", 120
            ),
            warning_burn_rate=_env_float(source, "SLO_WARNING_BURN_RATE", 2.0),
            critical_burn_rate=_env_float(source, "SLO_CRITICAL_BURN_RATE", 6.0),
            alert_short_window=_env_int(source, "SLO_ALERT_SHORT_SECONDS", windows[0]),
            alert_long_window=_env_int(source, "SLO_ALERT_LONG_SECONDS", windows[-1]),
            confidence_level=_env_float(source, "SLO_CONFIDENCE_LEVEL", 0.95),
            max_batch_events=_env_int(source, "SLO_MAX_BATCH_EVENTS", 100_000),
            query_cache_size=_env_int(source, "SLO_QUERY_CACHE_SIZE", 512),
            stall_after_intent_ms=_env_int(
                source, "SLO_STALL_AFTER_INTENT_MS", 0
            ),
            sqlite_busy_timeout_ms=_env_int(
                source, "SLO_SQLITE_BUSY_TIMEOUT_MS", 30_000
            ),
        )
        settings.validate()
        return settings

    def validate(self) -> None:
        if self.bucket_seconds <= 0:
            raise ValueError("SLO_BUCKET_SECONDS must be positive")
        if any(window % self.bucket_seconds != 0 for window in self.windows):
            raise ValueError("Every SLO window must be a multiple of the base bucket")
        if len(set(self.windows)) != len(self.windows):
            raise ValueError("SLO_WINDOWS must not contain duplicates")
        if not (0.0 < self.slo_target < 1.0):
            raise ValueError("SLO_TARGET must be between 0 and 1 (exclusive)")
        if self.minimum_samples < 1:
            raise ValueError("SLO_MINIMUM_SAMPLES must be at least 1")
        if self.exact_event_limit < self.minimum_samples:
            raise ValueError("SLO_EXACT_EVENT_LIMIT must be >= SLO_MINIMUM_SAMPLES")
        if self.late_tolerance_seconds < 0:
            raise ValueError("SLO_LATE_TOLERANCE_SECONDS must be non-negative")
        if self.alert_short_window not in self.windows:
            raise ValueError("SLO_ALERT_SHORT_SECONDS must be configured in SLO_WINDOWS")
        if self.alert_long_window not in self.windows:
            raise ValueError("SLO_ALERT_LONG_SECONDS must be configured in SLO_WINDOWS")
        if self.alert_short_window >= self.alert_long_window:
            raise ValueError("The alert short window must be shorter than the long window")
        if not (0 < self.warning_burn_rate <= self.critical_burn_rate):
            raise ValueError("Burn-rate thresholds must satisfy 0 < warning <= critical")
        if not (0.5 < self.confidence_level < 1.0):
            raise ValueError("SLO_CONFIDENCE_LEVEL must be between 0.5 and 1")
        if self.max_batch_events < 1:
            raise ValueError("SLO_MAX_BATCH_EVENTS must be positive")
        if self.query_cache_size < 1:
            raise ValueError("SLO_QUERY_CACHE_SIZE must be at least 1")
        if self.stall_after_intent_ms < 0:
            raise ValueError("SLO_STALL_AFTER_INTENT_MS must be non-negative")

    @property
    def bucket_ms(self) -> int:
        return self.bucket_seconds * 1000

    @property
    def late_tolerance_ms(self) -> int:
        return self.late_tolerance_seconds * 1000

    def window_seconds_set(self) -> set[int]:
        return set(self.windows)


def align_down(value_ms: int, unit_ms: int) -> int:
    return (value_ms // unit_ms) * unit_ms


def align_up(value_ms: int, unit_ms: int) -> int:
    return -((-value_ms) // unit_ms) * unit_ms


def parse_timestamp_ms(value: object) -> int:
    """Accept ISO-8601 or epoch seconds/milliseconds and return UTC epoch ms."""

    if isinstance(value, bool) or value is None:
        raise invalid_request("timestamp must be an ISO-8601 string or epoch number")
    if isinstance(value, (int, float)):
        numeric = float(value)
        if numeric <= 0:
            raise invalid_request("timestamp must be positive", timestamp=value)
        result = int(round(numeric * 1000)) if abs(numeric) < 100_000_000_000 else int(numeric)
        return result
    if isinstance(value, str):
        text = value.strip()
        if not text:
            raise invalid_request("timestamp must not be empty")
        if re.fullmatch(r"[+-]?\d+(?:\.\d+)?", text):
            return parse_timestamp_ms(float(text))
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError as exc:
            raise invalid_request(f"Invalid ISO-8601 timestamp: {value!r}") from exc
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(round(parsed.astimezone(timezone.utc).timestamp() * 1000))
    raise invalid_request(f"Unsupported timestamp type: {type(value).__name__}")


def format_timestamp_ms(value_ms: int) -> str:
    dt = datetime.fromtimestamp(value_ms / 1000, tz=timezone.utc)
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")
