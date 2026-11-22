"""Central, deterministic configuration for thresholds, windows and scoring.

Values can be overridden per (service, metric) through the ``thresholds``
mapping; global defaults apply otherwise.  Nothing in this module is random.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

# Fixed-point scale used for all numeric contract calculations (see
# fixedpoint.py).  One unit = 10**SCALE micro-units.
FP_SCALE = 6

# Sliding aggregation window, expressed as a number of most-recent samples
# per (service, metric).  Deterministic and version-independent.
WINDOW_SIZE = 40

# Minimum number of samples inside a window before a quantile is defined.
MIN_SAMPLES = 4

# Nearest-rank quantile points emitted in every aggregation entry.
QUANTILE_POINTS = (0.50, 0.90, 0.99)

# Suppression window length in milliseconds.  Alerts sharing a fingerprint,
# service and level inside this window are merged into the suppression index.
SUPPRESSION_WINDOW_MS = 5 * 60_000  # 5 minutes

# Root-cause scoring weights (fixed-point integers, scale FP_SCALE).
W_HOPS = 600_000      # 0.6
W_TIME = 400_000      # 0.4

# A time delta larger than this (ms) contributes zero temporal proximity.
RCA_TIME_HALO_MS = 10 * 60_000  # 10 minutes

# Defensive bounds checked at every fixed-point arithmetic step.
MAX_HOPS = 32
MAX_TIME_DELTA_MS = 24 * 60 * 60_000  # one day
FP_MAX = 9_000_000_000_000_000  # below signed 64-bit ceiling with headroom


@dataclass
class Threshold:
    """Alerting thresholds for one (service, metric) pair or the default."""

    good_rate_min: float = 0.90   # minimum tolerated good-event rate
    latency_max_us: int = 50_000  # p99 latency upper bound, microseconds
    level: str = "WARNING"


@dataclass
class Settings:
    db_path: Path = Path("data/pipeline.db")
    window_size: int = WINDOW_SIZE
    min_samples: int = MIN_SAMPLES
    quantile_points: tuple[float, ...] = QUANTILE_POINTS
    suppression_window_ms: int = SUPPRESSION_WINDOW_MS
    default_threshold: Threshold = field(default_factory=Threshold)
    # key = (service, metric) -> Threshold
    thresholds: dict[tuple[str, str], Threshold] = field(default_factory=dict)

    def threshold_for(self, service: str, metric: str) -> Threshold:
        return self.thresholds.get((service, metric), self.default_threshold)


def default_db_path() -> Path:
    override = os.environ.get("RCA_DB_PATH")
    return Path(override) if override else Path("data/pipeline.db")


def get_settings(db_path: str | os.PathLike[str] | None = None) -> Settings:
    """Build the default settings used by the API and tests.

    A stricter ``CRITICAL`` rule is configured for payment latency so that the
    deterministic sample set drives a multi-level alert scenario.
    """
    settings = Settings(db_path=Path(db_path) if db_path else default_db_path())
    settings.thresholds[("payment", "request_latency_us")] = Threshold(
        good_rate_min=0.85, latency_max_us=45_000, level="CRITICAL"
    )
    return settings
