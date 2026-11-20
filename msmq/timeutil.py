from __future__ import annotations

import time
from typing import Literal

from .errors import E_TIME_PRECISION_UNSUPPORTED, MQError

SubMsPolicy = Literal["truncate", "reject"]

US_PER_MS = 1_000


def now_us() -> int:
    """Wall-clock time normalized to the component's millisecond grid."""

    return (time.time_ns() // 1_000_000) * US_PER_MS


def ms_to_us(value_ms: int) -> int:
    return int(value_ms) * US_PER_MS


def us_to_ms_floor(value_us: int) -> int:
    return int(value_us) // US_PER_MS


def normalize_us(value_us: int, policy: SubMsPolicy, *, field: str, index: int) -> tuple[int, bool]:
    """Normalize microseconds to the internal millisecond grid.

    ``truncate`` is intentionally floor-based.  ``reject`` is available when a
    caller needs a strict lossless import and does not want sub-millisecond
    precision silently discarded.
    """

    value_us = int(value_us)
    if value_us % US_PER_MS == 0:
        return value_us, False
    if policy == "reject":
        raise MQError(
            E_TIME_PRECISION_UNSUPPORTED,
            "sub-millisecond timestamp cannot be represented losslessly",
            http_status=422,
            details={"index": index, "field": field, "value_us": value_us, "policy": policy},
        )
    return (value_us // US_PER_MS) * US_PER_MS, True


def check_policy(policy: str) -> SubMsPolicy:
    if policy not in {"truncate", "reject"}:
        raise MQError(
            E_TIME_PRECISION_UNSUPPORTED,
            "unsupported sub-millisecond policy",
            http_status=422,
            details={"policy": policy, "supported": ["truncate", "reject"]},
        )
    return policy  # type: ignore[return-value]

