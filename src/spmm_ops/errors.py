"""Stable business errors used by the library and CLI."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping


BUSINESS_CODES = frozenset(
    {
        "E_SHAPE_INVALID",
        "E_BAD_CSR",
        "E_DUP_INDEX",
        "E_NUMERIC_MISMATCH",
        "E_MEMORY_BUDGET",
        "E_STALE_PUBLISH",
        "E_REFERENCE_MISSING",
        "E_REFERENCE_INVALID",
        "E_DTYPE_UNSUPPORTED",
        "E_DTYPE_MISMATCH",
        "E_LOCK_TIMEOUT",
        "E_CACHE_CORRUPT",
        "E_USAGE",
        "E_IO",
    }
)


@dataclass
class SpMMError(Exception):
    """A deterministic, machine-readable error.

    ``code`` is the stable business code; ``details`` contains structured
    context and ``location`` points at the exact array/row/column when known.
    """

    code: str
    message: str
    details: Mapping[str, Any] = field(default_factory=dict)
    location: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        if self.code not in BUSINESS_CODES:
            raise ValueError(f"unknown business code: {self.code}")
        super().__init__(f"{self.code}: {self.message}")

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "ok": False,
            "code": self.code,
            "message": self.message,
            "details": dict(self.details),
        }
        if self.location is not None:
            out["location"] = dict(self.location)
        return out


def fail(
    code: str,
    message: str,
    *,
    details: Mapping[str, Any] | None = None,
    location: Mapping[str, Any] | None = None,
) -> None:
    raise SpMMError(code, message, details or {}, location)

