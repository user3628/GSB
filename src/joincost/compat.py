from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .errors import BusinessError, E_FORMAT_UNSUPPORTED

ACCEPT = "ACCEPT"
DEGRADE_ACCEPT = "DEGRADE_ACCEPT"
REJECT = "REJECT"

COMPAT_MATRIX: dict[str, dict[int, str]] = {
    "binary": {1: DEGRADE_ACCEPT, 2: ACCEPT},
    "jsonl": {1: DEGRADE_ACCEPT, 2: ACCEPT},
}

KNOWN_REQUIRED_FEATURES = {
    "optional_field_bitmap",
    "correlation_coefficient",
    "stale_keys",
    "crc32",
    "big_endian",
}


@dataclass(frozen=True)
class CompatibilityVerdict:
    format_name: str
    format_version: int
    decision: str
    reasons: tuple[dict[str, Any], ...] = ()
    rejected_code: str | None = None

    @property
    def accepted(self) -> bool:
        return self.decision != REJECT

    def public_dict(self) -> dict[str, Any]:
        return {
            "format": self.format_name,
            "format_version": self.format_version,
            "decision": self.decision,
            "reasons": list(self.reasons),
            "rejected_code": self.rejected_code,
        }


def decide(
    format_name: str,
    format_version: int,
    required_features: list[str] | None = None,
) -> CompatibilityVerdict:
    reasons: list[dict[str, Any]] = []
    matrix = COMPAT_MATRIX.get(format_name)
    if matrix is None:
        reasons.append(
            {
                "kind": "unknown_format",
                "message": f"format {format_name!r} is not in the compatibility matrix",
            }
        )
        return CompatibilityVerdict(
            format_name, format_version, REJECT, tuple(reasons), E_FORMAT_UNSUPPORTED
        )
    decision = matrix.get(format_version)
    if decision is None:
        supported = sorted(matrix)
        reasons.append(
            {
                "kind": "unsupported_version",
                "format_version": format_version,
                "supported_versions": supported,
                "message": "format version is newer than or absent from the matrix",
            }
        )
        return CompatibilityVerdict(
            format_name, format_version, REJECT, tuple(reasons), E_FORMAT_UNSUPPORTED
        )
    if required_features:
        unknown = sorted(set(required_features) - KNOWN_REQUIRED_FEATURES)
        if unknown:
            reasons.append(
                {
                    "kind": "unknown_required_fields",
                    "unknown_required_features": unknown,
                    "message": "snapshot declares required fields not understood by this build",
                }
            )
            return CompatibilityVerdict(
                format_name, format_version, REJECT, tuple(reasons), E_FORMAT_UNSUPPORTED
            )
    if decision == DEGRADE_ACCEPT or reasons:
        return CompatibilityVerdict(format_name, format_version, DEGRADE_ACCEPT, tuple(reasons))
    return CompatibilityVerdict(format_name, format_version, ACCEPT)


def ensure_supported(verdict: CompatibilityVerdict, extra_report: dict[str, Any]) -> None:
    if verdict.accepted:
        return
    details = {
        "import_report": extra_report,
        "compatibility": verdict.public_dict(),
    }
    raise BusinessError(
        E_FORMAT_UNSUPPORTED,
        "statistics format or version is not supported",
        details,
    )


def degradation(
    *,
    relation: str | None,
    field_name: str,
    action: str,
    reason: str,
    old_value: Any = None,
    new_value: Any = None,
) -> dict[str, Any]:
    item: dict[str, Any] = {
        "relation": relation,
        "field": field_name,
        "action": action,
        "reason": reason,
    }
    if old_value is not None:
        item["old_value"] = old_value
    if new_value is not None:
        item["new_value"] = new_value
    return item

