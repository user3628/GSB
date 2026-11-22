"""Explicit format/version compatibility matrix."""

from __future__ import annotations

from typing import Any

from .models import NegotiationResult, SnapshotManifest


CURRENT_FORMAT_VERSION = 2
LEGACY_FORMAT_VERSION = 1
FUTURE_FORMAT_VERSION = 3

COMPATIBILITY_MATRIX: dict[str, dict[int, dict[str, Any]]] = {
    "binary": {
        1: {
            "verdict": "DEGRADE",
            "reasons": [
                "version 1 has no late_count; default 0 is deterministic",
                "version 1 stores window_end_ago_ms; convert with generated_at_ms",
            ],
        },
        2: {"verdict": "ACCEPT", "reasons": ["current supported version"]},
        3: {
            "verdict": "REJECT",
            "code": "E_FORMAT_UNSUPPORTED",
            "reasons": ["version 3 declares required fields unknown to this runtime"],
        },
    },
    "jsonl": {
        1: {
            "verdict": "DEGRADE",
            "reasons": [
                "version 1 has no late_count; default 0 is deterministic",
                "version 1 stores window_end_ago_us; convert with generated_at_ms",
            ],
        },
        2: {"verdict": "ACCEPT", "reasons": ["current supported version"]},
        3: {
            "verdict": "REJECT",
            "code": "E_FORMAT_UNSUPPORTED",
            "reasons": ["version 3 declares required fields unknown to this runtime"],
        },
    },
}


def compatibility_document() -> dict[str, Any]:
    return {
        "current_version": CURRENT_FORMAT_VERSION,
        "legacy_version": LEGACY_FORMAT_VERSION,
        "future_version": FUTURE_FORMAT_VERSION,
        "verdicts": ["ACCEPT", "DEGRADE", "REJECT"],
        "matrix": {
            format_name: {
                str(version): rule
                for version, rule in version_map.items()
            }
            for format_name, version_map in COMPATIBILITY_MATRIX.items()
        },
        "upgrade_rules": {
            "legacy_missing_late_count": {
                "action": "default_to_zero",
                "recorded_as": "degradation.late_count_defaulted",
            },
            "legacy_relative_time": {
                "absolute_window_start_ms": (
                    "generated_at_ms - window_end_ago_ms - window_length_ms"
                ),
                "jsonl_absolute_window_start_us": (
                    "generated_at_ms*1000 - window_end_ago_us - window_length_ms*1000"
                ),
                "then": "normalize to integer milliseconds with caller truncation rule",
            },
            "future_unknown_required": {
                "action": "reject_before_import",
                "code": "E_FORMAT_UNSUPPORTED",
                "preserve_existing_state": True,
            },
        },
    }


def negotiate(
    file_format: str,
    manifest: SnapshotManifest,
    unknown_required_fields: tuple[str, ...] = (),
) -> NegotiationResult:
    version = manifest.format_version
    format_matrix = COMPATIBILITY_MATRIX.get(file_format)
    if format_matrix is None:
        raise ValueError(f"unknown format: {file_format}")

    unknown = tuple(
        field
        for field in dict.fromkeys(
            (*manifest.required_extensions, *unknown_required_fields)
        )
        if field
    )
    if unknown:
        return NegotiationResult(
            file_format=file_format,
            format_version=version,
            verdict="REJECT",
            manifest=manifest,
            reasons=(
                "input declares required fields not understood by this runtime",
            ),
            code="E_FORMAT_UNSUPPORTED",
            unknown_required_fields=unknown,
        )

    rule = format_matrix.get(version)
    if rule is None:
        return NegotiationResult(
            file_format=file_format,
            format_version=version,
            verdict="REJECT",
            manifest=manifest,
            reasons=(
                f"format version {version} is outside the explicit compatibility matrix",
            ),
            code="E_FORMAT_UNSUPPORTED",
        )

    verdict = str(rule["verdict"])
    degradations: tuple[dict[str, Any], ...] = ()
    if verdict == "DEGRADE":
        degradations = (
            {
                "action": "late_count_defaulted",
                "from": None,
                "to": 0,
                "rule": "version_1_missing_late_count",
            },
            {
                "action": "relative_time_converted",
                "rule": "generated_at_ms - window_end_ago - window_length",
                "target": "absolute_window_start_ms",
            },
        )
    return NegotiationResult(
        file_format=file_format,
        format_version=version,
        verdict=verdict,
        manifest=manifest,
        reasons=tuple(str(item) for item in rule.get("reasons", ())),
        degradations=degradations,
        code=rule.get("code"),
    )
