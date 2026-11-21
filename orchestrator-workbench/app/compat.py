"""Explicit format/version compatibility matrix and negotiation verdict.

Verdicts:
  accept            - native precision, every required field is present
  degraded_accept   - older/looser format: missing fields are filled with
                      declared defaults and the detail is recorded
  reject            - unknown high version carrying unknown required fields

The matrix is declared once below so that the negotiation reason always points
at an explicit rule identifier rather than ad-hoc branching.
"""
from __future__ import annotations

from dataclasses import dataclass

from app.codecs import binary, jsonl, yaml_like
from app.errors import ErrorCode, WorkbenchError
from app.models import (
    DecodedPayload,
    DegradationEntry,
    FormatName,
    NegotiationDecision,
    NegotiationReport,
)


@dataclass(frozen=True)
class MatrixRule:
    rule_id: str
    fmt: FormatName
    min_version: int
    max_version: int
    decision: NegotiationDecision
    basis: str


COMPATIBILITY_MATRIX: tuple[MatrixRule, ...] = (
    MatrixRule(
        "BIN-V1-NATIVE",
        FormatName.BINARY,
        1,
        1,
        NegotiationDecision.ACCEPT,
        "binary v1 fixed-point permille weights, length prefixes, presence bitmap and CRC32",
    ),
    MatrixRule(
        "JSONL-V1-FLOAT-DEGRADE",
        FormatName.JSONL,
        1,
        1,
        NegotiationDecision.DEGRADED_ACCEPT,
        "jsonl v1 float weights are quantized to permille fixed-point under the caller rule",
    ),
    MatrixRule(
        "YAML-V1-LEGACY-DEGRADE",
        FormatName.YAML,
        1,
        1,
        NegotiationDecision.DEGRADED_ACCEPT,
        "yaml subset v1 has no weight/budget/expected_version fields; declared defaults are applied",
    ),
    MatrixRule(
        "FUTURE-REQUIRED-REJECT",
        FormatName.BINARY,
        2,
        99,
        NegotiationDecision.REJECT,
        "version newer than the reader declares unknown required fields",
    ),
    MatrixRule(
        "FUTURE-REQUIRED-REJECT",
        FormatName.JSONL,
        2,
        99,
        NegotiationDecision.REJECT,
        "version newer than the reader declares unknown required fields",
    ),
)

KNOWN_MAX_VERSION = {
    FormatName.BINARY: binary.HIGHEST_KNOWN_VERSION,
    FormatName.JSONL: jsonl.HIGHEST_KNOWN_VERSION,
    FormatName.YAML: yaml_like.SUPPORTED_VERSION,
}


def negotiate(payload: DecodedPayload) -> NegotiationReport:
    header = payload.header
    fmt = header.format
    version = header.format_version

    matching = [
        rule
        for rule in COMPATIBILITY_MATRIX
        if rule.fmt is fmt and rule.min_version <= version <= rule.max_version
    ]
    if not matching:
        raise WorkbenchError(
            ErrorCode.E_VERSION_UNSUPPORTED,
            f"no compatibility rule for {fmt.value} v{version}",
            {"format": fmt.value, "format_version": version},
        )
    rule = matching[0]

    if header.unknown_required_fields or rule.decision is NegotiationDecision.REJECT:
        raise WorkbenchError(
            ErrorCode.E_VERSION_UNSUPPORTED,
            f"{fmt.value} v{version} contains unknown required fields and is rejected",
            {
                "format": fmt.value,
                "format_version": version,
                "matrix_rule": rule.rule_id,
                "unknown_required_fields": header.unknown_required_fields,
            },
        )
    if version > KNOWN_MAX_VERSION[fmt]:
        raise WorkbenchError(
            ErrorCode.E_VERSION_UNSUPPORTED,
            f"{fmt.value} v{version} is above known reader version {KNOWN_MAX_VERSION[fmt]}",
        )

    reasons = [rule.basis]
    degradations: list[DegradationEntry] = []

    if fmt is FormatName.YAML:
        reasons.append("yaml records carry no weight, budget or expected_version")
        for record in payload.records:
            degradations.append(
                DegradationEntry(
                    resource_id=record.resource_id,
                    field="weight",
                    reason="legacy yaml subset omits weight; default 0 permille applied",
                    applied_value=0.0,
                )
            )
            degradations.append(
                DegradationEntry(
                    resource_id=record.resource_id,
                    field="budget",
                    reason="legacy yaml subset omits budget; default 0 applied",
                    applied_value=0.0,
                )
            )
            degradations.append(
                DegradationEntry(
                    resource_id=record.resource_id,
                    field="expected_version",
                    reason="legacy yaml subset omits expected_version; default 1 applied",
                    applied_value=1.0,
                )
            )
    elif fmt is FormatName.JSONL:
        reasons.append("jsonl float weights are quantized to permille fixed-point")
        for record in payload.records:
            if "weight" not in record.present_fields:
                degradations.append(
                    DegradationEntry(
                        resource_id=record.resource_id,
                        field="weight",
                        reason="jsonl record omits weight; default 0 permille applied",
                        applied_value=0.0,
                    )
                )
            if "budget" not in record.present_fields:
                degradations.append(
                    DegradationEntry(
                        resource_id=record.resource_id,
                        field="budget",
                        reason="jsonl record omits budget; default 0 applied",
                        applied_value=0.0,
                    )
                )
            if "expected_version" not in record.present_fields:
                degradations.append(
                    DegradationEntry(
                        resource_id=record.resource_id,
                        field="expected_version",
                        reason="jsonl record omits expected_version; default 1 applied",
                        applied_value=1.0,
                    )
                )

    return NegotiationReport(
        format=fmt,
        format_version=version,
        decision=rule.decision,
        matrix_rule=rule.rule_id,
        reasons=reasons,
        degradations=degradations,
        unknown_required_fields=list(header.unknown_required_fields),
    )
