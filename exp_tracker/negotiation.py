"""Format detection, version compatibility matrix and negotiation verdicts.

The compatibility matrix is *explicit*: each (format kind, format version)
pair declares exactly one disposition. Three verdicts exist:

* ``ACCEPT``          -> imported as-is;
* ``DOWNGRADE_ACCEPT``-> older/coarser data; defaults are back-filled and a
                          per-record downgrade detail is attached;
* ``REJECT``          -> the batch never enters the store.

Two rejection business codes are distinguished:
``E_VERSION_RETIRED`` for a version that was once supported but is now
retired, and ``E_FORMAT_UNSUPPORTED`` for an unknown/future version carrying
fields the current reader cannot understand.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List

from . import binary, jsonl_fmt
from .errors import ErrorCode, TrackerError

ACCEPT = "accept"
DOWNGRADE_ACCEPT = "downgrade_accept"
REJECT = "reject"

# Defaults used when an older producer omitted parameters entirely.
DEFAULT_PARAMS: Dict[str, Any] = {
    "learning_rate": 0.01,
    "batch_size": 32,
    "max_epochs": 10,
}

# Binary v1 stores ms timestamps (3 fewer sub-ms digits) and has no
# downgrade-log field; jsonl v1 omits the params block; jsonl v0 is retired.
@dataclass(frozen=True)
class MatrixEntry:
    kind: str
    format_version: int
    disposition: str
    reason: str
    precision: str          # "us" or "ms"
    params_present: bool
    reject_code: str = ""


@dataclass
class NegotiationResult:
    kind: str
    format_version: int
    verdict: str
    reason: str
    precision: str
    params_present: bool
    downgrades: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "kind": self.kind,
            "format_version": self.format_version,
            "verdict": self.verdict,
            "reason": self.reason,
            "precision": self.precision,
            "params_present": self.params_present,
            "downgrades": self.downgrades,
        }


COMPATIBILITY_MATRIX = [
    MatrixEntry("binary", binary.BIN_V2, ACCEPT, "current binary format", "us", True),
    MatrixEntry("binary", binary.BIN_V1, DOWNGRADE_ACCEPT,
                "binary v1 uses millisecond timestamps", "ms", True),
    MatrixEntry("jsonl", jsonl_fmt.JSON_V2, ACCEPT, "current jsonl format", "us", True),
    MatrixEntry("jsonl", jsonl_fmt.JSON_V1, DOWNGRADE_ACCEPT,
                "jsonl v1 may omit the params block", "us", False),
    MatrixEntry("jsonl", 0, REJECT, "jsonl v0 is retired", "ms", False,
                reject_code=ErrorCode.VERSION_RETIRED),
]


def _matrix_lookup(kind: str, version: int) -> MatrixEntry:
    for entry in COMPATIBILITY_MATRIX:
        if entry.kind == kind and entry.format_version == version:
            return entry
    # Unknown / future version: treated as carrying unknown required fields.
    raise TrackerError(
        ErrorCode.FORMAT_UNSUPPORTED,
        "format version is not present in the compatibility matrix",
        {"kind": kind, "format_version": version},
        http_status=415,
    )


def detect(path: str) -> Dict[str, object]:
    """Detect format kind and declared version without importing anything."""
    with open(path, "rb") as handle:
        prefix = handle.read(4)
    if prefix == binary.MAGIC:
        return binary.inspect_file(path)
    return jsonl_fmt.inspect_file(path)


def precheck(path: str) -> NegotiationResult:
    """Negotiation precheck.

    Inspects the input and applies the matrix. It performs no writes and never
    changes committed state; rejection is raised as a ``TrackerError``.
    """
    info = detect(path)
    kind = str(info["kind"])
    version = int(info["format_version"])  # type: ignore[arg-type]
    entry = _matrix_lookup(kind, version)
    if entry.disposition == REJECT:
        raise TrackerError(
            entry.reject_code or ErrorCode.FORMAT_UNSUPPORTED,
            entry.reason,
            {"kind": kind, "format_version": version},
            http_status=415,
        )
    downgrades: List[Dict[str, Any]] = []
    if entry.disposition == DOWNGRADE_ACCEPT:
        downgrades.append(
            {
                "kind": kind,
                "format_version": version,
                "reason": entry.reason,
                "precision": entry.precision,
                "backfilled_params": {} if entry.params_present else dict(DEFAULT_PARAMS),
            }
        )
    return NegotiationResult(
        kind=kind,
        format_version=version,
        verdict=entry.disposition,
        reason=entry.reason,
        precision=entry.precision,
        params_present=entry.params_present,
        downgrades=downgrades,
    )


def decode_with_negotiation(path: str):
    """Decode a file after matrix negotiation.

    Returns ``(runs, negotiation)``. For downgrade inputs each run is enriched
    with back-filled default params and a per-record downgrade detail. Raises
    on rejected/corrupt input before any caller writes to the store.
    """
    negotiation = precheck(path)
    if negotiation.kind == "binary":
        decoded = [run for _, run in binary.decode_file(path)]
    else:
        decoded = [run for _, run in jsonl_fmt.decode_file(path)]

    if negotiation.verdict == DOWNGRADE_ACCEPT:
        for run in decoded:
            detail = {
                "kind": negotiation.kind,
                "format_version": negotiation.format_version,
                "reason": negotiation.reason,
                "actions": [],
            }
            if not negotiation.params_present and not run.params:
                run.params = dict(DEFAULT_PARAMS)
                detail["actions"].append(
                    {"type": "backfill_params", "params": dict(DEFAULT_PARAMS)}
                )
            if negotiation.precision == "ms":
                detail["actions"].append(
                    {"type": "ms_to_us_grid", "note": "timestamps already on ms grid"}
                )
            run.downgrades.append(detail)
    return decoded, negotiation
