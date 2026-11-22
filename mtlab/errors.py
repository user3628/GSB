from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class MtlError(Exception):
    """Stable business error with a machine-readable payload."""

    code: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        super().__init__(self.message)

    def as_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, "details": self.details}


@dataclass
class GracefulStop(Exception):
    """Raised at a checkpoint-safe unit boundary after a stop request."""

    tier: str
    stage: str
    completed_stages: list[str] = field(default_factory=list)


def empty_sentence_error(
    *,
    pair_id: str,
    tier: str,
    side: str,
    source: str,
    target: str,
    noise_flags: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "code": "E_EMPTY_SENTENCE",
        "pair_id": pair_id,
        "tier": tier,
        "side": side,
        "reason": "empty_or_punctuation_only_after_normalization",
        "source": source,
        "target": target,
        "noise_flags": sorted(noise_flags or []),
    }


def numeric_error(
    *,
    lang_pair: str,
    message: str,
    positions: list[dict[str, Any]],
) -> MtlError:
    return MtlError(
        code="E_NUMERIC",
        message=message,
        details={"lang_pair": lang_pair, "positions": positions},
    )


def decode_error(
    *,
    pair_id: str,
    step_index: int,
    message: str,
) -> MtlError:
    return MtlError(
        code="E_DECODE_FAILED",
        message=message,
        details={"pair_id": pair_id, "step_index": step_index},
    )

