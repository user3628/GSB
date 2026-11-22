"""Stable business error codes for the retrieval-answer workbench.

Every code is a stable string identifier; :class:`WorkbenchError` carries the
code plus a structured context (session id, turn, stage) so failures are never
silent and are reproducible from on-disk diagnostics.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict

E_RETRIEVAL_EMPTY = "E_RETRIEVAL_EMPTY"
E_NUMERIC = "E_NUMERIC"
E_ANSWER_INVALID = "E_ANSWER_INVALID"
E_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
E_INVALID_STAGE = "E_INVALID_STAGE"
E_INCREMENTAL_MISMATCH = "E_INCREMENTAL_MISMATCH"
E_BUDGET_EXCEEDED = "E_BUDGET_EXCEEDED"
E_CONFIG_INVALID = "E_CONFIG_INVALID"
E_IMPORT_INVALID = "E_IMPORT_INVALID"

ALL_CODES = (
    E_RETRIEVAL_EMPTY,
    E_NUMERIC,
    E_ANSWER_INVALID,
    E_STALE_CHECKPOINT,
    E_INVALID_STAGE,
    E_INCREMENTAL_MISMATCH,
    E_BUDGET_EXCEEDED,
    E_CONFIG_INVALID,
    E_IMPORT_INVALID,
)


@dataclass
class WorkbenchError(Exception):
    """Raised for expected, code-identified business failures."""

    code: str
    message: str
    stage: str = ""
    session_id: str = ""
    turn_index: int | None = None
    context: Dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:  # pragma: no cover - trivial
        loc = self.session_id
        if self.turn_index is not None:
            loc = f"{loc}#turn{self.turn_index}"
        parts = [self.code]
        if self.stage:
            parts.append(f"stage={self.stage}")
        if loc:
            parts.append(f"at={loc}")
        parts.append(self.message)
        return " ".join(parts)

    def to_dict(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "code": self.code,
            "message": self.message,
            "stage": self.stage,
            "session_id": self.session_id,
            "turn_index": self.turn_index,
        }
        data.update(self.context)
        return data
