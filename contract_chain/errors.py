"""Stable, serializable error codes used across the contract-chain pipeline."""


class ChainError(Exception):
    """Base error carrying a stable machine-readable code and context payload."""

    code = "E_CHAIN"

    def __init__(self, message, details=None):
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def to_dict(self):
        return {"code": self.code, "message": self.message, "details": self.details}


class RefParseError(ChainError):
    """Raised / recorded when a citation raw_text cannot resolve to a location."""

    code = "E_REF_PARSE"


class UnitUnresolvedError(ChainError):
    """Raised / recorded when a term cannot be normalized into the fixed domain."""

    code = "E_UNIT_UNRESOLVED"


class EvidenceBrokenError(ChainError):
    """Raised when an imported evidence manifest is missing a required link."""

    code = "E_EVIDENCE_BROKEN"


class IncrementalMismatchError(ChainError):
    """Raised when incremental rebuild diverges from a from-scratch full run."""

    code = "E_INCREMENTAL_MISMATCH"


class StaleCheckpointError(ChainError):
    """Raised when a publisher proposes a non-advancing stage sequence number."""

    code = "E_STALE_CHECKPOINT"


class InvalidStageError(ChainError):
    """Raised when a stage is published without its predecessor stage present."""

    code = "E_INVALID_STAGE"


class StopRequested(Exception):
    """Internal control-flow signal for cooperative graceful stopping."""

    def __init__(self, at_ref=None):
        super().__init__("stop requested")
        self.at_ref = at_ref
