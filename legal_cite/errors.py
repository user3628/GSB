"""Business error codes and structured exceptions.

All error codes are stable strings.  Functions raise :class:`LegalCiteError`
carrying the code plus a machine-readable ``details`` payload so callers
(library, CLI, HTTP layer) can serialise diagnostics uniformly.
"""

E_CITATION_UNPARSED = "E_CITATION_UNPARSED"
E_ELEMENT_INVALID = "E_ELEMENT_INVALID"
E_EVIDENCE_BROKEN = "E_EVIDENCE_BROKEN"
E_VERSION_SHAPE_MISMATCH = "E_VERSION_SHAPE_MISMATCH"
E_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
E_INVALID_STAGE = "E_INVALID_STAGE"
E_INCREMENTAL_MISMATCH = "E_INCREMENTAL_MISMATCH"

ALL_ERROR_CODES = (
    E_CITATION_UNPARSED,
    E_ELEMENT_INVALID,
    E_EVIDENCE_BROKEN,
    E_VERSION_SHAPE_MISMATCH,
    E_STALE_CHECKPOINT,
    E_INVALID_STAGE,
    E_INCREMENTAL_MISMATCH,
)


class LegalCiteError(Exception):
    """Structured error with a stable code and diagnostic details."""

    def __init__(self, code, message, details=None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = dict(details or {})

    def to_dict(self):
        return {"code": self.code, "message": self.message, "details": self.details}

    def __str__(self):
        return f"{self.code}: {self.message} {self.details}".rstrip()
