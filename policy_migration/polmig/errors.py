"""Business error codes for the policy migration component."""


class MigrationError(Exception):
    code = "E_INTERNAL"

    def __init__(self, message, details=None):
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def to_dict(self):
        return {"code": self.code, "message": self.message, "details": self.details}


class PolicyInvalid(MigrationError):
    code = "E_POLICY_INVALID"


class RecordCorrupt(MigrationError):
    code = "E_RECORD_CORRUPT"


class FormatUnsupported(MigrationError):
    code = "E_FORMAT_UNSUPPORTED"


class MigrationRejected(MigrationError):
    code = "E_MIGRATION_REJECTED"


class LifetimeOverlap(MigrationError):
    code = "E_LIFETIME_OVERLAP"


class ParentCycle(MigrationError):
    code = "E_PARENT_CYCLE"


class ParentDepthExceeded(MigrationError):
    code = "E_PARENT_DEPTH_EXCEEDED"


class StaleBase(MigrationError):
    code = "E_STALE_BASE"


class ConflictingBatch(MigrationError):
    code = "E_CONFLICTING_BATCH"


class BatchNotFound(MigrationError):
    code = "E_BATCH_NOT_FOUND"


HTTP_STATUS = {
    "E_POLICY_INVALID": 422,
    "E_RECORD_CORRUPT": 422,
    "E_FORMAT_UNSUPPORTED": 415,
    "E_MIGRATION_REJECTED": 422,
    "E_LIFETIME_OVERLAP": 422,
    "E_PARENT_CYCLE": 422,
    "E_PARENT_DEPTH_EXCEEDED": 422,
    "E_STALE_BASE": 409,
    "E_CONFLICTING_BATCH": 409,
    "E_BATCH_NOT_FOUND": 404,
    "E_INTERNAL": 500,
}
