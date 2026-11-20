"""Business error codes returned by the planner and the HTTP API."""


class QoptError(Exception):
    """Base class for business errors with a stable machine-readable code."""

    code = "E_INTERNAL"
    http_status = 400

    def __init__(self, message, details=None, **kw):
        super().__init__(message)
        self.message = message
        self.details = {**(details or {}), **kw}

    def to_dict(self):
        return {
            "ok": False,
            "error": {
                "code": self.code,
                "message": self.message,
                "details": self.details,
            },
        }


class SnapshotInvalidError(QoptError):
    code = "E_SNAPSHOT_INVALID"


class StatsMissingError(QoptError):
    code = "E_STATS_MISSING"


class SnapshotVersionMismatchError(QoptError):
    code = "E_SNAPSHOT_VERSION_MISMATCH"


class SnapshotNotLoadedError(QoptError):
    code = "E_SNAPSHOT_NOT_LOADED"


class QueryInvalidError(QoptError):
    code = "E_QUERY_INVALID"


class BadJsonError(QoptError):
    code = "E_BAD_JSON"


class NotFoundError(QoptError):
    code = "E_NOT_FOUND"
    http_status = 404
