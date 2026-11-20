"""统一业务错误码与异常。

HTTP API 与 Python 调用共用同一组错误码，API 层把异常序列化为
``{"error": {"code": ..., "message": ..., "detail": ...}}`` 的 JSON。
"""


class ErrorCode:
    E_LINE_INVALID = "E_LINE_INVALID"
    E_CONFLICTING_RECORD = "E_CONFLICTING_RECORD"
    E_INVALID_TRANSITION = "E_INVALID_TRANSITION"
    E_STALE_BASE = "E_STALE_BASE"
    E_BATCH_NOT_FOUND = "E_BATCH_NOT_FOUND"
    E_RECORD_NOT_FOUND = "E_RECORD_NOT_FOUND"
    E_LOCKED = "E_LOCKED"
    E_INTERNAL = "E_INTERNAL"


HTTP_STATUS = {
    ErrorCode.E_LINE_INVALID: 400,
    ErrorCode.E_CONFLICTING_RECORD: 409,
    ErrorCode.E_INVALID_TRANSITION: 409,
    ErrorCode.E_STALE_BASE: 409,
    ErrorCode.E_BATCH_NOT_FOUND: 404,
    ErrorCode.E_RECORD_NOT_FOUND: 404,
    ErrorCode.E_LOCKED: 423,
    ErrorCode.E_INTERNAL: 500,
}


class LogIndexError(Exception):
    """所有业务异常的基类，携带稳定的业务码。"""

    code = ErrorCode.E_INTERNAL

    def __init__(self, message: str, detail=None):
        super().__init__(message)
        self.message = message
        self.detail = detail if detail is not None else {}

    def to_dict(self) -> dict:
        return {"code": self.code, "message": self.message, "detail": self.detail}


class LineInvalidError(LogIndexError):
    code = ErrorCode.E_LINE_INVALID


class ConflictingRecordError(LogIndexError):
    code = ErrorCode.E_CONFLICTING_RECORD


class InvalidTransitionError(LogIndexError):
    code = ErrorCode.E_INVALID_TRANSITION


class StaleBaseError(LogIndexError):
    code = ErrorCode.E_STALE_BASE


class BatchNotFoundError(LogIndexError):
    code = ErrorCode.E_BATCH_NOT_FOUND


class RecordNotFoundError(LogIndexError):
    code = ErrorCode.E_RECORD_NOT_FOUND


class LockedError(LogIndexError):
    code = ErrorCode.E_LOCKED
