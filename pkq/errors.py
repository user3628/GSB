"""统一业务错误码。所有失败响应为 {"error": {"code": ..., "message": ..., "details": {...}}}。"""
from __future__ import annotations


class PkqError(Exception):
    code = "E_INTERNAL"
    http_status = 500

    def __init__(self, message: str = "", **details):
        super().__init__(message or self.code)
        self.message = message or self.code
        self.details = details

    def to_payload(self) -> dict:
        return {"error": {"code": self.code, "message": self.message, "details": self.details}}


class SnapshotInvalid(PkqError):
    code = "E_SNAPSHOT_INVALID"
    http_status = 409


class BlockCorrupt(PkqError):
    code = "E_BLOCK_CORRUPT"
    http_status = 422

    def __init__(self, message: str = "", block_id: int = -1, **details):
        super().__init__(message, block_id=block_id, **details)
        self.block_id = block_id


class QueryTooLarge(PkqError):
    code = "E_QUERY_TOO_LARGE"
    http_status = 413

    def __init__(self, message: str = "", limit: int = 0, **details):
        super().__init__(message, limit=limit, **details)
        self.limit = limit


class ScanQueueFull(PkqError):
    code = "E_SCAN_QUEUE_FULL"
    http_status = 429


class PageOutOfRange(PkqError):
    code = "E_PAGE_OUT_OF_RANGE"
    http_status = 416


class PageConsumed(PkqError):
    code = "E_PAGE_CONSUMED"
    http_status = 409


class ConflictingQuery(PkqError):
    code = "E_CONFLICTING_QUERY"
    http_status = 409


class SessionNotFound(PkqError):
    code = "E_SESSION_NOT_FOUND"
    http_status = 404
