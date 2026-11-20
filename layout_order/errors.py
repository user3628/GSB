"""稳定业务错误码。

所有引擎级错误均抛出 :class:`LayoutError`，携带机器可读的 ``code`` 与
结构化 ``details``，CLI/HTTP 层据此映射为退出码与 HTTP 状态码。
"""

from __future__ import annotations

from typing import Any, Mapping


class LayoutError(Exception):
    def __init__(self, code: str, message: str, details: Mapping[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = dict(details or {})

    def to_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, "details": self.details}


# 错误码 -> HTTP 状态码（CLI 统一以退出码 2 表示业务错误）
HTTP_STATUS: dict[str, int] = {
    "E_BOX_INVALID": 422,
    "E_COLUMN_UNASSIGNED": 422,
    "E_ORDER_INVALID": 422,
    "E_STALE_CHECKPOINT": 409,
    "E_INVALID_STAGE": 400,
    "E_INCREMENTAL_MISMATCH": 409,
    "E_DOC_NOT_FOUND": 404,
    "E_NOT_FOUND": 404,
}
