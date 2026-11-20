from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class AppError(Exception):
    code: str
    message: str
    status_code: int = 400
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        super().__init__(self.message)

    def payload(self) -> dict[str, Any]:
        return {
            "ok": False,
            "code": self.code,
            "message": self.message,
            "details": self.details,
        }


def policy_invalid(field: str, reason: str, **details: Any) -> AppError:
    return AppError(
        "E_POLICY_INVALID",
        f"策略字段 {field} 无效：{reason}",
        status_code=422,
        details={"field": field, "reason": reason, **details},
    )


def version_malformed(value: Any, field: str = "version") -> AppError:
    return AppError(
        "E_VERSION_MALFORMED",
        f"版本段 {value!r} 无法解析",
        status_code=422,
        details={"field": field, "value": value, "expected": r"[v]N(.N)*([-._]N)*"},
    )


def format_incompatible(reason: str, **details: Any) -> AppError:
    return AppError(
        "E_FORMAT_INCOMPATIBLE",
        f"格式或版本不兼容：{reason}",
        status_code=409,
        details={"reason": reason, **details},
    )


def format_malformed(reason: str, **details: Any) -> AppError:
    return AppError(
        "E_FORMAT_MALFORMED",
        f"格式数据损坏或截断：{reason}",
        status_code=422,
        details={"reason": reason, **details},
    )

