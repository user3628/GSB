from __future__ import annotations

from typing import Any


class BusinessError(Exception):
    """An expected domain failure with a stable business code."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        http_status: int = 422,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.http_status = http_status
        self.details = details or {}

    def as_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "ok": False,
            "code": self.code,
            "message": self.message,
        }
        if self.details:
            payload["details"] = self.details
        return payload


def report_invalid(message: str, details: dict[str, Any] | None = None) -> BusinessError:
    return BusinessError(
        "E_REPORT_INVALID",
        message,
        http_status=422,
        details=details,
    )


def round_not_found(round_id: str) -> BusinessError:
    return BusinessError(
        "E_ROUND_NOT_FOUND",
        f"修复轮次不存在: {round_id}",
        http_status=404,
        details={"round_id": round_id},
    )


def group_not_found(group_id: str) -> BusinessError:
    return BusinessError(
        "E_GROUP_NOT_FOUND",
        f"副本组不存在: {group_id}",
        http_status=404,
        details={"group_id": group_id},
    )


def conflicting_round(message: str, details: dict[str, Any] | None = None) -> BusinessError:
    return BusinessError(
        "E_CONFLICTING_ROUND",
        message,
        http_status=409,
        details=details,
    )


def conflicting_report(message: str, details: dict[str, Any] | None = None) -> BusinessError:
    return BusinessError(
        "E_CONFLICTING_REPORT",
        message,
        http_status=409,
        details=details,
    )


def version_mismatch(message: str, details: dict[str, Any] | None = None) -> BusinessError:
    return BusinessError(
        "E_GROUP_VERSION_MISMATCH",
        message,
        http_status=409,
        details=details,
    )


def normalization_required() -> BusinessError:
    return BusinessError(
        "E_NORMALIZATION_REQUIRED",
        "聚合请求必须显式指定 normalization，禁止静默选择归一化方案",
        http_status=400,
    )


def normalization_unknown(value: Any) -> BusinessError:
    return BusinessError(
        "E_NORMALIZATION_UNKNOWN",
        f"未知归一化方案: {value!r}",
        http_status=400,
        details={
            "allowed": ["ALL_EQUAL", "REPORTED_RATIO", "MISSING_ZERO"],
        },
    )


def progress_out_of_range(message: str, details: dict[str, Any] | None = None) -> BusinessError:
    return BusinessError(
        "E_PROGRESS_OUT_OF_RANGE",
        message,
        http_status=422,
        details=details,
    )


def weight_zero(message: str, details: dict[str, Any] | None = None) -> BusinessError:
    return BusinessError(
        "E_WEIGHT_ZERO",
        message,
        http_status=422,
        details=details,
    )

