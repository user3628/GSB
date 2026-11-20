"""业务错误码与错误对象定义。

所有可预期的业务失败都使用稳定的错误码，便于上层 NLP 服务做程序化分支处理。
库内部抛出 :class:`SegTagError`，在公开接口边界统一转换为不可变的
:class:`SegError` 错误对象返回，保证"出错时不部分写入输出"。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

# 数值异常：-inf / NaN / 对数输入为非正概率
E_NUMERIC = "E_NUMERIC"
# 非法标签序列：B/M/E/S 结构冲突、词性不在词性表、标签长度与字符数不符
E_INVALID_TAG = "E_INVALID_TAG"
# 空句输入
E_EMPTY = "E_EMPTY"
# 非法平滑系数（非正、非有限、非数值）
E_SMOOTHING = "E_SMOOTHING"
# 训练样本格式错误
E_FORMAT = "E_FORMAT"
# 配置错误（如未知标记为空）
E_CONFIG = "E_CONFIG"

ALL_CODES = (E_NUMERIC, E_INVALID_TAG, E_EMPTY, E_SMOOTHING, E_FORMAT, E_CONFIG)


@dataclass(frozen=True)
class SegError:
    """稳定的业务错误对象，可安全跨接口传递与序列化。"""

    code: str
    message: str
    sentence_index: Optional[int] = None
    char_index: Optional[int] = None
    details: tuple = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "sentence_index": self.sentence_index,
            "char_index": self.char_index,
            "details": list(self.details),
        }

    def __str__(self) -> str:  # pragma: no cover - 便于日志直接打印
        return f"[{self.code}] {self.message}"


class SegTagError(Exception):
    """库内部使用的业务异常，公开接口会将其转换为 :class:`SegError`。"""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        sentence_index: Optional[int] = None,
        char_index: Optional[int] = None,
        details: tuple = (),
    ) -> None:
        super().__init__(f"[{code}] {message}")
        self.code = code
        self.message = message
        self.sentence_index = sentence_index
        self.char_index = char_index
        self.details = tuple(details)

    def to_error(self) -> SegError:
        return SegError(
            code=self.code,
            message=self.message,
            sentence_index=self.sentence_index,
            char_index=self.char_index,
            details=self.details,
        )

