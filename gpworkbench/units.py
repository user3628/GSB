"""量纲字符串解析：全部使用整数运算，禁止浮点参与资源核算。"""

from __future__ import annotations

import re

from .errors import SpecValidationError

# 有符号 64 位整数上界，作为统一溢出阈值
MAX_INT64 = (1 << 63) - 1

_MEMORY_RE = re.compile(r"^\s*(\d+)\s*(Ki|Mi|Gi|Ti|B)?\s*$")
_COMPUTE_RE = re.compile(r"^\s*(\d+)(m)?\s*$")
_INT_RE = re.compile(r"^\s*\d+\s*$")

_MEMORY_MULTIPLIER = {
    "Ki": 1024,
    "Mi": 1024 ** 2,
    "Gi": 1024 ** 3,
    "Ti": 1024 ** 4,
    "B": 1,
    None: 1,
}


def _check_range(value: int, field: str, allow_zero: bool) -> int:
    if value < 0:
        raise SpecValidationError(f"{field} 不允许为负数", field, value)
    if not allow_zero and value == 0:
        raise SpecValidationError(f"{field} 必须为正整数", field, value)
    if value > MAX_INT64:
        raise SpecValidationError(f"{field} 超出 int64 上界", field, value)
    return value


def parse_memory(value, field: str = "memory") -> int:
    """把 40Gi/512Mi/1024B/裸字节整数解析为字节整数。"""
    if isinstance(value, bool):
        raise SpecValidationError(f"{field} 类型非法", field, value)
    if isinstance(value, int):
        # 显式整数字节允许 0（例如节点零占用），负数/溢出仍被拒绝
        return _check_range(value, field, allow_zero=True)
    if not isinstance(value, str):
        raise SpecValidationError(f"{field} 必须是量纲字符串或整数", field, value)
    match = _MEMORY_RE.match(value)
    if not match:
        raise SpecValidationError(f"{field} 量纲无法解析: {value!r}", field, value)
    number = int(match.group(1))
    unit = match.group(2)
    result = number * _MEMORY_MULTIPLIER[unit]
    return _check_range(result, field, allow_zero=True)


def parse_compute(value, field: str = "compute_share") -> int:
    """把 250m / 裸整数解析为千分之一算力份额整数（250m -> 250）。"""
    if isinstance(value, bool):
        raise SpecValidationError(f"{field} 类型非法", field, value)
    if isinstance(value, int):
        return _check_range(value, field, allow_zero=True)
    if not isinstance(value, str):
        raise SpecValidationError(f"{field} 必须是量纲字符串或整数", field, value)
    match = _COMPUTE_RE.match(value)
    if not match:
        raise SpecValidationError(f"{field} 算力量纲无法解析: {value!r}", field, value)
    return _check_range(int(match.group(1)), field, allow_zero=True)


def parse_positive_int(value, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        if isinstance(value, str) and _INT_RE.match(value):
            value = int(value.strip())
        else:
            raise SpecValidationError(f"{field} 必须是整数", field, value)
    return _check_range(value, field, allow_zero=False)


def parse_non_negative_int(value, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        if isinstance(value, str) and _INT_RE.match(value):
            value = int(value.strip())
        else:
            raise SpecValidationError(f"{field} 必须是非负整数", field, value)
    return _check_range(value, field, allow_zero=True)
