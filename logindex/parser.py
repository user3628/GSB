"""确定性日志行解析。

样本行格式（四个管道分隔字段，消息内允许出现管道符）::

    1700000000123|auth-svc|INFO|user login ok|extra|pipes

- 时间戳：13 位 Unix 毫秒整数
- 服务名：``[A-Za-z0-9._-]+``
- 级别：DEBUG / INFO / WARN / ERROR / FATAL
- 消息：非空文本

任意一行不合法即抛 :class:`LineInvalidError`，由调用方保证整批不入库。
"""

from __future__ import annotations

import re
from typing import Iterable, List

from .errors import LineInvalidError
from .models import LogRecord

VALID_LEVELS = frozenset({"DEBUG", "INFO", "WARN", "ERROR", "FATAL"})
SERVICE_RE = re.compile(r"^[A-Za-z0-9._-]+$")


def parse_line(line: str, line_no: int) -> LogRecord:
    """解析单行；空行/缺字段/字段非法都返回 E_LINE_INVALID。"""
    raw = line.rstrip("\r\n")
    if not raw.strip():
        raise LineInvalidError(
            "空行无法解析", {"line_no": line_no, "reason": "empty line"}
        )
    parts = raw.split("|", 3)
    if len(parts) != 4:
        raise LineInvalidError(
            "字段数量不足，期望 timestamp|service|level|message",
            {"line_no": line_no, "reason": "expected 4 pipe-separated fields"},
        )
    ts_text, service, level, message = (p.strip() for p in parts)

    if not re.fullmatch(r"\d{13}", ts_text):
        raise LineInvalidError(
            "时间戳必须是 13 位毫秒整数",
            {"line_no": line_no, "reason": "timestamp not a 13-digit millisecond int"},
        )
    timestamp_ms = int(ts_text)

    if not SERVICE_RE.fullmatch(service):
        raise LineInvalidError(
            "服务名仅允许字母数字及 . _ -",
            {"line_no": line_no, "reason": "invalid service name", "value": service},
        )
    if level not in VALID_LEVELS:
        raise LineInvalidError(
            f"未知日志级别: {level}",
            {
                "line_no": line_no,
                "reason": "unknown level",
                "value": level,
                "allowed": sorted(VALID_LEVELS),
            },
        )
    if not message:
        raise LineInvalidError(
            "消息不能为空", {"line_no": line_no, "reason": "empty message"}
        )
    return LogRecord(timestamp_ms, service, level, message, line_no)


def parse_lines(lines: Iterable[str]) -> List[LogRecord]:
    """解析整批文本；任一非法行抛错且不返回部分结果。"""
    records: List[LogRecord] = []
    for idx, line in enumerate(lines, start=1):
        records.append(parse_line(line, idx))
    return records
