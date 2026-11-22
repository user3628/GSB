"""确定性 JSON 规范化与摘要。

同一份逻辑内容在任何时间、任何进程（含 Windows / Linux）上都产生逐字节相同的
规范化文本与 SHA-256 摘要，这是快照摘要校验与重复提交幂等判定的基础。

规则：
- 字典按 key 排序后输出；
- 紧凑分隔符 ``,`` / ``:``，无多余空白；
- 不转义非 ASCII 字符（UTF-8 字节直接参与摘要）；
- 列表保持调用方给定顺序（样本与服务层负责把资源、父列表排好序）。
"""

from __future__ import annotations

import hashlib
import json
from typing import Any


def canonical_bytes(payload: Any) -> bytes:
    text = json.dumps(
        payload,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )
    return text.encode("utf-8")


def canonical_text(payload: Any) -> str:
    return canonical_bytes(payload).decode("utf-8")


def digest(payload: Any) -> str:
    """对规范化字节计算 SHA-256，返回 64 位小写十六进制。"""
    return hashlib.sha256(canonical_bytes(payload)).hexdigest()


def snapshot_digest(version: int, generated_at: str, resources: list[dict[str, Any]]) -> str:
    """快照摘要固定只覆盖版本号、生成时间与资源集合，不含摘要字段本身。"""
    normalized_resources = [_normalize_resource(item) for item in resources]
    normalized_resources.sort(key=lambda item: item["id"])
    envelope = {
        "version": version,
        "generated_at": generated_at,
        "resources": normalized_resources,
    }
    return digest(envelope)


def request_fingerprint(kind: str, payload: Any) -> str:
    """提交幂等指纹：提交类型 + 请求体规范化摘要。"""
    return digest({"kind": kind, "payload": payload})


def _normalize_resource(item: dict[str, Any]) -> dict[str, Any]:
    result = dict(item)
    if "parents" in result and isinstance(result["parents"], list):
        result["parents"] = sorted(result["parents"])
    return result
