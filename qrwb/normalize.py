"""阶段一：查询规范化与多路改写派发。"""

from __future__ import annotations

import unicodedata
from dataclasses import dataclass, asdict
from typing import Any, Dict, List

from .config import Config
from .errors import E_EMPTY_QUERY, make_error
from .util import fingerprint


def _is_cjk(ch: str) -> bool:
    return "\u4e00" <= ch <= "\u9fff"


def _is_latin(ch: str) -> bool:
    # NFKC 后全角拉丁已转为半角
    return ch.isascii() and ch.isalpha()


def normalize_query(raw_query: str) -> str:
    """NFKC -> 全半角统一 -> 小写化 -> 空白折叠。"""
    text = unicodedata.normalize("NFKC", raw_query)
    text = text.lower()
    # 各类空白（含全角空格 U+3000，NFKC 后通常已转 U+0020）统一折叠为单空格
    text = " ".join(text.split())
    return text.strip()


@dataclass(frozen=True)
class Token:
    text: str
    kind: str          # cjk | latin
    start: int         # 在规范化字符串中的字符偏移
    end: int

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def tokenize(text: str) -> List[Token]:
    """中文按单字切分，拉丁按连续串切分，并记录字符偏移。"""
    tokens: List[Token] = []
    i = 0
    n = len(text)
    while i < n:
        ch = text[i]
        if ch.isspace():
            i += 1
            continue
        if _is_cjk(ch):
            tokens.append(Token(ch, "cjk", i, i + 1))
            i += 1
        elif _is_latin(ch):
            start = i
            while i < n and (_is_latin(text[i]) or text[i].isdigit()):
                i += 1
            tokens.append(Token(text[start:i], "latin", start, i))
        else:
            # 标点等：跳过但不作为词项
            i += 1
    return tokens


def build_dispatch_plan(record: Dict[str, Any], cfg: Config) -> Dict[str, Any]:
    """生成确定性派发计划；空查询产出 E_EMPTY_QUERY。"""
    raw_query = record.get("raw_query", "")
    normalized = normalize_query(raw_query)
    tokens = tokenize(normalized)

    base = {
        "query_id": record["query_id"],
        "lang": record.get("lang"),
        "locale": record.get("locale"),
        "channel": record.get("channel"),
        "click_history": list(record.get("click_history", [])),
        "raw_query": raw_query,
        "normalized_query": normalized,
        "tokens": [token.to_dict() for token in tokens],
    }

    if not tokens:
        err = make_error(
            E_EMPTY_QUERY,
            "规范化后不含任何有效词项（空查询或仅含标点）",
            query_id=record["query_id"],
            stage="dispatch",
            raw_query=raw_query,
            normalized_query=normalized,
            lang=record.get("lang"),
            channel=record.get("channel"),
        )
        plan = dict(base)
        plan.update({
            "status": "error",
            "error": err.to_dict(),
            "routes": {},
            "plan_fingerprint": fingerprint(base),
        })
        return plan

    has_cjk = any(t.kind == "cjk" for t in tokens)
    has_latin = any(t.kind == "latin" for t in tokens)

    routes: Dict[str, Dict[str, Any]] = {}
    # 同义词路：词项存在即启用
    if cfg.route_enabled("synonym"):
        routes["synonym"] = {
            "enabled": True,
            "max_candidates": cfg.route_max("synonym"),
            "eligible_tokens": [t.text for t in tokens],
        }
    # 字形近似路：仅对拉丁连续串启用（中文单字以同义/桥接覆盖）
    if cfg.route_enabled("glyph"):
        latin_tokens = [t.text for t in tokens if t.kind == "latin"]
        routes["glyph"] = {
            "enabled": bool(latin_tokens),
            "max_candidates": cfg.route_max("glyph"),
            "eligible_tokens": latin_tokens,
        }
    # 跨语言桥接路：混排或任意词项可桥接时启用
    if cfg.route_enabled("bridge"):
        routes["bridge"] = {
            "enabled": has_cjk or has_latin,
            "max_candidates": cfg.route_max("bridge"),
            "eligible_tokens": [t.text for t in tokens],
            "target_lang": "zh" if has_latin else "en",
        }

    plan = dict(base)
    plan.update({
        "status": "dispatched",
        "routes": routes,
        "plan_fingerprint": fingerprint(base),
    })
    return plan
