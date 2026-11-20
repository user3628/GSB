"""前端阶段 1：文本编码（本地哈希嵌入）。

处理链：Unicode NFKC 规范化 -> 空白折叠 -> 小写化 ->
分词（拉丁/数字词 + 非 ASCII 字母逐字成词）->
带符号特征哈希（BLAKE2b，64 维）-> L2 单位化。

全程确定性：不依赖随机数与第三方模型，同输入同配置逐字节一致。
"""

import hashlib
import unicodedata

import numpy as np

from .codec import fingerprint
from .errors import E_MISSING_INPUT, E_NUMERIC, PipeError, StageFailure

EMBED_DIM = 64


def normalize_text(text: str) -> str:
    if not isinstance(text, str):
        raise PipeError(E_MISSING_INPUT, "text field missing or not a string",
                        stage="text_encoding")
    text = unicodedata.normalize("NFKC", text)
    text = " ".join(text.split())
    return text.lower()


def tokenize(normalized: str) -> list[str]:
    tokens: list[str] = []
    buf: list[str] = []
    for ch in normalized:
        if ch.isascii() and ch.isalnum():
            buf.append(ch)
            continue
        if buf:
            tokens.append("".join(buf))
            buf = []
        if not ch.isascii() and not ch.isspace():
            cat = unicodedata.category(ch)
            if cat.startswith("L") or cat.startswith("N"):
                tokens.append(ch)
    if buf:
        tokens.append("".join(buf))
    return tokens


def _token_signature(token: str, dim: int) -> tuple[int, int]:
    digest = hashlib.blake2b(token.encode("utf-8"), digest_size=16).digest()
    idx = int.from_bytes(digest[:8], "big") % dim
    sign = 1.0 if digest[8] & 1 else -1.0
    return idx, sign


def hash_embedding(text: str, dim: int = EMBED_DIM) -> tuple[np.ndarray, str, list[str]]:
    """返回 (单位嵌入向量, 规范化后文本, 分词列表)。"""
    normalized = normalize_text(text)
    tokens = tokenize(normalized)
    if not tokens:
        raise PipeError(E_MISSING_INPUT,
                        "text contains no usable tokens after normalization",
                        stage="text_encoding")
    vec = np.zeros(dim, dtype=np.float64)
    for token in tokens:
        idx, sign = _token_signature(token, dim)
        vec[idx] += sign
    norm = float(np.linalg.norm(vec))
    if norm == 0.0 or not np.isfinite(norm):
        raise PipeError(E_NUMERIC,
                        f"embedding norm is non-finite or zero (norm={norm!r})",
                        stage="text_encoding")
    vec = vec / norm
    if not np.all(np.isfinite(vec)):
        raise PipeError(E_NUMERIC, "embedding contains NaN or Inf",
                        stage="text_encoding")
    return vec, normalized, tokens


def text_input_fingerprint(record: dict) -> str:
    """单条文本输入版本指纹（基于原始 id/text/lang）。"""
    return fingerprint({
        "id": record.get("id"),
        "text": record.get("text"),
        "lang": record.get("lang"),
    })


def encode_unit(record: dict, config: dict) -> dict:
    """不可拆分样本单元：单条文本 -> 嵌入记录。"""
    sample_id = str(record.get("id")) if record.get("id") is not None else "<unknown>"
    if not record.get("text"):
        raise PipeError(E_MISSING_INPUT, "text field is empty",
                        stage="text_encoding", sample_id=sample_id)
    vec, normalized, tokens = hash_embedding(
        record["text"], int(config["embedding_dim"]))
    return {
        "id": sample_id,
        "lang": record.get("lang"),
        "normalized_text": normalized,
        "tokens": tokens,
        "embedding": [float(x) for x in vec],
        "input_fingerprint": text_input_fingerprint(record),
    }


def build_product(items: list[dict], config: dict) -> dict:
    return {
        "stage": "text_encoding",
        "items": items,
        "embeddings": {it["id"]: it["embedding"] for it in items},
        "summary": {
            "record_count": len(items),
            "embedding_dim": int(config["embedding_dim"]),
            "item_fingerprints": sorted(
                (it["id"], it["input_fingerprint"]) for it in items),
        },
    }


def run(records: list[dict], config: dict) -> dict:
    """同步入口（测试/单步使用）：逐单元编码并聚合错误。"""
    items: list[dict] = []
    errors: list[dict] = []
    seen: set[str] = set()
    for record in sorted(records, key=lambda r: str(r.get("id"))):
        sample_id = str(record.get("id")) if record.get("id") is not None else "<unknown>"
        if sample_id in seen:
            errors.append(PipeError(E_MISSING_INPUT,
                                    f"duplicate text id {sample_id!r}",
                                    stage="text_encoding",
                                    sample_id=sample_id).to_dict())
            continue
        seen.add(sample_id)
        try:
            items.append(encode_unit(record, config))
        except PipeError as exc:
            errors.append(exc.to_dict())
    if errors:
        raise StageFailure("text_encoding", errors)
    return build_product(items, config)
