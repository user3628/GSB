"""对数域 Viterbi 联合标签解码器。

- 标签为 ``B/M/E/S-词性`` 联合标签，标签顺序固定，argmax 取首个最大值，
  同一输入必然得到确定结果。
- 全程在对数域做加法，禁止直接连乘概率，避免数值下溢。
- 每个字符的发射向量、每步累积概率都做有限性检查；一旦出现 -inf / NaN
  立即抛出携带句号与字符位置的 E_NUMERIC，不静默返回空结果或伪造路径。
- 解码结果按 (模型指纹, 句子) 缓存；概率表版本变化时缓存整体失效，
  旧版本结果不会被误用。
"""
from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np

from .errors import E_EMPTY, E_NUMERIC, SegTagError
from .validate import validate_and_segment


@dataclass(frozen=True)
class DecodeResult:
    text: str
    tags: tuple
    words: tuple
    logprob: float


def _where(sentence_index) -> str:
    return f"第 {sentence_index} 句" if sentence_index is not None else "当前句"


class Decoder:
    def __init__(self, tables) -> None:
        self.cache_hits = 0
        self.cache_misses = 0
        self.invalidations = 0
        self._cache: dict = {}
        self._load(tables)

    def _load(self, tables) -> None:
        self._tables = tables
        self._tags = list(tables.tags)
        n = len(self._tags)
        self._init = np.array([tables.init_log[t] for t in self._tags], dtype=np.float64)
        self._trans = np.empty((n, n), dtype=np.float64)
        for i, prev in enumerate(self._tags):
            for j, cur in enumerate(self._tags):
                self._trans[i, j] = tables.trans_logprob(prev, cur)

    def set_tables(self, tables) -> None:
        """切换到新版本概率表：缓存失效并记录一次可观测的版本失效事件。"""
        self._load(tables)
        self._cache.clear()
        self.invalidations += 1

    def clear_cache(self) -> None:
        self._cache.clear()

    @property
    def fingerprint(self) -> str:
        return self._tables.fingerprint

    def cache_info(self) -> dict:
        return {
            "fingerprint": self.fingerprint,
            "size": len(self._cache),
            "hits": self.cache_hits,
            "misses": self.cache_misses,
            "invalidations": self.invalidations,
        }

    def _emit_vector(self, char: str) -> np.ndarray:
        tables = self._tables
        return np.array([tables.emit_logprob(t, char) for t in self._tags], dtype=np.float64)

    def decode(self, text: str, *, sentence_index=None, use_cache=True) -> DecodeResult:
        if text is None or text == "":
            raise SegTagError(E_EMPTY, "输入为空句，无法解码", sentence_index=sentence_index)
        key = (self._tables.fingerprint, text)
        if use_cache:
            cached = self._cache.get(key)
            if cached is not None:
                self.cache_hits += 1
                return cached
            self.cache_misses += 1
        result = self._decode_impl(text, sentence_index)
        if use_cache:
            self._cache[key] = result
        return result

    def _decode_impl(self, text: str, sentence_index) -> DecodeResult:
        where = _where(sentence_index)
        n = len(self._tags)
        length = len(text)

        emit_rows = []
        for i, ch in enumerate(text):
            row = self._emit_vector(ch)
            bad = np.nonzero(~np.isfinite(row))[0]
            if bad.size:
                j = int(bad[0])
                raise SegTagError(
                    E_NUMERIC,
                    f"{where}第 {i} 字 {ch!r} 的发射对数概率非法: "
                    f"标签 {self._tags[j]!r} 取值 {float(row[j])!r}",
                    sentence_index=sentence_index,
                    char_index=i,
                    details=(("tag", self._tags[j]), ("value", repr(float(row[j])))),
                )
            emit_rows.append(row)

        if not np.isfinite(self._init).all() or not np.isfinite(self._trans).all():
            raise SegTagError(
                E_NUMERIC,
                f"{where}解码前概率表含 -inf/NaN，拒绝解码",
                sentence_index=sentence_index,
                char_index=0,
            )

        dp = self._init + emit_rows[0]
        if not np.isfinite(dp).all():
            raise SegTagError(
                E_NUMERIC,
                f"{where}第 0 字处 Viterbi 初始累积概率出现 -inf/NaN",
                sentence_index=sentence_index,
                char_index=0,
            )

        backptr = np.zeros((length, n), dtype=np.int64)
        for t in range(1, length):
            cand = dp[:, None] + self._trans + emit_rows[t][None, :]
            best = np.argmax(cand, axis=0)
            dp = cand[best, np.arange(n)]
            backptr[t] = best
            if not np.isfinite(dp).all():
                raise SegTagError(
                    E_NUMERIC,
                    f"{where}第 {t} 字处 Viterbi 累积概率出现 -inf/NaN",
                    sentence_index=sentence_index,
                    char_index=t,
                )

        last = int(np.argmax(dp))
        logprob = float(dp[last])
        if not math.isfinite(logprob):
            raise SegTagError(
                E_NUMERIC,
                f"{where}最优路径联合对数概率非法: {logprob!r}",
                sentence_index=sentence_index,
                char_index=length - 1,
            )

        path = [last]
        for t in range(length - 1, 0, -1):
            last = int(backptr[t][last])
            path.append(last)
        path.reverse()
        tags = tuple(self._tags[i] for i in path)

        # 词边界与词性约束校验：非法结果抛出 E_INVALID_TAG，整句回滚
        words = validate_and_segment(text, tags, self._tables.pos_tags, sentence_index=sentence_index)
        return DecodeResult(text=text, tags=tags, words=words, logprob=logprob)

