"""多句批量解码与确定性汇总。

按输入文件顺序、文件内行序逐句解码；总对数概率按同序累加。
为防批量归约引入顺序相关误差，汇总后再对每句独立重解码并按同序累加做
交叉校验，两者差值不得超过 1e-9。
单句失败只记录为该句的错误对象，不影响其他句子，也不写入部分结果。
"""
from __future__ import annotations

from dataclasses import dataclass

from .errors import SegError, SegTagError

BATCH_TOLERANCE = 1e-9


@dataclass(frozen=True)
class SentenceRecord:
    file: str
    line: int
    text: str
    ok: bool
    words: tuple | None
    logprob: float | None
    error: SegError | None


@dataclass(frozen=True)
class BatchReport:
    records: tuple
    total_logprob: float
    total_words: int
    ok_count: int
    error_count: int
    fingerprint: str
    crosscheck_logprob: float
    crosscheck_diff: float
    tolerance: float
    consistent: bool


def batch_decode(decoder, paths, *, tolerance: float = BATCH_TOLERANCE) -> BatchReport:
    records: list[SentenceRecord] = []
    total_logprob = 0.0
    total_words = 0
    sent_index = 0

    for path in paths:
        with open(path, "r", encoding="utf-8") as fh:
            for line_no, raw in enumerate(fh, 1):
                text = raw.rstrip("\r\n")
                try:
                    result = decoder.decode(text, sentence_index=sent_index)
                except SegTagError as exc:
                    records.append(
                        SentenceRecord(path, line_no, text, False, None, None, exc.to_error())
                    )
                else:
                    records.append(
                        SentenceRecord(path, line_no, text, True, result.words, result.logprob, None)
                    )
                    total_logprob += result.logprob
                    total_words += len(result.words)
                sent_index += 1

    # 交叉校验：逐句独立解码（绕过缓存），按同序累加
    crosscheck = 0.0
    for rec in records:
        if rec.ok:
            crosscheck += decoder.decode(rec.text, use_cache=False).logprob
    diff = abs(total_logprob - crosscheck)

    ok_count = sum(1 for r in records if r.ok)
    return BatchReport(
        records=tuple(records),
        total_logprob=total_logprob,
        total_words=total_words,
        ok_count=ok_count,
        error_count=len(records) - ok_count,
        fingerprint=decoder.fingerprint,
        crosscheck_logprob=crosscheck,
        crosscheck_diff=diff,
        tolerance=tolerance,
        consistent=diff <= tolerance,
    )

