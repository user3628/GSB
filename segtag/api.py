"""对外集成接口：Segmenter 门面。

构造类方法（如 Segmenter.train）在参数非法时抛出 SegTagError；
逐句与批量接口不抛业务异常，而是返回携带稳定错误码的结果对象，
保证出错时不部分写入输出。
"""
from __future__ import annotations

from dataclasses import dataclass

from .batch import BatchReport, batch_decode
from .decoder import Decoder
from .errors import E_INVALID_TAG, SegError, SegTagError
from .model import DEFAULT_UNKNOWN_TOKEN, Corpus, estimate
from .validate import validate_and_segment


@dataclass(frozen=True)
class SentenceResult:
    """单句结果：ok=False 时 error 携带稳定错误码，words/logprob 一律为 None。"""

    ok: bool
    text: str
    words: tuple | None
    logprob: float | None
    tags: tuple | None
    error: SegError | None


class Segmenter:
    """中文分词与词性联合标注器，供其他 NLP 服务以库形式集成。"""

    def __init__(self, corpus, smoothing=0.1, unknown_token=DEFAULT_UNKNOWN_TOKEN):
        self._corpus = corpus
        self._smoothing = smoothing
        self._unknown_token = unknown_token
        self._decoder = None
        self._rebuild()

    @classmethod
    def train(cls, train_path, smoothing=0.1, unknown_token=DEFAULT_UNKNOWN_TOKEN):
        """从本地标注样本训练。非法平滑系数抛 E_SMOOTHING，样本格式错误抛 E_FORMAT。"""
        return cls(Corpus.from_file(train_path), smoothing, unknown_token)

    def _rebuild(self):
        tables = estimate(self._corpus, self._smoothing, self._unknown_token)
        self._tables = tables
        if self._decoder is None:
            self._decoder = Decoder(tables)
        else:
            self._decoder.set_tables(tables)

    @property
    def fingerprint(self):
        """当前概率表版本指纹；平滑系数变化时指纹必然变化。"""
        return self._tables.fingerprint

    @property
    def smoothing(self):
        return self._tables.smoothing

    @property
    def pos_tags(self):
        return self._tables.pos_tags

    def set_smoothing(self, alpha):
        """调整平滑系数并重建概率表；缓存随之失效，旧版本结果不会被误用。"""
        old = self._smoothing
        self._smoothing = alpha
        try:
            self._rebuild()
        except SegTagError:
            self._smoothing = old
            raise

    def cache_info(self):
        """缓存与版本状态查询：指纹、规模、命中/未命中、失效次数。"""
        return self._decoder.cache_info()

    def tag_sentence(self, text, *, sentence_index=None):
        """标注单句；空句返回 E_EMPTY，数值异常返回 E_NUMERIC，非法标签返回 E_INVALID_TAG。"""
        try:
            result = self._decoder.decode(text, sentence_index=sentence_index)
        except SegTagError as exc:
            return SentenceResult(False, text, None, None, None, exc.to_error())
        return SentenceResult(True, text, result.words, result.logprob, result.tags, None)

    def tag_files(self, paths):
        """批量标注多个 UTF-8 文本文件（每行一句），返回确定性汇总报告。"""
        return batch_decode(self._decoder, list(paths))

    def validate_tags(self, text, tags):
        """校验外部给定的联合标签序列；非法时返回 E_INVALID_TAG 错误对象。"""
        tags = tuple(tags)
        try:
            words = validate_and_segment(text, tags, self._tables.pos_tags)
        except SegTagError as exc:
            return SentenceResult(False, text, None, None, tags, exc.to_error())
        return SentenceResult(True, text, words, None, tags, None)

    def inject_emission_for_test(self, char, tag, logprob):
        """测试注入入口：篡改指定 (标签, 字符) 的发射对数概率。

        仅用于本地确定性测试，验证 E_NUMERIC 行为；注入后清空解码缓存。
        """
        if tag not in self._tables.tags:
            raise SegTagError(E_INVALID_TAG, f"注入标签 {tag!r} 不在标签集中")
        self._tables.emit_log[(tag, char)] = float(logprob)
        self._decoder.clear_cache()

