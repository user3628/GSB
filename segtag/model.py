"""概率表估计：从本地标注样本统计计数，拉普拉斯平滑后以对数概率存储。

- 初始 / 转移 / 发射三类概率全部在对数域保存，解码阶段只做加法，禁止连乘。
- 未见字符回退到可配置未知标记（默认 ``<UNK>``）的发射项；
  未见标签转移回退到对应前驱的平滑回退值；未知前驱回退到均匀分布。
- 估计结果通过规范 JSON + SHA-256 生成版本指纹，重复运行逐字节一致。
"""
from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass

from .errors import E_CONFIG, E_FORMAT, E_NUMERIC, E_SMOOTHING, SegTagError

BMES = "BMES"
DEFAULT_UNKNOWN_TOKEN = "<UNK>"
_FINGERPRINT_VERSION = "segtag-tables-v1"


def safe_log(p, *, sentence_index=None, char_index=None, context=""):
    """对数入口的统一校验：非数值、非有限或非正概率一律报 E_NUMERIC。"""
    if isinstance(p, bool) or not isinstance(p, (int, float)):
        raise SegTagError(
            E_NUMERIC,
            f"对数输入不是数值: {p!r} ({context})",
            sentence_index=sentence_index,
            char_index=char_index,
        )
    value = float(p)
    if not math.isfinite(value) or value <= 0.0:
        raise SegTagError(
            E_NUMERIC,
            f"对数输入为非正或非有限概率: {p!r} ({context})",
            sentence_index=sentence_index,
            char_index=char_index,
        )
    return math.log(value)


def check_smoothing(smoothing) -> float:
    """校验平滑系数：必须是正的有限实数，否则报 E_SMOOTHING。"""
    if isinstance(smoothing, bool) or not isinstance(smoothing, (int, float)):
        raise SegTagError(E_SMOOTHING, f"非法平滑系数: {smoothing!r}，要求为正有限实数")
    alpha = float(smoothing)
    if not math.isfinite(alpha) or alpha <= 0.0:
        raise SegTagError(E_SMOOTHING, f"非法平滑系数: {smoothing!r}，要求为正有限实数")
    return alpha


@dataclass(frozen=True)
class WordTag:
    surface: str
    pos: str


def parse_labeled_line(line: str, line_no: int) -> list[WordTag]:
    """解析 ``词/词性 词/词性 ...`` 形式的标注行。"""
    tokens: list[WordTag] = []
    for raw in line.split():
        if "/" not in raw:
            raise SegTagError(E_FORMAT, f"训练样本第 {line_no} 行存在缺少 '/' 的词元: {raw!r}")
        surface, pos = raw.rsplit("/", 1)
        if not surface or not pos:
            raise SegTagError(E_FORMAT, f"训练样本第 {line_no} 行词元格式非法: {raw!r}")
        tokens.append(WordTag(surface, pos))
    return tokens


def word_to_bmes(surface: str) -> list[str]:
    n = len(surface)
    if n == 1:
        return ["S"]
    return ["B"] + ["M"] * (n - 2) + ["E"]


class Corpus:
    """已解析的标注语料，保持文件行序，可重复用于不同平滑系数的估计。"""

    def __init__(self, sentences):
        self.sentences = [list(s) for s in sentences]

    @classmethod
    def from_file(cls, path) -> "Corpus":
        sentences = []
        with open(path, "r", encoding="utf-8") as fh:
            for line_no, raw in enumerate(fh, 1):
                line = raw.strip()
                if not line:
                    continue
                sentences.append(parse_labeled_line(line, line_no))
        if not sentences:
            raise SegTagError(E_FORMAT, f"训练文件为空或无有效样本: {path}")
        return cls(sentences)

    @staticmethod
    def char_tag_stream(sent) -> tuple[list[str], list[str]]:
        """把一句 ``WordTag`` 序列展开为字序列与 BMES-词性 联合标签序列。"""
        chars: list[str] = []
        tags: list[str] = []
        for wt in sent:
            for ch, b in zip(wt.surface, word_to_bmes(wt.surface)):
                chars.append(ch)
                tags.append(f"{b}-{wt.pos}")
        return chars, tags


class ProbabilityTables:
    """对数域概率表。所有查询都经过未知标记回退，返回值保证有限（除非被测试注入篡改）。"""

    def __init__(
        self,
        *,
        smoothing: float,
        unknown_token: str,
        pos_tags: tuple,
        tags: tuple,
        init_log: dict,
        trans_log: dict,
        trans_backoff_log: dict,
        unk_prev_log: float,
        emit_log: dict,
        emit_backoff_log: dict,
        vocab: tuple,
        canonical_json: str,
        fingerprint: str,
    ) -> None:
        self.smoothing = smoothing
        self.unknown_token = unknown_token
        self.pos_tags = pos_tags
        self.tags = tags
        self.init_log = init_log
        self.trans_log = trans_log
        self.trans_backoff_log = trans_backoff_log
        self.unk_prev_log = unk_prev_log
        self.emit_log = emit_log
        self.emit_backoff_log = emit_backoff_log
        self.vocab = vocab
        self._canonical_json = canonical_json
        self.fingerprint = fingerprint

    def emit_logprob(self, tag: str, char: str) -> float:
        """发射对数概率；未见字符回退到未知标记 ``unknown_token`` 的平滑项。"""
        value = self.emit_log.get((tag, char))
        if value is None:
            value = self.emit_log.get((tag, self.unknown_token))
        if value is None:
            value = self.emit_backoff_log[tag]
        return value

    def trans_logprob(self, prev: str, cur: str) -> float:
        """转移对数概率；未见转移回退到前驱的平滑回退值，未知前驱回退到均匀分布。"""
        value = self.trans_log.get((prev, cur))
        if value is None:
            value = self.trans_backoff_log.get(prev, self.unk_prev_log)
        return value

    def to_canonical_json(self) -> str:
        """规范 JSON 序列化（排序键、紧凑分隔符），重复运行逐字节一致。"""
        return self._canonical_json


def estimate(corpus: Corpus, smoothing=0.1, unknown_token: str = DEFAULT_UNKNOWN_TOKEN) -> ProbabilityTables:
    """从标注语料估计对数概率表（拉普拉斯平滑，平滑系数 alpha>0）。"""
    alpha = check_smoothing(smoothing)
    if not isinstance(unknown_token, str) or not unknown_token:
        raise SegTagError(E_CONFIG, f"未知标记必须是非空字符串: {unknown_token!r}")

    pos_tags = tuple(sorted({wt.pos for sent in corpus.sentences for wt in sent}))
    tags = tuple(f"{b}-{p}" for p in pos_tags for b in BMES)
    n_tags = len(tags)

    init_count = {t: 0 for t in tags}
    trans_count: dict = {}
    trans_row = {t: 0 for t in tags}
    emit_count: dict = {}
    emit_row = {t: 0 for t in tags}
    vocab: set = set()
    n_sent = 0

    for sent in corpus.sentences:
        chars, tag_seq = Corpus.char_tag_stream(sent)
        if not tag_seq:
            continue
        n_sent += 1
        init_count[tag_seq[0]] += 1
        for prev, cur in zip(tag_seq, tag_seq[1:]):
            trans_count[(prev, cur)] = trans_count.get((prev, cur), 0) + 1
            trans_row[prev] += 1
        for ch, tg in zip(chars, tag_seq):
            emit_count[(tg, ch)] = emit_count.get((tg, ch), 0) + 1
            emit_row[tg] += 1
            vocab.add(ch)

    vocab_sorted = tuple(sorted(vocab))

    init_log = {}
    denom = n_sent + alpha * n_tags
    for t in tags:
        init_log[t] = safe_log((init_count[t] + alpha) / denom, context=f"初始概率 {t}")

    trans_log = {}
    trans_backoff_log = {}
    for prev in tags:
        row_denom = trans_row[prev] + alpha * n_tags
        trans_backoff_log[prev] = safe_log(alpha / row_denom, context=f"转移回退 {prev}")
    for (prev, cur), count in trans_count.items():
        row_denom = trans_row[prev] + alpha * n_tags
        trans_log[(prev, cur)] = safe_log((count + alpha) / row_denom, context=f"转移概率 {prev}->{cur}")
    unk_prev_log = safe_log(1.0 / n_tags, context="未知前驱转移回退")

    # 发射词表包含全部已见字符外加一个未知标记槽位
    n_emit = len(vocab_sorted) + 1
    emit_log = {}
    emit_backoff_log = {}
    for t in tags:
        row_denom = emit_row[t] + alpha * n_emit
        emit_backoff_log[t] = safe_log(alpha / row_denom, context=f"发射回退 {t}")
    for (tg, ch), count in emit_count.items():
        row_denom = emit_row[tg] + alpha * n_emit
        emit_log[(tg, ch)] = safe_log((count + alpha) / row_denom, context=f"发射概率 {tg}<-{ch!r}")

    payload = {
        "version": _FINGERPRINT_VERSION,
        "smoothing": repr(alpha),
        "unknown_token": unknown_token,
        "pos_tags": list(pos_tags),
        "tags": list(tags),
        "init_log": sorted(init_log.items()),
        "trans_log": sorted((f"{a}->{b}", v) for (a, b), v in trans_log.items()),
        "trans_backoff_log": sorted(trans_backoff_log.items()),
        "unk_prev_log": unk_prev_log,
        "emit_log": sorted((f"{t}<-{c}", v) for (t, c), v in emit_log.items()),
        "emit_backoff_log": sorted(emit_backoff_log.items()),
        "vocab": list(vocab_sorted),
    }
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    fingerprint = hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    return ProbabilityTables(
        smoothing=alpha,
        unknown_token=unknown_token,
        pos_tags=pos_tags,
        tags=tags,
        init_log=init_log,
        trans_log=trans_log,
        trans_backoff_log=trans_backoff_log,
        unk_prev_log=unk_prev_log,
        emit_log=emit_log,
        emit_backoff_log=emit_backoff_log,
        vocab=vocab_sorted,
        canonical_json=canonical,
        fingerprint=fingerprint,
    )

