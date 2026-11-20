"""B/M/E/S 词边界与词性约束校验。

解码输出的联合标签序列必须能无歧义地重组为词序列：
- 标签数与字符数一致，每个字符恰好被一个标签覆盖；
- 标签形如 ``B-词性`` / ``M-词性`` / ``E-词性`` / ``S-词性``，词性必须在词性表中；
- S 为单字词且不得出现在未闭合词内；B 开启词，M 延续词，E 闭合词；
- 同一个词内所有字的词性必须一致；句末不允许存在未闭合的词。
任何违反都会抛出 E_INVALID_TAG，调用方据此回滚整句输出，不得部分写入。
"""
from __future__ import annotations

from dataclasses import dataclass

from .errors import E_INVALID_TAG, SegTagError


@dataclass(frozen=True)
class Word:
    """一个词：起止字符偏移（end 为开区间）、词面、词性。"""

    start: int
    end: int
    surface: str
    pos: str


def _fail(message, sentence_index, char_index, tag=None):
    details = (("tag", tag),) if tag is not None else ()
    raise SegTagError(
        E_INVALID_TAG,
        message,
        sentence_index=sentence_index,
        char_index=char_index,
        details=details,
    )


def validate_and_segment(text: str, tags, pos_tags, *, sentence_index=None) -> tuple:
    """校验联合标签序列并重组为词序列；非法时抛出 E_INVALID_TAG。"""
    tags = tuple(tags)
    if len(tags) != len(text):
        _fail(
            f"标签数 {len(tags)} 与字符数 {len(text)} 不一致",
            sentence_index,
            min(len(tags), len(text)),
        )

    words: list[Word] = []
    open_start = None
    open_pos = None

    for i, tag in enumerate(tags):
        if not isinstance(tag, str) or "-" not in tag:
            _fail(f"第 {i} 字标签格式非法: {tag!r}", sentence_index, i, tag)
        bmes, pos = tag.split("-", 1)
        if bmes not in ("B", "M", "E", "S") or not pos:
            _fail(f"第 {i} 字标签格式非法: {tag!r}", sentence_index, i, tag)
        if pos not in pos_tags:
            _fail(f"第 {i} 字词性 {pos!r} 不在词性表中", sentence_index, i, tag)

        if bmes == "S":
            if open_start is not None:
                _fail(f"第 {i} 字单字词标签 S 出现在未闭合词内", sentence_index, i, tag)
            words.append(Word(i, i + 1, text[i], pos))
        elif bmes == "B":
            if open_start is not None:
                _fail(f"第 {i} 字 B 标签嵌套开启新词", sentence_index, i, tag)
            open_start, open_pos = i, pos
        elif bmes == "M":
            if open_start is None:
                _fail(f"第 {i} 字 M 标签缺少对应的 B", sentence_index, i, tag)
            if pos != open_pos:
                _fail(f"第 {i} 字词性 {pos!r} 与词首词性 {open_pos!r} 不一致", sentence_index, i, tag)
        else:  # E
            if open_start is None:
                _fail(f"第 {i} 字 E 标签缺少对应的 B", sentence_index, i, tag)
            if pos != open_pos:
                _fail(f"第 {i} 字词性 {pos!r} 与词首词性 {open_pos!r} 不一致", sentence_index, i, tag)
            words.append(Word(open_start, i + 1, text[open_start : i + 1], open_pos))
            open_start, open_pos = None, None

    if open_start is not None:
        _fail(f"句末存在未闭合的词（起始于第 {open_start} 字）", sentence_index, len(tags) - 1)

    return tuple(words)

