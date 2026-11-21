"""Unicode-safe sentence segmentation and deterministic lexical utilities."""

from __future__ import annotations

import math
import re
import unicodedata
from collections import Counter
from typing import Any, Iterable

from .errors import input_too_short

_CJK_RE = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]")
_LATIN_WORD_RE = re.compile(r"[A-Za-z]+(?:['’\-][A-Za-z]+)*")
_TOKEN_RE = re.compile(r"[a-z0-9]+(?:['’\-][a-z0-9]+)*|[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]")
_TIME_RE = re.compile(
    r"(?:今天|昨日|昨日晚间|本周|当地时间|上午|下午|晚间|清晨|"
    r"\b(?:today|yesterday|monday|tuesday|wednesday|thursday|friday|saturday|sunday|"
    r"january|february|march|april|may|june|july|august|september|october|november|december)\b)",
    re.IGNORECASE,
)
_PLACE_RE = re.compile(r"(?:在北京|在上海|在纽约|在伦敦|在当地|in Beijing|in Shanghai|in New York|in London|locally)", re.IGNORECASE)

_STRONG_BOUNDARIES = set("。！？!?；;")
_WEAK_BOUNDARY = "."
_CLOSERS = set("”’\"'）)】]》」』…")
_WHITESPACE = set(" \t\r\n\u3000")

_STOPWORDS_ZH = set(
    "的 了 和 与 及 或 在 是 为 对 从 到 将 已 会 有 也 就 都 而 并 被 让 向 于 其 这 那 一个 一种 我们 他们 你们 记者 表示 认为 指出 称 说 进行 相关 以及 目前 今日 今日 今天 昨日 本次 这个 这些 其中 由于 因为 所以 如果 但是 可以 可能 已经 正在 通过 关于"
    .split()
)
_STOPWORDS_EN = set(
    "a an and are as at be been being but by for from had has have he her his i in into is it its of on or our she that the their them they this those to was we were will with would you your said says report reports according also more most new news about after before over under than then there these those which who whom whose what when where why how"
    .split()
)


def nfkc(text: str) -> str:
    return unicodedata.normalize("NFKC", text)


def normalized_text(text: str) -> str:
    return nfkc(text).casefold()


def tokenize(text: str, *, keep_stopwords: bool = False) -> list[str]:
    tokens = _TOKEN_RE.findall(normalized_text(text))
    if keep_stopwords:
        return tokens
    result: list[str] = []
    for token in tokens:
        if _CJK_RE.fullmatch(token):
            if token not in _STOPWORDS_ZH:
                result.append(token)
        elif token not in _STOPWORDS_EN and len(token) > 1:
            result.append(token)
    return result


def _sentence_end(body: str, index: int) -> int | None:
    char = body[index]
    if char in _STRONG_BOUNDARIES:
        return index + 1
    if char == _WEAK_BOUNDARY:
        previous_is_digit = index > 0 and body[index - 1].isdigit()
        next_is_digit = index + 1 < len(body) and body[index + 1].isdigit()
        if previous_is_digit and next_is_digit:
            return None
        if index + 1 < len(body) and body[index + 1] not in _WHITESPACE and body[index + 1] not in _CLOSERS:
            return None
        return index + 1
    return index + 1


def split_sentences(body: str) -> list[dict[str, Any]]:
    """Split text at Chinese/English punctuation and retain exact offsets.

    Offsets refer to the original, unnormalized body. Whitespace between
    sentences is not included; all non-whitespace characters of the body are
    represented by exactly one sentence.
    """
    if not isinstance(body, str):
        return []
    length = len(body)
    sentences: list[dict[str, Any]] = []
    cursor = 0
    while cursor < length:
        while cursor < length and body[cursor] in _WHITESPACE:
            cursor += 1
        if cursor >= length:
            break
        start = cursor
        index = cursor
        while index < length:
            char = body[index]
            if char in _STRONG_BOUNDARIES or char == _WEAK_BOUNDARY:
                boundary_end = _sentence_end(body, index)
                if boundary_end is not None:
                    end = boundary_end
                    while end < length and (body[end] in _CLOSERS or body[end] in "。！？!?；;"):
                        end += 1
                    segment = body[start:end].strip()
                    if segment:
                        real_start = start + (len(body[start:end]) - len(body[start:end].lstrip()))
                        real_end = start + len(body[start:end].rstrip())
                        sentences.append(
                            {
                                "index": len(sentences),
                                "start": real_start,
                                "end": real_end,
                                "text": body[real_start:real_end],
                            }
                        )
                    cursor = end
                    break
            if char == "\n":
                # A blank line is a paragraph boundary even without punctuation.
                lookahead = index
                while lookahead < length and body[lookahead] in _WHITESPACE:
                    lookahead += 1
                if lookahead >= length or "\n" in body[index:lookahead]:
                    segment = body[start:index].strip()
                    if segment:
                        real_start = start + (len(body[start:index]) - len(body[start:index].lstrip()))
                        real_end = start + len(body[start:index].rstrip())
                        sentences.append(
                            {
                                "index": len(sentences),
                                "start": real_start,
                                "end": real_end,
                                "text": body[real_start:real_end],
                            }
                        )
                    cursor = lookahead
                    break
            index += 1
        else:
            segment = body[start:length].strip()
            if segment:
                real_start = start + (len(body[start:length]) - len(body[start:length].lstrip()))
                real_end = start + len(body[start:length].rstrip())
                sentences.append(
                    {
                        "index": len(sentences),
                        "start": real_start,
                        "end": real_end,
                        "text": body[real_start:real_end],
                    }
                )
            cursor = length

    for sentence in sentences:
        sentence["tokens"] = tokenize(sentence["text"])
        sentence["normalized"] = normalized_text(sentence["text"])
    return sentences


def prepare_sentences(doc: dict[str, Any], config: dict[str, Any]) -> list[dict[str, Any]]:
    body = doc.get("body", "")
    minimum = int(config["runtime"]["min_body_chars"])
    if not isinstance(body, str) or len(body.strip()) < minimum:
        raise input_too_short(str(doc.get("doc_id", "")), len(body.strip()) if isinstance(body, str) else 0, minimum)
    sentences = split_sentences(body)
    if not sentences or sum(len(sentence["text"].strip()) for sentence in sentences) < minimum:
        raise input_too_short(str(doc.get("doc_id", "")), len(body.strip()), minimum)
    return sentences


def language_profile(text: str) -> dict[str, Any]:
    cjk = len(_CJK_RE.findall(text))
    latin_words = len(_LATIN_WORD_RE.findall(text))
    latin_letters = sum(len(word) for word in _LATIN_WORD_RE.findall(text))
    if cjk and latin_words:
        if cjk >= 4 and latin_letters >= 4:
            detected = "mixed"
        elif cjk >= latin_letters:
            detected = "zh"
        else:
            detected = "en"
    elif cjk:
        detected = "zh"
    elif latin_words:
        detected = "en"
    else:
        detected = "unknown"
    return {
        "detected": detected,
        "cjk_chars": cjk,
        "latin_words": latin_words,
        "latin_letters": latin_letters,
    }


def language_consistency(doc_language: str, candidate_text: str) -> tuple[float, bool, str]:
    detected = str(language_profile(candidate_text)["detected"])
    if detected == "unknown":
        return 0.2, False, "EMPTY_OR_UNDETECTABLE_LANGUAGE"
    if detected == doc_language:
        return 1.0, True, ""
    if doc_language == "mixed" and detected in {"zh", "en", "mixed"}:
        return 0.82 if detected in {"zh", "en"} else 1.0, True, ""
    if doc_language == "zh" and detected == "mixed":
        return 0.8, True, ""
    if doc_language == "en" and detected == "mixed":
        return 0.8, True, ""
    return 0.0, False, "LANGUAGE_MISMATCH"


def key_points(doc: dict[str, Any], sentences: list[dict[str, Any]], *, limit: int = 14) -> list[dict[str, Any]]:
    title_tokens = tokenize(doc.get("title", ""))
    title_counts = Counter(title_tokens)
    counts: Counter[str] = Counter()
    for sentence in sentences:
        counts.update(sentence["tokens"])
    scored: list[tuple[float, str, str]] = []
    for term, count in counts.items():
        title_weight = title_counts.get(term, 0)
        weight = 1.0 + math.log1p(count) + 1.4 * title_weight
        kind = "title" if title_weight else "body"
        scored.append((weight, term, kind))
    for term, count in title_counts.items():
        if term not in counts:
            scored.append((2.0 + math.log1p(count), term, "title"))
    scored.sort(key=lambda item: (-item[0], item[1]))
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for weight, term, kind in scored:
        if term in seen:
            continue
        seen.add(term)
        result.append({"term": term, "weight": round(weight, 6), "kind": kind})
        if len(result) >= limit:
            break
    return result


def coverage_ratio(text: str, points: list[dict[str, Any]]) -> tuple[float, list[str], list[str]]:
    present_tokens = set(tokenize(text, keep_stopwords=True))
    total = sum(point["weight"] for point in points)
    if total <= 0:
        return 1.0, [], []
    covered_weight = 0.0
    covered: list[str] = []
    missing: list[str] = []
    for point in points:
        if point["term"] in present_tokens:
            covered_weight += point["weight"]
            covered.append(point["term"])
        else:
            missing.append(point["term"])
    return covered_weight / total, covered, missing


def coverage_for_sentences(sentence_indices: Iterable[int], sentences: list[dict[str, Any]], points: list[dict[str, Any]]) -> float:
    selected = [sentences[index] for index in sorted(set(sentence_indices))]
    text = " ".join(sentence["text"] for sentence in selected)
    return coverage_ratio(text, points)[0]


def target_band(config: dict[str, Any], target_length: str) -> dict[str, int]:
    targets = config["targets"]
    if target_length not in targets:
        raise KeyError(target_length)
    band = targets[target_length]
    return {
        "min_chars": int(band["min_chars"]),
        "max_chars": int(band["max_chars"]),
        "candidate_count": int(band.get("candidate_count", 3)),
    }
