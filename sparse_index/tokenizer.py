"""Unicode normalization and the deterministic Chinese/ASCII tokenizer."""

from __future__ import annotations

import re
import unicodedata
from collections import Counter
from dataclasses import dataclass
from typing import Any, Iterable

from .errors import error


_WHITESPACE_RE = re.compile(r"\s+", re.UNICODE)
_ASCII_ALNUM_RE = re.compile(r"[a-z0-9]+")


def normalize_text(text: str) -> str:
    """NFKC, whitespace folding, and lowercase in a fixed order."""

    normalized = unicodedata.normalize("NFKC", str(text))
    normalized = _WHITESPACE_RE.sub(" ", normalized).strip()
    return normalized.lower()


def normalize_token(text: str) -> str:
    return normalize_text(text)


def is_han(character: str) -> bool:
    codepoint = ord(character)
    return (
        0x3400 <= codepoint <= 0x4DBF
        or 0x4E00 <= codepoint <= 0x9FFF
        or 0xF900 <= codepoint <= 0xFAFF
        or 0x20000 <= codepoint <= 0x2FA1F
        or 0x30000 <= codepoint <= 0x323AF
    )


def tokenize_text(text: str) -> list[str]:
    """Tokenize a title or body using Han unigrams/bigrams and ASCII runs."""

    normalized = normalize_text(text)
    tokens: list[str] = []
    index = 0
    length = len(normalized)
    while index < length:
        character = normalized[index]
        if is_han(character):
            end = index + 1
            while end < length and is_han(normalized[end]):
                end += 1
            run = normalized[index:end]
            tokens.extend(run)
            tokens.extend(run[position : position + 2] for position in range(len(run) - 1))
            index = end
            continue
        if character.isascii() and character.isalnum():
            match = _ASCII_ALNUM_RE.match(normalized, index)
            if match is not None:
                tokens.append(match.group(0))
                index = match.end()
                continue
        index += 1
    return tokens


def filtered_counts(text: str, stopwords: set[str]) -> Counter[str]:
    return Counter(token for token in tokenize_text(text) if token and token not in stopwords)


@dataclass(frozen=True)
class TokenizedDocument:
    doc_id: str
    title: str
    body: str
    lang: str
    source: str
    empty_body: bool
    doc_length: int
    title_terms: list[list[int]]
    body_terms: list[list[int]]
    term_freqs: list[list[int]]


def _pair_counts(counter: Counter[str], term_ids: dict[str, int]) -> list[list[int]]:
    return [
        [term_ids[term], int(frequency)]
        for term, frequency in sorted(counter.items(), key=lambda item: term_ids[item[0]])
    ]


def read_jsonl_documents(paths: Iterable[Any]) -> list[dict[str, Any]]:
    documents: list[dict[str, Any]] = []
    for raw_path in paths:
        from pathlib import Path

        path = Path(raw_path)
        with path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                if not line.strip():
                    continue
                try:
                    import json

                    record = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise error(
                        "E_DATA",
                        "JSONL 行无法解析",
                        file=path.name,
                        line=line_number,
                        reason=str(exc),
                    )
                if not isinstance(record, dict):
                    raise error(
                        "E_DATA",
                        "JSONL 文档必须是对象",
                        file=path.name,
                        line=line_number,
                    )
                doc_id = record.get("doc_id")
                if doc_id is None or str(doc_id) == "":
                    raise error("E_DATA", "文档缺少 doc_id", file=path.name, line=line_number)
                record["doc_id"] = str(doc_id)
                record["title"] = str(record.get("title", ""))
                record["body"] = str(record.get("body", ""))
                record["lang"] = str(record.get("lang", "unknown"))
                record["source"] = str(record.get("source", path.stem))
                record["_source_file"] = path.name
                record["_line_number"] = line_number
                documents.append(record)
    return documents


def tokenize_documents(
    documents: list[dict[str, Any]],
    stopwords: Iterable[str],
    stop_check: Any = None,
) -> tuple[list[dict[str, Any]], list[TokenizedDocument]]:
    """Build the vocabulary and per-document term frequency records."""

    normalized_stopwords = {normalize_token(item) for item in stopwords}
    normalized_stopwords.discard("")
    seen_ids: set[str] = set()
    temporary: list[tuple[dict[str, Any], Counter[str], Counter[str], str, str]] = []
    vocabulary: set[str] = set()

    for document in documents:
        if stop_check is not None and stop_check():
            from .errors import GracefulStop

            raise GracefulStop("收到停止请求，已在文档边界退出")
        doc_id = document["doc_id"]
        if doc_id in seen_ids:
            raise error("E_DATA", "doc_id 重复", doc_id=doc_id)
        seen_ids.add(doc_id)
        title_normalized = normalize_text(document["title"])
        body_normalized = normalize_text(document["body"])
        title_counts = filtered_counts(document["title"], normalized_stopwords)
        body_counts = filtered_counts(document["body"], normalized_stopwords)
        vocabulary.update(title_counts)
        vocabulary.update(body_counts)
        temporary.append(
            (
                document,
                title_counts,
                body_counts,
                title_normalized,
                body_normalized,
            )
        )

    ordered_terms = sorted(vocabulary)
    term_ids = {term: term_id for term_id, term in enumerate(ordered_terms)}
    tokenized: list[TokenizedDocument] = []
    for document, title_counts, body_counts, title_normalized, body_normalized in sorted(
        temporary, key=lambda item: item[0]["doc_id"]
    ):
        combined = title_counts + body_counts
        tokenized.append(
            TokenizedDocument(
                doc_id=document["doc_id"],
                title=title_normalized,
                body=body_normalized,
                lang=document["lang"],
                source=document["source"],
                empty_body=(body_normalized == ""),
                doc_length=sum(combined.values()),
                title_terms=_pair_counts(title_counts, term_ids),
                body_terms=_pair_counts(body_counts, term_ids),
                term_freqs=_pair_counts(combined, term_ids),
            )
        )
    return [{"term_id": index, "term": term} for index, term in enumerate(ordered_terms)], tokenized


def tokenizer_metadata(config: Any) -> dict[str, Any]:
    """A compact, observable description of the tokenizer implementation."""

    return {
        "normalization": ["NFKC", "whitespace_fold", "lowercase"],
        "han": "unigram+adjacent_bigram",
        "latin_digits": "contiguous_ascii_alnum_run",
        "stopword_filter": "after_tokenization",
        "configured": config.stage_config(1).get("tokenizer", {}),
    }
