"""Deterministic text normalization, segmentation and phoneme mapping."""

from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable

from .config import SplitConfig
from .errors import invalid_text
from .utils import canonical_json, sha256_json


_WHITESPACE_RE = re.compile(r"\s+", re.UNICODE)


def _is_cjk(ch: str) -> bool:
    code = ord(ch)
    return (
        0x3400 <= code <= 0x4DBF
        or 0x4E00 <= code <= 0x9FFF
        or 0xF900 <= code <= 0xFAFF
        or 0x20000 <= code <= 0x2FA1F
    )


def _is_ascii_alnum(ch: str) -> bool:
    return ("a" <= ch <= "z") or ("A" <= ch <= "Z") or ("0" <= ch <= "9")


@dataclass(frozen=True)
class Token:
    text: str
    char_start: int
    char_end: int
    kind: str  # cjk | latin | digit | pause | space | unknown

    @property
    def visible_chars(self) -> int:
        return 0 if self.kind == "space" else len(self.text)


@dataclass(frozen=True)
class PhonemeToken:
    global_index: int
    char_start: int
    char_end: int
    char: str
    phone: str
    frames: int
    kind: str
    marker: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "PhonemeToken":
        return cls(**value)


@dataclass(frozen=True)
class TextChunk:
    index: int
    text: str
    char_start: int
    char_end: int
    phone_start: int
    phone_end: int
    visible_chars: int
    phonemes: tuple[PhonemeToken, ...]

    @property
    def frame_count(self) -> int:
        return sum(item.frames for item in self.phonemes)

    def to_dict(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "text": self.text,
            "char_start": self.char_start,
            "char_end": self.char_end,
            "phone_start": self.phone_start,
            "phone_end": self.phone_end,
            "visible_chars": self.visible_chars,
            "frame_count": self.frame_count,
            "phonemes": [item.to_dict() for item in self.phonemes],
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "TextChunk":
        return cls(
            index=int(value["index"]),
            text=str(value["text"]),
            char_start=int(value["char_start"]),
            char_end=int(value["char_end"]),
            phone_start=int(value["phone_start"]),
            phone_end=int(value["phone_end"]),
            visible_chars=int(value["visible_chars"]),
            phonemes=tuple(PhonemeToken.from_dict(item) for item in value["phonemes"]),
        )


@dataclass(frozen=True)
class FrontendResult:
    utt_id: str
    raw_text: str
    normalized_text: str
    language: str
    chunks: tuple[TextChunk, ...]
    phoneme_count: int
    frame_count: int
    phoneme_table_digest: str

    @property
    def markers(self) -> tuple[str, ...]:
        result: list[str] = []
        for chunk in self.chunks:
            for item in chunk.phonemes:
                result.extend([item.marker] * item.frames)
        return tuple(result)

    @property
    def phonemes(self) -> tuple[PhonemeToken, ...]:
        return tuple(item for chunk in self.chunks for item in chunk.phonemes)


class PhonemeTable:
    def __init__(self, raw: dict[str, Any], source_path: Path):
        if raw.get("schema_version") != 1:
            raise ValueError("phoneme table schema_version must be 1")
        self.raw = raw
        self.source_path = source_path
        self.chars: dict[str, dict[str, Any]] = raw["chars"]
        self.latin: dict[str, dict[str, Any]] = raw["latin"]
        self.digits: dict[str, dict[str, Any]] = raw["digits"]
        self.punctuation: dict[str, dict[str, Any]] = raw["punctuation"]
        self.whitespace: dict[str, Any] = raw["whitespace"]
        self.digest = sha256_json(raw)

    @classmethod
    def load(cls, path: Path) -> "PhonemeTable":
        raw = json.loads(path.read_text(encoding="utf-8"))
        return cls(raw, path)

    def lookup(self, token: Token, char: str) -> dict[str, Any] | None:
        if token.kind == "cjk":
            return self.chars.get(char)
        if token.kind == "latin":
            return self.latin.get(char.lower())
        if token.kind == "digit":
            return self.digits.get(char)
        if token.kind == "pause":
            return self.punctuation.get(char)
        if token.kind == "space":
            return self.whitespace
        return None

    def entry_digest(self) -> str:
        return self.digest


def normalize_text(text: str) -> str:
    normalized = unicodedata.normalize("NFKC", text)
    normalized = _WHITESPACE_RE.sub(" ", normalized)
    return normalized


def tokenize(normalized: str, punctuation: set[str]) -> list[Token]:
    tokens: list[Token] = []
    i = 0
    n = len(normalized)
    while i < n:
        ch = normalized[i]
        start = i
        if ch == " ":
            tokens.append(Token(" ", i, i + 1, "space"))
            i += 1
            continue
        if _is_cjk(ch):
            tokens.append(Token(ch, i, i + 1, "cjk"))
            i += 1
            continue
        if "A" <= ch <= "Z" or "a" <= ch <= "z":
            i += 1
            while i < n and (("A" <= normalized[i] <= "Z") or ("a" <= normalized[i] <= "z")):
                i += 1
            tokens.append(Token(normalized[start:i], start, i, "latin"))
            continue
        if "0" <= ch <= "9":
            i += 1
            while i < n and "0" <= normalized[i] <= "9":
                i += 1
            tokens.append(Token(normalized[start:i], start, i, "digit"))
            continue
        if ch in punctuation:
            tokens.append(Token(ch, i, i + 1, "pause"))
            i += 1
            continue
        tokens.append(Token(ch, i, i + 1, "unknown"))
        i += 1
    return tokens


def _validate_split_params(split: SplitConfig, utt_id: str) -> None:
    if (
        not isinstance(split.first_chunk_min_chars, int)
        or isinstance(split.first_chunk_min_chars, bool)
        or split.first_chunk_min_chars <= 0
    ):
        raise invalid_text(
            utt_id=utt_id,
            chunk_index=0,
            char_position=0,
            message="first_chunk_min_chars must be a positive integer",
            field="split.first_chunk_min_chars",
        )
    if (
        not isinstance(split.subsequent_chunk_chars, int)
        or isinstance(split.subsequent_chunk_chars, bool)
        or split.subsequent_chunk_chars <= 0
    ):
        raise invalid_text(
            utt_id=utt_id,
            chunk_index=0,
            char_position=0,
            message="subsequent_chunk_chars must be a positive integer",
            field="split.subsequent_chunk_chars",
        )
    if not isinstance(split.punctuation_forced, bool):
        raise invalid_text(
            utt_id=utt_id,
            chunk_index=0,
            char_position=0,
            message="punctuation_forced must be boolean",
            field="split.punctuation_forced",
        )
    if not isinstance(split.max_budget_splits, int) or split.max_budget_splits < 0:
        raise invalid_text(
            utt_id=utt_id,
            chunk_index=0,
            char_position=0,
            message="max_budget_splits must be a non-negative integer",
            field="split.max_budget_splits",
        )


def _segment_tokens(tokens: list[Token], split: SplitConfig, utt_id: str) -> list[tuple[int, int]]:
    if not tokens:
        raise invalid_text(
            utt_id=utt_id, chunk_index=0, char_position=0, message="text is empty after normalization"
        )
    spans: list[tuple[int, int]] = []
    i = 0
    chunk_index = 0
    while i < len(tokens):
        target = split.first_chunk_min_chars if chunk_index == 0 else split.subsequent_chunk_chars
        min_required = target if chunk_index == 0 else max(1, target // 2)
        end = i
        visible = 0
        punctuation_boundary: int | None = None
        while end < len(tokens):
            visible += tokens[end].visible_chars
            end += 1
            if (
                split.punctuation_forced
                and tokens[end - 1].kind == "pause"
                and visible >= min_required
            ):
                punctuation_boundary = end
            if visible >= target:
                break
        if end <= i:
            end = i + 1
        if split.punctuation_forced and punctuation_boundary is not None:
            end = punctuation_boundary
        elif split.punctuation_forced and end < len(tokens):
            # Prefer the next punctuation boundary even when a long Latin token
            # or a spacing-heavy span first crosses the target.  The lookahead is
            # bounded by one target's worth of tokens to avoid unbounded blocks.
            forward_limit = max(1, target)
            scan = end
            while scan < len(tokens) and scan - end < forward_limit:
                if tokens[scan].kind == "pause":
                    end = scan + 1
                    break
                scan += 1
        spans.append((i, end))
        i = end
        chunk_index += 1
    return spans


def front_end(
    *,
    utt_id: str,
    text: str,
    language: str,
    split: SplitConfig,
    phoneme_table: PhonemeTable,
) -> FrontendResult:
    _validate_split_params(split, utt_id)
    if language not in {"zh", "en"}:
        raise invalid_text(
            utt_id=utt_id,
            chunk_index=0,
            char_position=0,
            message="language must be 'zh' or 'en'",
            language=language,
        )
    if not isinstance(text, str):
        raise invalid_text(
            utt_id=utt_id,
            chunk_index=0,
            char_position=0,
            message="text must be a string",
        )
    normalized = normalize_text(text)
    if not normalized or not normalized.strip():
        raise invalid_text(
            utt_id=utt_id,
            chunk_index=0,
            char_position=0,
            message="text is empty after NFKC and whitespace normalization",
        )

    punctuation = set(phoneme_table.punctuation.keys())
    tokens = tokenize(normalized, punctuation)
    spans = _segment_tokens(tokens, split, utt_id)

    chunks: list[TextChunk] = []
    global_index = 0
    for chunk_index, (left, right) in enumerate(spans):
        phone_items: list[PhonemeToken] = []
        for token in tokens[left:right]:
            for char_offset, char in enumerate(token.text):
                entry = phoneme_table.lookup(token, char)
                if entry is None:
                    raise invalid_text(
                        utt_id=utt_id,
                        chunk_index=chunk_index,
                        char_position=token.char_start + char_offset,
                        message=f"character {char!r} is absent from the phoneme table",
                        character=char,
                        unicode_name=unicodedata.name(char, "<unnamed>"),
                    )
                frames = int(entry["frames"])
                if frames <= 0:
                    raise invalid_text(
                        utt_id=utt_id,
                        chunk_index=chunk_index,
                        char_position=token.char_start + char_offset,
                        message="phoneme frame count must be positive",
                        character=char,
                    )
                phone_items.append(
                    PhonemeToken(
                        global_index=global_index,
                        char_start=token.char_start + char_offset,
                        char_end=token.char_start + char_offset + 1,
                        char=char,
                        phone=str(entry["phone"]),
                        frames=frames,
                        kind=token.kind,
                        marker=str(entry["marker"]),
                    )
                )
                global_index += 1
        first_token = tokens[left]
        last_token = tokens[right - 1]
        part_text = normalized[first_token.char_start : last_token.char_end]
        chunks.append(
            TextChunk(
                index=chunk_index,
                text=part_text,
                char_start=first_token.char_start,
                char_end=last_token.char_end,
                phone_start=phone_items[0].global_index,
                phone_end=phone_items[-1].global_index + 1,
                visible_chars=sum(token.visible_chars for token in tokens[left:right]),
                phonemes=tuple(phone_items),
            )
        )

    joined = "".join(chunk.text for chunk in chunks)
    if joined != normalized:
        raise invalid_text(
            utt_id=utt_id,
            chunk_index=0,
            char_position=0,
            message="internal chunk coverage invariant failed",
            normalized_text=normalized,
            joined_text=joined,
            context="segmentation must preserve every normalized character",
        )

    return FrontendResult(
        utt_id=utt_id,
        raw_text=text,
        normalized_text=normalized,
        language=language,
        chunks=tuple(chunks),
        phoneme_count=global_index,
        frame_count=sum(chunk.frame_count for chunk in chunks),
        phoneme_table_digest=phoneme_table.entry_digest(),
    )


def front_end_single_chunk(
    *,
    utt_id: str,
    text: str,
    language: str,
    split: SplitConfig,
    phoneme_table: PhonemeTable,
) -> FrontendResult:
    """One-shot reference path used by the consistency validator.

    It intentionally bypasses split policy while retaining the same normalized
    token stream, global phoneme indices and frames-per-phone decisions.
    """

    _validate_split_params(split, utt_id)
    normalized = normalize_text(text)
    if not normalized or not normalized.strip():
        raise invalid_text(
            utt_id=utt_id, chunk_index=0, char_position=0, message="text is empty after normalization"
        )
    punctuation = set(phoneme_table.punctuation.keys())
    tokens = tokenize(normalized, punctuation)
    phones: list[PhonemeToken] = []
    for index, token in enumerate(tokens):
        for char_offset, char in enumerate(token.text):
            entry = phoneme_table.lookup(token, char)
            if entry is None:
                raise invalid_text(
                    utt_id=utt_id,
                    chunk_index=0,
                    char_position=token.char_start + char_offset,
                    message=f"character {char!r} is absent from the phoneme table",
                    character=char,
                )
            phones.append(
                PhonemeToken(
                    global_index=len(phones),
                    char_start=token.char_start + char_offset,
                    char_end=token.char_start + char_offset + 1,
                    char=char,
                    phone=str(entry["phone"]),
                    frames=int(entry["frames"]),
                    kind=token.kind,
                    marker=str(entry["marker"]),
                )
            )
    chunk = TextChunk(
        index=0,
        text=normalized,
        char_start=0,
        char_end=len(normalized),
        phone_start=0,
        phone_end=len(phones),
        visible_chars=sum(token.visible_chars for token in tokens),
        phonemes=tuple(phones),
    )
    return FrontendResult(
        utt_id=utt_id,
        raw_text=text,
        normalized_text=normalized,
        language=language,
        chunks=(chunk,),
        phoneme_count=len(phones),
        frame_count=sum(phone.frames for phone in phones),
        phoneme_table_digest=phoneme_table.entry_digest(),
    )


def rebuild_chunk(frontend: FrontendResult, chunk_index: int, phones: Iterable[PhonemeToken]) -> TextChunk:
    items = tuple(phones)
    if not items:
        raise ValueError("a block must contain at least one phoneme")
    return TextChunk(
        index=chunk_index,
        text=frontend.normalized_text[items[0].char_start : items[-1].char_end],
        char_start=items[0].char_start,
        char_end=items[-1].char_end,
        phone_start=items[0].global_index,
        phone_end=items[-1].global_index + 1,
        visible_chars=sum(1 for item in items if item.kind != "space"),
        phonemes=items,
    )


def table_canonical_json(table: PhonemeTable) -> str:
    return canonical_json(table.raw)
