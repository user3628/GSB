from __future__ import annotations

import re
import unicodedata
from collections import Counter
from dataclasses import dataclass
from typing import Callable, Sequence

CJK_RANGES: tuple[tuple[int, int], ...] = (
    (0x3400, 0x4DBF),
    (0x4E00, 0x9FFF),
    (0xF900, 0xFAFF),
    (0x20000, 0x3134F),
)
PUNCT_CHARS = set(".,!?;:。，！？；：、…—–'\"“”‘’()[]{}<>《》〈〉/\\|+-=*&%$#@~`^_·")
_WHITESPACE_RE = re.compile(r"\s+", re.UNICODE)


def _is_cjk(char: str) -> bool:
    code = ord(char)
    return any(lo <= code <= hi for lo, hi in CJK_RANGES)


def _is_latin(char: str) -> bool:
    return ("a" <= char <= "z") or ("0" <= char <= "9") or ("A" <= char <= "Z")


def normalize_text(text: str) -> str:
    """NFKC, whitespace folding, full/half-width unification and lowercasing."""
    if not isinstance(text, str):
        text = ""
    # NFKC performs the required compatibility-width normalization.  The
    # explicit full-width ASCII translation is intentionally redundant and
    # keeps the contract obvious to callers and tests.
    text = unicodedata.normalize("NFKC", text)
    text = "".join(
        chr(ord(char) - 0xFEE0) if 0xFF01 <= ord(char) <= 0xFF5E else char
        for char in text
    )
    text = _WHITESPACE_RE.sub(" ", text).strip()
    return text.lower()


def _token_kind(char: str) -> str:
    if _is_cjk(char):
        return "zh"
    if _is_latin(char):
        return "lat"
    if char in PUNCT_CHARS or unicodedata.category(char).startswith("P"):
        return "punct"
    return "sym"


def pretokenize(normalized: str) -> list[dict[str, object]]:
    """Split Chinese into single characters and Latin runs into whole tokens."""
    tokens: list[dict[str, object]] = []
    i = 0
    length = len(normalized)
    while i < length:
        char = normalized[i]
        if char.isspace():
            i += 1
            continue
        kind = _token_kind(char)
        if kind == "lat":
            end = i + 1
            while end < length and _is_latin(normalized[end]):
                end += 1
            tokens.append(
                {"text": normalized[i:end], "start": i, "end": end, "lang": "lat"}
            )
            i = end
        else:
            tokens.append(
                {
                    "text": normalized[i : i + 1],
                    "start": i,
                    "end": i + 1,
                    "lang": kind,
                }
            )
            i += 1
    return tokens


def has_content_tokens(tokens: Sequence[dict[str, object]]) -> bool:
    return any(token["lang"] in {"zh", "lat"} for token in tokens)


def base_token_texts(text: str) -> list[str]:
    return [str(token["text"]) for token in pretokenize(normalize_text(text))]


@dataclass(frozen=True)
class BpeRule:
    rank: int
    lang: str
    left: str
    right: str
    count: int

    def as_dict(self) -> dict[str, object]:
        return {
            "rank": self.rank,
            "lang": self.lang,
            "left": self.left,
            "right": self.right,
            "count": self.count,
            "merged": self.left + self.right,
        }


def _expand_atoms(normalized: str, tokens: Sequence[dict[str, object]]) -> list[dict[str, object]]:
    atoms: list[dict[str, object]] = []
    for token in tokens:
        text = str(token["text"])
        start = int(token["start"])
        lang = str(token["lang"])
        # Latin runs remain whole pretokenization units.  BPE merges adjacent
        # Chinese characters (and would merge adjacent Latin units if a caller
        # supplied a whitespace-free stream) while preserving exact offsets.
        atoms.append(
            {
                "text": text,
                "start": start,
                "end": start + len(text),
                "lang": lang,
                "merge_source": [text],
                "merge_rules": [],
            }
        )
    return atoms


def _eligible_pair(left: dict[str, object], right: dict[str, object]) -> bool:
    return (
        left["lang"] == right["lang"]
        and left["lang"] in {"zh", "lat"}
        and int(left["end"]) == int(right["start"])
    )


def _pair_counts(sequences: Sequence[Sequence[dict[str, object]]]) -> Counter[tuple[str, str, str]]:
    counts: Counter[tuple[str, str, str]] = Counter()
    for sequence in sequences:
        for left, right in zip(sequence, sequence[1:]):
            if _eligible_pair(left, right):
                counts[(str(left["lang"]), str(left["text"]), str(right["text"]))] += 1
    return counts


def _merge_sequence(
    sequence: Sequence[dict[str, object]], rule: BpeRule
) -> list[dict[str, object]]:
    merged: list[dict[str, object]] = []
    i = 0
    while i < len(sequence):
        if i + 1 < len(sequence):
            left = sequence[i]
            right = sequence[i + 1]
            if (
                _eligible_pair(left, right)
                and left["lang"] == rule.lang
                and left["text"] == rule.left
                and right["text"] == rule.right
            ):
                merged.append(
                    {
                        "text": str(left["text"]) + str(right["text"]),
                        "start": int(left["start"]),
                        "end": int(right["end"]),
                        "lang": rule.lang,
                        "merge_source": list(left["merge_source"]) + list(right["merge_source"]),
                        "merge_rules": list(left["merge_rules"])
                        + list(right["merge_rules"])
                        + [rule.rank],
                    }
                )
                i += 2
                continue
        merged.append(dict(sequence[i]))
        i += 1
    return merged


def effective_bpe_limit(base_limit: int, tier: str) -> int:
    multiplier = {"small": 1, "medium": 2, "large": 4}.get(tier, 1)
    return max(0, int(base_limit) * multiplier)


def learn_and_apply_bpe(
    sequences: Sequence[Sequence[dict[str, object]]],
    *,
    merge_limit: int,
    min_count: int,
    stop_check: Callable[[], None] | None = None,
) -> tuple[list[BpeRule], list[list[dict[str, object]]]]:
    """Learn deterministic BPE rules and return merged atom sequences.

    Work is deliberately performed in sorted/count order and uses left-to-right
    non-overlapping merges.  Separate language streams prevent cross-language
    merges, and offset adjacency prevents merges across whitespace.
    """
    current = [list(sequence) for sequence in sequences]
    rules: list[BpeRule] = []
    for rank in range(1, merge_limit + 1):
        if stop_check:
            stop_check()
        counts = _pair_counts(current)
        candidates = [
            (count, lang, left, right)
            for (lang, left, right), count in counts.items()
            if count >= min_count
        ]
        if not candidates:
            break
        # Highest count wins; ties are resolved by stable lexical ordering.
        count, lang, left, right = sorted(
            candidates, key=lambda item: (-item[0], item[1], item[2], item[3])
        )[0]
        rule = BpeRule(rank=rank, lang=lang, left=left, right=right, count=count)
        rules.append(rule)
        current = [_merge_sequence(sequence, rule) for sequence in current]
    return rules, current


def public_token(atom: dict[str, object]) -> dict[str, object]:
    return {
        "text": str(atom["text"]),
        "start": int(atom["start"]),
        "end": int(atom["end"]),
        "lang": str(atom["lang"]),
        "merge_source": list(atom["merge_source"]),
        "merge_rules": list(atom["merge_rules"]),
    }


def encode_with_tokens(
    source: str,
    target: str,
    *,
    tier: str,
    bpe_base_limit: int,
    bpe_min_count: int,
) -> tuple[list[dict[str, object]], list[dict[str, object]], dict[str, object], dict[str, int]]:
    """Convenience entry point for one pair; primarily used by tests."""
    normalized_source = normalize_text(source)
    normalized_target = normalize_text(target)
    source_base = pretokenize(normalized_source)
    target_base = pretokenize(normalized_target)
    source_atoms = _expand_atoms(normalized_source, source_base)
    target_atoms = _expand_atoms(normalized_target, target_base)
    rules, merged = learn_and_apply_bpe(
        [source_atoms, target_atoms],
        merge_limit=effective_bpe_limit(bpe_base_limit, tier),
        min_count=bpe_min_count,
    )
    source_tokens = [public_token(atom) for atom in merged[0]]
    target_tokens = [public_token(atom) for atom in merged[1]]
    meta = {
        "normalizer": "nfkc_ws_lower_v1",
        "tokenizer": "cjk_char_latin_run_v1",
        "bpe_merge_limit": effective_bpe_limit(bpe_base_limit, tier),
        "bpe_min_count": bpe_min_count,
        "rules": [rule.as_dict() for rule in rules],
    }
    counts = {
        "source_atoms": len(source_atoms),
        "target_atoms": len(target_atoms),
        "source_tokens": len(source_tokens),
        "target_tokens": len(target_tokens),
    }
    return source_tokens, target_tokens, meta, counts

