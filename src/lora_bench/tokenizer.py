"""Deterministic byte-level tokenizer (UTF-8 codepoints, no external assets)."""
from __future__ import annotations

from typing import Sequence

PAD_ID = 0
BOS_ID = 1
EOS_ID = 2
# Token ids 3..258 map onto raw UTF-8 bytes 0..255.
BYTE_OFFSET = 3
VOCAB_SIZE = 259
MAX_BYTES = 256


def encode(text: str, max_len: int) -> list[int]:
    payload = text.encode("utf-8")[: max(1, max_len - 2)][:MAX_BYTES]
    return [BOS_ID] + [b + BYTE_OFFSET for b in payload] + [EOS_ID]


def decode(ids: Sequence[int]) -> str:
    raw = bytes(i - BYTE_OFFSET for i in ids if BYTE_OFFSET <= i < BYTE_OFFSET + 256)
    return raw.decode("utf-8", errors="ignore")


def pad_batch(sequences: list[list[int]], max_len: int) -> list[list[int]]:
    padded = []
    for seq in sequences:
        trimmed = seq[:max_len]
        padded.append(trimmed + [PAD_ID] * (max_len - len(trimmed)))
    return padded
