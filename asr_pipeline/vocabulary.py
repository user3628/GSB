"""Local vocabulary, deterministic orthogonal embeddings and transition weights."""

import math

from .hashing import stable_hash

VOCABULARY = (
    "hello",
    "world",
    "weather",
    "report",
    "meeting",
    "alpha",
    "system",
    "ready",
    "check",
    "signal",
    "pipeline",
    "确认",
    "开始",
    "结束",
    "记录",
    "同步",
    "alpha确认",
    "check记录",
    "signal同步",
)

CJK_RANGES = ((0x3400, 0x4DBF), (0x4E00, 0x9FFF), (0xF900, 0xFAFF))


def _is_cjk(char):
    code = ord(char)
    return any(lo <= code <= hi for lo, hi in CJK_RANGES)


def is_mixed_script(word):
    """A word is mixed-script when it contains both CJK and Latin/digit chars."""
    has_cjk = any(_is_cjk(char) for char in word)
    has_latin = any(
        ("a" <= char.lower() <= "z") or ("0" <= char <= "9") for char in word
    )
    return has_cjk and has_latin


def _raw_vector(index, dimension, attempt):
    return [
        (stable_hash("embedding", index, attempt, band) % 2000) / 1000.0 - 1.0
        for band in range(dimension)
    ]


def _build_embeddings(dimension):
    """Deterministic unit-norm vectors made orthogonal via Gram-Schmidt."""
    orthonormal = []
    for index in range(len(VOCABULARY)):
        projected = None
        for attempt in range(64):
            candidate = _raw_vector(index, dimension, attempt)
            projected = list(candidate)
            for basis in orthonormal:
                overlap = sum(
                    projected[band] * basis[band] for band in range(dimension)
                )
                projected = [
                    projected[band] - overlap * basis[band]
                    for band in range(dimension)
                ]
            norm = math.sqrt(sum(value * value for value in projected))
            if norm >= 1e-6:
                break
        norm = math.sqrt(sum(value * value for value in projected))
        if norm < 1e-6:
            raise ValueError("embedding dimension cannot separate the vocabulary")
        orthonormal.append([value / norm for value in projected])
    return orthonormal


_EMBEDDING_CACHE = {}


def word_embedding(word, dimension):
    if dimension not in _EMBEDDING_CACHE:
        _EMBEDDING_CACHE[dimension] = _build_embeddings(dimension)
    index = VOCABULARY.index(word)
    return [round(value, 9) for value in _EMBEDDING_CACHE[dimension][index]]


def transition_weight(prev_word, next_word):
    h = stable_hash("transition", prev_word, next_word)
    return ((h % 1000) / 1000.0 - 0.5) * 0.1


def signature_bin(word, available_bins):
    """Deterministic, unique FFT bin assigned to each vocabulary word."""
    index = VOCABULARY.index(word)
    low, high = 2, 2 + available_bins
    return low + index % (high - low)
