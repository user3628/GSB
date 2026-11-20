"""Text normalization and 64-bit SimHash (pure stdlib, deterministic)."""

import hashlib
import unicodedata
from collections import Counter

SIMHASH_BITS = 64


def normalize_text(text):
    """Unicode NFKC -> lowercase -> whitespace collapse."""
    text = unicodedata.normalize("NFKC", text)
    text = text.lower()
    return " ".join(text.split())


def tokenize(norm_text):
    """Approximate tokens: whitespace split of the normalized text."""
    return norm_text.split() if norm_text else []


def token_count(norm_text):
    return len(tokenize(norm_text))


def _token_hash(token):
    digest = hashlib.blake2b(token.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big")


def simhash64(tokens):
    """Frequency-weighted 64-bit SimHash. Deterministic across runs."""
    weights = Counter(tokens)
    accum = [0] * SIMHASH_BITS
    for token, weight in weights.items():
        hashed = _token_hash(token)
        for bit in range(SIMHASH_BITS):
            if (hashed >> bit) & 1:
                accum[bit] += weight
            else:
                accum[bit] -= weight
    fingerprint = 0
    for bit in range(SIMHASH_BITS):
        if accum[bit] > 0:
            fingerprint |= 1 << bit
    return fingerprint


def hamming64(a, b):
    return (a ^ b).bit_count()
