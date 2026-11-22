"""64-bit SimHash with a 4x16-bit band index for near-duplicate detection.

Token hashing uses blake2b (stable across processes, unlike built-in hash).
Bit accumulation is vectorized with numpy. The band index splits the 64-bit
hash into 4 bands of 16 bits; two hashes are candidates when they share at
least one band, and are confirmed duplicates when their Hamming distance is
within a configurable threshold.
"""
from __future__ import annotations

import hashlib
from collections import Counter
from typing import Iterable

import numpy as np

SIMHASH_BITS = 64
BAND_COUNT = 4
BAND_BITS = 16
BAND_MASK = (1 << BAND_BITS) - 1


def token_hash64(token: str) -> int:
    digest = hashlib.blake2b(token.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big")


def simhash64(features: Iterable[str]) -> int:
    counts = Counter(features)
    if not counts:
        return 0
    accumulator = np.zeros(SIMHASH_BITS, dtype=np.int64)
    bit_positions = np.arange(SIMHASH_BITS, dtype=np.uint64)
    for token, weight in sorted(counts.items()):
        hashed = np.uint64(token_hash64(token))
        bits = (hashed >> bit_positions) & np.uint64(1)
        accumulator += np.where(bits == 1, weight, -weight)
    value = 0
    for idx in range(SIMHASH_BITS):
        if accumulator[idx] > 0:
            value |= 1 << idx
    return value


def hamming_distance(left: int, right: int) -> int:
    return (left ^ right).bit_count()


def band_keys(hash_value: int) -> tuple:
    return tuple(
        (hash_value >> (BAND_BITS * idx)) & BAND_MASK for idx in range(BAND_COUNT)
    )


class BandIndex:
    """Inverted index from 16-bit band keys to kept-sample positions."""

    def __init__(self) -> None:
        self._bands: list[dict[int, list[int]]] = [dict() for _ in range(BAND_COUNT)]
        self.hashes: list[int] = []

    def add(self, hash_value: int) -> int:
        position = len(self.hashes)
        self.hashes.append(hash_value)
        for band_idx, key in enumerate(band_keys(hash_value)):
            self._bands[band_idx].setdefault(key, []).append(position)
        return position

    def candidates(self, hash_value: int) -> list[int]:
        found: set[int] = set()
        for band_idx, key in enumerate(band_keys(hash_value)):
            found.update(self._bands[band_idx].get(key, ()))
        return sorted(found)
