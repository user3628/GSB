"""Deterministic LRU query cache.

Cache keys embed the sample fingerprint, the config version and the stage
fingerprint so a hit is guaranteed to be the byte-identical result of
recomputing against the same inputs.  Capacity eviction only drops entries;
it can never change a visible result.
"""
from __future__ import annotations

from collections import OrderedDict

from .fingerprint import canonical_json_bytes, hash_obj


class QueryCache:
    def __init__(self, capacity: int) -> None:
        if capacity < 1:
            raise ValueError("cache capacity must be >= 1")
        self.capacity = capacity
        self._store: OrderedDict[str, bytes] = OrderedDict()
        self.hits = 0
        self.misses = 0
        self.evictions = 0

    def make_key(
        self,
        kind: str,
        params: dict,
        sample_fingerprint: str,
        config_version: str,
        stage_fingerprint: str,
    ) -> str:
        return hash_obj({
            "kind": kind,
            "params": params,
            "sample_fingerprint": sample_fingerprint,
            "config_version": config_version,
            "stage_fingerprint": stage_fingerprint,
        })

    def get_or_compute(self, key: str, compute) -> tuple[bytes, bool]:
        if key in self._store:
            self.hits += 1
            self._store.move_to_end(key)
            return self._store[key], True
        self.misses += 1
        value = compute()
        if not isinstance(value, bytes):
            value = canonical_json_bytes(value)
        self._store[key] = value
        self._store.move_to_end(key)
        while len(self._store) > self.capacity:
            self._store.popitem(last=False)
            self.evictions += 1
        return value, False

    def clear(self) -> None:
        self._store.clear()

    def stats(self) -> dict:
        return {
            "capacity": self.capacity,
            "size": len(self._store),
            "hits": self.hits,
            "misses": self.misses,
            "evictions": self.evictions,
        }
