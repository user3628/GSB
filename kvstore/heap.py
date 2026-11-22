from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True, slots=True)
class HeapEntry:
    expires_ms: int
    key: str
    version: int
    token: str

    def order(self) -> tuple[int, str, str]:
        return (self.expires_ms, self.key, self.token)


class ExpiryIndex:
    """An indexed min-heap with one live entry per key.

    The position map makes upsert/remove true replacements: an old expiry for a
    key is removed before the new entry is inserted. In addition, every entry
    carries the version token of the index reference that created it. The token
    check is a second line of defence against a stale entry becoming reachable
    after a delete/recreate sequence.
    """

    __slots__ = ("_heap", "_positions")

    def __init__(self, entries: Iterable[HeapEntry] | None = None) -> None:
        self._heap: list[HeapEntry] = []
        self._positions: dict[str, int] = {}
        for entry in sorted(entries or [], key=HeapEntry.order):
            self._insert(entry)

    def __len__(self) -> int:
        return len(self._heap)

    def __contains__(self, key: str) -> bool:
        return key in self._positions

    def clone(self) -> "ExpiryIndex":
        other = ExpiryIndex()
        other._heap = list(self._heap)
        other._positions = dict(self._positions)
        return other

    def upsert(self, key: str, expires_ms: int | None, version: int, token: str) -> None:
        if key in self._positions:
            self._remove_at(self._positions[key])
        if expires_ms is None:
            return
        self._insert(HeapEntry(expires_ms, key, version, token))

    def remove(self, key: str) -> HeapEntry | None:
        pos = self._positions.get(key)
        if pos is None:
            return None
        return self._remove_at(pos)

    def peek(self) -> HeapEntry | None:
        return self._heap[0] if self._heap else None

    def pop(self) -> HeapEntry | None:
        if not self._heap:
            return None
        return self._remove_at(0)

    def pop_due(self, now_ms: int) -> list[HeapEntry]:
        due: list[HeapEntry] = []
        while self._heap and self._heap[0].expires_ms <= now_ms:
            due.append(self._remove_at(0))
        return due

    def entries(self) -> list[HeapEntry]:
        return sorted(self._heap, key=HeapEntry.order)

    def token_for(self, key: str) -> str | None:
        pos = self._positions.get(key)
        return None if pos is None else self._heap[pos].token

    def stale_entries(self, index: dict[str, object]) -> list[HeapEntry]:
        stale: list[HeapEntry] = []
        for entry in self._heap:
            ref = index.get(entry.key)
            token = getattr(ref, "token", None)
            if token != entry.token:
                stale.append(entry)
        return stale

    def duplicate_keys(self) -> list[str]:
        return []

    def _insert(self, entry: HeapEntry) -> None:
        self._heap.append(entry)
        pos = len(self._heap) - 1
        self._positions[entry.key] = pos
        self._sift_up(pos)

    def _remove_at(self, pos: int) -> HeapEntry:
        entry = self._heap[pos]
        last = self._heap.pop()
        self._positions.pop(entry.key, None)
        if pos < len(self._heap):
            self._heap[pos] = last
            self._positions[last.key] = pos
            self._sift_down(pos)
            self._sift_up(pos)
        return entry

    def _sift_up(self, pos: int) -> None:
        item = self._heap[pos]
        while pos > 0:
            parent = (pos - 1) // 2
            if self._heap[parent].order() <= item.order():
                break
            self._heap[pos] = self._heap[parent]
            self._positions[self._heap[pos].key] = pos
            pos = parent
        self._heap[pos] = item
        self._positions[item.key] = pos

    def _sift_down(self, pos: int) -> None:
        size = len(self._heap)
        item = self._heap[pos]
        while True:
            left = pos * 2 + 1
            if left >= size:
                break
            right = left + 1
            child = left
            if right < size and self._heap[right].order() < self._heap[left].order():
                child = right
            if item.order() <= self._heap[child].order():
                break
            self._heap[pos] = self._heap[child]
            self._positions[self._heap[pos].key] = pos
            pos = child
        self._heap[pos] = item
        self._positions[item.key] = pos

