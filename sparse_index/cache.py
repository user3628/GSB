"""Persistent deterministic LRU cache containing query result objects only."""

from __future__ import annotations

import copy
import threading
from collections import OrderedDict
from pathlib import Path
from typing import Any

from .utils import atomic_write_json, load_json


class QueryCache:
    def __init__(
        self,
        path: Path,
        capacity: int,
        config_fingerprint: str,
    ):
        self.path = path
        self.capacity = max(0, int(capacity))
        self.config_fingerprint = config_fingerprint
        self._lock = threading.RLock()
        self._clock = 0
        self._entries: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self.hits = 0
        self.misses = 0
        self.evictions = 0
        self._load()

    def _load(self) -> None:
        payload = load_json(self.path)
        if not isinstance(payload, dict) or payload.get("schema_version") != 1:
            return
        if payload.get("config_fingerprint") != self.config_fingerprint:
            return
        raw_entries = payload.get("entries", [])
        if not isinstance(raw_entries, list):
            return
        for entry in raw_entries:
            if not isinstance(entry, dict):
                continue
            key = entry.get("key")
            last_used = entry.get("last_used")
            result = entry.get("result")
            if not isinstance(key, str) or not isinstance(last_used, int) or not isinstance(result, dict):
                continue
            self._entries[key] = {
                "last_used": last_used,
                "result": result,
            }
            self._clock = max(self._clock, last_used)
        while len(self._entries) > self.capacity:
            self._entries.popitem(last=False)
            self.evictions += 1

    def get(self, key: str) -> dict[str, Any] | None:
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                self.misses += 1
                return None
            self._clock += 1
            entry["last_used"] = self._clock
            self._entries.move_to_end(key)
            self.hits += 1
            return copy.deepcopy(entry["result"])

    def put(self, key: str, result: dict[str, Any]) -> None:
        if self.capacity <= 0:
            with self._lock:
                self.evictions += 1
            return
        with self._lock:
            self._clock += 1
            self._entries[key] = {
                "last_used": self._clock,
                "result": copy.deepcopy(result),
            }
            self._entries.move_to_end(key)
            while len(self._entries) > self.capacity:
                self._entries.popitem(last=False)
                self.evictions += 1
            self._save_locked()

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()
            self.hits = 0
            self.misses = 0
            self.evictions = 0
            self._save_locked()

    def _save_locked(self) -> None:
        payload = {
            "schema_version": 1,
            "config_fingerprint": self.config_fingerprint,
            "capacity": self.capacity,
            "clock": self._clock,
            "entries": [
                {
                    "key": key,
                    "last_used": entry["last_used"],
                    "result": entry["result"],
                }
                for key, entry in sorted(self._entries.items(), key=lambda item: item[1]["last_used"])
            ],
        }
        atomic_write_json(self.path, payload)

    def stats(self) -> dict[str, Any]:
        with self._lock:
            return {
                "capacity": self.capacity,
                "size": len(self._entries),
                "hits": self.hits,
                "misses": self.misses,
                "evictions": self.evictions,
                "config_fingerprint": self.config_fingerprint,
            }
