"""Process-level write serialisation.

SQLite already guarantees single-writer semantics at file level; these locks
make the guarantee explicit at the session/object boundary so that:

* part submission and merge for one session never interleave in this process;
* concurrent same-part submissions are evaluated as a critical section
  (unique constraint + idempotency rules are still the durable backstop);
* merges targeting the same object key serialise so version numbers cannot
  be allocated twice.

Read paths take no process locks; WAL lets them run against a committed
snapshot while a writer holds the database write lock.
"""
from __future__ import annotations

import threading
from collections import defaultdict


class LockRegistry:
    def __init__(self) -> None:
        self._session_locks: dict[str, threading.RLock] = defaultdict(threading.RLock)
        self._object_locks: dict[str, threading.RLock] = defaultdict(threading.RLock)
        self._guard = threading.Lock()

    def session_lock(self, session_id: str) -> threading.RLock:
        with self._guard:
            return self._session_locks[session_id]

    def object_lock(self, object_key: str) -> threading.RLock:
        with self._guard:
            return self._object_locks[object_key]


# One registry per process; the FastAPI app holds the same instance.
registry = LockRegistry()
