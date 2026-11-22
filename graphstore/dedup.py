"""同请求标识的去重执行器。

同一 ``request_id`` 的并发提交只有一个实际执行，其余线程等待并复用结果；
若提交体的规范化哈希与首次提交不一致，返回 ``E_CONFLICTING_REQUEST``。
"""

from __future__ import annotations

import threading
from typing import Callable, Dict

from .errors import ConflictingRequestError


class RequestDeduplicator:
    def __init__(self):
        self._lock = threading.Condition()
        self._inflight: Dict[str, dict] = {}

    def run_once(self, request_id: str, submitted_hash: str,
                 work: Callable[[], dict]) -> dict:
        with self._lock:
            entry = self._inflight.get(request_id)
            if entry is not None:
                if entry["hash"] != submitted_hash:
                    raise ConflictingRequestError(
                        "同一 request_id 提交了内容不一致的请求",
                        {"request_id": request_id},
                    )
                while not entry["done"]:
                    self._lock.wait()
                if entry["error"] is not None:
                    raise entry["error"]
                return entry["result"]
            entry = {"hash": submitted_hash, "done": False, "result": None, "error": None}
            self._inflight[request_id] = entry

        try:
            result = work()
        except Exception as exc:
            with self._lock:
                entry["done"] = True
                entry["error"] = exc
                self._inflight.pop(request_id, None)
                self._lock.notify_all()
            raise
        with self._lock:
            entry["done"] = True
            entry["result"] = result
            self._inflight.pop(request_id, None)
            self._lock.notify_all()
        return result
