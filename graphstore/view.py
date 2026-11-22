"""不可变快照视图与版本注册表。

查询通过 :class:`SnapshotView` 访问图数据与索引，视图一旦获取就固定在某个
版本上；后台加载新版本只会替换注册表中的当前视图，正在执行的查询仍持有旧
视图并在旧版本上完成。引用计数归零且该版本不再是当前版本后，视图被回收。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Dict, Optional

from .errors import VersionRetiredError
from .indexes import IndexBundle
from .snapshot import GraphSnapshot


@dataclass(frozen=True, slots=True)
class SnapshotView:
    snapshot: GraphSnapshot
    index: IndexBundle

    @property
    def version(self) -> int:
        return self.snapshot.version


class ViewRegistry:
    def __init__(self):
        self._lock = threading.Condition()
        self._current: Optional[SnapshotView] = None
        self._views: Dict[int, SnapshotView] = {}
        self._refs: Dict[int, int] = {}

    def publish(self, view: SnapshotView):
        with self._lock:
            old_version = self._current.version if self._current else None
            self._views[view.version] = view
            self._refs.setdefault(view.version, 0)
            self._current = view
            self._reap_locked(old_version)
            self._lock.notify_all()

    def acquire(self, version: Optional[int] = None) -> SnapshotView:
        with self._lock:
            if not self._views:
                raise VersionRetiredError(
                    "当前没有任何已提交的图快照", {"requested_version": version}
                )
            target_version = version if version is not None else self._current.version
            view = self._views.get(target_version)
            if view is None:
                raise VersionRetiredError(
                    "请求绑定的快照版本已退役",
                    {"requested_version": target_version,
                     "current_version": self._current.version},
                )
            self._refs[target_version] = self._refs.get(target_version, 0) + 1
            return view

    def release(self, view: SnapshotView):
        with self._lock:
            version = view.version
            self._refs[version] -= 1
            if self._current is None or version != self._current.version:
                self._reap_locked(None)

    @property
    def current_version(self) -> Optional[int]:
        with self._lock:
            return self._current.version if self._current else None

    def versions(self):
        with self._lock:
            return sorted(self._views)

    def wait_until_version(self, version: int, timeout: float = 5.0) -> bool:
        with self._lock:
            if self.current_version == version:
                return True
            self._lock.wait(timeout=timeout)
            return self.current_version == version

    def _reap_locked(self, keep_version: Optional[int]):
        current_version = self._current.version if self._current else None
        for stale_version in [v for v in self._views
                              if v != current_version and self._refs.get(v, 0) == 0]:
            if keep_version is not None and stale_version == keep_version:
                continue
            self._views.pop(stale_version, None)
            self._refs.pop(stale_version, None)
