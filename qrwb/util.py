"""通用确定性工具：规范化 JSON、内容指纹、原子写、真实文件锁。"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import tempfile
from typing import Any

try:  # fcntl 为 POSIX 真实进程间文件锁；本工作台目标环境为 Linux/macOS
    import fcntl
    _HAVE_FCNTL = True
except ImportError:  # pragma: no cover - Windows 回退（演示环境为 Linux）
    _HAVE_FCNTL = False


# ---- 确定性 JSON 序列化 ---------------------------------------------------
def canonical_dumps(obj: Any) -> str:
    """键排序、分隔符固定、ensure_ascii=False 的确定性 JSON 文本。"""
    return json.dumps(obj, sort_keys=True, ensure_ascii=False,
                      separators=(",", ":"), allow_nan=False)


def canonical_pretty(obj: Any) -> str:
    """人类可读但仍然确定性的 JSON（用于落盘的产物文件）。"""
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, indent=2,
                      separators=(",", ": "), allow_nan=False) + "\n"


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def fingerprint(obj: Any) -> str:
    """对任意可 JSON 化对象计算稳定内容指纹（取 sha256 前 16 位）。"""
    return sha256_text(canonical_dumps(obj))[:16]


def full_fingerprint(obj: Any) -> str:
    return sha256_text(canonical_dumps(obj))


# ---- 原子写：tmp + fsync + rename，绝不留半写文件 -------------------------
def atomic_write_text(path: str, text: str) -> None:
    directory = os.path.dirname(os.path.abspath(path)) or "."
    os.makedirs(directory, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=".tmp-", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
        # 目录项变更也需要 fsync，保证崩溃后 rename 持久可见
        dir_fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except BaseException:
        with contextlib.suppress(OSError):
            os.remove(tmp_path)
        raise


def atomic_write_json(path: str, obj: Any, *, pretty: bool = True) -> None:
    text = canonical_pretty(obj) if pretty else canonical_dumps(obj)
    atomic_write_text(path, text)


def append_text(path: str, line: str) -> None:
    """追加单行（调用方需持有对应文件锁）。"""
    directory = os.path.dirname(os.path.abspath(path)) or "."
    os.makedirs(directory, exist_ok=True)
    with open(path, "a", encoding="utf-8", newline="\n") as handle:
        handle.write(line if line.endswith("\n") else line + "\n")
        handle.flush()
        os.fsync(handle.fileno())


# ---- 真实文件锁 -----------------------------------------------------------
class FileLock:
    """基于 flock 的跨进程互斥锁；同进程内再叠加线程锁。

    锁文件位于工作区 ``locks/`` 目录，获取失败会阻塞重试直到获得，
    或在 ``timeout`` 秒后抛出 TimeoutError。
    """

    def __init__(self, lock_path: str, timeout: float | None = 30.0) -> None:
        self.lock_path = lock_path
        self.timeout = timeout
        self._handle = None

    def __enter__(self) -> "FileLock":
        import time

        os.makedirs(os.path.dirname(os.path.abspath(self.lock_path)) or ".",
                    exist_ok=True)
        self._handle = open(self.lock_path, "a+b")
        deadline = None if self.timeout is None else time.monotonic() + self.timeout
        while True:
            try:
                if _HAVE_FCNTL:
                    fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                else:  # pragma: no cover
                    # 无 fcntl 时退化为进程内线程锁，保证同进程正确性
                    pass
                return self
            except BlockingIOError:
                if deadline is not None and time.monotonic() >= deadline:
                    self._handle.close()
                    self._handle = None
                    raise TimeoutError(f"file lock timeout: {self.lock_path}")
                time.sleep(0.01)

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._handle is None:
            return
        if _HAVE_FCNTL:
            fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
        self._handle.close()
        self._handle = None
