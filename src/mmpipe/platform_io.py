"""跨平台文件锁与原子替换原语。

- 文件锁：POSIX 使用 fcntl.flock；Windows 使用 msvcrt 锁定区域。
- 原子替换：同目录临时文件 + fsync + os.replace（Windows/Linux 均为原子语义）。
- 线程池：concurrent.futures.ThreadPoolExecutor（标准库，全平台可用）。
"""

import os
import tempfile
from contextlib import contextmanager


@contextmanager
def file_lock(path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    handle = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        if os.name == "nt":  # pragma: no cover - Windows 分支在 Linux CI 不执行
            import msvcrt
            msvcrt.locking(handle, msvcrt.LK_LOCK, 1)
            try:
                yield
            finally:
                msvcrt.locking(handle, msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            fcntl.flock(handle, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle, fcntl.LOCK_UN)
    finally:
        os.close(handle)


def _fsync_dir(dir_path: str) -> None:
    if os.name == "nt":  # pragma: no cover
        return
    fd = os.open(dir_path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write(path: str, data: bytes) -> None:
    """同目录临时文件写入并原子替换；任意位置中断都不会产生半写目标文件。"""
    dir_path = os.path.dirname(os.path.abspath(path))
    os.makedirs(dir_path, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(
        prefix="." + os.path.basename(path) + ".", suffix=".tmp", dir=dir_path)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
        _fsync_dir(dir_path)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except FileNotFoundError:
            pass
        raise


def cleanup_tmp_files(dir_path: str) -> list[str]:
    """恢复时清理上次中断遗留的临时文件。"""
    removed: list[str] = []
    if not os.path.isdir(dir_path):
        return removed
    for name in os.listdir(dir_path):
        if name.endswith(".tmp") or (".tmp." in name):
            full = os.path.join(dir_path, name)
            try:
                os.unlink(full)
                removed.append(full)
            except OSError:
                pass
    return removed


def append_jsonl(path: str, record: dict) -> None:
    from .codec import canonical_dumps
    line = canonical_dumps(record) + b"\n"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    try:
        os.write(fd, line)
        os.fsync(fd)
    finally:
        os.close(fd)


def read_jsonl(path: str) -> list[dict]:
    import json
    if not os.path.exists(path):
        return []
    out: list[dict] = []
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out
