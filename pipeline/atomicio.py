"""原子文件写入、规范化 JSON、跨平台文件锁。

原子写入流程：先写同目录临时文件 -> flush/fsync -> os.replace 替换目标 ->
fsync 目录。任何中途崩溃都只会留下临时文件，目标位置要么是旧内容要么是
新内容，绝不会出现半写记录。
"""

import contextlib
import json
import os
import tempfile


def canonical_json_bytes(obj):
    """规范化 JSON 字节流。

    - sort_keys=True，字典顺序不影响结果
    - 非 ASCII 字符直接以 UTF-8 输出（ensure_ascii=False）
    - 分隔符固定，无尾随空白，行尾固定为 \\n
    - allow_nan=False：NaN/Infinity 直接报错，避免非标准 JSON 落盘
    """
    text = json.dumps(
        obj,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )
    return (text + "\n").encode("utf-8")


def atomic_write(path, data):
    """原子写入字节串。"""
    path = os.fspath(path)
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=".tmp-", suffix=".part", dir=directory)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
        _fsync_dir(directory)
    except BaseException:
        with contextlib.suppress(OSError):
            os.remove(tmp_path)
        raise


def atomic_write_json(path, obj):
    atomic_write(path, canonical_json_bytes(obj))


def read_json(path):
    with open(path, "rb") as handle:
        raw = handle.read()
    return json.loads(raw.decode("utf-8")), raw


def _fsync_dir(directory):
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        fd = os.open(directory, flags)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


class FileLock:
    """跨进程排他文件锁（POSIX 用 fcntl，Windows 用 msvcrt）。"""

    def __init__(self, path):
        self.path = os.fspath(path)
        os.makedirs(os.path.dirname(os.path.abspath(self.path)), exist_ok=True)
        self._handle = None

    def acquire(self):
        if self._handle is not None:
            return self
        self._handle = open(self.path, "a+b")
        if os.name == "nt":  # pragma: no cover - 仅 Windows 走该分支
            import msvcrt

            msvcrt.locking(self._handle.fileno(), msvcrt.LK_LOCK, 1)
        else:
            import fcntl

            fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX)
        return self

    def release(self):
        if self._handle is None:
            return
        try:
            if os.name == "nt":  # pragma: no cover
                import msvcrt

                self._handle.seek(0)
                msvcrt.locking(self._handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
        finally:
            self._handle.close()
            self._handle = None

    def __enter__(self):
        return self.acquire()

    def __exit__(self, exc_type, exc, tb):
        self.release()
        return False
