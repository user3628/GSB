"""Cross-platform advisory file lock (Windows msvcrt / POSIX fcntl)."""

import os


class FileLock:
    """Exclusive advisory lock on a file; used to serialize stage publish."""

    def __init__(self, path):
        self.path = str(path)
        self._fh = None

    def __enter__(self):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self._fh = open(self.path, "a+b")
        if os.name == "nt":
            import msvcrt
            self._fh.seek(0)
            # LK_LOCK blocks (with retries) until the byte is available.
            msvcrt.locking(self._fh.fileno(), msvcrt.LK_LOCK, 1)
        else:
            import fcntl
            fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX)
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if os.name == "nt":
                import msvcrt
                self._fh.seek(0)
                msvcrt.locking(self._fh.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
        finally:
            self._fh.close()
            self._fh = None
        return False
