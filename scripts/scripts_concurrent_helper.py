import json
import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)
from asr_pipeline.checkpoint import FileLock

try:
    import fcntl

    def _try(handle):
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except BlockingIOError:
            return False
except ImportError:
    import msvcrt

    def _try(handle):
        try:
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            return True
        except OSError:
            return False


def main():
    target = sys.argv[2]
    if os.path.isdir(target):
        os.makedirs(target, exist_ok=True)
        path = os.path.join(target, "publish.lock")
    else:
        os.makedirs(os.path.dirname(target), exist_ok=True)
        path = target
    with open(path, "a+b") as handle:
        print(json.dumps({"acquired": _try(handle)}))


if __name__ == "__main__":
    main()
