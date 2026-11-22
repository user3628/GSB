from __future__ import annotations

import fcntl
import os
import sys
import tempfile
from pathlib import Path
from typing import Any


def environment_report(workspace: Path | str | None = None) -> dict[str, Any]:
    python_ok = sys.version_info >= (3, 10)
    numpy_result: dict[str, Any]
    try:
        import numpy

        numpy_result = {"available": True, "version": numpy.__version__}
    except Exception as exc:
        numpy_result = {"available": False, "error": str(exc)}
    workspace_result: dict[str, Any] = {"checked": False}
    if workspace is not None:
        root = Path(workspace).resolve()
        root.mkdir(parents=True, exist_ok=True)
        descriptor, name = tempfile.mkstemp(prefix=".write-check-", dir=root)
        try:
            os.write(descriptor, b"ok")
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
            os.unlink(name)
        lock_path = root / ".lock-check"
        lock_descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o644)
        try:
            fcntl.flock(lock_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(lock_descriptor, fcntl.LOCK_UN)
        finally:
            os.close(lock_descriptor)
            try:
                lock_path.unlink()
            except FileNotFoundError:
                pass
        workspace_result = {
            "checked": True,
            "path": str(root),
            "writable": True,
            "file_lock_supported": True,
        }
    return {
        "status": "ok" if python_ok and numpy_result["available"] else "error",
        "python_version": sys.version.split()[0],
        "python_minimum_satisfied": python_ok,
        "numpy": numpy_result,
        "workspace": workspace_result,
    }
