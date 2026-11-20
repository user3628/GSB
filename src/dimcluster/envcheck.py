from __future__ import annotations

import os
import platform
import socket
import sys
from pathlib import Path
from typing import Any

import numpy as np
import sklearn

from .util import FileLock, atomic_write_bytes, ensure_workspace


def environment_check(root: Path) -> dict[str, Any]:
    paths = ensure_workspace(Path(root).resolve())
    probe = paths["state"] / "tmp" / "atomic-probe.bin"
    payload = b"dimcluster-atomic-probe\n"
    atomic_write_bytes(probe, payload)
    atomic_ok = probe.read_bytes() == payload
    lock_path = paths["state"] / "tmp" / "lock-probe.lock"
    lock_ok = False
    try:
        with FileLock(lock_path, timeout=2):
            lock_ok = True
    except Exception:
        lock_ok = False
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", 0))
        port = int(sock.getsockname()[1])
        loopback_ok = True
    except OSError:
        port = None
        loopback_ok = False
    finally:
        sock.close()
    try:
        probe.unlink()
    except FileNotFoundError:
        pass
    return {
        "ok": bool(atomic_ok and lock_ok and loopback_ok),
        "python": {
            "version": sys.version.split()[0],
            "executable": sys.executable,
            "meets_3_11": sys.version_info >= (3, 11),
        },
        "packages": {
            "numpy": np.__version__,
            "scikit-learn": sklearn.__version__,
        },
        "platform": {
            "os_name": os.name,
            "platform": platform.platform(),
            "filesystem": platform.system(),
        },
        "workspace": {
            "root": str(Path(root).resolve()),
            "writable": all(path.exists() for path in paths.values()),
            "atomic_replace_probe": atomic_ok,
            "file_lock_probe": lock_ok,
            "lock_implementation": "msvcrt.locking" if os.name == "nt" else "fcntl.flock",
        },
        "http": {
            "loopback_bind_probe": loopback_ok,
            "ephemeral_port": port,
        },
        "windows_note": (
            "Windows lock and os.replace code paths are implemented; the current runtime "
            "is not Windows, so native Windows validation was not performed here."
        ),
    }
