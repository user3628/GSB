"""Windows-style path handling.

Configuration and persisted records use Windows-style paths (backslash
separators, e.g. ``workspace\\state\\manifest.json``). The runtime resolves
those logical paths onto the native operating system path style, so the same
project runs unchanged on Windows and on POSIX without any container or
external service.
"""
from __future__ import annotations

import os
from pathlib import Path, PureWindowsPath


def to_windows(path: str | os.PathLike) -> str:
    """Render any path using Windows backslash separators."""
    return PureWindowsPath(str(path).replace("/", "\\")).as_posix().replace("/", "\\")


def resolve(path: str | os.PathLike, root: str | os.PathLike | None = None) -> Path:
    """Resolve a Windows-style logical path to a native filesystem path.

    Relative Windows paths are joined under ``root``. Absolute Windows paths
    (``C:\\...``) have their drive letter stripped on non-Windows hosts and are
    then rooted under ``root``; on native Windows they are used verbatim.
    """
    text = str(path).replace("\\", "/")
    # Strip a Windows drive letter (e.g. "C:") if present.
    if len(text) >= 2 and text[1] == ":":
        text = text[2:]
    native = Path(text)

    if os.name == "nt" and native.is_absolute():
        return native
    if native.is_absolute():
        # POSIX host with a POSIX-absolute path.
        if root is None:
            return native
        return Path(root) / native.relative_to(native.anchor)
    if root is None:
        return native
    return Path(root) / native


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path
