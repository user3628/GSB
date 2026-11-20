from __future__ import annotations

import sys
import types
from pathlib import Path

from xrel_workbench import locking
from xrel_workbench.locking import FileLock


def test_windows_msvcrt_lock_branch(monkeypatch, tmp_path: Path) -> None:
    calls: list[tuple[int, int]] = []

    def locking_call(fd: int, mode: int, length: int) -> None:
        calls.append((mode, length))

    fake_msvcrt = types.SimpleNamespace(
        LK_LOCK=1,
        LK_UNLCK=2,
        locking=locking_call,
    )
    monkeypatch.setitem(sys.modules, "msvcrt", fake_msvcrt)
    monkeypatch.setattr(locking.os, "name", "nt")
    with FileLock(tmp_path / "windows.lock"):
        assert calls == [(fake_msvcrt.LK_LOCK, 1)]
    assert calls == [(fake_msvcrt.LK_LOCK, 1), (fake_msvcrt.LK_UNLCK, 1)]
