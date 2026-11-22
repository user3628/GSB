"""Repository-local Python entry point for Windows and POSIX shells."""

from __future__ import annotations

from gspipe.cli import main


if __name__ == "__main__":
    raise SystemExit(main())
